/*******************************************************************************
 * Copyright (c) 2026.
 * IWIN-FINS Lab, Shanghai Jiao Tong University.
 *******************************************************************************/

#pragma once

// ============================================================================
// g_state — 全局运行时状态（进程级共享对象）
// 集中存放跨组件共享的运行时状态：pipeline_g（解析态 Pipeline：cache JSON 双缓冲 +
// parse_pipeline/check_topology 两段解析）+ library_g（算法定位表 so_ctx）+ graph_g
// （PrecedenceGraph 单份运行图 + 调度依据；公开成员 mtx/cv/stopped/pending + 无锁原语
// expand_hp/grab_ready_workload/grab_delay_workload/is_hp_done/is_hp_empty/rollover_hp/trigger_workload_ready）。
// 数据流：RPC 存 JSON → pending → 主线程调度循环图静止时 commit+parse+check_topology+expand_hp 重建。
// 装配点写法与语义细节见 docs/precedence_graph_design.md 及各类型前注释。
// ============================================================================

// ============================================================================
// 全局宏：FINS_TIMING — 执行耗时统计开关（编译期，全库可见）
//   1（默认）：record_exec 写 exec_us_hist_ 环形队列 + worker 完成事件做 job/exec 计时聚合
//   0       ：热路径零计时开销——record_exec 变空操作、bind_job 闭包与 worker 不量时钟
// 关闭方式：编译期 -DFINS_TIMING=0（实时部署减负；功能验证期默认开，便于观察耗时）。
// ============================================================================

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <deque>
#include <dlfcn.h>
#include <fstream>   // FINS_EXPORT_DAG_PATH 导出 dag JSON 用
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <set>
#include <stdexcept>
#include <unordered_map>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "algo/algo_base.hpp"
#include "form.hpp"
#include "mesg/mesg.hpp"
#include "third_party/json.hpp"
#include "utils/time.hpp"
#include "utils/tracing.hpp"

namespace fins::rt {
  struct Library;
  struct Pipeline;
  struct Workload;
  struct PrecedenceGraph;

  /// 硬件状态监控，由监控器更新
  inline std::vector<float> core_usages_g{};
  inline std::atomic<float> mem_usage_g{};
  /// 外部回调槽（命名空间级 inline，装配点直接赋值注入；main 主线程调度循环每轮持 mtx 集中更新）。
  inline std::function<double(std::deque<double>)> wcet_updater = nullptr;
  /// 外部回调槽（命名空间级 inline，装配点直接赋值注入；优先级唯一来源：键函数 → 顶点调度优先级，
  /// grab 决策点现算；nullptr = 就绪堆退化为纯 FIFO——优先级不可预设，须经此函数拿到）。
  inline std::function<int(util::DirectedAcyclicGraph<Workload, Message>&, const Workload&)> priority_updater = nullptr;
  /// 外部回调槽（求makespan）ms
  inline std::function<double(util::DirectedAcyclicGraph<Workload, Message>&)> makespan_updater = nullptr;

  /** @brief .so 加载上下文（library_g.so_ctx 的元素）：构造=dlopen 装载 + dlsym 解析 C 工厂符号并
   *  填 loaded_keys，析构=dlclose 卸载，take_keys() 取定位键；装配点经 on_library_* 回调维护表。 */
  struct Plugin {
    void *handle = nullptr;
    std::string so_path;
    std::vector<std::string> loaded_keys;  // 本 so 产出的算法 key（删除时按 so 取走）

    typedef void (*DestroyPluginFunc)(AlgoBase *);
    typedef int (*GetPluginCountFunc)();
    typedef const char *(*GetAlgoNameFunc)(int);
    typedef const char *(*GetAlgoVersionFunc)(int);
    typedef AlgoBase *(*CreateAlgoFunc)(const char *);

    DestroyPluginFunc destroy_plugin = nullptr;
    GetPluginCountFunc get_plugin_count = nullptr;
    GetAlgoNameFunc get_algo_name = nullptr;
    GetAlgoVersionFunc get_algo_version = nullptr;
    CreateAlgoFunc create_algo = nullptr;

    /** @brief 构造 = 装载：dlopen(path) + dlsym 解析 5 个 C 工厂符号 + 枚举本 so 全部算法填
     *  loaded_keys（{name}:{version}，即 library_g.so_ctx 的定位键）。
     *  调用方：PluginLoader 的 on_library_add/on_library_modify 直接 make_shared<Plugin>(path)。
     * @param path .so 文件路径（绝对/相对均按 dlopen 规则）
     * @retval 无（失败抛 std::runtime_error——dlopen 失败 → dlerror 文本；缺必需符号 →
     *  "Missing required C-symbols"；已开的 handle 在 catch 内 dlclose 清理，
     *  构造抛 → 析构不调用 → 防泄漏）
     */
    explicit Plugin(const std::string &path)  {
      so_path = path;
      handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
      if (!handle) throw std::runtime_error(dlerror());
      try {
        destroy_plugin = (DestroyPluginFunc) dlsym(handle, "destroy_plugin");
        get_plugin_count = (GetPluginCountFunc) dlsym(handle, "get_plugin_count");
        get_algo_name = (GetAlgoNameFunc) dlsym(handle, "get_algo_name");
        get_algo_version = (GetAlgoVersionFunc) dlsym(handle, "get_algo_version");
        create_algo = (CreateAlgoFunc) dlsym(handle, "create_algo");

        if (!create_algo || !destroy_plugin || !get_plugin_count || !get_algo_name || !get_algo_version)
          throw std::runtime_error("Missing required C-symbols");

        loaded_keys.clear();
        const int count = get_plugin_count();
        for (int i = 0; i < count; ++i)
          loaded_keys.emplace_back(std::string(get_algo_name(i)) + ":" + get_algo_version(i));
      } catch (...) {
        if (handle) dlclose(handle);   // 构造失败清理，防 handle 泄漏（构造抛 → 析构不调用）
        handle = nullptr;
        throw;
      }
    }

    /** @brief 析构 = 物理卸载：dlclose 释放句柄。**引用计数归零才触发**——算法实例删除器持
     *  shared_ptr<Plugin> 保活，实例未全销毁期间库不卸载（保活语义见 expand_hp()）。
     * @retval 无
     */
    ~Plugin() { if (handle) dlclose(handle); handle = nullptr; }

    /** @brief 取走并清空本 so 的算法定位键（删除/替换路径装配点调用——析构不能返回值，
     *  单独保留供删除时回收 [name:version] 定位键）。
     * @retval std::vector<std::string> 本 so 的 [name:version] 键列表（move 走，原成员已清空）
     */
    std::vector<std::string> take_keys() { return std::move(loaded_keys); }
  };
  /** @brief so 上下文表（library_g）：[so_path] → Plugin，唯一算法定位数据源；装配点回调维护。 */
  struct Library{
    util::TBBMap<std::shared_ptr<Plugin>> so_ctx;

    /** @brief 取全部已注册算法定位键（[name]:[version]，跨所有 so 的 loaded_keys 聚合去重），
     *  与 Pipeline::algo_keys() 配套供装配点做算法就绪检查（expand_hp 前比对）。
     * @retval std::set<std::string> 全部已注册算法键（去重）
     */
    [[nodiscard]] std::set<std::string> algo_keys() const {
      std::set<std::string> keys;
      for (const auto &val: so_ctx | std::views::values)
        for (const auto &k : val->loaded_keys)
          keys.insert(k);
      return keys;
    }
  };
  inline Library library_g;

  /** @brief 节点解析态（Pipeline 内嵌，parse_pipeline 产物）：纯数据，字段含 id/name/version、
   *  端口名数组、config_cache、loop、period/wcet/deadline/cap。 */
  struct NodeInfo {
    std::string id;                                      // 节点在图中的唯一标识（顶点名 id:{k} 前缀）
    std::string name;                                    // 算法名（[name:version] = so 表定位键）
    std::string version;                                 // 算法版本（定位键）

    double period{0};                                    // 执行周期（ms；0 = 未配置，走支配继承）
    double deadline{1};                                  // 相对截止期（ms；缺省已折成 wcet）
    double wcet{1};                                      // 最坏执行时间（ms；缺省 1）
    size_t cap{10};                                     // 滑动窗口容量（hist 长度：loop 反馈历史槽每端口保留帧数；默认 10，config 顶层 "cap" 可配）

    std::vector<std::string> input_ports;
    std::vector<std::string> output_ports;
    std::vector<nlohmann::json> config_cache;

    std::map<std::string, size_t> loop;                      // loop 端口 → 迭代步 N（回喂过去 N 帧；窗口长度 = N）

    /** @brief 逐节点自解析：全部结构校验 + 字段抽取（Pipeline::parse 只拆封顶层后逐个调用
     *  本构造器）。parameters 为**位置式取值表** config_cache——只取 p["value"]、名字丢弃
     *  （顺序 = config "parameters" 数组元素顺序 = AlgoFunc 配置段相对序号，
     *  见 algo_func.hpp 头注释顺序保证链）。
     * @param n 节点 JSON 对象（必填 name/version/id，可选 parameters/inputs/outputs/wcet/deadline/period/cap/loop）
     * @param at 错误定位上下文串（如 "nodes[i]."，错误消息前缀用）
     * @retval 无（格式违反抛 std::invalid_argument）
     */
    NodeInfo(const nlohmann::json &n, const std::string &at) {
      if (!n.is_object()) throw std::invalid_argument("[parse_dataflow] " + at + "须为对象");
      if (!n.contains("name") || !n["name"].is_string())
        throw std::invalid_argument("[parse_dataflow] " + at + "name 必填 string");
      if (!n.contains("version") || !n["version"].is_string())
        throw std::invalid_argument("[parse_dataflow] " + at + "version 必填 string");
      if (!n.contains("id") || !n["id"].is_string())
        throw std::invalid_argument("[parse_dataflow] " + at + "id 必填 string");

      if (n.contains("parameters")) {
        if (!n["parameters"].is_array())
          throw std::invalid_argument("[parse_dataflow] " + at + "parameters 须为 array");
        for (size_t j = 0; j < n["parameters"].size(); ++j) {
          const auto &p = n["parameters"][j];
          if (!p.is_object() || !p.contains("value"))
            throw std::invalid_argument(
                "[parse_dataflow] " + at + "parameters[" + std::to_string(j) + "] 须含 value");
          config_cache.push_back(p["value"]);   // 位置式值表（名字丢弃，顺序保留）
        }
      }
      // 端口名数组：inputs/outputs 为 string 数组（顺序 = AlgoFunc 参数顺序），同名端口直连
      for (const char *f : {"inputs", "outputs"}) {
        if (!n.contains(f)) continue;
        if (!n[f].is_array())
          throw std::invalid_argument("[parse_dataflow] " + at + f + " 须为 string 数组");
        for (size_t j = 0; j < n[f].size(); ++j)
          if (!n[f][j].is_string())
            throw std::invalid_argument(
                "[parse_dataflow] " + at + std::string(f) + "[" + std::to_string(j) +
                "] 元素须为 string（端口名）");
      }
      for (const char *f : {"wcet", "deadline", "period", "cap"}) {
        if (n.contains(f) && !n[f].is_number())
          throw std::invalid_argument("[parse_dataflow] " + at + f + " 须为 number");
      }
      // 源头约束（无输入节点必填 period）属图结构合法性，由第二级 Pipeline::check_topology 审查
      // （本构造器只做逐节点 json 格式校验，不查跨节点图结构）。

      // ── 字段抽取（图侧/运行时不再接触原始 JSON）──
      id       = n["id"].get<std::string>();
      name     = n["name"].get<std::string>();
      version  = n["version"].get<std::string>();
      period   = n.contains("period")   ? n["period"].get<double>()   : 0.0;
      wcet     = n.contains("wcet")     ? n["wcet"].get<double>()     : 1.0;
      deadline = n.contains("deadline") ? n["deadline"].get<double>() : period;  // 缺省 = period
      cap      = n.contains("cap")      ? n["cap"].get<size_t>() : 10;        // 缺省 10
      if (n.contains("inputs") && n["inputs"].is_array())
        input_ports = n["inputs"].get<std::vector<std::string>>();
      if (n.contains("outputs") && n["outputs"].is_array())
        output_ports = n["outputs"].get<std::vector<std::string>>();
      if (n.contains("loop")) {
        if (!n["loop"].is_object())
          throw std::invalid_argument("[parse_dataflow] " + at + "loop 须为 object");
        for (const auto &[port, arg] : n["loop"].items()) {
          if (!arg.is_number())
            throw std::invalid_argument("[parse_dataflow] " + at + "loop." + port + " 须为 number（迭代步 N）");
          const double d = arg.get<double>();   // 迭代步 = 回溯最近 N 帧（须为正整数，窗口长度 = N）
          if (d < 1.0 || d != std::floor(d))
            throw std::invalid_argument("[parse_dataflow] " + at + "loop." + port + " 须为正整数迭代步 N（收到 " + std::to_string(d) + "）");
          loop[port] = static_cast<size_t>(d);
        }
      }
    }
  };
  /** @brief Pipeline — dataflow 配置（解析态；全局单份 pipeline_g）：cache = 原始配置 JSON 双缓冲
   *  （RPC 写缓冲份不解析 → pending → 主线程调度循环图静止时 commit + parse_pipeline 填 nodes），
   *  标准形式 = 节点对象数组（name/version/id 必填 + parameters/inputs/outputs/wcet/deadline/period/
   *  loop 可选），违反抛 std::invalid_argument；loop 端口反馈走 message_hist_ 数据槽。 */
  struct Pipeline {
    /// 原始数据
    util::DoubleBuff<nlohmann::json> cache;

    /// 解析产物：每节点 1 个 NodeInfo 解析态（字段见上方——Pipeline 的一部分）。parse 无返回、直接写
    /// 本成员；expand_hp() 只读本表（图侧/运行时不再接触原始 JSON）。实例化出的
    /// 具体算法实例由图侧 expand_hp 局部 by_id 持有，不替换本表——Workload 是纯数据。
    std::vector<NodeInfo> nodes;

    /** @brief 第一级解析：拆封 script 顶层 + 逐个触发 NodeInfo 构造（逐节点格式校验在
     *  构造器内），直接写本实例 nodes；格式违反抛异常（收包前调充当 json 格式审查）。
     *  跨节点图结构合法性不在此处，由第二级 check_topology 审查。
     * @param script 原始配置 JSON——顶层须为 array / {nodes:[...]} / 单节点对象 / null（空表）
     * @retval 无（违反抛 std::invalid_argument）
     */
    void parse_pipeline(const nlohmann::json &script) {
      nodes.clear();
      std::vector<nlohmann::json> raw_nodes;
      if (script.is_null()) return;   // 空配置 → 空表（expand_hp() 幂等）
      if (script.is_array()) {
        raw_nodes = script.get<std::vector<nlohmann::json>>();
      } else if (script.is_object() && script.contains("nodes") && script["nodes"].is_array()) {
        raw_nodes = script["nodes"].get<std::vector<nlohmann::json>>();
      } else if (script.is_object() && script.contains("name")) {
        raw_nodes.push_back(script);
      } else {
        throw std::invalid_argument(
            "[parse_dataflow] dataflow 顶层须为 array / {nodes:[...]} / 单节点对象");
      }

      // 拆封到若干 NodeInfo 各自完成解析（构造器自解析：格式校验 + 字段抽取 + config_cache 取值表）。
      // 跨节点图结构合法性（单写者 / 源周期）不在本函数——由第二级 check_topology 审查。
      for (size_t i = 0; i < raw_nodes.size(); ++i)
        nodes.emplace_back(raw_nodes[i], "nodes[" + std::to_string(i) + "].");
    }

    /** @brief 第二级图结构审查：读本实例 nodes（第一级 parse 已拆到 NodeInfo，逐节点格式校验
     *  已过），查跨节点图结构合法性，违反抛异常。主线程调度循环图静止时在 parse_pipeline 与
     *  expand_hp 之间显式调用（对全局 pipeline_g）：
     *  ① 单写者约束：同名输出端口至多一个生产者（数据流语义）；多写者直接拒绝，否则图侧
     *     expand_hp 绑定边对每个 producer 都建边、闭包读哪条取决于遍历顺序（不确定）；
     *  ② 源周期：无输入节点（input_ports 空）无上游驱动，须主动周期执行，必填 period；
     *  ③ loop 自反馈：loop 端口须同时在本节点 inputs 与 outputs 中声明（否则静默失效）。
     * @retval 无（违反抛 std::invalid_argument）
     */
    void check_topology() const {
      std::map<std::string, std::vector<size_t>> producers;
      for (size_t i = 0; i < nodes.size(); ++i)
        for (const auto &pn : nodes[i].output_ports)
          producers[pn].push_back(i);
      for (const auto &[P, ps] : producers)
        if (ps.size() > 1)
          throw std::invalid_argument("[check_topology] 输出端口 '" + P + "' 有多个生产者（单写者约束），非法配置");
      for (size_t i = 0; i < nodes.size(); ++i)
        if (nodes[i].input_ports.empty() && nodes[i].period <= 0)
          throw std::invalid_argument("[check_topology] nodes[" + std::to_string(i) + "] 无输入节点必填 period");
      // ④ 孤立输入拒绝：每个非 loop 输入端口须恰好有生产者（数据流边只来自节点输出；无生产者 →
      //    该节点退化为“伪根”——无 tp、无继承周期、expand 后只跑一次且永不重放，还会让周期兄弟
      //    因它永不完成而无法翻页）。loop 端口例外：producer 是节点自身（自反馈，③ 已查其 input/output 声明）。
      for (size_t i = 0; i < nodes.size(); ++i)
        for (const auto &pn : nodes[i].input_ports)
          if (!nodes[i].loop.count(pn) && !producers.count(pn))
            throw std::invalid_argument("[check_topology] nodes[" + std::to_string(i) +
                                        "] 输入端口 '" + pn + "' 无生产者（孤立输入），非法配置");
      // ③ loop 自反馈合法性：loop 端口须同时在本节点 inputs 与 outputs 中声明——反馈历史来自本
      //    节点同名输出；缺 input → 闭包不喂它（pack_inputs 只遍历 input_ports），缺 output →
      //    route_outputs 永不为它记历史，两者都会静默失效。
      for (size_t i = 0; i < nodes.size(); ++i) {
        const auto &ins  = nodes[i].input_ports;
        const auto &outs = nodes[i].output_ports;
        for (const auto &[pn, _] : nodes[i].loop) {
          if (std::find(ins.begin(), ins.end(), pn) == ins.end())
            throw std::invalid_argument("[check_topology] nodes[" + std::to_string(i) +
                                        "] loop 端口 '" + pn + "' 须在本节点 inputs 中声明");
          if (std::find(outs.begin(), outs.end(), pn) == outs.end())
            throw std::invalid_argument("[check_topology] nodes[" + std::to_string(i) +
                                        "] loop 端口 '" + pn + "' 须在本节点 outputs 中声明（自反馈）");
        }
      }
    }

    /** @brief 取本 pipeline 引用的全部算法定位键（[name]:[version] 列表，与 Plugin::loaded_keys 同构），
     *  供装配点与 library_g.so_ctx 比对做算法就绪检查（expand_hp 前调用，未全部注册则 defer）。
     *  非破坏性：只读 nodes 生成键列表（对比 Plugin::take_keys 的 move 语义——比对后还需用 nodes 建图，
     *  不能清空）。
     * @retval std::vector<std::string> 每节点 name:version（顺序 = nodes 顺序；同算法多节点可出现重复键）
     */
    [[nodiscard]] std::vector<std::string> algo_keys() const {
      std::vector<std::string> keys;
      keys.reserve(nodes.size());
      for (const auto &node : nodes)
        keys.emplace_back(node.name + ":" + node.version);
      return keys;
    }

  private:
    /// RPC 并发写 cache.write() 的串行锁（wr_lock() 返回；装配点 lock_guard 持用）。
    std::mutex wr_mtx_;
  public:
    /** @brief RPC 写入端串行锁访问：多 /update 并发写 pipeline_g.cache.write() JSON 份不撕裂；
     *  main/worker 侧（commit/read 消费）不持本锁。装配点 handler 用法：
     *  std::lock_guard lk(pipeline_g.wr_lock());
     * @retval std::mutex& cache 写串行锁引用（wr_mtx_）
     */
    std::mutex &wr_lock() { return wr_mtx_; }
  };
  inline Pipeline pipeline_g;

  /** @brief 图顶点 = 正常 job 实例 + 多维权值（纯数据，无生命周期状态——就绪/完成由
   *  PrecedenceGraph 侧增量状态 pred_left_/done_/ready_ 判定，装配点完成事件回锁调 trigger_workload_ready）。
   *  优先级不预设：就绪堆排序键由装配点注入的 priority_updater 键函数 grab 前现算（唯一来源；
   *  未注入 → 退化为纯 FIFO），图侧不存储静态优先级。 */
  struct Workload {
    std::string id{};             // 顶点名（格式 {节点id}:{k}，如 cam:0/cam:1）——expand_hp ⑥ 建顶点时填 vtx（同 dag 的 map 键）；
    std::string name{};           // 节点名（来自 Pipeline::NodeInfo.name；区别于 id 顶点名 = {name}:{k}）——装配点/测试按节点名识别
    size_t k{0};                  // 超周期内实例序号（expand_hp ⑥ 建顶点填；update_abs_deadline 滚动校正用）

    double period{0};
    double deadline{1};           // 相对截止期（ms；缺省 = wcet）

    double ddl{0};                // 绝对截止期（ms；滚动排期 = 主线程事件驱动 update_abs_deadline 按当前
    double wcet{1};               // 最坏执行时间（ms；缺省 1）

    std::function<void()> job;    // 执行体（闭包捕获实例 + 预解析绑定边引用，运行时零查找取帧/发布）
  };

  /// 一次 execute 耗时样本（exec_us_hist_ 元素）：us 用时 + 完成时间戳（排序键，保留最新）
  struct ExecSample {
    double us{0};                 // 执行耗时（us）
    double ts{0};                 // 完成时间戳（util::now_us；队列按 ts 升序）
  };

  /** @brief PrecedenceGraph — 数据流图 + 调度依据（单份运行图 graph_g）。公开成员 = 调度状态
   *  mtx/cv/stopped/pending + 图数据 dag（DAG<Workload, Message>，顶点 {id}:{k}、边=绑定边
   *  Message 槽）+ 超周期/hyper_start_ms + 历史统计（mesg_hist_cap/message_hist_/exec_us_hist_/exec_hist_cap）。
   *  就绪 = pred_left 增量计数（私有 pred_left_/in_degree_/done_/ready_）。方法全为无锁原语
   *  （expand_hp/grab_ready_workload/grab_delay_workload/is_hp_done/is_hp_empty/rollover_hp/trigger_workload_ready），
   *  调用方持 mtx。 */
  struct PrecedenceGraph {
    // ── public：图数据 + 无锁原语（方法不碰锁，前提调用方持 mtx；带锁事务在装配点
    //    on_execute 回调 / 主线程调度循环）──
    util::DirectedAcyclicGraph<Workload, Message> dag;  // 顶点带权、边=Message 槽

    double hyper_period_ms{0};       // 超周期长度（ms）
    double hyper_start_ms{0};        // 当前超周期起点（ms；expand 初始化 = 当前真实时钟、rollover_hp 回绕更新 = 当前真实时钟）
    uint64_t graph_version{0};       // 图结构版本号（expand_hp 重建后 ++；main 线程持 mtx 写读）。

    /** @brief 记录一帧到历史滑动窗口数据槽（满丢最旧；TBBMap accessor 按端口锁，只锁本端口
     *  历史槽字段）。历史槽 = loop 反馈窗口（容量 cap）与显式周期节点“最新一帧”读（容量 1）。
     *  追加按 Message.timestamp（采集时间戳，pub 时置 now_us）升序插入，队列恒按时间有序，尽量
     *  保留最新数据——乱序完成的旧帧插到前面、满 cap 丢最旧（ts 最小）。
     * @param id 输出端口名（历史槽键）
     * @param mesg 数据帧（Message）
     * @retval 无
     */
    void record_mesg(const std::string &id, const Message& mesg) {
      TBBMAP_UPDATE(message_hist_, id, [&](auto &q) {   // 无则默认构造插入、有则定位（持写锁，仅本端口）
        const size_t cap = mesg_hist_cap.count(id) ? mesg_hist_cap.at(id) : 100;  // 缺省 100（同 record_exec；勿 operator[]→0，否则 pop_front 空 deque = UB）
        // 按采集时间戳升序插入（乱序完成的旧帧放前面）；满 cap 从最旧（ts 最小）丢，保留最新。
        const auto it = std::lower_bound(q.begin(), q.end(), mesg,
            [](const Message &a, const Message &b) { return a.timestamp < b.timestamp; });
        q.insert(it, mesg);
        while (q.size() > cap) q.pop_front();  // 满丢最旧（cap 运行时只读，调用方已 guard >0）
      });
    }
    std::map<std::string, size_t> mesg_hist_cap{};   // 滑动窗口容量（expand_hp 填充：loop 反馈输出端口 → producer 节点 NodeInfo.cap，默认 10 可配；expand_hp 重建时清空重算；运行时只读无并发写）
    std::vector<nlohmann::json> hist_export_;        // hist 隐式依赖导出表（bind_job 段 3 填：loop 自反馈 / 显式周期“最新一帧”读；export_dag 输出 hist_edges，独立于前序 edges；expand 重建时清空）
    util::TBBMap<std::deque<Message>> message_hist_;  // 运行时：输出端口名 → 最近 cap 帧滑动窗口（loop 反馈历史槽；

    /** @brief 记录一次 execute 耗时到节点环形队列（按完成时间戳升序插入、满 cap 丢最旧——保留最新；
     *  TBBMap accessor 按节点锁，只锁本节点字段）。
     * @param id 节点 id
     * @param us 执行耗时（us，steady_clock 计时）
     * @retval 无
     */
    void record_exec(const std::string &id, double us) {
      const double ts = fins::util::now_us();   // 完成时间戳（排序键）
      TBBMAP_UPDATE(exec_us_hist_, id, [&](auto &q) {   // 无则默认构造插入、有则定位（持写锁，仅本节点）
        const size_t cap = exec_hist_cap.count(id) ? exec_hist_cap.at(id) : 100;  // 缺省 100（未填充时勿取 operator[]→0，否则 pop_front 空 deque = UB）
        // 按完成时间戳升序插入（乱序完成的旧样本插前面）；满 cap 从最旧（ts 最小）丢，保留最新。
        const auto it = std::lower_bound(q.begin(), q.end(), ts,
            [](const ExecSample &a, double b) { return a.ts < b; });
        q.insert(it, ExecSample{us, ts});
        while (q.size() > cap) q.pop_front();  // 满丢最旧（环形语义）
      });
    }
    std::map<std::string, size_t> exec_hist_cap{};   // 环形队列容量（可配：每节点保留最近 N 次 execute 耗时；未配置节点由 record_exec count/at 兜底缺省 100）
    util::TBBMap<std::deque<ExecSample>> exec_us_hist_;  // 算法键 → 最近 execute 耗时样本（us + 完成 ts；按 ts 升序、满 cap 丢最旧，保留最新；TBBMap accessor 按算法锁）

    // ── 调度状态公开成员（装配点直接读写：worker on_execute 回调 / main 主线程调度循环
    //    持 mtx 调用下述无锁原语；std::mutex 不可重入——持锁期间勿再 lock()，会死锁）──
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<bool> stopped{false};
    std::atomic<bool> pending{false};

  private:
    // ── 就绪增量调度状态（私有；持 mtx 访问，装配点经无锁原语间接使用）──
    std::map<std::string, size_t> pred_left_;   // 剩余未完成前序数（含 seq/绑定/tp 挂靠边）
    std::map<std::string, size_t> in_degree_;   // 入度基准（expand 填；rollover 重置 pred_left_ 用）

    struct ReadyItem {                          // 就绪集元素：id + 入队序号 + 排序键
      std::string id;
      size_t seq;                               // 入队序号（全局递增；prio 相等时 seq 小者先出 = FIFO 精确）
      int prio;                                 // 排序键（grab 前由 priority_updater 键函数现算，唯一来源；未注入恒 0 → 退化为纯 FIFO）
    };
    struct ReadyItemLess {                      // 就绪堆比较器（最大堆）：prio 高者在顶；相等 → seq 小者先出 = FIFO。全序。
      bool operator()(const ReadyItem &a, const ReadyItem &b) const {
        if (a.prio != b.prio) return a.prio < b.prio;
        return a.seq > b.seq;
      }
    };
    util::LazyMaxHeap<ReadyItem, ReadyItemLess> ready_;                // 就绪堆（懒最大堆；push 只入队，grab 前 rebuild 后堆序成立）
    size_t ready_seq_{0};                       // 入队序号（expand/rollover 时重置 0）

    std::vector<std::string> tp_order_;                   // 时间点释放顺序（pin_sync 按 offset 升序填全量 tp id；rollover 重放）
    size_t tp_released_{0};                     // 游标：下一个待释放 tp 在 tp_order_ 的下标

    /** @brief ① 端口索引：输出/输入端口名 → 节点 + 一跳邻居（loop 端口跳过——反馈 producer
     *  是自身，无绑定边）。单写者约束已在 Pipeline::check_topology 校验（同名输出端口多 producer
     *  拒绝），这里只建索引供拓扑/继承周期用；先预建所有节点的一跳邻居条目（空集）→ ③ 拓扑序
     *  对 const map 用 .at() 安全。
     * @param nodes 解析态节点表（只读）
     * @param producers [out] 输出端口名 → producer 节点 id 列表
     * @param consumers [out] 输入端口名 → consumer 节点 id 列表（loop 端口不构成消费者）
     * @param in_producers [out] 节点 id → 输入一跳邻居（producer 集）
     * @param out_consumers [out] 节点 id → 输出一跳邻居（consumer 集）
     * @retval 无
     */
    static void build_port_index(const std::vector<NodeInfo> &nodes,
                          std::map<std::string, std::vector<std::string>> &producers,
                          std::map<std::string, std::vector<std::string>> &consumers,
                          std::map<std::string, std::set<std::string>> &in_producers,
                          std::map<std::string, std::set<std::string>> &out_consumers) {
      for (const auto &info : nodes) {
        in_producers.emplace(info.id, std::set<std::string>{});
        out_consumers.emplace(info.id, std::set<std::string>{});
      }
      for (const auto &info : nodes) {
        for (const auto &pn : info.output_ports)
          producers[pn].push_back(info.id);
        for (const auto &pn : info.input_ports)
          if (!info.loop.count(pn))   // loop 端口：不构成消费者（无自环绑定边）
            consumers[pn].push_back(info.id);
      }
      for (const auto &info : nodes) {
        for (const auto &pn : info.input_ports)
          if (!info.loop.count(pn))   // loop 端口：反馈 producer 是自身，不构成拓扑依赖
            for (const auto &p : producers[pn])
              in_producers[info.id].insert(p);
        for (const auto &pn : info.output_ports)
          for (const auto &cc : consumers[pn])
            out_consumers[info.id].insert(cc);
      }
    }

    /** @brief ② 超周期：lcm(全部显式周期节点 period)（无周期节点 → 返回 0 不回绕）。
     *  整数毫秒 lcm（std::lcm）：period 就近取整为整数毫秒后参与整数 lcm——gcd 恒整数，无浮点
     *  病态（旧 T+0.5 hack 会静默算错：lcm(1,100.5)=201 而非 100）。非整数毫秒 period 不拒绝，
     *  FINS_LOG_WARN 提示后就近取整继续（周期小数毫秒按整数毫秒近似，实例数可能偏差，预警）。
     * @param nodes 解析态节点表（只读；只统计 info.period > 0 的节点）
     * @retval double 超周期长度（ms）；无周期节点返回 0
     */
    static double build_hyper_period(const std::vector<NodeInfo> &nodes) {
      long long hp = 1;   // 整数毫秒 lcm 累乘
      bool any_periodic = false;
      for (const auto &info : nodes) {
        const double T = info.period;   // ms（已由 parse 抽取）
        if (T <= 0) continue;
        any_periodic = true;
        const long long Ti = std::llround(T);   // 就近取整毫秒
        if (std::abs(T - (double)Ti) > 1e-6)   // 非整数毫秒：不拒绝，WARN + 就近取整继续
          FINS_LOG_WARN("[build_hyper_period] 节点 '{}' period 非整数毫秒: {}，就近取整为 {}ms", info.id, T, Ti);
        hp = std::lcm(hp, Ti);   // 整数 lcm：gcd 恒整数，无浮点病态
      }
      return any_periodic ? static_cast<double>(hp) : 0.0;
    }

    /** @brief ③ 拓扑序：BFS 从源展开（生产者先于消费者）；环未覆盖的节点补入末尾。
     *  入参 in_producers/out_consumers 已含全部节点条目（① 预建）→ .at() 安全。
     * @param nodes 解析态节点表（只读）
     * @param in_producers 节点 id → 输入 producer 邻居集（只读，含全部节点条目）
     * @param out_consumers 节点 id → 输出 consumer 邻居集（只读）
     * @retval std::vector<std::string> 拓扑序节点 id 列表
     */
    static std::vector<std::string> build_topo_order(
        const std::vector<NodeInfo> &nodes,
        const std::map<std::string, std::set<std::string>> &in_producers,
        const std::map<std::string, std::set<std::string>> &out_consumers) {
      std::vector<std::string> topo;
      std::map<std::string, size_t> indeg;
      std::deque<std::string> q;
      for (const auto &info : nodes) {
        indeg[info.id] = in_producers.at(info.id).size();
        if (in_producers.at(info.id).empty()) q.push_back(info.id);
      }
      while (!q.empty()) {
        const std::string id = q.front(); q.pop_front();
        topo.push_back(id);
        for (const auto &oc : out_consumers.at(id))
          if (--indeg[oc] == 0) q.push_back(oc);
      }
      for (const auto &info : nodes)
        if (std::find(topo.begin(), topo.end(), info.id) == topo.end()) topo.push_back(info.id);
      return topo;
    }

    /** @brief ④ 实例化：为每个 NodeInfo 从 so 表（入参 so_ctx）构造具体算法实例（插件 C 工厂）。
     *  配置注入 = 遍历 info.config_cache 逐个 configure（位置式解码，顺序 = config
     *  "parameters" 数组元素值顺序 = AlgoFunc 配置段相对序号）：AlgoFunc 解码写 configs_
     *  类型化帧（execute 零解析）。实例不替换 pipeline.nodes（NodeInfo 是纯数据）——全部
     *  job 实例共享 1 个（连续 job precedence 边保证串行），由局部 by_id 持有。
     * @param nodes 解析态节点表（只读）
     * @param so_ctx 算法定位表（[so_path] → Plugin；只读）
     * @param by_id [out] 节点 id → 具体算法实例
     * @param by_info [out] 节点 id → 解析态指针（只读别名）
     * @retval 无（[name:version] 未注册抛 std::runtime_error）
     */
    static void build_instances(const std::vector<NodeInfo> &nodes,
                                const util::TBBMap<std::shared_ptr<Plugin>> &so_ctx,
                                std::map<std::string, std::shared_ptr<AlgoBase>> &by_id,
                                std::map<std::string, const NodeInfo *> &by_info) {
      for (const auto &info : nodes) {
        const std::string &nm = info.name;
        const std::string key = nm + ":" + info.version;

        // 遍历 so 表（显式入参 so_ctx）定位算法（[name:version] 落在哪个 so 的 loaded_keys）
        std::shared_ptr<AlgoBase> algo;
        std::shared_ptr<Plugin> pctx;
        for (const auto &[so_path, pc] : so_ctx) {
          bool found = false;
          for (const auto &k : pc->loaded_keys)
            if (k == key) { pctx = pc; found = true; break; }
          if (found) break;
        }
        if (!pctx) throw std::runtime_error("Unregistered algorithm name in map: " + key);

        // C 工厂实例化；shared_ptr 删除器持 ctx → 实例存活期间库不卸载
        algo = std::shared_ptr<AlgoBase>(
            pctx->create_algo(key.c_str()),
            [pctx](AlgoBase *p) { if (pctx->destroy_plugin && p) pctx->destroy_plugin(p); });

        // 配置注入：顺序 = info.config_cache（位置式值表，顺序 = config "parameters" 数组元素
        // 值顺序 = AlgoFunc 配置段相对序号）——AlgoFunc 位置式解码写 configs_（execute 零解析）。
        for (const auto &v : info.config_cache)
          algo->configure("", v);

        by_id[info.id] = std::move(algo);
        by_info[info.id] = &info;
      }
    }

    /** @brief ⑤ 支配周期 + Replication：逐节点定最终执行周期 + job 实例数。支配原则：后级执行
     *  周期默认与前级严格一致——未显式配置 period 的节点继承前级周期（多前级继承最短周期前级）；
     *  只有显式配置了 period 才与前级不同（multi-hop 速率变化）。HP 只统计显式配置周期
     *  （② 的 lcm），继承周期不计入（但继承值必为某显式周期，整除 HP）。
     * @param topo 拓扑序节点 id 列表（只读；前级已定最终周期）
     * @param by_info 节点 id → 解析态指针（只读）
     * @param producers 输出端口名 → producer 节点 id 列表（只读；继承源查找用）
     * @param hyper_period 超周期长度（ms；② 的结果）
     * @param period_final [out] 节点 id → 最终执行周期（ms）
     * @param node_count [out] 节点 id → job 实例数（HP/T，无周期 → 1）
     * @retval 无
     */
    static void build_dominance(const std::vector<std::string> &topo,
                                const std::map<std::string, const NodeInfo *> &by_info,
                                const std::map<std::string, std::vector<std::string>> &producers,
                                const double hyper_period,
                                std::map<std::string, double> &period_final,
                                std::map<std::string, size_t> &node_count) {
      for (const auto &id : topo) {
        const auto *info = by_info.at(id);
        double T = info->period;                   // 显式配置周期（0 = 未配置 → 走继承）
        if (T <= 0) {
          // 未配置 → 继承前级：多前级取最短周期前级（topo 序保证前级已定最终周期）
          std::string trig;
          double best = 0;
          for (const auto &pn : info->input_ports)
            if (!info->loop.contains(pn)) {  // loop 端口：反馈 producer 是自身，不参与继承
              auto pit = producers.find(pn);
              if (pit == producers.end()) continue;   // 孤立输入端口（无 producer）→ 无继承源
              for (const auto &p : pit->second) {
                const double pt = period_final.contains(p) ? period_final[p] : 0;
                if (trig.empty() || pt < best) { trig = p; best = pt; }
              }
            }
          T = (!trig.empty() && period_final.contains(trig)) ? period_final[trig] : 0.0;
        }
        period_final[id] = T;
        node_count[id] = (T > 0) ? static_cast<size_t>(std::lround(hyper_period / T)) : 1;
      }
    }

    /** @brief ⑥ 建顶点：每节点展开 node_count 个 job 实例顶点 {id}:{k}（k=0..N-1），载荷 =
     *  attrs 基础（period/deadline/wcet；priority 不预设，排序键由运行时 priority_updater 键函数现算）。
     *  abs_deadline 不在建图期排期——由运行时 update_abs_deadline 按真实时钟 + v.k·period 滚动校正
     *  （无 release/相位）。
     * @param dag 目标图（就地加顶点）
     * @param nodes 解析态节点表（只读）
     * @param period_final 节点 id → 最终周期（只读；⑤ 的结果）
     * @param node_count 节点 id → 实例数（只读；⑤ 的结果）
     * @retval 无
     */
    static void build_vertex(util::DirectedAcyclicGraph<Workload, Message> &dag,
                               const std::vector<NodeInfo> &nodes,
                               const std::map<std::string, double> &period_final,
                               const std::map<std::string, size_t> &node_count) {
      for (const auto &info : nodes) {
        const size_t N = node_count.at(info.id);
        for (size_t k = 0; k < N; ++k) {
          Workload v;
          v.k            = k;   // 实例序号（update_abs_deadline 滚动校正用）
          v.name         = info.name;   // 节点名（NodeInfo.name；id 顶点名 = {name}:{k}）
          v.period       = period_final.at(info.id);
          v.deadline     = info.deadline;   // parse 已折到 wcet（缺省 = wcet）
          v.wcet         = info.wcet;
          dag.add_node(info.id + ":" + std::to_string(k), std::move(v));
        }
      }
    }

    /** @brief ⑦ 建边：同名端口直连绑定边（数据驱动 precedence）。规则（2026-09-02 拍板）：
     *  · 无 seq 连续边（全删）——任务实例不靠自身 A1→A2 前序串行，串行由释放时间点（显式周期）
     *    或数据源 job 完成（无显式周期跟随）传递；
     *  · 绑定边只给“无显式周期”节点（info.period≤0，纯数据流跟随）：consumer 每输入端口 pn 绑
     *    输出 pn 的唯一 producer（单写者由 check_topology 保证），整数式 pk=((k+1)·Np-1)/Nc 连
     *    producer:{pk} → consumer:{k}（时段内最新已完成帧；同速率一一对应；快→慢绑末帧；
     *    慢→快共享帧；恒有边）；
     *  · 显式周期（info.period>0）节点 = 时间触发，不建任何数据前序边——它执行时从 producer 输出
     *    端口的历史槽读“最新一帧”（队列长度 1），见 bind_job 段 1 注册 / pack_inputs 的 read_latest；
     *  · loop 端口（loop 中）永远无绑定边——反馈走 message_hist_ 滑动窗口。
     * @param dag 目标图（就地加边）
     * @param nodes 解析态节点表（只读）
     * @param node_count 节点 id → 实例数（只读）
     * @retval 无
     */
    static void build_edge(util::DirectedAcyclicGraph<Workload, Message> &dag,
                            const std::vector<NodeInfo> &nodes,
                            const std::map<std::string, size_t> &node_count) {
      std::map<std::string, std::string> producer_of;   // 输出端口名 → producer 节点 id（首生产者，函数内局部）
      for (const auto &info : nodes)
        for (const auto &pn : info.output_ports)
          if (!producer_of.count(pn)) producer_of[pn] = info.id;
      for (const auto &info : nodes) {
        if (info.period > 0) continue;   // 显式周期（时间触发）节点：无数据绑定边（读 hist 最新，见 bind_job）
        const size_t Nc = node_count.at(info.id);
        for (const auto &pn : info.input_ports) {
          if (info.loop.count(pn)) continue;   // loop 端口无绑定边
          auto pit = producer_of.find(pn);
          if (pit == producer_of.end()) continue;   // 输入端口无生产者（孤立输入）→ 无边
          const std::string &p = pit->second;
          const size_t Np = node_count.at(p);
          for (size_t k = 0; k < Nc; ++k) {
            const long long pk = ((long long)(k + 1) * (long long)Np - 1) / (long long)Nc;
            dag.add_edge(p + ":" + std::to_string(pk), info.id + ":" + std::to_string(k), pn, Message{});
          }
        }
      }
    }

    /** @brief ⑦.5 时间链：解析同步时间点（显式周期节点释放时刻并集）→ 建时间点顶点（job = 延迟
     *  （sleep_until 绝对释放时刻，timer 拿到 w 直接 job() 即实现延迟）、period=相对 hyper_start_ms
     *  的释放偏移）+ 挂靠边 tp:s → {id}:{k}（释放约束：时间点 Finished 任务才就绪）。仅显式周期
     *  节点（info.period>0）产生同步点并挂靠；继承周期节点仍纯数据流驱动。时间点按绝对释放时刻聚合
     *  ——多任务共享同一时间点（如两个 50ms 任务与一个 100ms 任务同时刻释放共用该点），锚定真实
     *  时钟消除旧 delay 的漂移。注意顺序：须在 build_edge 之后调用（其挂靠边引用的任务顶点已由
     *  build_vertex 建好、时间点顶点自建）。
     * @param nodes 解析态节点表（只读）
     * @param node_count 节点 id → 实例数（只读）
     * @retval 无
     */
    void bind_sync(const std::vector<NodeInfo> &nodes,
                  const std::map<std::string, size_t> &node_count) {
      std::set<double> sync_points;   // 同步点集合：显式周期节点释放时刻并集（去重升序）
      for (const auto &info : nodes) {
        if (info.period <= 0) continue;
        const size_t N = node_count.at(info.id);
        for (size_t k = 0; k < N; ++k) sync_points.insert((double)k * info.period);
      }

      std::map<double, std::string> tp_id;   // 偏移 → 时间点顶点 id（序号化，无精度碰撞）
      size_t seq = 0;
      for (const double off : sync_points) tp_id[off] = "tp:" + std::to_string(seq++);

      for (const auto &[off, id] : tp_id) {
        tp_order_.push_back(id);   // 升序 offset = 释放顺序（grab_delay_workload 按序取，与原 min-period 扫描等价）
      }

      double prev_off = 0.0;   // 理论延迟基准：相对前一个同步点的间隔（首个 tp:0 = 0，立即释放）
      for (const auto &[off, id] : tp_id) {
        Workload v;
        v.id     = id;
        v.name   = "time";
        v.wcet   = off - prev_off;
        v.job    = [this, off]() {
          const double at = hyper_start_ms + off;
          const auto until = std::chrono::steady_clock::now()
              + std::chrono::microseconds((long long)(at - fins::util::now_ms()) * 1000ll);
          while (!stopped.load() && std::chrono::steady_clock::now() < until)
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        };
        dag.add_node(id, std::move(v));

        prev_off = off;
      }

      for (const auto &info : nodes) {
        if (info.period <= 0) continue;

        const size_t N = node_count.at(info.id);
        for (size_t k = 0; k < N; ++k) {
          const double off = (double)k * info.period;
          dag.add_edge(tp_id.at(off), info.id + ":" + std::to_string(k), "time", Message{});
        }
      }
    }

    /** @brief ⑨a 入度基准（**无锁原语，前提调用方持 mtx**；仅 expand_hp 尾部调用）：先收集全部
     *  顶点 id 再遍历填 in_degree_/pred_left_ 基准（初始值 = 入度）——两趟规避 range 内嵌套
     *  accessor（for_each_vertex 遍历期间勿嵌套 in_nodes 的 accessor）。
     * @retval 无
     */
    void build_pred() {
      std::vector<std::string> ids;
      dag.for_each_vertex([&](const std::string &id, const Workload &) { ids.push_back(id); });
      for (const auto &id : ids) {
        const size_t deg = dag.in_nodes(id).size();
        in_degree_[id] = deg;
        pred_left_[id] = deg;
      }
    }

    /**
     * @brief ⑧ 填 job 执行体闭包：按 NodeInfo 端口序打包输入/输出 array → AlgoBase execute →
     *        输出路由下游绑定边 + record_mesg 维护 loop 滑动窗口，运行时不再接触原始 JSON。
     *        闭包捕获稳定解析态（shared_ptr<const NodeInfo> 每节点 1 份按 k 共享）+ 算法实例 by_id；
     *        输入逐输入端口取绑定边帧（loop 端口从 message_hist_ 聚合最近 N 帧——N = config 迭代步，
     *        恒长 N、未满头部补 0，运行时直出 typed std::vector<int> 进对应下标（方案 A：AlgoFunc/
     *        插件侧只声明 std::vector<int> 接收，不参与转换）。hist 容量
     *        mesg_hist_cap[输出端口] = 该端口作为 loop 反馈被消费时 producer 节点的 NodeInfo.cap
     *        （config 顶层 "cap"，默认 10），只对 loop 反馈输出端口建历史槽；单写者约束保证
     *        每输出端口唯一 producer → 直接赋值（非 max）；message_hist_ 跨重建保留不清。
     * @param nodes 解析态节点表（只读）
     * @param by_id 节点 id → 算法实例（只读）
     * @param node_count 节点 id → job 实例数（只读）
     * @retval 无
     */
    void bind_job(const std::vector<NodeInfo> &nodes,
                    const std::map<std::string, std::shared_ptr<AlgoBase>> &by_id,
                    const std::map<std::string, size_t> &node_count) {
      // loop 端口 → 聚合窗口帧数 = config 迭代步 N（扁平 loop map；无需换算，直取）

      // ── 段 1：历史容量表 mesg_hist_cap[输出端口] ──
      //  ① loop 反馈端口：容量 = 本节点 cap（config 顶层 "cap"，默认 10），自反馈窗口 N 帧；
      //  ② 显式周期（时间触发）节点的非 loop 输入：读其 producer 输出端口的“最新一帧”（队列长度 1）
      //    ——为此给该输出端口建容量 1 的历史槽；若已由 ① 注册则保留其较大 cap（不降级）。
      //    route_outputs 见 mesg_hist_cap 含该端口才 record_mesg，故须先在此登记。
      std::set<std::string> out_ports;   // 全部节点输出端口名并集（判定“有 producer”）
      for (const auto &info : nodes)
        for (const auto &pn : info.output_ports) out_ports.insert(pn);
      for (const auto &info : nodes)
        for (const auto &[port, _] : info.loop) mesg_hist_cap[port] = info.cap;   // ① loop 自反馈槽
      for (const auto &info : nodes) {
        if (info.period <= 0) continue;   // 仅显式周期节点以“最新一帧”方式读输入
        for (const auto &pn : info.input_ports)
          if (!info.loop.count(pn) && out_ports.count(pn) && !mesg_hist_cap.count(pn))
            mesg_hist_cap[pn] = 1;   // ② 最新一帧槽（读 producer 输出端口的最近帧）
      }

      // ── 段 2：每个节点 → 每实例填 job 闭包（闭包捕获稳定解析态 + 算法实例 + loop 聚合窗口）──
      for (const auto &info : nodes) {
        auto sinfo = std::make_shared<const NodeInfo>(info);           // 闭包捕获稳定共享解析态
        const auto &algo = by_id.at(info.id);
        const size_t n = node_count.at(info.id);
        const auto loop_w = info.loop;   // loop 端口 → 聚合窗口帧数（迭代步 N，直取 config）

        for (size_t k = 0; k < n; ++k) {
          const std::string vtx = info.id + ":" + std::to_string(k);   // 顶点名现拼（无 JobInst）
          dag.mutate_vertex(vtx, [this, sinfo, algo, vtx, loop_w](Workload &v) {
            v.id = vtx;   // Workload.id 实际填充（grab_ready_workload 返回 Workload* 含 id，装配点直做完成事件用）

            // 按 tag 分组一次性解析（O(边数) 替代 O(端口×边数) 的逐端口 edges_to/from）
            auto in_groups  = dag.edges_to_grouped(vtx);
            auto out_groups = dag.edges_from_grouped(vtx);
            std::vector<std::vector<std::reference_wrapper<Message>>> in_refs(sinfo->input_ports.size());
            for (size_t i = 0; i < in_refs.size(); ++i)
              if (!loop_w.count(sinfo->input_ports[i])) {   // 非 loop 端口：查分组表（单写者 → 至多一条）
                const auto it = in_groups.find(sinfo->input_ports[i]);
                if (it != in_groups.end()) in_refs[i] = it->second;
              }
            std::vector<std::vector<std::reference_wrapper<Message>>> out_refs(sinfo->output_ports.size());
            for (size_t i = 0; i < out_refs.size(); ++i) {
              const auto it = out_groups.find(sinfo->output_ports[i]);
              if (it != out_groups.end()) out_refs[i] = it->second;
            }

            v.job = [this,
              sinfo,
              algo,
              loop_w,
              in_refs = std::move(in_refs), out_refs = std::move(out_refs)]() {
              // ── 功能 1a：loop 端口 → 滑动窗口历史槽聚合最近 N 帧为 typed std::vector<int>
              auto collect_loop_window = [this](const std::string &pn, size_t w) {
                std::vector<int> vals;   // 恒长 w；未满头部 0 占位，尾部覆盖最近真实帧
                vals.resize(w);
                { // TBBMap const_accessor 只锁本端口历史槽（与 record_mesg 同端口写互斥；不同端口并发读）
                  util::TBBMap<std::deque<Message>>::const_accessor a;
                  if (message_hist_.find(a, pn)) {
                    const auto &h = a->second;   // 队列按 timestamp 升序，尾部 = 最新真实帧
                    const size_t take = std::min(w, h.size());
                    for (size_t j = 0; j < take; ++j)
                      vals[w - take + j] = *h[h.size() - take + j].p_shared<int>();   // 逐帧取真实值
                  }
                  // find 失败（理论不发生：producer 已先执行并 record_mesg 压槽）→ 全 0 占位
                }
                Message m;
                *m.p_mutable<std::vector<int>>() = std::move(vals);
                return m;
              };

              // ── 功能 1b：显式周期（时间触发）节点读“最新一帧”——从其 producer 输出端口历史槽
              //      （容量 1，bind_job 段 1 注册）取最近一帧；时间触发无数据前序，执行时才取样。
              //      producer 尚未产出（槽空）→ 0 占位（当前 SDK 载荷为 int）。
              auto read_latest = [this](const std::string &pn) {
                Message m;
                { // TBBMap const_accessor 读（与 record_mesg 同端口写互斥）
                  util::TBBMap<std::deque<Message>>::const_accessor a;
                  if (message_hist_.find(a, pn) && !a->second.empty())
                    m = a->second.back();   // 队列按 timestamp 升序，back = 最新一帧（拷贝，shared_ptr 保活）
                }
                if (m.frame) return m;
                Message z;
                *z.p_mutable<int>() = 0;
                return z;
              };

              // ── 功能 1：打包输入 array（loop 聚合 / 周期节点读最新 / 普通端口预解析 in_refs；
              //      按端口序，算法按位置取不碰端口名）──
              auto pack_inputs = [this, sinfo, loop_w, collect_loop_window, read_latest, &in_refs]() {
                std::vector<Message> inputs(sinfo->input_ports.size());
                for (size_t i = 0; i < inputs.size(); ++i) {
                  const std::string &pn = sinfo->input_ports[i];

                  if (loop_w.count(pn))
                    inputs[i] = collect_loop_window(pn, loop_w.at(pn));
                  else if (sinfo->period > 0)
                    inputs[i] = read_latest(pn);   // 显式周期：读 producer 最新一帧（无数据前序边）
                  else
                    inputs[i] = in_refs[i].empty() ? Message{} : in_refs[i][0].get();   // 跟随节点：绑定边预解析引用
                }
                return inputs;
              };

              // ── 功能 2：execute + record_exec（耗时统计：execute 前后 steady_clock 计时 us，
              //      不含输入打包/输出路由；按算法键（info.name）环形队列，expand_hp 重建保留不清）──
              auto execute_and_time = [this, sinfo, algo](std::vector<Message> &inputs) {
                std::vector<Message> outputs(sinfo->output_ports.size());   // 输出按端口序预构造 array（算法按位置写）

                const auto _t0 = std::chrono::steady_clock::now();

#ifdef FINS_EXPORT_TRACING_PATH
                fins::util::trace_record(fins::util::TraceKind::EXECUTE, sinfo->id);   // 执行（job 开始）
#endif

                algo->execute(inputs, outputs);   // 配置已建图期注入 algo 实例（AlgoFunc configs_ 类型化帧，execute 零解析）

#ifdef FINS_EXPORT_TRACING_PATH
                fins::util::trace_record(fins::util::TraceKind::COMPLETE, sinfo->id);   // 执行（job 完成）
#endif

                record_exec(sinfo->name, std::chrono::duration<double, std::micro>(
                    std::chrono::steady_clock::now() - _t0).count());   // 键 = 算法键（非节点 id：多实例/多节点同类算法归并聚合）

                return outputs;
              };

              // ── 功能 3：路由输出（写预解析下游引用共享帧 + 有 loop 消费者才存历史槽）──
              auto route_outputs = [this, sinfo, &out_refs](std::vector<Message> &outputs) {
                for (size_t i = 0; i < outputs.size(); ++i) {
                  for (auto &e : out_refs[i])
                    e.get() = outputs[i];   // 写全部下游绑定边（生产者消费者共享帧）
                  const std::string &pn = sinfo->output_ports[i];
                  const auto cap_it = mesg_hist_cap.find(pn);   // 锁外执行：const 查找避 operator[] 并发写 UB
                  if (cap_it != mesg_hist_cap.end() && cap_it->second > 0)   // 有 loop 消费者才存历史（cap>0）；record_mesg 满 cap 丢最旧
                    record_mesg(pn, outputs[i]);
                }
              };

              auto inputs  = pack_inputs();
              auto outputs = execute_and_time(inputs);
              route_outputs(outputs);
            };
          });
        }
      }

      // ── 段 3：hist 隐式依赖导出表（hist_export_；export_dag → hist_edges，独立于前序 edges）──
      //  loop   ：consumer 依赖自身输出端口历史槽的前 win 帧（往前找 win 个更早实例；k=0 无更早帧不画）。
      //  latest ：显式周期节点读 producer 输出端口“最新一帧”（队列长 1），锚 producer 末实例。
      hist_export_.clear();
      std::map<std::string, std::string> producer_of;   // 输出端口名 → producer 节点 id（单写者保证唯一）
      for (const auto &info : nodes)
        for (const auto &pn : info.output_ports)
          if (!producer_of.count(pn)) producer_of[pn] = info.id;
      for (const auto &info : nodes) {
        const size_t n = node_count.at(info.id);
        for (size_t k = 0; k < n; ++k) {
          const std::string vtx = info.id + ":" + std::to_string(k);
          for (const auto &[pn, win] : info.loop) {   // ① loop 自反馈：标注它实际依赖的历史帧
            // “往前找 win 帧” = 本节点输出历史槽中该实例之前的最近 win 帧（同一超周期内为前 k 个实例；
            // 跨超周期历史在本轮顶点集内不可见，不画）。k=0 首实例无更早帧 → 不画（也不画自环）。
            const size_t mx = std::min<size_t>(win, k);
            for (size_t w = 1; w <= mx; ++w)
              hist_export_.push_back({{"from", info.id + ":" + std::to_string(k - w)},
                                      {"to", vtx}, {"port", pn},
                                      {"mode", "loop"}, {"win", win}});
          }
          if (info.period > 0) {   // ② 显式周期“最新一帧”读（非 loop 输入，且存在 producer）
            for (const auto &pn : info.input_ports) {
              if (info.loop.count(pn)) continue;
              const auto pit = producer_of.find(pn);
              if (pit == producer_of.end()) continue;
              const size_t Np = node_count.at(pit->second);
              hist_export_.push_back({{"from", pit->second + ":" + std::to_string(Np - 1)},
                                      {"to", vtx}, {"port", pn},
                                      {"mode", "latest"}, {"win", size_t(1)}});
            }
          }
        }
      }
    }

    /** @brief ⑨b 初始就绪（**无锁原语，前提调用方持 mtx**；仅 expand_hp 尾部调用，须在 build_pred
     *  之后）：无前序依赖（pred_left==0）且非 tp 门顶点入就绪集。有效配置源节点必显式 period →
     *  必有 tp 门 → pred_left≥1，初始就绪为空，图启动由 timer 释放 tp:0（offset 0 ≈ hyper_start
     *  立即）触发；此趟为无 tp 门顶点兜底。
     * @retval 无
     */
    void seed_ready() {
      std::vector<std::string> ids;
      dag.for_each_vertex([&](const std::string &id, const Workload &) { ids.push_back(id); });
      for (const auto &id : ids)
        if (id.rfind("tp:", 0) != 0 && pred_left_[id] == 0)
          ready_.push({id, ready_seq_++, 0});
    }

  public:
    /**
     * @brief 建图唯一入口（**无锁原语，前提调用方持 mtx**）：清空旧图 + 8 步单函数建图——
     *        ① 端口索引 build_port_index → ② 超周期 build_hyper_period →
     *        ③ 拓扑序 build_topo_order → ④ 实例化 build_instances → ⑤ 支配周期/实例数
     *        build_dominance → ⑥ 建顶点 build_vertex → ⑦ 建边 build_edge → ⑦.5 时间链
     *        build_sync_points（同步时间点顶点 + 挂靠边）→ ⑧ 填 job 闭包 bind_job → ⑨ 就绪增量
     *        初始化（pred_left_/in_degree_ 入度基准）。超周期起点 = 当前真实时钟；开头清调度增量
     *        状态（pred_left_/in_degree_/done_/ready_），只清 mesg_hist_cap 容量表，
     *        message_hist_ 历史槽跨重建保留不清。空配置 → 空图（调度状态一致清空）。
     * @param pipeline 解析态 Pipeline（调用方传 pipeline_g 或局部，取其 nodes 快照）
     * @param library  算法定位 Library（调用方传 library_g，取其 so_ctx 快照）
     * @retval 无
     */
    void expand_hp(const Pipeline &pipeline, const Library &library) {
      hyper_start_ms = fins::util::now_ms();   // 新配置展开起点 = 当前真实时钟（勿残留上次回绕后的起点）
      mesg_hist_cap.clear();   // 只清容量表（新配置重算）；message_hist_ 历史槽跨重建保留不清（同 exec_us_hist_）
      pred_left_.clear();   // 就绪增量状态：空配置早退也一致清空
      in_degree_.clear();
      dag.clear();

      // 新配置全量清（map 释放）；世代号一并归零
      done_.clear();
      done_count_ = 0;
      done_gen_ = 0;

      // 入队序号随就绪集一并重置（新周期从头计序）
      ready_.clear();
      ready_seq_ = 0;
      tp_order_.clear();
      tp_released_ = 0;

      const auto nodes = pipeline.nodes;   // 入参快照（拷贝，防外部改）
      const auto so_ctx = library.so_ctx;
      if (nodes.empty()) { ++graph_version; return; }   // 空配置 → 空图（幂等；结构已清空，同样发失效信号）

      // ① 端口索引：producers/consumers + 一跳邻居
      std::map<std::string, std::vector<std::string>> producers, consumers;
      std::map<std::string, std::set<std::string>> in_producers, out_consumers;
      build_port_index(nodes, producers, consumers, in_producers, out_consumers);

      // ② 超周期：HP = lcm(周期节点)
      hyper_period_ms = build_hyper_period(nodes);

      // ③ 拓扑序：BFS 从源展开
      const auto topo = build_topo_order(nodes, in_producers, out_consumers);

      // ④ 实例化：by_id/by_info
      std::map<std::string, std::shared_ptr<AlgoBase>> by_id;
      std::map<std::string, const NodeInfo *> by_info;
      build_instances(nodes, so_ctx, by_id, by_info);

      // ⑤ 支配周期 + 实例数
      std::map<std::string, double> period_final;
      std::map<std::string, size_t> node_count;
      build_dominance(topo, by_info, producers, hyper_period_ms, period_final, node_count);

      // ⑥ 建顶点 {id}:{k}
      build_vertex(dag, nodes, period_final, node_count);

      // ⑦ seq 连续边 + 绑定边
      build_edge(dag, nodes, node_count);

      // ⑦.5 时间链：同步时间点顶点 + 挂靠边（图编辑好后钉时间约束）
      bind_sync(nodes, node_count);

      // ⑨ 就绪增量初始化：入度基准（build_pred）→ 初始就绪（seed_ready，私有函数，见 bind_job 之后）
      build_pred();

      // ⑧ job 闭包
      bind_job(nodes, by_id, node_count);

      // ⑨b 初始就绪（seed_ready，私有函数，见 bind_job 之后）
      seed_ready();

      ++graph_version;   // 结构重建完成 → makespan 结构缓存失效信号（须在全部建图步骤后）

#ifdef FINS_EXPORT_DGRAPH_PATH
      std::ofstream(FINS_EXPORT_DGRAPH_PATH) << export_dag().dump(2);
#endif
    }

    /**
     * @brief 超周期回绕（**无锁原语，前提调用方持 mtx**；主线程调度循环图静止时调用）：
     *        调度增量状态重置（done_/ready_ 清空、tp_released_ 游标归零、pred_left_ 重置回
     *        in_degree_ 基准，不 clear dag——顶点对象存活）。回绕后全部顶点未完成：job 顶点由前序
     *        完成事件逐级释放，源节点由其 tp 门被计时线程重新拉取释放。
     *
     *        ★ 释放节拍与翻页解耦（2026-09-04 拍板）：
     *        回绕仍由“本超周期完工”（is_hp_done）触发（干完即翻页），但超周期起点**不再拨回完工
     *        时刻**，而是推进到“绝对网格上的下一未来边界”（超周期起点按 H 整拍对齐 expand 起点）。
     *        于是周期任务只在真实周期锚点（起点+offset）释放——早排空自然空等到下一边界，不提前放
     *        下一拍；排空晚于边界（过载）时错过的整拍被跳过、对齐下一未来边界并告警（不累积漂移）。
     *        非周期（事件）任务不受影响：绑定前序完成即就绪即跑。tp job 实时读 hyper_start_ms →
     *        自动对齐新起点；loop 数据槽跨周期保留不清；abs_deadline 由 update_abs_deadline 滚动
     *        校正，非本函数职责。
     * @retval 无
     */
    void rollover_hp() {
      // 超周期起点推进到绝对网格上的下一未来边界（ceil(delta/H)·H；delta=距本拍起点已过 ms）。
      // 早完工（delta<H）→ +H：下一边界在完工之后，tp 睡到边界才放 → 周期任务不提前释放；
      // 过载（delta≥H）→ 跳过已错过的整拍、对齐 >now 的边界；k=1 正常，k≥2 告警跳过拍数。
      const double hp_ = hyper_period_ms;
      if (hp_ > 0.0) {
        const double delta = fins::util::now_ms() - hyper_start_ms;
        double k = 1.0;                                  // 至少推进一拍
        if (delta > 0.0) {                               // ceil(delta/H)，保证下一拍 ≥ now 的最近边界
          const double d = delta / hp_;
          k = std::floor(d);
          if (d > k) k += 1.0;
        }
        if (k >= 2.0)
          FINS_LOG_WARN("[rollover_hp] 超周期过载：排空晚于边界，跳过 {} 个释放拍，下一边界对齐 {:.1f}ms", (long long)(k - 1.0), hyper_start_ms + k * hp_);
        hyper_start_ms += k * hp_;
      } else {
        hyper_start_ms = fins::util::now_ms();           // 无显式周期（理论上不进回绕）保持旧行为
      }

#if FINS_CAL_WCET
      update_wcet_estimation();
#endif


#if FINS_CAL_MAKESPAN
      if (const double makespan = makespan_updater(dag, num_worker); makespan > hyper_period_ms)
        FINS_LOG_WARN("[rollover_hp] 超周期过载：makespan={:.2f}ms > hyper_period={:.2f}ms", makespan, hyper_period_ms);
#endif

#if FINS_STATIC_PRIORITY
      for (auto &item : ready_.data())
        item.prio = priority_updater(dag, dag.vertex(item.id), num_worker);
      ready_.rebuild();   // 按最新 prio 重建堆（O(n)）
#endif

      ++done_gen_;   // 世代化 clear：O(1) 重置，免释放 done_ 的 unordered_map 节点（原 std::set::clear 每顶点一次释放）
      done_count_ = 0;
      ready_.clear();   // 图静止时应空，防御清（LazyMaxHeap 为 vector，clear 保容量 O(1)）
      ready_seq_ = 0;   // 入队序号随就绪集重置（新周期从头计序）
      tp_released_ = 0;   // tp 全部重新释放（tp_order_ 不清——同一批时间点按原序重放）

      // pred_left 重置回入度基准：两 map 键集相同且均按键有序 → 锁步遍历，O(n) 免逐顶点 at() 查找
      {
        auto it_deg = in_degree_.begin();
        for (auto &[id, pl] : pred_left_) { pl = it_deg->second; ++it_deg; }
      }
    }

    /**
     * @brief 图静止判定（**无锁原语，前提调用方持 mtx**）：已完成顶点数（含 tp 全计）== dag
     *        顶点数。每顶点每超周期恰完成一次（trigger_workload_ready 幂等防御）→ done_.size() ≤ dag.size()；
     *        主线程调度循环持锁调本原语，图静止后决定 expand_hp / rollover_hp / wait。
     * @retval bool true = 图静止（全部顶点完成 / 空图 0==0）
     */
    bool is_hp_done() { return done_count_ == dag.size(); }

    bool is_hp_empty() { return dag.size() == 0; }

    bool is_workload_ready() { return !ready_.empty(); }

    /**
     * @brief 拉取就绪顶点（**无锁原语，前提调用方持 mtx**；装配点 on_execute 回调事务内部调用）：
     *        就绪集（util::LazyMaxHeap 懒最大堆，pred_left 减到 0 时经 ready_.push({s, ready_seq_++, 0})
     *        内联入队，prio 入队占位 0）中取优先级最高者 → mutate_vertex 回调取图内可变指针（无状态标记，拉走即隐式
     *        运行中）。优先级唯一来源 = 装配点注入的 priority_updater 键函数（顶点 → 调度优先级，
     *        可读 *this 全图状态如 hyper_start_ms/核心负载）：grab 前对每个就绪顶点现算覆盖占位 0；
     *        未注入回调 → prio 恒 0 → 就绪堆退化为纯 FIFO。再 rebuild() 按最新 prio 重建堆、
     *        pop_max() O(log n) 取顶；prio 相等时按入队序号 seq 小者先出 = 精确 FIFO。ddl 由主线程
     *        update_abs_deadline 集中维护，非本函数职责。
     * @retval Workload* 图内顶点指针（含 id/job；图静止期间 expand/rollover 不重建 → 稳定不悬垂）；
     *                   nullptr = 无就绪顶点
     */
    Workload *grab_ready_workload() {
      if (ready_.empty()) return nullptr;

#if FINS_DYNAMIC_PRIORITY
      update_abs_deadline();

      for (auto &item : ready_.data())
        item.prio = priority_updater(dag, dag.vertex(item.id), num_worker);
#endif

      ready_.rebuild();                                // 按最新 prio 重建堆（O(n)）

      const ReadyItem item = ready_.pop_max();         // 取 prio 最高者；相等按 seq FIFO（O(log n)
      Workload *picked = nullptr;
      dag.mutate_vertex(item.id, [&picked](Workload &x) { picked = &x; });

      return picked;
    }

    /**
     * @brief 拉取下一个待释放时间点（**无锁原语，前提调用方持 mtx**；装配点计时线程调用，与
     *        grab_ready_workload 对称——worker 拿计算 job、timer 拿延迟时间点）：按 pin_sync 建图
     *        时预排的释放顺序（tp_order_ 升序偏移）游标取下一个，无全图扫描。timer 拿到后与 worker
     *        对称：锁外执行其 job（sleep_until 睡到释放时刻，job 内实时读 hyper_start_ms → rollover
     *        平移自动对齐）→ 回锁 trigger_workload_ready + notify。全部已释放 → nullptr。
     * @retval Workload* 图内时间点顶点指针；nullptr = 无待释放时间点
     */
    Workload *grab_delay_workload() {
      if (tp_released_ >= tp_order_.size()) return nullptr;   // 空配置/一次性图/已全部释放

      const std::string id = tp_order_[tp_released_++];       // 按预排顺序取下一个（每 tp 恰一次，游标前移天然防重）
      Workload *picked = nullptr;
      dag.mutate_vertex(id, [&picked](Workload &x) { picked = &x; });

      return picked;
    }

    /**
     * @brief 完成事件（**无锁原语，前提调用方持 mtx**；装配点 worker/timer 回锁后调）：标记
     *        id 完成（done_ 插入，幂等防御）→ 对每个后继 out_nodes 递减 pred_left_，减到 0 =
     *        全部前序（含 seq/绑定/tp 挂靠边）完成 = 就绪，非 tp 者入 ready_ 就绪集。job 完成与
     *        tp 释放（tp 是 job 顶点前序，挂靠边）走同一传播路径。
     * @param id 已完成顶点 id（job 顶点或 "tp:" 时间点顶点）
     * @retval 无
     */
    std::unordered_map<std::string, uint64_t> done_;   // 顶点 id → 完成世代号（世代化 clear：rollover O(1) 重置，免释放节点）
    uint64_t done_gen_{0};                              // 当前世代号（rollover/expand 递增；done_[id]==gen ⇒ 本世代已完成）
    size_t done_count_{0};                              // 本世代已完成顶点数（is_hp_done 用；rollover 归零）
    void trigger_workload_ready(const std::string &id) {
      {   // 幂等防御（世代化 done_：本世代已完成 → 跳过；正常每顶点每超周期恰完成一次）
        auto it = done_.find(id);
        if (it != done_.end() && it->second == done_gen_) return;
        if (it == done_.end()) done_.emplace(id, done_gen_);
        else it->second = done_gen_;
        ++done_count_;
      }

      for (const auto &s : dag.out_nodes(id)) {
        auto it = pred_left_.find(s);
        if (it == pred_left_.end() || it->second == 0) continue;   // 未知/已就绪 → 跳过（防重复递减）
        if (--it->second == 0 && s.rfind("tp:", 0) != 0)   // 减到 0 = 恰好一次就绪
          ready_.push({s, ready_seq_++, 0});   // 推刚就绪的后继 s（勿推已完成前序 id）
      }
    }

  private:
    /**
     * @brief 滚动校正全部 job 顶点的 abs_deadline（**无锁原语，前提调用方持 mtx**；主线程调度
     *        循环事件驱动唤醒后调用）：基于当前真实时钟 now，从
     *        hyper_start_ms 起按超周期滚动起点到 now 所在时窗，abs_deadline = 滚动后起点 +
     *        (k+1)·deadline——执行快慢不定时 deadline 始终对齐真实时间轴（供 priority 键函数/
     *        将来 EDF 消费），不再依赖回绕副作用。超周期内幂等；无超周期（hyper_period_ms<=0）
     *        不滚动（保持 expand 设置值）。
     * @warning 调用时不可同时修改 dag 状态
     * @retval 无
     */
    void update_abs_deadline() {
      const double now = fins::util::now_ms();
      const double period = hyper_period_ms;
      dag.for_each_vertex([&](const std::string &, Workload &v) {
        if (!v.job) return;
        double start = hyper_start_ms;
        if (period > 0 && now > start + period)
          start += std::floor((now - start) / period) * period;
        v.ddl = start + (double)(v.k + 1) * v.deadline;
      });
    }

    /**
     * @brief 集中回写 wcet（无锁原语，前提调用方持 mtx；主线程调度循环每轮 update_abs_deadline 旁调用）：
     *         遍历图顶点，对有执行历史的普通节点调 wcet_updater(该顶点历史 deque) 现算覆盖 v.wcet。
     *         tp 顶点无 job → 跳过（wcet 建图期理论写死 = 相邻同步点间隔）；无历史顶点 → 跳过（保留建图期默认）。
     * @warning 必须在所有 job 都结束时才能调用
     * @retval 无
     *
     */
    void update_wcet_estimation() {
      dag.for_each_vertex([&](const std::string &, Workload &v) {
        if (!v.job) return;
        if (v.id.rfind("tp:", 0) != 0)
          TBBMAP_READ(exec_us_hist_, v.name, [&](const auto &hist) {   // 键 = 算法键（record_exec 用 info.name；v.id = {name}:{k} 对不上）
            if (!hist.empty() && wcet_updater) {
              std::deque<double> vals;          // 统计槽只要用时序列（wcet_updater 槽签名 deque<double>）
              for (const auto &s : hist) vals.push_back(s.us);
              v.wcet = wcet_updater(vals);
            }
          });
      });
    }

    /** @brief 导出 dag 为 JSON（调试/可视化：顶点集合 + 边集合 + 超周期参数）。
     *  私有，供内部调试/导出调用（调用方持 mtx）。顶点含 id/name/k/period/deadline/wcet/ddl/
     *  has_job/kind（"job" | "timepoint"）；边含 from/to/tag + message 槽状态（是否有帧/类型）。
     * @retval nlohmann::json 图 JSON（调用方决定落盘 dump(2) 或消费）
     */
    nlohmann::json export_dag() {
      nlohmann::json j;
      j["hyper_start_ms"]  = hyper_start_ms;
      j["hyper_period_ms"] = hyper_period_ms;

      j["vertices"] = nlohmann::json::array();
      dag.for_each_vertex([&](const std::string &id, const Workload &v) {
        j["vertices"].push_back({
          {"id", id},
          {"name", v.name},
          {"k", v.k},
          {"period", v.period},
          {"deadline", v.deadline},
          {"wcet", v.wcet},
          {"ddl", v.ddl},
          {"has_job", static_cast<bool>(v.job)},
          {"kind", id.rfind("tp:", 0) == 0 ? "timepoint" : "job"},
        });
      });

      j["edges"] = nlohmann::json::array();
      dag.for_each_edge([&](const std::string &from, const std::string &to,
                            const std::string &tag, const Message &m) {
        j["edges"].push_back({
          {"from", from},
          {"to", to},
          {"tag", tag},
          {"message", {{"has_frame", m.frame != nullptr}, {"type", m.type_name}}},
        });
      });

      j["hist_edges"] = hist_export_;   // hist 隐式依赖（loop 自反馈 / 周期“最新一帧”读），与前序 edges 分开
      return j;
    }
  };
  inline PrecedenceGraph graph_g;

  // 其他全局单例（PluginLoader/RPCListener/ThreadPool）：其 hpp 均 include g_state 拿全局对象，
  // 故反向 include 会成循环依赖——此处不定义，由业务代码在运行时 .instance() 初始化。

} // namespace fins::rt
