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
// expand_hp/grab_ready_workload/is_hp_done/rollover_hp）。
// 数据流：RPC 存 JSON → pending → 主线程调度循环图静止时 commit+parse+check_topology+expand_hp 重建。
// 装配点写法与语义细节见 docs/precedence_graph_design.md 及各类型前注释。
// ============================================================================

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <deque>
#include <dlfcn.h>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "algo/algo_base.hpp"
#include "form.hpp"
#include "mesg/mesg.hpp"
#include "third_party/json.hpp"
#include "utils/time.hpp"

namespace fins::rt {
  struct Library;
  struct Pipeline;
  struct PrecedenceGraph;

  /// 硬件状态监控，由监控器更新
  inline std::vector<float> core_usages_g{};
  inline std::atomic<float> mem_usage_g{};
  /// 外部回调槽（命名空间级 inline，装配点直接赋值注入；main 主线程调度循环每轮持 mtx 集中更新）。
  inline std::function<void(PrecedenceGraph&)> wcet_updater = nullptr;
  /// 外部回调槽（命名空间级 inline，装配点直接赋值注入；main 主线程调度循环每轮持 mtx 集中更新）。
  inline std::function<void(PrecedenceGraph&)> priority_updater = nullptr;

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
  };
  inline Library library_g;

  /** @brief 节点解析态（Pipeline 内嵌，parse_pipeline 产物）：纯数据，字段含 id/name/version、
   *  端口名数组、config_cache、loop_timer/loop_step、period/wcet/deadline/priority/cap。 */
  struct NodeInfo {
    std::string id;                                      // 节点在图中的唯一标识（顶点名 id:{k} 前缀）
    std::string name;                                    // 算法名（[name:version] = so 表定位键）
    std::string version;                                 // 算法版本（定位键）

    float period{0};                                     // 执行周期（ms；0 = 未配置，走支配继承）
    float deadline{1};                                   // 相对截止期（ms；缺省已折成 wcet）
    float wcet{1};                                       // 最坏执行时间（ms；缺省 1）
    int priority{0};                                     // 调度优先级（缺省 0；expand_hp ⑥ 填顶点 Workload.priority 初值）
    size_t cap{10};                                     // 滑动窗口容量（hist 长度：loop 反馈历史槽每端口保留帧数；默认 10，config 顶层 "cap" 可配）

    std::vector<std::string> input_ports;
    std::vector<std::string> output_ports;
    std::vector<nlohmann::json> config_cache;

    std::map<std::string, float> loop_timer;                 // loop 端口 → 定义（显式端口补充定义）
    std::map<std::string, float> loop_step;                  // loop 端口 → 定义（显式端口补充定义）

    /** @brief 逐节点自解析：全部结构校验 + 字段抽取（Pipeline::parse 只拆封顶层后逐个调用
     *  本构造器）。parameters 为**位置式取值表** config_cache——只取 p["value"]、名字丢弃
     *  （顺序 = config "parameters" 数组元素顺序 = AlgoFunc 配置段相对序号，
     *  见 algo_func.hpp 头注释顺序保证链）。
     * @param n 节点 JSON 对象（必填 name/version/id，可选 parameters/inputs/outputs/wcet/deadline/period/priority/cap/loop）
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
      if (n.contains("priority") && !n["priority"].is_number_integer())
        throw std::invalid_argument("[parse_dataflow] " + at + "priority 须为 integer");
      // 源头约束（无输入节点必填 period）属图结构合法性，由第二级 Pipeline::check_topology 审查
      // （本构造器只做逐节点 json 格式校验，不查跨节点图结构）。

      // ── 字段抽取（图侧/运行时不再接触原始 JSON）──
      id       = n["id"].get<std::string>();
      name     = n["name"].get<std::string>();
      version  = n["version"].get<std::string>();
      period   = n.contains("period")   ? n["period"].get<float>()   : 0.0f;
      wcet     = n.contains("wcet")     ? n["wcet"].get<float>()     : 1.0f;
      deadline = n.contains("deadline") ? n["deadline"].get<float>() : wcet;  // 缺省 = wcet
      priority = n.contains("priority") ? n["priority"].get<int>()   : 0;    // 缺省 0
      cap      = n.contains("cap")      ? n["cap"].get<size_t>() : 10;        // 缺省 10
      if (n.contains("inputs") && n["inputs"].is_array())
        input_ports = n["inputs"].get<std::vector<std::string>>();
      if (n.contains("outputs") && n["outputs"].is_array())
        output_ports = n["outputs"].get<std::vector<std::string>>();
      if (n.contains("loop") && n["loop"].is_object()) {
        for (const auto &[mode, ports] : n["loop"].items()) {
          if (!ports.is_object()) continue;
          for (const auto &[port, arg] : ports.items()) {
            if (mode == "timer") loop_timer[port] = arg.get<float>();  // 观测周期 ms
            else                loop_step[port]  = arg.get<float>();  // 迭代步 N
          }
        }
      }
    }
  };
  /** @brief Pipeline — dataflow 配置（解析态；全局单份 pipeline_g）：cache = 原始配置 JSON 双缓冲
   *  （RPC 写缓冲份不解析 → pending → 主线程调度循环图静止时 commit + parse_pipeline 填 nodes），
   *  标准形式 = 节点对象数组（name/version/id 必填 + parameters/inputs/outputs/wcet/deadline/period/
   *  priority/loop 可选），违反抛 std::invalid_argument；loop 端口反馈走 message_hist_ 数据槽。 */
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
     *  ② 源周期：无输入节点（input_ports 空）无上游驱动，须主动周期执行，必填 period。
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

  /** @brief 图顶点 = 正常 job 实例 + 多维权值 + 生命周期状态（调度依据）。job 闭包只执行不碰
   *  state（Running 由 grab_ready_workload 拉取置、Finished 由装配点回锁直做、回绕重置 Pending）；
   *  priority = 调度优先级（grab_ready_workload 选最优先就绪）；就绪 = 前序全 Finished。 */
  struct Workload {
    /** @brief 顶点生命周期状态（标记"走到哪一步"，非"是什么"——顶点类别由 job 是否非空区分）。 */
    enum class State { Pending, Ready, Running, Finished };
    State state{State::Pending};  // 生命周期：拉取（grab_ready_workload）置 Running → 装配点回锁直做置 Finished；回绕重置 Pending

    std::string id{};             // 顶点名（格式 {节点id}:{k}，如 cam:0/cam:1）——expand_hp ⑥ 建顶点时填 vtx（同 dag 的 map 键）；
    std::string name{};           // 节点名（来自 Pipeline::NodeInfo.name；区别于 id 顶点名 = {name}:{k}）——装配点/测试按节点名识别
    size_t k{0};                  // 超周期内实例序号（expand_hp ⑥ 建顶点填；update_abs_deadline 滚动校正用）


    float period{0};
    float deadline{1};            // 相对截止期（ms；缺省 = wcet）

    float ddl{0};                 // 绝对截止期（ms；滚动排期 = 主线程每轮 update_abs_deadline 按当前
    float wcet{1};                // 最坏执行时间（ms；缺省 1）
    int priority{0};

    std::function<void()> job;    // 执行体（闭包捕获实例 + 节点配置，执行时现查边取帧/发布）
  };

  /** @brief PrecedenceGraph — 数据流图 + 调度依据（单份运行图 graph_g）。公开成员 = 调度状态
   *  mtx/cv/stopped/pending + 图数据 dag（DAG<Workload, Message>，顶点 {id}:{k}、边=绑定边
   *  Message 槽）+ 超周期/hyper_start_ms + 历史统计（mesg_hist_cap/message_hist_/exec_us_hist_/exec_hist_cap）。
   *  方法全为无锁原语（expand_hp/grab_ready_workload/is_hp_done/rollover_hp 等），调用方持 mtx。 */
  struct PrecedenceGraph {
    // ── public：图数据 + 无锁原语（方法不碰锁，前提调用方持 mtx；带锁事务在装配点
    //    on_execute 回调 / 主线程调度循环）──
    util::DirectedAcyclicGraph<Workload, Message> dag;  // 顶点带权、边=Message 槽

    float hyper_period_ms{0};       // 超周期长度（ms）
    float hyper_start_ms{0};    // 当前超周期起点（ms；expand 初始化 = 当前真实时钟、rollover_hp 回绕更新 = 当前真实时钟）

    // ── 历史数据统计（loop 反馈滑动窗口，按输出端口名）
    std::map<std::string, size_t> mesg_hist_cap{};   // 滑动窗口容量（expand_hp 填充：loop 反馈输出端口 → producer 节点 NodeInfo.cap，默认 10 可配；expand_hp 重建时清空重算；运行时只读无并发写）
    util::TBBMap<std::deque<Message>> message_hist_;  // 运行时：输出端口名 → 最近 cap 帧滑动窗口（loop 反馈历史槽；
    /** @brief 记录一帧到 loop 反馈滑动窗口数据槽（满丢最旧；TBBMap accessor 按端口锁，
     *  只锁本端口历史槽字段）。
     * @param id 输出端口名（历史槽键）
     * @param mesg 数据帧（Message）
     * @retval 无
     */
    void record_mesg(const std::string &id, const Message& mesg) {
      TBBMAP_UPDATE(message_hist_, id, [&](auto &q) {   // 无则默认构造插入、有则定位（持写锁，仅本端口）
        if (q.size() >= mesg_hist_cap[id]) q.pop_front();  // 满丢最旧（环形；cap 运行时只读，调用方已 guard >0）
        q.push_back(mesg);
      });
    }

    // ── 算法执行耗时统计（按节点环形队列，仅 execute 耗时）
    std::map<std::string, size_t> exec_hist_cap{};   // 环形队列容量（可配：每节点保留最近 N 次 execute 耗时；当前无填充 → 缺省 0，既有语义保持）
    util::TBBMap<std::deque<double>> exec_us_hist_;  // 节点 id → 最近 execute 耗时（us；TBBMap accessor 按节点锁，只锁本节点字段）
    /** @brief 记录一次 execute 耗时到节点环形队列（满丢最旧；TBBMap accessor 按节点锁，
     *  只锁本节点字段）。
     * @param id 节点 id
     * @param us 执行耗时（us，steady_clock 计时）
     * @retval 无
     */
    void record_exec(const std::string &id, double us) {
      TBBMAP_UPDATE(exec_us_hist_, id, [&](auto &q) {   // 无则默认构造插入、有则定位（持写锁，仅本节点）
        const size_t cap = exec_hist_cap.count(id) ? exec_hist_cap.at(id) : 100;  // 缺省 100（未填充时勿取 operator[]→0，否则 pop_front 空 deque = UB）
        if (q.size() >= cap) q.pop_front();  // 满丢最旧（环形）
        q.push_back(us);
      });
    }

    // ── 调度状态公开成员（装配点直接读写：worker on_execute 回调 / main 主线程调度循环
    //    持 mtx 调用下述无锁原语；std::mutex 不可重入——持锁期间勿再 lock()，会死锁）──
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<bool> stopped{false};
    std::atomic<bool> pending{false};

  private:
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
          if (!info.loop_timer.count(pn) && !info.loop_step.count(pn))   // loop 端口：不构成消费者（无自环绑定边）
            consumers[pn].push_back(info.id);
      }
      for (const auto &info : nodes) {
        for (const auto &pn : info.input_ports)
          if (!info.loop_timer.count(pn) && !info.loop_step.count(pn))   // loop 端口：反馈 producer 是自身，不构成拓扑依赖
            for (const auto &p : producers[pn])
              in_producers[info.id].insert(p);
        for (const auto &pn : info.output_ports)
          for (const auto &cc : consumers[pn])
            out_consumers[info.id].insert(cc);
      }
    }

    /** @brief ② 超周期：lcm(全部显式周期节点 period)（无周期节点 → 返回 0 不回绕）。
     * @param nodes 解析态节点表（只读；只统计 info.period > 0 的节点）
     * @retval float 超周期长度（ms）；无周期节点返回 0
     */
    static float build_hyper_period(const std::vector<NodeInfo> &nodes) {
      long long hp = 1;
      bool any_periodic = false;
      for (const auto &info : nodes) {
        const float T = info.period;   // 已由 parse 抽取
        if (T <= 0) continue;
        any_periodic = true;
        long long a = hp, b = (long long)(T + 0.5f);
        while (b) { long long r = a % b; a = b; b = r; }   // a = gcd(hp, T)
        hp = (hp / a) * (long long)(T + 0.5f);
      }
      return any_periodic ? (float)hp : 0.0f;
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
                                const float hyper_period,
                                std::map<std::string, float> &period_final,
                                std::map<std::string, size_t> &node_count) {
      for (const auto &id : topo) {
        const auto *info = by_info.at(id);
        float T = info->period;                    // 显式配置周期（0 = 未配置 → 走继承）
        if (T <= 0) {
          // 未配置 → 继承前级：多前级取最短周期前级（topo 序保证前级已定最终周期）
          std::string trig;
          float best = 0;
          for (const auto &pn : info->input_ports)
            if (!info->loop_timer.count(pn) && !info->loop_step.count(pn)) {  // loop 端口：反馈 producer 是自身，不参与继承
              auto pit = producers.find(pn);
              if (pit == producers.end()) continue;   // 孤立输入端口（无 producer）→ 无继承源
              for (const auto &p : pit->second) {
                const float pt = period_final.count(p) ? period_final[p] : 0;
                if (trig.empty() || pt < best) { trig = p; best = pt; }
              }
            }
          T = (!trig.empty() && period_final.count(trig)) ? period_final[trig] : 0.0f;
        }
        period_final[id] = T;
        node_count[id] = (T > 0) ? (size_t)(hyper_period / T + 0.5f) : 1;
      }
    }

    /** @brief ⑥ 建顶点：每节点展开 node_count 个 job 实例顶点 {id}:{k}（k=0..N-1），载荷 =
     *  attrs 基础（period/deadline/wcet/priority）+ abs_deadline 按 job 序排 = 超周期起点 +
     *  (k+1)·deadline（无 release/相位）。
     * @param dag 目标图（就地加顶点）
     * @param hyper_start_ms 超周期起点（ms；abs_deadline 排期基准）
     * @param nodes 解析态节点表（只读）
     * @param period_final 节点 id → 最终周期（只读；⑤ 的结果）
     * @param node_count 节点 id → 实例数（只读；⑤ 的结果）
     * @retval 无
     */
    static void build_vertices(util::DirectedAcyclicGraph<Workload, Message> &dag, float hyper_start_ms,
                               const std::vector<NodeInfo> &nodes,
                               const std::map<std::string, float> &period_final,
                               const std::map<std::string, size_t> &node_count) {
      for (const auto &info : nodes) {
        const size_t N = node_count.at(info.id);
        for (size_t k = 0; k < N; ++k) {
          Workload v;
          v.k            = k;   // 实例序号（update_abs_deadline 滚动校正用）
          v.name         = info.name;   // 节点名（NodeInfo.name；id 顶点名 = {name}:{k}）
          v.period       = period_final.at(info.id);
          v.deadline     = info.deadline;   // parse 已折到 wcet（缺省 = wcet）
          v.priority     = info.priority;   // grab_ready_workload 选最优先就绪的初值（priority_updater 运行时更新）
          v.wcet         = info.wcet;
          v.ddl          = hyper_start_ms + (float)(k + 1) * info.deadline;
          dag.add_node(info.id + ":" + std::to_string(k), std::move(v));
        }
      }
    }

    /** @brief ⑥.5 显式周期节点 job 间插入 delay 假节点：{id}:{k} → delay:{id}:{k} → {id}:{k+1}。
     *  周期性任务时间语义——每对相邻 job 间真延迟一个执行周期 period（sleep offset=period，
     *  占 worker）；delay 假节点无数据边（纯时间门控），前序关系经两段 seq 边（{id}:{k} 完成 →
     *  delay sleep → {id}:{k+1} 就绪）。只对显式配置 period 的节点（info.period > 0）；
     *  继承周期节点不插（跟随 producer 节奏）。delay 假顶点不调 record_exec（时间门控非算法
     *  执行，不污染 exec_us_hist_）；完成事件由装配点通用回锁直做照常推进。
     * @param dag 目标图（就地加 delay 假顶点）
     * @param hyper_start_ms 超周期起点（ms；abs_deadline 排期基准）
     * @param nodes 解析态节点表（只读）
     * @param period_final 节点 id → 最终周期（只读；offset 取值源）
     * @param node_count 节点 id → 实例数（只读）
     * @retval 无
     */
    static void build_delays(util::DirectedAcyclicGraph<Workload, Message> &dag, float hyper_start_ms,
                             const std::vector<NodeInfo> &nodes,
                             const std::map<std::string, float> &period_final,
                             const std::map<std::string, size_t> &node_count) {
      for (const auto &info : nodes) {
        if (info.period <= 0) continue;                  // 仅显式配置周期节点
        const size_t N = node_count.at(info.id);
        const float offset = period_final.at(info.id);   // = info.period（显式周期）
        for (size_t k = 0; k + 1 < N; ++k) {
          Workload v;
          v.id           = "delay:" + info.id + ":" + std::to_string(k);
          v.name         = "delay";
          v.k            = k + 1;                        // 第 k+1 周期步进
          v.priority     = info.priority;
          v.period       = offset;
          v.deadline     = offset;
          v.wcet         = offset;                       // sleep 时长 = period
          v.ddl          = hyper_start_ms + (float)(k + 2) * offset;
          v.job          = [offset]() {                  // 纯时间门控，无数据路由
            std::this_thread::sleep_for(std::chrono::duration<float, std::milli>(offset));
          };
          dag.add_node(v.id, std::move(v));
        }
      }
    }

    /** @brief ⑦ 建边：seq 连续边（同节点实例串行）+ 同名端口直连绑定边。
     *  seq 边：{id}:{k} → {id}:{k+1}，边名 "seq:"+id（Replication 多实例串行——job 实例按超周期
     *  序逐个执行，保证同节点内串行）。显式周期节点（info.period>0）经 delay 假节点拆两段
     *  （{id}:{k} → delay:{id}:{k} → {id}:{k+1}，见 ⑥.5 build_delays）；继承周期节点单段。
     *  绑定边：consumer 每输入端口 pn 绑输出 pn 的唯一 producer 节点（单写者已由 check_topology
     *  保证唯一），整数式 pk=((k+1)·Np-1)/Nc 连 producer:{pk} → consumer:{k}（时段内最新已完成帧，
     *  同速率一一对应；快 producer→慢 consumer 绑末帧；慢 producer→快 consumer 共享帧；恒有边）。
     *  loop 端口（loop_timer/loop_step 中）无绑定边——反馈走运行时数据槽 message_hist_（滑动窗口）。
     * @param dag 目标图（就地加边）
     * @param nodes 解析态节点表（只读）
     * @param node_count 节点 id → 实例数（只读）
     * @retval 无
     */
    static void build_edges(util::DirectedAcyclicGraph<Workload, Message> &dag,
                            const std::vector<NodeInfo> &nodes,
                            const std::map<std::string, size_t> &node_count) {
      for (const auto &info : nodes) {
        const size_t N = node_count.at(info.id);
        const bool periodic = info.period > 0;   // 显式周期节点：job 间经 delay 假节点（周期步进）
        for (size_t k = 0; k + 1 < N; ++k) {
          if (periodic) {
            const std::string dv = "delay:" + info.id + ":" + std::to_string(k);
            dag.add_edge(info.id + ":" + std::to_string(k), dv, "seq:" + info.id, Message{});
            dag.add_edge(dv, info.id + ":" + std::to_string(k + 1), "seq:" + info.id, Message{});
          } else {
            dag.add_edge(info.id + ":" + std::to_string(k), info.id + ":" + std::to_string(k + 1),
                         "seq:" + info.id, Message{});
          }
        }
      }
      std::map<std::string, std::string> producer_of;   // 输出端口名 → producer 节点 id（首生产者，函数内局部）
      for (const auto &info : nodes)
        for (const auto &pn : info.output_ports)
          if (!producer_of.count(pn)) producer_of[pn] = info.id;
      for (const auto &info : nodes) {
        const size_t Nc = node_count.at(info.id);
        for (const auto &pn : info.input_ports) {
          if (info.loop_timer.count(pn) || info.loop_step.count(pn)) continue;   // loop 端口无绑定边
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

    /**
     * @brief ⑧ 填 job 执行体闭包：按 NodeInfo 端口序打包输入/输出 array → AlgoBase execute →
     *        输出路由下游绑定边 + record_mesg 维护 loop 滑动窗口，运行时不再接触原始 JSON。
     *        闭包捕获稳定解析态（shared_ptr<const NodeInfo> 每节点 1 份按 k 共享）+ 算法实例 by_id；
     *        输入逐输入端口取绑定边帧（loop 端口从 message_hist_ 聚合最近 w 帧，恒长 w、窗口未满
     *        开头补空 Message 占位，pub<vector<Message>> 进对应下标）。hist 容量
     *        mesg_hist_cap[输出端口] = 该端口作为 loop 反馈被消费时 producer 节点的 NodeInfo.cap
     *        （config 顶层 "cap"，默认 10），只对 loop 反馈输出端口建历史槽；单写者约束保证
     *        每输出端口唯一 producer → 直接赋值（非 max）；message_hist_ 跨重建保留不清。
     * @param nodes 解析态节点表（只读）
     * @param hyper_period 超周期 ms（loop_timer 观测周期/节点周期 → 窗口帧数换算）
     * @param by_id 节点 id → 算法实例（只读）
     * @param node_count 节点 id → job 实例数（只读）
     * @retval 无
     */
    void build_jobs(const std::vector<NodeInfo> &nodes, float hyper_period,
                    const std::map<std::string, std::shared_ptr<AlgoBase>> &by_id,
                    const std::map<std::string, size_t> &node_count) {
      for (const auto &info : nodes) {
        for (const auto &[port, _] : info.loop_step)
          mesg_hist_cap[port] = info.cap;
        for (const auto &[port, _] : info.loop_timer)
          mesg_hist_cap[port] = info.cap;
      }
      for (const auto &info : nodes) {
        auto sinfo = std::make_shared<const NodeInfo>(info);   // 闭包捕获稳定共享解析态
        const auto &algo = by_id.at(info.id);
        const size_t n = node_count.at(info.id);
        // Loop 聚合窗口：显式端口补充定义（Pipeline::NodeInfo.loop_step / loop_timer 两 map）——
        // step 定长迭代步 N；timer 观测周期/节点周期 → 窗口帧数（ceil）。
        std::map<std::string, size_t> loop_w;   // loop 端口 → 聚合窗口（帧数，恒长定长）
        if (!info.loop_step.empty() || !info.loop_timer.empty()) {
          const float Tnode = (n > 0 && hyper_period > 0) ? hyper_period / (float)n : 0.0f;
          for (const auto &[port, arg] : info.loop_step)
            loop_w[port] = (size_t)arg;   // "step"：迭代步 N（定长窗口）
          for (const auto &[port, arg] : info.loop_timer)
            loop_w[port] = (size_t)std::ceil(arg / Tnode);   // "timer"：观测周期/节点周期 → 窗口帧数
        }
        for (size_t k = 0; k < n; ++k) {
          const std::string vtx = info.id + ":" + std::to_string(k);   // 顶点名现拼（无 JobInst）
          dag.mutate_vertex(vtx, [this, sinfo, algo, vtx, loop_w](Workload &v) {
            v.id = vtx;   // 顶点名现拼 {节点id}:{k} → Workload.id 实际填充（grab_ready_workload 返回 Workload* 含 id，装配点直做完成事件用）
            v.job = [this, sinfo, algo, vtx, loop_w]() {
              // 状态修改移出 job：Running 由 grab_ready_workload() 拉取时置、Finished 由 on_execute 回调事务
              // 回锁直做置（完成事件不在闭包内）；本闭包只执行（打包输入 → execute → 路由输出）。
              // 输入：按 NodeInfo输入端口序打包 array（算法按位置取，不碰端口名）
              std::vector<Message> inputs(sinfo->input_ports.size());
              for (size_t i = 0; i < inputs.size(); ++i) {
                const std::string &pn = sinfo->input_ports[i];
                if (loop_w.count(pn)) {
                  // Loop 反馈：从滑动窗口历史槽聚合——最近 w 帧真实历史（message_hist_[pn]，seq
                  // 边保证前序 job 已执行压入；窗口跨周期保留、跨重建保留，回绕不清，满丢最旧）。
                  // 恒长 w 契约：窗口未满（刚启动/超周期初期）开头补空 Message 0 值占位
                  // （frame=nullptr，算法须判断 .frame 跳过/当 0，勿 sub——sub 空帧抛异常）。
                  const size_t w = loop_w.at(pn);
                  std::vector<Message> frames(w);   // 恒长 w：默认占位（空 Message 0 值）→ 尾部覆盖真实帧
                  { // TBBMap const_accessor 只锁本端口历史槽字段（与 record_mesg 同端口写互斥；不同端口并发读）
                    util::TBBMap<std::deque<Message>>::const_accessor a;
                    if (message_hist_.find(a, pn)) {
                      const auto &h = a->second;
                      const size_t take = std::min(w, h.size());
                      for (size_t j = 0; j < take; ++j)
                        frames[w - take + j] = h[h.size() - take + j];
                    }
                    // find 失败（理论不发生：seq 边保证 producer 已先执行压槽）→ 全占位，等价原空槽
                  }
                  Message m;
                  auto vv = m.pub<std::vector<Message>>();
                  *vv = std::move(frames);
                  inputs[i] = std::move(m);
                } else {
                  auto es = dag.edges_to(vtx, pn);
                  if (!es.empty()) inputs[i] = es[0].get();  // 绑定边唯一 → 该 producer job 帧
                }
              }
              // 输出：按 NodeInfo输出端口序预构造 array（算法按位置写）
              std::vector<Message> outputs(sinfo->output_ports.size());
              // 执行：配置已建图期注入 algo 实例（AlgoFunc configs_ 类型化帧，execute 零解析）。
              // 耗时统计：execute 前后 steady_clock 计时（us）→ record_exec(节点 id, us)——仅
              // execute 耗时，不含输入打包/输出路由；按节点环形队列，expand_hp 重建保留不清。
              const auto _t0 = std::chrono::steady_clock::now();
              algo->execute(inputs, outputs);
              record_exec(sinfo->id, std::chrono::duration<double, std::micro>(
                  std::chrono::steady_clock::now() - _t0).count());
              for (size_t i = 0; i < outputs.size(); ++i) {
                const std::string &pn = sinfo->output_ports[i];
                for (auto &e : dag.edges_from(vtx, pn))
                  e.get() = outputs[i];  // 写全部下游绑定边（生产者消费者共享帧）
                if (mesg_hist_cap[pn] > 0)   // 有 loop 消费者才存历史（cap>0）；record_mesg 满 cap 丢最旧
                  record_mesg(pn, outputs[i]);
              }
            };
          });
        }
      }
    }

  public:
    /**
     * @brief 建图唯一入口（**无锁原语，前提调用方持 mtx**）：清空旧图 + 8 步单函数建图——
     *        ① 端口索引 build_port_index → ② 超周期 build_hyper_period →
     *        ③ 拓扑序 build_topo_order → ④ 实例化 build_instances → ⑤ 支配周期/实例数
     *        build_dominance → ⑥ 建顶点 build_vertices → ⑥.5 显式周期节点插 delay 假节点
     *        build_delays → ⑦ 建边 build_edges → ⑧ 填 job 闭包 build_jobs。超周期起点 = 当前
     *        真实时钟；只清 mesg_hist_cap 容量表，message_hist_ 历史槽跨重建保留不清。空配置 → 空图。
     * @param pipeline 解析态 Pipeline（调用方传 pipeline_g 或局部，取其 nodes 快照）
     * @param library  算法定位 Library（调用方传 library_g，取其 so_ctx 快照）
     * @retval 无
     */
    void expand_hp(const Pipeline &pipeline, const Library &library) {
      dag.clear();
      hyper_period_ms = 0;
      hyper_start_ms = fins::util::now_ms();   // 新配置展开起点 = 当前真实时钟（勿残留上次回绕后的起点）
      mesg_hist_cap.clear();   // 只清容量表（新配置重算）；message_hist_ 历史槽跨重建保留不清（同 exec_us_hist_）

      const auto nodes = pipeline.nodes;   // 入参快照（拷贝，防外部改）
      const auto so_ctx = library.so_ctx;
      if (nodes.empty()) return;        // 空配置 → 空图（幂等，parse 已产空表）

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
      std::map<std::string, float> period_final;
      std::map<std::string, size_t> node_count;
      build_dominance(topo, by_info, producers, hyper_period_ms, period_final, node_count);

      // ⑥ 建顶点 {id}:{k}
      build_vertices(dag, hyper_start_ms, nodes, period_final, node_count);

      // ⑥.5 显式周期节点 job 间插 delay 假节点（真延迟 offset=period）
      build_delays(dag, hyper_start_ms, nodes, period_final, node_count);

      // ⑦ seq（显式周期节点经 delay）+ 绑定边
      build_edges(dag, nodes, node_count);

      // ⑧ job 闭包
      build_jobs(nodes, hyper_period_ms, by_id, node_count);
    }

    /**
     * @brief 超周期回绕（**无锁原语，前提调用方持 mtx**；主线程调度循环图静止时调用）：
     *        起点更新为当前真实时钟 + 全部 job 顶点重置 Pending（seq 边不闭合，回绕后 {id}:{0}
     *        由 grab_ready_workload 重新拉取；loop 数据槽跨周期保留不清；abs_deadline 由主线程
     *        每轮 update_abs_deadline 滚动校正，非本函数职责）。
     * @retval 无
     */
    void rollover_hp() {
      hyper_start_ms = fins::util::now_ms();   // 起点更新为当前真实时钟
      dag.for_each_vertex([&](const std::string &, Workload &v) {
        if (v.job) v.state = Workload::State::Pending;  // 有执行体 → 重置待执行
      });
    }

    /**
     * @brief 图静止判定（**无锁原语，前提调用方持 mtx**；range 遍历只读 state，无嵌套 accessor）：
     *        全部 job 顶点是否 Finished（无 Pending/Ready/Running 的执行体）。主线程调度循环
     *        持锁调本原语，图静止后决定 expand_hp / rollover_hp / wait。
     * @retval bool true = 图静止（全部执行体 Finished / 空图）
     */
    bool is_hp_done() {
      bool idle = true;
      dag.for_each_vertex([&](const std::string &, const Workload &w) {
        if (w.job && w.state != Workload::State::Finished) idle = false;
      });
      return idle;
    }

    /**
     * @brief 拉取最优先就绪顶点并置 Running（**无锁原语，前提调用方持 mtx**；装配点 on_execute
     *        回调事务内部调用）。两趟扫描规避 TBBMap range 遍历锁内嵌套 accessor 死锁
     *        （spin_rw_mutex 不可重入）：① for_each_vertex 只收集候选 id（Pending + 有执行体），
     *        不嵌套 accessor；② 逐个候选查前序（in_nodes const_accessor）全 Finished 且
     *        priority 最大者。
     * @retval Workload* 图内顶点指针（含 id/job，state 已置 Running；图静止期间 expand/rollover
     *                   不重建 → 稳定不悬垂）；nullptr = 无就绪顶点
     */
    Workload *grab_ready_workload() {
      std::vector<std::string> cand;
      dag.for_each_vertex([&](const std::string &id, const Workload &w) {
        if (w.state == Workload::State::Pending && w.job) cand.push_back(id);
      });
      std::optional<std::string> best;
      int best_p = std::numeric_limits<int>::min();
      for (const auto &id : cand) {
        if (dag.vertex(id).state != Workload::State::Pending) continue;
        bool ok = true;
        for (const auto &from : dag.in_nodes(id))
          if (dag.vertex(from).state != Workload::State::Finished) { ok = false; break; }
        if (ok && dag.vertex(id).priority > best_p) { best_p = dag.vertex(id).priority; best = id; }
      }
      if (!best) return nullptr;
      Workload *picked = nullptr;   // vertex(id) 是 const 读取，须经 mutate_vertex 回调取可变指针
      dag.mutate_vertex(*best, [&picked](Workload &x) { x.state = Workload::State::Running; picked = &x; });
      return picked;   // 图内顶点指针（含 id/job；state 已是 Running；图静止期间 expand/rollover 不重建 → 稳定不悬垂）
    }

    /**
     * @brief 滚动校正全部 job 顶点的 abs_deadline（**无锁原语，前提调用方持 mtx**；主线程调度
     *        循环每轮 priority_updater 之前调用）：基于当前真实时钟 now，从 hyper_start_ms 起按
     *        超周期滚动起点到 now 所在时窗，abs_deadline = 滚动后起点 + (k+1)·deadline——
     *        执行快慢不定时 deadline 始终对齐真实时间轴（供 priority_updater/将来 EDF 消费），
     *        不再依赖回绕副作用。超周期内幂等；无超周期（hyper_period_ms<=0）不滚动（保持
     *        expand 设置值）。
     * @retval 无
     */
    void update_abs_deadline() {
      const float now = fins::util::now_ms();
      const float period = hyper_period_ms;
      dag.for_each_vertex([&](const std::string &, Workload &v) {
        if (!v.job) return;
        float start = hyper_start_ms;
        if (period > 0 && now > start + period)
          start += std::floor((now - start) / period) * period;
        v.ddl = start + (float)(v.k + 1) * v.deadline;
      });
    }
  };
  inline PrecedenceGraph graph_g;

  // 其他全局单例（PluginLoader/RPCListener/ThreadPool）：其 hpp 均 include g_state 拿全局对象，
  // 故反向 include 会成循环依赖——此处不定义，由业务代码在运行时 .instance() 初始化。

} // namespace fins::rt
