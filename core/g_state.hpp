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
#include <fstream> // FINS_EXPORT_DAG_PATH 导出 dag JSON 用
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <numeric>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>
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
  struct Workload;
  struct PrecedenceGraph;

  /// 硬件状态监控，由监控器更新
  inline std::vector<float> core_usages_g{};
  inline std::atomic<float> mem_usage_g{};
  /// 外部回调槽（命名空间级 inline，装配点直接赋值注入；main 主线程调度循环每轮持 mtx 集中更新）。
  inline std::function<double(std::deque<double>)> wcet_updater = nullptr;
  /// 外部回调槽（命名空间级 inline，装配点直接赋值注入；优先级唯一来源：键函数 → 顶点调度优先级，
  /// grab 决策点现算；nullptr = 就绪堆退化为纯 FIFO——优先级不可预设，须经此函数拿到）。
  inline std::function<int(util::DirectedAcyclicGraph<Workload, Message> &, const Workload &)> priority_updater =
      nullptr;
  /// 外部回调槽（求makespan）ms
  inline std::function<double(util::DirectedAcyclicGraph<Workload, Message> &)> makespan_updater = nullptr;

  /** @brief .so 加载上下文（library_g.so_ctx 的元素）：构造=dlopen 装载 + dlsym 解析 C 工厂符号并
   *  填 loaded_keys，析构=dlclose 卸载，take_keys() 取定位键；装配点经 on_library_* 回调维护表。 */
  struct Plugin {
    void *handle = nullptr;
    std::string so_path;
    std::vector<std::string> loaded_keys; // 本 so 产出的算法 key（删除时按 so 取走）

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
    explicit Plugin(const std::string &path) {
      so_path = path;
      handle = dlopen(path.c_str(), RTLD_NOW | RTLD_LOCAL);
      if (!handle)
        throw std::runtime_error(dlerror());
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
        if (handle)
          dlclose(handle); // 构造失败清理，防 handle 泄漏（构造抛 → 析构不调用）
        handle = nullptr;
        throw;
      }
    }

    /** @brief 析构 = 物理卸载：dlclose 释放句柄。**引用计数归零才触发**——算法实例删除器持
     *  shared_ptr<Plugin> 保活，实例未全销毁期间库不卸载（保活语义见 expand_hp()）。
     * @retval 无
     */
    ~Plugin() {
      if (handle)
        dlclose(handle);
      handle = nullptr;
    }

    /** @brief 取走并清空本 so 的算法定位键（删除/替换路径装配点调用——析构不能返回值，
     *  单独保留供删除时回收 [name:version] 定位键）。
     * @retval std::vector<std::string> 本 so 的 [name:version] 键列表（move 走，原成员已清空）
     */
    std::vector<std::string> take_keys() { return std::move(loaded_keys); }
  };
  /** @brief so 上下文表（library_g）：[so_path] → Plugin，唯一算法定位数据源；装配点回调维护。 */
  struct Library {
    util::TBBMap<std::shared_ptr<Plugin>> so_ctx;

    /** @brief 取全部已注册算法定位键（[name]:[version]，跨所有 so 的 loaded_keys 聚合去重），
     *  与 Pipeline::algo_keys() 配套供装配点做算法就绪检查（expand_hp 前比对）。
     * @retval std::set<std::string> 全部已注册算法键（去重）
     */
    [[nodiscard]] std::set<std::string> algo_keys() const {
      std::set<std::string> keys;
      for (const auto &val: so_ctx | std::views::values)
        for (const auto &k: val->loaded_keys)
          keys.insert(k);
      return keys;
    }
  };
  inline Library library_g;

  /** @brief 节点解析态（Pipeline 内嵌，parse_pipeline 产物）：纯数据，字段含 id/name/version、
   *  端口名数组、config_keys/config_cache、hist/event、period/wcet/deadline。 */
  struct NodeInfo {
    std::string id; // 节点在图中的唯一标识（顶点名 id:{k} 前缀）
    std::string name; // 算法名（[name:version] = so 表定位键）
    std::string version; // 算法版本（定位键）

    double period{0}; // 执行周期（ms；>0 = 时间触发。0 = 未声明 period → 必须声明 event，
                      // 见 check_topology ⑦ 二选一）
    double deadline{0}; // 相对截止期（ms；缺省 0 = 未声明，排序中视为最紧急）
    double wcet{1}; // 最坏执行时间（ms；缺省 1）

    size_t exec_cap{0}; // **预留字段**（JSON `cap`）：本节点算法保留多少次 execute 耗时样本
                        // （0 = 未声明 → 图侧缺省 100）。对应图侧 exec_hist_cap（键 = **算法名**，
                        // 同算法多节点共享一份历史），当前**未接线**：解析/校验已就绪，
                        // 接入方式见 docs/pipeline_json_schema.md

    std::vector<std::string> input_ports;
    std::vector<std::string> output_ports;
    std::vector<nlohmann::json> config_cache;
    std::vector<std::string> config_keys; // configs 的键名（c{节点序号}_{配置序号}，与数据端口 p{i}_{j} 同构；
                                          // 本类只验"格式+序号后缀"，节点序号部分由 check_topology ⑦ 校验）

    std::map<std::string, size_t> hist; // hist 端口 → 窗口长度 N（触发时读该字段缓存最近 N 帧；语义合法性——键 ∈ inputs
                                        // / N>2 / 不与 event 重叠——由 check_topology ④ 审查）

    std::map<std::string, size_t> event; // event 端口 → 抽稀倍数 N（**声明 event 即事件触发**：不产生 tp 时间点，
                                         // 由 event 端口的 producer 完成事件释放；N = 支配节点每产出 N 帧本节点触发
                                         // 1 次 → 虚拟周期 = N × 支配节点周期。多个 event 端口时虚拟周期最小者 =
                                         // 支配端口，只有它计入就绪等待（阻塞边），其余端口建边但不参与等齐。
                                         // 语义合法性——键 ∈ inputs / N ≥ 1 / 不与 hist 重叠——由 check_topology ⑥ 审查）

    /** @brief 该节点的某输入端口是否建数据绑定边（绑定边 = 前序依赖 = 触发来源）。四处判定共用
     *  本函数：Pipeline::check_topology ⑤ 环检测、build_port_index 消费者索引、build_edge 建边、
     *  bind_job 段 1 历史槽登记。规则（2026-09-19 拍板）：时间触发（period>0）节点输入全部为
     *  历史槽取样读、恒无边；事件触发节点只在 event 端口上建边（触发源），hist 端口与其余输入
     *  为历史槽读。触发模式二选一由 check_topology ⑦ 保证（period>0 ⟺ event 为空）。
     * @param pn 输入端口名
     * @retval true = 建绑定边（阻塞或非阻塞，见 build_edge）
     */
    [[nodiscard]] bool port_has_edge(const std::string &pn) const {
      return period <= 0 && event.count(pn) > 0;
    }

    /** @brief 逐节点自解析：全部结构校验 + 字段抽取（Pipeline::parse 只拆封顶层后逐个调用
     *  本构造器）。configs 为**位置式取值表** config_cache——每项 {c<下标>: 值}，只取值、名字丢弃
     *  （顺序 = configs 数组元素顺序 = AlgoFunc 配置段相对序号，见 algo_func.hpp 头注释顺序保证链）。
     *  字段顺序约定（生成器按此写出，解析不依赖顺序）：
     *    id / name / version                      标识
     *    configs / inputs / outputs               配置 + 端口（三者连着）
     *    hist                                     窗口读声明
     *    event | period                           触发模式（二选一，见 check_topology ⑦）
     *    wcet / deadline / cap                    可有可无的属性
     * @param n 节点 JSON 对象（必填 id/name/version；可选 configs/inputs/outputs/wcet/deadline/hist/cap，
     *          以及 timer 的 period、event 的 event）。字段全表与语义见 docs/pipeline_json_schema.md
     * @param at 错误定位上下文串（如 "nodes[i]."，错误消息前缀用）
     * @retval 无（格式违反抛 std::invalid_argument）
     */
    NodeInfo(const nlohmann::json &n, const std::string &at) {
      if (!n.is_object())
        throw std::invalid_argument("[parse_dataflow] " + at + "须为对象");
      if (!n.contains("name") || !n["name"].is_string())
        throw std::invalid_argument("[parse_dataflow] " + at + "name 必填 string");
      if (!n.contains("version") || !n["version"].is_string())
        throw std::invalid_argument("[parse_dataflow] " + at + "version 必填 string");
      if (!n.contains("id") || !n["id"].is_string())
        throw std::invalid_argument("[parse_dataflow] " + at + "id 必填 string");
      // 旧字段名 parameters → configs（2026-09-19 更名+改形），命中即拒并指明改法
      if (n.contains("parameters"))
        throw std::invalid_argument("[parse_dataflow] " + at +
                                    "parameters 已更名为 configs，且形态改为 [{c{i}_0: 值}, {c{i}_1: 值}, ...]"
                                    "（i = 本节点序号，与输出端口 p{i}_{j} 同构）");
      // configs：位置式取值表 config_cache —— 每项为单键对象，键 = 顺序编号 c0/c1/...（"端口依次赋值"，
      // 键必须与下标一致，写错即拒 → 配置自校验）；只取 value 入表，名字丢弃（顺序 = AlgoFunc 配置段序号）
      if (n.contains("configs")) {
        if (!n["configs"].is_array())
          throw std::invalid_argument("[parse_dataflow] " + at + "configs 须为数组 [{c0: 值}, {c1: 值}, ...]");
        for (size_t j = 0; j < n["configs"].size(); ++j) {
          const auto &p = n["configs"][j];
          const std::string atC = at + "configs[" + std::to_string(j) + "]";
          if (!p.is_object() || p.size() != 1)
            throw std::invalid_argument("[parse_dataflow] " + atC + " 须为单键对象 {c" + std::to_string(j) + ": 值}");
          // 键形 = c{节点序号}_{配置序号}，与数据端口 p{i}_{j} 同构（节点序号部分在 check_topology ⑦ 校验）
          const std::string key = p.begin().key();
          const std::string suffix = "_" + std::to_string(j);
          if (key.size() < 2 || key[0] != 'c' || key.compare(key.size() - suffix.size(), suffix.size(), suffix) != 0)
            throw std::invalid_argument("[parse_dataflow] " + atC + " 的键须形如 'c{节点序号}_" + std::to_string(j) +
                                        "'（与数据端口 p{i}_{j} 同构、按序编号），收到 '" + key + "'");
          config_keys.push_back(key);
          config_cache.push_back(p.begin().value()); // 位置式值表（值顺序 = 配置段序号）
        }
      }
      // 端口名数组：inputs/outputs 为 string 数组（顺序 = AlgoFunc 参数顺序），同名端口直连
      for (const char *f: {"inputs", "outputs"}) {
        if (!n.contains(f))
          continue;
        if (!n[f].is_array())
          throw std::invalid_argument("[parse_dataflow] " + at + f + " 须为 string 数组");
        for (size_t j = 0; j < n[f].size(); ++j)
          if (!n[f][j].is_string())
            throw std::invalid_argument("[parse_dataflow] " + at + std::string(f) + "[" + std::to_string(j) +
                                        "] 元素须为 string（端口名）");
      }
      for (const char *f: {"wcet", "deadline", "period", "cap"}) {
        if (n.contains(f) && !n[f].is_number())
          throw std::invalid_argument("[parse_dataflow] " + at + f + " 须为 number");
      }
      if (n.contains("cap")) { // 预留字段：正整数的 execute 耗时样本保留条数（详见成员声明处注释）
        const double d = n["cap"].get<double>();
        if (d < 1.0 || d != std::floor(d))
          throw std::invalid_argument("[parse_dataflow] " + at + "cap 须为正整数（保留的 execute 耗时样本条数）");
        exec_cap = static_cast<size_t>(d);
      }
      // 源头约束（无输入节点必填 period）属图结构合法性，由第二级 Pipeline::check_topology 审查
      // （本构造器只做逐节点 json 格式校验，不查跨节点图结构）。

      // ── 字段抽取（图侧/运行时不再接触原始 JSON）──
      id = n["id"].get<std::string>();
      name = n["name"].get<std::string>();
      version = n["version"].get<std::string>();
      period = n.contains("period") ? n["period"].get<double>() : 0.0;
      wcet = n.contains("wcet") ? n["wcet"].get<double>() : 1.0;
      // 缺省 0 = 未声明（框架不替用户预设）：0 参与排序即"最紧急"——EDF 用 (ddl − now) 升序，
      // ddl = 滚动起点 + (k+1)·deadline（update_abs_deadline），deadline=0 → ddl 最小 → 最先到期。
      // 注意：不显式写 deadline 的节点 ddl 全等于滚动起点，彼此同权（EDF 退化为就绪序）；
      // 要靠 EDF 区分先后就得在 JSON 里显式写 deadline。
      deadline = n.contains("deadline") ? n["deadline"].get<double>() : 0.0;

      if (n.contains("inputs") && n["inputs"].is_array())
        input_ports = n["inputs"].get<std::vector<std::string>>();
      if (n.contains("outputs") && n["outputs"].is_array())
        output_ports = n["outputs"].get<std::vector<std::string>>();
      // hist / event 同形：[{端口: 正整数}, ...]（每元素单键对象；同端口不得重复声明）。
      // 本构造器只做格式校验，语义合法性（键 ∈ inputs / 节点类型 / N 范围 / hist∩event）统一由
      // check_topology ④⑥ 审查。
      auto parse_port_map = [&](const char *field, std::map<std::string, size_t> &dst) {
        if (!n.contains(field))
          return;
        const auto &arr = n[field];
        if (!arr.is_array())
          throw std::invalid_argument(std::string("[parse_dataflow] ") + at + field +
                                      " 须为数组 [{端口名: 正整数}, ...]");
        for (size_t e = 0; e < arr.size(); ++e) {
          const auto &el = arr[e];
          const std::string atEl = at + field + "[" + std::to_string(e) + "]";
          if (!el.is_object() || el.size() != 1)
            throw std::invalid_argument("[parse_dataflow] " + atEl + " 须为单键对象（键 = 输入端口名）");
          auto it = el.items().begin();
          const std::string port = it.key();
          const auto &arg = it.value();
          if (!arg.is_number())
            throw std::invalid_argument("[parse_dataflow] " + atEl + " 的值须为 number（正整数）");
          const double d = arg.get<double>();
          if (d < 1.0 || d != std::floor(d))
            throw std::invalid_argument("[parse_dataflow] " + atEl + " 的值须为正整数（收到 " + std::to_string(d) + "）");
          if (!dst.emplace(port, static_cast<size_t>(d)).second)
            throw std::invalid_argument("[parse_dataflow] " + atEl + " 端口 '" + port + "' 重复声明");
        }
      };
      parse_port_map("hist", hist);
      parse_port_map("event", event);
    }
  };
  /** @brief Pipeline — dataflow 配置（解析态；全局单份 pipeline_g）：cache = 原始配置 JSON 双缓冲
   *  （RPC 写缓冲份不解析 → pending → 主线程调度循环图静止时 commit + parse_pipeline 填 nodes），
   *  标准形式 = 节点对象数组（id/name/version 必填 + configs/inputs/outputs/wcet/deadline/hist，
   *  hist 可选），违反抛 std::invalid_argument；显式周期节点输入经 message_hist_ 字段缓存取样（hist 窗口/最新标量）。
   */
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
      if (script.is_null())
        return; // 空配置 → 空表（expand_hp() 幂等）
      if (script.is_array()) {
        raw_nodes = script.get<std::vector<nlohmann::json>>();
      } else if (script.is_object() && script.contains("nodes") && script["nodes"].is_array()) {
        raw_nodes = script["nodes"].get<std::vector<nlohmann::json>>();
      } else if (script.is_object() && script.contains("name")) {
        raw_nodes.push_back(script);
      } else {
        throw std::invalid_argument("[parse_dataflow] dataflow 顶层须为 array / {nodes:[...]} / 单节点对象");
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
     *  ③ 孤立输入：每个输入端口（含显式周期节点的窗口输入/自反馈字段）须有生产者；hist 指向无人
     *     产出的字段 = 悬空窗口，同样拒绝；
     *  ④ hist 语义合法性（唯一审查点）：hist 键须在本节点 inputs 中、窗口长度 N>2、不得与 event 键
     *     重叠，且节点须为时间触发（period>0）或事件触发（声明了 event）——既无 period 又无 event 的
     *     节点输入全是绑定边、没有字段历史槽可读，声明 hist 无意义；
     *  ⑤ 数据驱动前序边无环：建绑定边的输入（即 event 端口——时间触发节点恒无边）构成
     *     producer→consumer 前序边，该子图须无环——事件链互相喂会令调度永久挂起
     *     （pred_left 永不归零），故直接拒绝。
     *  ⑥ event 语义合法性（唯一审查点）：event 键须在本节点 inputs 中、抽稀倍数 N ≥ 1、不得与 hist
     *     键重叠。虚拟周期 = min over event 端口 (N × 支配节点周期)；不整除标称 HP 时由
     *     build_dominance **拓宽超周期**容纳（不再拒绝），此处只查逐节点内的静态合法性。
     *  ⑦ 触发模式二选一（**不支持隐式事件节点**）：period>0 ⟺ 时间触发、event 非空 ⟺ 事件触发，
     *     两者必须恰好声明其一（period 会静默压过 event；都无则节点静默不跑）。
     *  ⑧ configs 键形：每项 {c{节点序号}_{配置序号}: 值}，与数据端口 p{i}_{j} 同构；节点序号须等于
     *     本节点下标（配置被搬错节点/节点顺序被改时直接暴露）。
     * @retval 无（违反抛 std::invalid_argument）
     */
    void check_topology() const {
      std::map<std::string, std::vector<size_t>> producers;
      for (size_t i = 0; i < nodes.size(); ++i)
        for (const auto &pn: nodes[i].output_ports)
          producers[pn].push_back(i);
      for (const auto &[P, ps]: producers)
        if (ps.size() > 1)
          throw std::invalid_argument("[check_topology] 输出端口 '" + P + "' 有多个生产者（单写者约束），非法配置");
      for (size_t i = 0; i < nodes.size(); ++i)
        if (nodes[i].input_ports.empty() && nodes[i].period <= 0)
          throw std::invalid_argument("[check_topology] nodes[" + std::to_string(i) +
                                      "] 无输入节点无上游驱动，须时间触发（period>0）");
      // ③ 孤立输入拒绝：每个输入端口（含显式周期节点的窗口输入/自反馈字段）须有生产者（数据流边
      //    只来自节点输出；无生产者 → 该节点退化为“伪根”——无 tp 释放点、虚拟周期定不出来、expand 后只跑一次且
      //    永不重放，还会让周期兄弟因它永不完成而无法翻页）。周期节点自反馈（hist 字段 = 自身输出）
      //    经自身 output 入 producers 而通过；hist 指向无人产出的字段 = 悬空窗口 → 拒绝。
      for (size_t i = 0; i < nodes.size(); ++i)
        for (const auto &pn: nodes[i].input_ports)
          if (!producers.count(pn))
            throw std::invalid_argument("[check_topology] nodes[" + std::to_string(i) + "] 输入端口 '" + pn +
                                        "' 无生产者（孤立输入），非法配置");
      // ④ hist / ⑥ event 语义合法性（唯一审查点）：键须在本节点 inputs 中声明、hist 窗口长度 N>2
      //    （N>2 = 真多帧窗口；读最新单帧请勿声明 hist）、event 抽稀倍数 N ≥ 1、两者键不得重叠
      //    （hist = 窗口读、event = 触发源，同一端口语义互斥），且声明 hist 的节点须 timed 或 event
      //    （既无 period 又无 event 的节点输入全是绑定边，没有字段历史槽可读）。NodeInfo 解析只做
      //    结构校验，语义统一在此审查。
      for (size_t i = 0; i < nodes.size(); ++i) {
        const auto &ni = nodes[i];
        const std::string at = "[check_topology] nodes[" + std::to_string(i) + "]";
        auto in_inputs = [&](const std::string &pn) {
          return std::find(ni.input_ports.begin(), ni.input_ports.end(), pn) != ni.input_ports.end();
        };
        if (!ni.hist.empty() && ni.period <= 0 && ni.event.empty())
          throw std::invalid_argument(at + " 声明 hist 的节点须为时间触发（period>0）或事件触发（声明 event 端口）");
        for (const auto &[pn, N]: ni.hist) {
          if (!in_inputs(pn))
            throw std::invalid_argument(at + " hist 端口 '" + pn + "' 须在本节点 inputs 中声明");
          if (N <= 2)
            throw std::invalid_argument(at + " hist 端口 '" + pn + "' 窗口长度 N 须大于 2");
          if (ni.event.count(pn))
            throw std::invalid_argument(at + " 端口 '" + pn + "' 不得同时声明 hist 与 event（窗口读与触发源互斥）");
        }
        for (const auto &[pn, N]: ni.event) {
          if (!in_inputs(pn))
            throw std::invalid_argument(at + " event 端口 '" + pn + "' 须在本节点 inputs 中声明");
          if (N < 1)
            throw std::invalid_argument(at + " event 端口 '" + pn + "' 抽稀倍数 N 须 ≥ 1");
        }
        // ⑦ 触发模式二选一（**判据就是字段本身，无独立 type 字段**；不支持隐式事件节点）：
        //    period>0 ⟺ 时间触发、event 非空 ⟺ 事件触发。两者都写时 period 会静默压过 event
        //    （port_has_edge 对 period>0 恒返回 false）；都不写则触发方式只能靠运行时猜
        //    （既无 tp 释放点也无绑定边 → 节点静默不跑）。故必须恰好其一。
        const bool has_period = ni.period > 0;
        const bool has_event = !ni.event.empty();
        if (has_period && has_event)
          throw std::invalid_argument(at + " period 与 event 互斥（二选一触发模式），不得同时声明");
        if (!has_period && !has_event)
          throw std::invalid_argument(at + " 须声明触发模式之一：period（时间触发）或 event（事件触发）");
        // ⑧ configs 键的"节点序号"须 = 本节点下标（c{i}_{j} 与 p{i}_{j} 同一编号体系；
        //    节点顺序被改动 / 配置被搬错节点时直接暴露，而不是静默按位置注入错值）
        for (size_t j = 0; j < ni.config_keys.size(); ++j) {
          const std::string want = "c" + std::to_string(i) + "_" + std::to_string(j);
          if (ni.config_keys[j] != want)
            throw std::invalid_argument(at + " configs[" + std::to_string(j) + "] 键应为 '" + want +
                                        "'（c{节点序号}_{配置序号}，与输出端口 p{i}_{j} 同构），收到 '" +
                                        ni.config_keys[j] + "'");
        }
      }
      // ⑤ 数据驱动前序边无环：只有建绑定边的输入端口构成 producer→consumer 前序边（判定统一走
      //    port_has_edge：时间触发节点恒无边；事件触发节点仅 event 端口——hist
      //    端口与"最新单帧读"端口都不是前序边）。Kahn 拓扑检测残余 = 环（含依赖环而未归零的节点）
      //    → 拒绝，避免调度 pred_left 永不归零而永久挂起。
      {
        std::vector<int> indeg(nodes.size(), 0);
        std::vector<std::vector<size_t>> outs(nodes.size()); // producer idx → consumer idx
        for (size_t c = 0; c < nodes.size(); ++c) {
          for (const auto &pn: nodes[c].input_ports) {
            if (!nodes[c].port_has_edge(pn))
              continue; // 无绑定边的输入（hist 窗口读 / 最新单帧读）不是前序边
            auto it = producers.find(pn);
            if (it == producers.end())
              continue; // 无 producer（③ 已拒，防御）
            for (const size_t p: it->second) {
              outs[p].push_back(c);
              ++indeg[c];
            } // 单写者 → 至多一条
          }
        }
        std::vector<size_t> q;
        for (size_t i = 0; i < nodes.size(); ++i)
          if (indeg[i] == 0)
            q.push_back(i);
        size_t seen = 0;
        for (size_t h = 0; h < q.size(); ++h) {
          ++seen;
          for (const size_t c: outs[q[h]])
            if (--indeg[c] == 0)
              q.push_back(c);
        }
        if (seen != nodes.size()) {
          std::string ids;
          bool first = true;
          for (size_t i = 0; i < nodes.size(); ++i)
            if (indeg[i] > 0) {
              if (!first)
                ids += ", ";
              ids += nodes[i].id;
              first = false;
            }
          throw std::invalid_argument(
              "[check_topology] 数据驱动前序边存在环（事件节点互相喂，无 hist），环内/依赖环的节点: " + ids);
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
      for (const auto &node: nodes)
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
    std::string id{}; // 顶点名（格式 {节点id}:{k}，如 cam:0/cam:1）——expand_hp ⑥ 建顶点时填 vtx（同 dag 的 map 键）；
    std::string
        name{}; // 节点名（来自 Pipeline::NodeInfo.name；区别于 id 顶点名 = {name}:{k}）——装配点/测试按节点名识别
    size_t k{0}; // 超周期内实例序号（expand_hp ⑥ 建顶点填；update_abs_deadline 滚动校正用）

    double period{0};
    double deadline{0}; // 相对截止期（ms；缺省 0 = 未声明，排序中视为最紧急）

    double ddl{0}; // 绝对截止期（ms；滚动排期 = 主线程事件驱动 update_abs_deadline 按当前
    double wcet{1}; // 最坏执行时间（ms；缺省 1）

    std::function<void()> job; // 执行体（闭包捕获实例 + 预解析绑定边引用，运行时零查找取帧/发布）
  };

  /// 一次 execute 耗时样本（exec_us_hist_ 元素）：us 用时 + 完成时间戳（排序键，保留最新）
  struct ExecSample {
    double us{0}; // 执行耗时（us）
    double ts{0}; // 完成时间戳（util::now_us；队列按 ts 升序）
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
    util::DirectedAcyclicGraph<Workload, Message> dag; // 顶点带权、边=Message 槽

    double hyper_period_ms{0}; // 超周期长度（ms）
    double hyper_start_ms{0}; // 当前超周期起点（ms；expand 初始化 = 当前真实时钟、rollover_hp 回绕更新 = 当前真实时钟）
    uint64_t graph_version{0}; // 图结构版本号（expand_hp 重建后 ++；main 线程持 mtx 写读）。

    /** @brief 记录一帧到历史滑动窗口数据槽（满丢最旧；TBBMap accessor 按端口锁，只锁本端口
     *  历史槽字段）。历史槽 = 显式周期节点取样的 producer 输出字段缓存（hist 窗口/最新标量；容量 mesg_hist_cap = 读者
     * max（hist N 或 1））。 追加按 Message.timestamp（采集时间戳，pub 时置 now_us）升序插入，队列恒按时间有序，尽量
     *  保留最新数据——乱序完成的旧帧插到前面、满 cap 丢最旧（ts 最小）。
     * @param id 输出端口名（历史槽键）
     * @param mesg 数据帧（Message）
     * @retval 无
     */
    void record_mesg(const std::string &id, const Message &mesg) {
      TBBMAP_UPDATE(message_hist_, id, [&](auto &q) { // 无则默认构造插入、有则定位（持写锁，仅本端口）
        const size_t cap = mesg_hist_cap.count(id)
                               ? mesg_hist_cap.at(id)
                               : 100; // 缺省 100（同 record_exec；勿 operator[]→0，否则 pop_front 空 deque = UB）
        // 按采集时间戳升序插入（乱序完成的旧帧放前面）；满 cap 从最旧（ts 最小）丢，保留最新。
        const auto it = std::lower_bound(q.begin(), q.end(), mesg,
                                         [](const Message &a, const Message &b) { return a.timestamp < b.timestamp; });
        q.insert(it, mesg);
        while (q.size() > cap)
          q.pop_front(); // 满丢最旧（cap 运行时只读，调用方已 guard >0）
      });
    }
    std::map<std::string, size_t> mesg_hist_cap{}; // 字段历史缓存保留容量（expand_hp 填充：被显式周期节点取样的字段 →
                                                   // 读者 max（hist N 或 1）；重建时清空重算；运行时只读无并发写）
    util::TBBMap<std::deque<Message>>
        message_hist_; // 运行时：输出端口名 → 最近 mesg_hist_cap 帧滑动窗口（周期节点窗口读的字段历史槽；跨重建保留）

    /** @brief 记录一次 execute 耗时到节点环形队列（按完成时间戳升序插入、满 cap 丢最旧——保留最新；
     *  TBBMap accessor 按节点锁，只锁本节点字段）。
     * @param id 节点 id
     * @param us 执行耗时（us，steady_clock 计时）
     * @retval 无
     */
    void record_exec(const std::string &id, double us) {
      const double ts = fins::util::now_us(); // 完成时间戳（排序键）
      TBBMAP_UPDATE(exec_us_hist_, id, [&](auto &q) { // 无则默认构造插入、有则定位（持写锁，仅本节点）
        const size_t cap = exec_hist_cap.count(id)
                               ? exec_hist_cap.at(id)
                               : 100; // 缺省 100（未填充时勿取 operator[]→0，否则 pop_front 空 deque = UB）
        // 按完成时间戳升序插入（乱序完成的旧样本插前面）；满 cap 从最旧（ts 最小）丢，保留最新。
        const auto it =
            std::lower_bound(q.begin(), q.end(), ts, [](const ExecSample &a, double b) { return a.ts < b; });
        q.insert(it, ExecSample{us, ts});
        while (q.size() > cap)
          q.pop_front(); // 满丢最旧（环形语义）
      });
    }
    std::map<std::string, size_t> exec_hist_cap{}; // 环形队列容量（可配：每节点保留最近 N 次 execute 耗时；未配置节点由
                                                   // record_exec count/at 兜底缺省 100）
    util::TBBMap<std::deque<ExecSample>> exec_us_hist_; // 算法键 → 最近 execute 耗时样本（us + 完成 ts；按 ts 升序、满
                                                        // cap 丢最旧，保留最新；TBBMap accessor 按算法锁）

    // ── 调度状态公开成员（装配点直接读写：worker on_execute 回调 / main 主线程调度循环
    //    持 mtx 调用下述无锁原语；std::mutex 不可重入——持锁期间勿再 lock()，会死锁）──
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<bool> stopped{false};
    std::atomic<bool> pending{false};

  private:
    // ── 就绪增量调度状态（私有；持 mtx 访问，装配点经无锁原语间接使用）──
    std::map<std::string, size_t> pred_left_; // 剩余未完成前序数（含 seq/绑定/tp 挂靠边）
    std::map<std::string, size_t> in_degree_; // 入度基准（expand 填；rollover 重置 pred_left_ 用）

    // ── 非阻塞边记账（form 的 DAG 只管前序结构、不认识"阻塞"这种调度语义，故留在图侧）──
    //  非阻塞边 = event 声明的**非支配**端口：帧照样送到下游共享槽，但不构成"等齐"前序
    //  （消费者不因该前序未完成而不就绪）。建边时登记，build_pred 扣减、完成递减跳过。
    std::map<std::string, std::set<std::string>> nonblocking_in_; // 顶点 id → 非阻塞入边的 tag 集合
    std::map<std::string, std::vector<std::string>> blocking_succ_; // 顶点 id → 阻塞后继（按边重复，递减配平用）

    struct ReadyItem { // 就绪集元素：id + 入队序号 + 排序键
      std::string id;
      size_t seq; // 入队序号（全局递增；prio 相等时 seq 小者先出 = FIFO 精确）
      int prio; // 排序键（grab 前由 priority_updater 键函数现算，唯一来源；未注入恒 0 → 退化为纯 FIFO）
    };
    struct ReadyItemLess { // 就绪堆比较器（最大堆）：prio 高者在顶；相等 → seq 小者先出 = FIFO。全序。
      bool operator()(const ReadyItem &a, const ReadyItem &b) const {
        if (a.prio != b.prio)
          return a.prio < b.prio;
        return a.seq > b.seq;
      }
    };
    util::LazyMaxHeap<ReadyItem, ReadyItemLess> ready_; // 就绪堆（懒最大堆；push 只入队，grab 前 rebuild 后堆序成立）
    size_t ready_seq_{0}; // 入队序号（expand/rollover 时重置 0）

    std::vector<std::string> tp_order_; // 时间点释放顺序（pin_sync 按 offset 升序填全量 tp id；rollover 重放）
    size_t tp_released_{0}; // 游标：下一个待释放 tp 在 tp_order_ 的下标

    /** @brief ① 端口索引：输出/输入端口名 → 节点 + 一跳邻居（显式周期节点的输入跳过——窗口读，
     *  无绑定边/不构成拓扑依赖）。单写者约束已在 Pipeline::check_topology 校验（同名输出端口多 producer
     *  拒绝），这里只建索引供拓扑排序 / 支配周期查找用；先预建所有节点的一跳邻居条目（空集）→ ③ 拓扑序
     *  对 const map 用 .at() 安全。
     * @param nodes 解析态节点表（只读）
     * @param producers [out] 输出端口名 → producer 节点 id 列表
     * @param consumers [out] 输入端口名 → consumer 节点 id 列表（显式周期节点输入不构成消费者）
     * @param in_producers [out] 节点 id → 输入一跳邻居（producer 集）
     * @param out_consumers [out] 节点 id → 输出一跳邻居（consumer 集）
     * @retval 无
     */
    static void build_port_index(const std::vector<NodeInfo> &nodes,
                                 std::map<std::string, std::vector<std::string>> &producers,
                                 std::map<std::string, std::vector<std::string>> &consumers,
                                 std::map<std::string, std::set<std::string>> &in_producers,
                                 std::map<std::string, std::set<std::string>> &out_consumers) {
      for (const auto &info: nodes) {
        in_producers.emplace(info.id, std::set<std::string>{});
        out_consumers.emplace(info.id, std::set<std::string>{});
      }
      for (const auto &info: nodes) {
        for (const auto &pn: info.output_ports)
          producers[pn].push_back(info.id);
        for (const auto &pn: info.input_ports)
          if (info.port_has_edge(pn)) // 无绑定边的输入为历史槽取样读（窗口/最新单帧），不构成消费者
            consumers[pn].push_back(info.id);
      }
      for (const auto &info: nodes) {
        for (const auto &pn: info.input_ports)
          if (info.port_has_edge(pn)) // 无绑定边 → 不构成拓扑前序依赖
            for (const auto &p: producers[pn])
              in_producers[info.id].insert(p);
        for (const auto &pn: info.output_ports)
          for (const auto &cc: consumers[pn])
            out_consumers[info.id].insert(cc);
      }
    }

    /** @brief ② 超周期：全部显式周期节点的有理最小公倍（无周期节点 → 返回 0 不回绕）。
     *  **不做整数毫秒取整**：保留 period 实值（如 12.5/14.2857 = 100/n 这类 100 的精确子因子），
     *  从最大周期起步找能整除全部周期的最小公共周期 H = k·Tmax（判 H/T_i 近整数，浮点容差）。
     *  llround 会把 12.5→13、破坏 100 的整除关系，虚增超周期（12.5 本整除 100）→ 图永排不空 →
     *  is_hp_done 永不成立 → rollover 永不触发，故弃用整数 lcm。整除判定用容差吸收 T 的浮点
     *  表示误差；k 达上限仍不能整除（周期实质不可通约/病态）→ **警告**并回退整数毫秒 lcm
     *  （保守公共倍数，仍是合法超周期，仅可能偏大）。
     * @param nodes 解析态节点表（只读；只统计 info.period > 0 的节点）
     * @retval double 超周期长度（ms）；无周期节点返回 0
     */
    static double build_hyper_period(const std::vector<NodeInfo> &nodes) {
      long long hp = 1; // 整数毫秒 lcm 累乘
      bool any_periodic = false;
      for (const auto &info: nodes) {
        const double T = info.period; // ms（已由 parse 抽取）
        if (T <= 0)
          continue;
        any_periodic = true;
        const long long Ti = std::llround(T); // 就近取整毫秒
        if (std::abs(T - (double) Ti) > 1e-6) // 非整数毫秒：不拒绝，WARN + 就近取整继续
          FINS_LOG_WARN("[build_hyper_period] 节点 '{}' period 非整数毫秒: {}，就近取整为 {}ms", info.id, T, Ti);
        hp = std::lcm(hp, Ti); // 整数 lcm：gcd 恒整数，无浮点病态
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
    static std::vector<std::string>
    build_topo_order(const std::vector<NodeInfo> &nodes,
                     const std::map<std::string, std::set<std::string>> &in_producers,
                     const std::map<std::string, std::set<std::string>> &out_consumers) {
      std::vector<std::string> topo;
      std::map<std::string, size_t> indeg;
      std::deque<std::string> q;
      for (const auto &info: nodes) {
        indeg[info.id] = in_producers.at(info.id).size();
        if (in_producers.at(info.id).empty())
          q.push_back(info.id);
      }
      while (!q.empty()) {
        const std::string id = q.front();
        q.pop_front();
        topo.push_back(id);
        for (const auto &oc: out_consumers.at(id))
          if (--indeg[oc] == 0)
            q.push_back(oc);
      }
      for (const auto &info: nodes)
        if (std::find(topo.begin(), topo.end(), info.id) == topo.end())
          topo.push_back(info.id);
      return topo;
    }

    /** @brief ④ 实例化：为每个 NodeInfo 从 so 表（入参 so_ctx）构造具体算法实例（插件 C 工厂）。
     *  配置注入 = 遍历 info.config_cache 逐个 configure（位置式解码，顺序 = config
     *  "configs" 数组元素值顺序 = AlgoFunc 配置段相对序号）：AlgoFunc 解码写 configs_
     *  类型化帧（execute 零解析）。实例不替换 pipeline.nodes（NodeInfo 是纯数据）——全部
     *  job 实例共享 1 个（连续 job precedence 边保证串行），由局部 by_id 持有。
     * @param nodes 解析态节点表（只读）
     * @param so_ctx 算法定位表（[so_path] → Plugin；只读）
     * @param by_id [out] 节点 id → 具体算法实例
     * @param by_info [out] 节点 id → 解析态指针（只读别名）
     * @retval 无（[name:version] 未注册抛 std::runtime_error）
     */
    static void build_instances(const std::vector<NodeInfo> &nodes, const util::TBBMap<std::shared_ptr<Plugin>> &so_ctx,
                                std::map<std::string, std::shared_ptr<AlgoBase>> &by_id,
                                std::map<std::string, const NodeInfo *> &by_info) {
      for (const auto &info: nodes) {
        const std::string &nm = info.name;
        const std::string key = nm + ":" + info.version;

        // 遍历 so 表（显式入参 so_ctx）定位算法（[name:version] 落在哪个 so 的 loaded_keys）
        std::shared_ptr<AlgoBase> algo;
        std::shared_ptr<Plugin> pctx;
        for (const auto &[so_path, pc]: so_ctx) {
          bool found = false;
          for (const auto &k: pc->loaded_keys)
            if (k == key) {
              pctx = pc;
              found = true;
              break;
            }
          if (found)
            break;
        }
        if (!pctx)
          throw std::runtime_error("Unregistered algorithm name in map: " + key);

        // C 工厂实例化；shared_ptr 删除器持 ctx → 实例存活期间库不卸载
        algo = std::shared_ptr<AlgoBase>(pctx->create_algo(key.c_str()), [pctx](AlgoBase *p) {
          if (pctx->destroy_plugin && p)
            pctx->destroy_plugin(p);
        });

        // 配置注入：顺序 = info.config_cache（位置式值表，顺序 = config "configs" 数组元素
        // 值顺序 = AlgoFunc 配置段相对序号）——AlgoFunc 位置式解码写 configs_（execute 零解析）。
        for (const auto &v: info.config_cache)
          algo->configure("", v);

        by_id[info.id] = std::move(algo);
        by_info[info.id] = &info;
      }
    }

    /** @brief ⑤ 支配周期 + Replication：逐节点定最终执行周期 + job 实例数。三类节点的周期来源：
     *  · 显式 period（时间触发）→ 直接用，与前级无关；
     *  · 显式 event（事件触发）→ 虚拟周期 = min over event 端口 (抽稀倍数 N × 该端口 producer 周期)，
     *    取最小者即支配端口；抽稀倍数 N=2 表示支配节点每产出 2 帧本节点触发 1 次；
     *  两者都无 / 同时声明两者 → 由 check_topology ⑦ 拒绝（不支持隐式事件节点）。
     *  超周期：标称 HP = ② timer 节点周期的 lcm。timer 的周期本身整除标称 HP，但 event 的
     *  N×支配周期 未必整除它（如 N=4、50ms → 200ms vs 标称 100ms）→ **不拒绝，而是拓宽 HP** 到
     *  能整除全部最终周期的最小倍数（三步：定周期 → 拓宽 → 算实例数）。拓宽是乘性的，已获得的
     *  整除性不会再丢，故单趟收敛；tp 释放点与实例数一并按新 HP 展开，每个顶点每超周期仍恰跑一次。
     *  仅当拓宽超过 HP_WIDEN_MAX_FACTOR 倍才拒绝（抽稀倍数与标称 HP 不可通约的病态配置）。
     * @param topo 拓扑序节点 id 列表（只读；前级已定最终周期）
     * @param by_info 节点 id → 解析态指针（只读）
     * @param producers 输出端口名 → producer 节点 id 列表（只读；event 支配源查找用）
     * @param hyper_period [in,out] 超周期长度（ms）：入 = ② 的标称 HP，出 = 拓宽后 HP
     * @param period_final [out] 节点 id → 最终执行周期（ms）
     * @param node_count [out] 节点 id → job 实例数（拓宽后 HP/T，无周期 → 1）
     * @param domi_port [out] 声明 event 的节点 id → 支配端口名（build_edge 据此定阻塞边）
     * @retval 无（event 无可用前级周期 / 拓宽超上限 → 抛 std::invalid_argument）
     */
    /// 超周期拓宽上限：拓宽后 / 标称 > 此倍数即拒绝（防抽稀倍数病态导致静态展开爆掉）
    static constexpr double HP_WIDEN_MAX_FACTOR = 100.0;

    /** @brief 把 hp 拓宽到能被 T 整除的最小倍数（hp 本身已整除则原样返回）。
     *  与 ② build_hyper_period 同一浮点容差口径：判 (hp·m)/T 近整数。m 试到步数上限仍找不到
     *  （与 hp 不可通约的病态周期）→ 原样返回，由调用方的失控保护拒绝该配置。
     * @param hp 当前超周期（ms，>0）
     * @param T  待容纳的周期（ms，>0）
     * @retval double 拓宽后的超周期（≥ hp）
     */
    static double widen_to_multiple(const double hp, const double T) {
      if (hp <= 0 || T <= 0)
        return hp;
      for (long long m = 1; m <= 1024; ++m) {
        const double r = hp * static_cast<double>(m) / T;
        const long long k = std::llround(r);
        if (k >= 1 && std::abs(r - static_cast<double>(k)) <= 1e-6)
          return hp * static_cast<double>(m);
      }
      return hp;
    }

    static void build_dominance(const std::vector<std::string> &topo,
                                const std::map<std::string, const NodeInfo *> &by_info,
                                const std::map<std::string, std::vector<std::string>> &producers,
                                double &hyper_period, std::map<std::string, double> &period_final,
                                std::map<std::string, size_t> &node_count,
                                std::map<std::string, std::string> &domi_port) {
      for (const auto &id: topo) {
        const auto *info = by_info.at(id);
        double T = info->period; // timer 节点的周期（event 节点无 period → 0，走下面的虚拟周期分支）
        if (T <= 0 && !info->event.empty()) {
          // 事件触发（显式 event 声明）：虚拟周期 = min over event 端口 (抽稀倍数 N × 该端口 producer 周期)。
          // 取最小者 = 支配端口，其绑定边为阻塞边（其余 event 端口只传帧、不参与等齐，见 build_edge）；
          // 并列时按端口名字典序取首个（map 迭代序，确定性）。
          std::string best_port;
          double best = 0;
          for (const auto &[pn, thin]: info->event) {
            auto pit = producers.find(pn);
            if (pit == producers.end() || pit->second.empty())
              continue; // 孤立输入（③ 已拒，防御）
            const std::string &p = pit->second.front(); // 单写者约束 → 至多一个 producer
            if (!period_final.contains(p))
              continue;
            const double cand = static_cast<double>(thin) * period_final.at(p);
            if (best_port.empty() || cand < best) {
              best_port = pn;
              best = cand;
            }
          }
          if (best_port.empty())
            throw std::invalid_argument("[build_dominance] nodes[" + id +
                                        "] event 端口的无可用周期的前级（producer 须能定出周期：timer 自身，或可递归定周期的 event）");
          domi_port[id] = best_port;
          T = best;
        } else if (T <= 0) {
          // 既无 period 又无 event：check_topology ⑦ 已拒（此处为防御，正常不可达）
          throw std::invalid_argument("[build_dominance] nodes[" + id +
                                      "] 既无 period 也无 event（check_topology ⑦ 应已拒）");
        }
        period_final[id] = T;
      }

      // ── 第二趟：超周期拓宽 ──
      // 标称 HP（② 的 lcm）只由**显式周期**节点决定，而 event 的抽稀倍数会引入落在其整除格
      // 之外的周期（如 N=4、支配源 50ms → 200ms，而标称 HP=100ms）。此时**不拒绝**，而是把
      // 超周期延到能整除全部最终周期的最小倍数：多拍一次静态展开，语义依旧自洽（每个顶点
      // 每超周期恰执行一次，tp 释放点与实例数同步变多）。
      // 乘性拓宽是单调的：HP 一旦被某 T 整除，再乘整数不会失去该性质 → 单趟即收敛。
      const double hp_nominal = hyper_period;
      for (const auto &[id, T]: period_final) {
        if (T <= 0 || hyper_period <= 0)
          continue;
        const double widened = widen_to_multiple(hyper_period, T);
        if (widened > hyper_period)
          hyper_period = widened;
      }
      // 失控保护：抽稀倍数病态（如 N 极大且与 HP 不可通约）会让静态展开爆掉——设上限拒绝，
      // 而不是把内存耗光。正常抽稀（N ≤ 4）绝不会触及。
      if (hyper_period > hp_nominal * HP_WIDEN_MAX_FACTOR)
        throw std::invalid_argument(
            "[build_dominance] 超周期由标称 " + std::to_string(hp_nominal) + "ms 拓宽至 " +
            std::to_string(hyper_period) + "ms，超过 " + std::to_string(HP_WIDEN_MAX_FACTOR) +
            " 倍上限（event 抽稀倍数与标称 HP 不可通约）；请调整抽稀倍数");
      if (hyper_period > hp_nominal)
        FINS_LOG_WARN("[build_dominance] 超周期由标称 {}ms 拓宽至 {}ms（event 抽稀倍数引入了不整除标称 HP 的周期）",
                      hp_nominal, hyper_period);

      // ── 第三趟：实例数 = 拓宽后 HP / T ──
      // T > 0 恒成立：timer 必 period>0（⑦），event 的虚拟周期在第一趟取不到就直接抛了
      for (const auto &[id, T]: period_final) {
        node_count[id] = static_cast<size_t>(std::llround(hyper_period / T));
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
    static void build_vertex(util::DirectedAcyclicGraph<Workload, Message> &dag, const std::vector<NodeInfo> &nodes,
                             const std::map<std::string, double> &period_final,
                             const std::map<std::string, size_t> &node_count) {
      for (const auto &info: nodes) {
        const size_t N = node_count.at(info.id);
        for (size_t k = 0; k < N; ++k) {
          Workload v;
          v.k = k; // 实例序号（update_abs_deadline 滚动校正用）
          v.name = info.name; // 节点名（NodeInfo.name；id 顶点名 = {name}:{k}）
          v.period = period_final.at(info.id);
          v.deadline = info.deadline; // parse 缺省 0（未声明）
          v.wcet = info.wcet;
          dag.add_node(info.id + ":" + std::to_string(k), std::move(v));
        }
      }
    }

    /** @brief ⑦ 建边：同名端口直连绑定边（数据驱动 precedence）。规则（2026-09-02 拍板，2026-09-19
     *  加入 event 非阻塞边）：
     *  · 无 seq 连续边（全删）——任务实例不靠自身 A1→A2 前序串行，串行由释放时间点（显式周期）
     *    或数据源 job 完成（事件触发）传递；
     *  · 建边范围 = port_has_edge（时间触发节点恒无边；事件触发节点仅 event 端口）。
     *    未建边的输入由消费者执行时从 producer 输出端口历史槽取样读（hist 窗口 / 最新一帧），
     *    见 bind_job 段 1 注册 / pack_inputs；
     *  · producer 绑定式：输出端口 pn 的唯一 producer（单写者由 check_topology 保证），整数式
     *    pk=((k+1)·Np-1)/Nc 连 producer:{pk} → consumer:{k}（时段内最新已完成帧；同速率一一对应；
     *    快→慢绑末帧；慢→快共享帧；恒有边）；
     *  · 阻塞性：event 节点的**支配端口**（虚拟周期最小者）建阻塞边 = 唯一释放条件；同节点其余
     *    event 端口建非阻塞边（帧可见但不参与等齐）。其余节点的绑定边全部阻塞（同改造前）。
     * @param nodes 解析态节点表（只读）
     * @param node_count 节点 id → 实例数（只读）
     * @param domi_port 节点 id → 支配端口名（只读；仅声明了 event 的节点有条目）
     * @retval 无
     */
    /** @brief 加边 + 非阻塞记账（唯一建边入口）：dag 只存前序结构，非阻塞边在调度侧等价于
     *  "不计入 pred_left"——故阻塞边入 blocking_succ_（完成递减用），非阻塞边记入对端
     *  nonblocking_in_（build_pred 从入度里扣、export_dag 标 blocking=false）。
     * @param from 源顶点 id
     * @param to   目标顶点 id
     * @param tag  端口名（tp 挂靠边为 "time"）
     * @param blocking true（默认）= 前序边；false = 非阻塞边（只传帧）
     * @retval 无
     */
    void add_edge_(const std::string &from, const std::string &to, const std::string &tag,
                   const bool blocking = true) {
      dag.add_edge(from, to, tag, Message{});
      if (blocking)
        blocking_succ_[from].push_back(to);
      else
        nonblocking_in_[to].insert(tag);
    }

    void build_edge(const std::vector<NodeInfo> &nodes,
                    const std::map<std::string, size_t> &node_count,
                    const std::map<std::string, std::string> &domi_port) {
      std::map<std::string, std::string> producer_of; // 输出端口名 → producer 节点 id（首生产者，函数内局部）
      for (const auto &info: nodes)
        for (const auto &pn: info.output_ports)
          if (!producer_of.count(pn))
            producer_of[pn] = info.id;
      for (const auto &info: nodes) {
        const size_t Nc = node_count.at(info.id);
        // 支配端口：只对事件触发节点（port_has_edge 为真者必有 event 声明 → domi_port 必有条目）
        const std::string domi = info.event.empty() ? std::string{} : domi_port.at(info.id);
        for (const auto &pn: info.input_ports) {
          if (!info.port_has_edge(pn))
            continue; // 历史槽取样读的输入（时间触发节点全部输入 / 事件节点的 hist 与非 event 输入）：无边
          auto pit = producer_of.find(pn);
          if (pit == producer_of.end())
            continue; // 输入端口无生产者（孤立输入）→ 无边
          const std::string &p = pit->second;
          const size_t Np = node_count.at(p);
          // 阻塞性：事件触发节点的支配端口（虚拟周期最小者）阻塞——它才是释放条件；同节点其余
          // event 端口建非阻塞边（只把帧送到下游 Message 槽，不参与等齐；记账见 add_edge_）。
          const bool blocking = pn == domi;
          for (size_t k = 0; k < Nc; ++k) {
            const long long pk = ((long long) (k + 1) * (long long) Np - 1) / (long long) Nc;
            add_edge_(p + ":" + std::to_string(pk), info.id + ":" + std::to_string(k), pn, blocking);
          }
        }
      }
    }

    /** @brief ⑦.5 时间链：解析同步时间点（显式周期节点释放时刻并集）→ 建时间点顶点（job = 延迟
     *  （sleep_until 绝对释放时刻，timer 拿到 w 直接 job() 即实现延迟）、period=相对 hyper_start_ms
     *  的释放偏移）+ 挂靠边 tp:s → {id}:{k}（释放约束：时间点 Finished 任务才就绪）。仅显式周期
     *  时间触发节点（period>0）产生同步点并挂靠；事件触发节点（event 非空）仍纯数据流驱动。时间点按绝对释放时刻聚合
     *  ——多任务共享同一时间点（如两个 50ms 任务与一个 100ms 任务同时刻释放共用该点），锚定真实
     *  时钟消除旧 delay 的漂移。注意顺序：须在 build_edge 之后调用（其挂靠边引用的任务顶点已由
     *  build_vertex 建好、时间点顶点自建）。
     * @param nodes 解析态节点表（只读）
     * @param node_count 节点 id → 实例数（只读）
     * @retval 无
     */
    void bind_sync(const std::vector<NodeInfo> &nodes, const std::map<std::string, size_t> &node_count) {
      std::set<double> sync_points; // 同步点集合：显式周期节点释放时刻并集（去重升序）
      for (const auto &info: nodes) {
        if (info.period <= 0)
          continue;
        const size_t N = node_count.at(info.id);
        for (size_t k = 0; k < N; ++k)
          sync_points.insert((double) k * info.period);
      }

      std::map<double, std::string> tp_id; // 偏移 → 时间点顶点 id（序号化，无精度碰撞）
      size_t seq = 0;
      for (const double off: sync_points)
        tp_id[off] = "tp:" + std::to_string(seq++);

      for (const auto &[off, id]: tp_id) {
        tp_order_.push_back(id); // 升序 offset = 释放顺序（grab_delay_workload 按序取，与原 min-period 扫描等价）
      }

      double prev_off = 0.0; // 理论延迟基准：相对前一个同步点的间隔（首个 tp:0 = 0，立即释放）
      for (const auto &[off, id]: tp_id) {
        Workload v;
        v.id = id;
        v.name = "time";
        v.wcet = off - prev_off;
        // tp job：睡到释放时刻 hyper_start_ms + off。job 内实时读 hyper_start_ms → rollover
        // 重锚自动对齐。用绝对时刻睡眠（内核 hrtimer 精确唤醒），仅以 10ms 为上限分批——
        // 既保留停止响应（stopped 置位后最多 10ms 退出），又不把释放时刻量化掉：
        // 原 while(now<until) sleep_for(1ms) 每拍醒 ~100 次，且醒来必晚 0~1ms（实测释放偏差
        // 中位 505µs / 最大 1023µs，而 n5 自身 wcet 仅 193µs）。
        v.job = [this, off]() {
          while (!stopped.load()) {
            const auto now = std::chrono::steady_clock::now();
            const auto target =   // 目标释放时刻（已过则为过去时刻 → sleep_until 立即返回）
                now + std::chrono::microseconds(
                          (long long) ((hyper_start_ms + off - fins::util::now_ms()) * 1000.0));
            if (target <= now)
              break;
            std::this_thread::sleep_until(std::min(target, now + std::chrono::milliseconds(10)));
          }
        };
        dag.add_node(id, std::move(v));

        prev_off = off;
      }

      for (const auto &info: nodes) {
        if (info.period <= 0)
          continue;

        const size_t N = node_count.at(info.id);
        for (size_t k = 0; k < N; ++k) {
          const double off = (double) k * info.period;
          add_edge_(tp_id.at(off), info.id + ":" + std::to_string(k), "time");
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
      for (const auto &id: ids) {
        // 阻塞入度 = 全部入边 − 非阻塞入边（event 非支配端口：只传帧，不参与等齐）
        const auto nb = nonblocking_in_.find(id);
        const size_t deg = dag.in_nodes(id).size() - (nb == nonblocking_in_.end() ? 0 : nb->second.size());
        in_degree_[id] = deg;
        pred_left_[id] = deg;
      }
    }

    /**
     * @brief ⑧ 填 job 执行体闭包：按 NodeInfo 端口序打包输入/输出 array → AlgoBase execute →
     *        输出路由下游绑定边 + record_mesg 维护字段历史槽，运行时不再接触原始 JSON。
     *        闭包捕获稳定解析态（shared_ptr<const NodeInfo> 每节点 1 份按 k 共享）+ 算法实例 by_id；
     *        输入打包：事件节点逐输入端口取绑定边帧；显式周期（period>0）节点——hist 声明的端口读
     *        producer 字段历史槽最近 N 帧（线格式 vector<Message>，只拷句柄、payload 共享；
     *        插件侧声明 fins::rt::History<T> 只读视图接收，零 payload 拷贝，长度 ≤ N 不补空帧），
     *        未声明 hist 的输入读“最新一帧”原始类型。
     *        历史容量 mesg_hist_cap[输出端口] = 该字段全部周期读者的保留长度 max（hist N 或 1），
     *        只对被周期读的字段建历史槽；单写者约束保证每输出端口唯一 producer；message_hist_ 跨重建保留不清。
     * @param nodes 解析态节点表（只读）
     * @param by_id 节点 id → 算法实例（只读）
     * @param node_count 节点 id → job 实例数（只读）
     * @retval 无
     */
    void bind_job(const std::vector<NodeInfo> &nodes,
              const std::map<std::string, std::shared_ptr<AlgoBase>> &by_id,
              const std::map<std::string, size_t> &node_count) {
      // ── 段 1：历史容量表 mesg_hist_cap[输出端口]（记录条件 + 保留长度）──
      //  登记范围 = 所有"从字段历史槽取样读"的输入 = **无绑定边**的输入（时间触发节点全部输入；
      //  event 节点的 hist 端口与其余非 event 输入）。建绑定边的输入直接读边帧、无需历史槽。
      //  保留容量 = 该字段全部历史槽读者的 max（hist N 或 1）。同字段多读者共享槽。
      //  route_outputs 见 mesg_hist_cap 含该端口才 record_mesg，故须先在此登记。
      std::set<std::string> out_ports; // 全部节点输出端口名并集（判定“有 producer”）
      for (const auto &info: nodes)
        for (const auto &pn: info.output_ports)
          out_ports.insert(pn);

      for (const auto &info: nodes) {
        for (const auto &pn: info.input_ports) {
          if (info.port_has_edge(pn) || !out_ports.count(pn))
            continue; // 建边的输入读边帧；无 producer 字段（check_topology 已拒，防御）
          const size_t w = info.hist.count(pn) ? info.hist.at(pn) : 1; // hist N（窗口）或 1（最新单帧）
          auto &cap = mesg_hist_cap[pn];
          if (w > cap)
            cap = w; // 保留容量 = 读者 max（同字段多读者共享槽，保证不丢窗内最新帧）
        }
      }

      // ── 段 2：每个节点 → 每实例填 job 闭包（闭包捕获稳定解析态 + 算法实例 + hist 窗口长度表）──
      for (const auto &info: nodes) {
        auto sinfo = std::make_shared<const NodeInfo>(info); // 闭包捕获稳定共享解析态
        const auto &algo = by_id.at(info.id);
        const size_t n = node_count.at(info.id);
        const auto hist_w = info.hist; // hist 端口 → 窗口长度 N（>2；未声明的周期输入走“最新单帧”）

        for (size_t k = 0; k < n; ++k) {
          const std::string vtx = info.id + ":" + std::to_string(k); // 顶点名现拼（无 JobInst）

          dag.mutate_vertex(vtx, [this, sinfo, algo, vtx, hist_w](Workload &v) {
            v.id = vtx; // Workload.id 实际填充

            // 按 tag 分组一次性解析（O(边数) 替代 O(端口×边数) 的逐端口 edges_to/from）
            auto in_groups = dag.edges_to_grouped(vtx);
            auto out_groups = dag.edges_from_grouped(vtx);

            std::vector<std::vector<std::reference_wrapper<Message>>> in_refs(
                sinfo->input_ports.size());
            for (size_t i = 0; i < in_refs.size(); ++i) {
              // 建了绑定边的输入端口才在分组表里（单写者 → 至多一条）；无边的输入（历史槽取样读）
              // 留空桶 → pack_inputs 走 hist 窗口 / 最新一帧分支
              const auto it = in_groups.find(sinfo->input_ports[i]);
              if (it != in_groups.end())
                in_refs[i] = it->second;
            }

            std::vector<std::vector<std::reference_wrapper<Message>>> out_refs(
                sinfo->output_ports.size());
            for (size_t i = 0; i < out_refs.size(); ++i) {
              const auto it = out_groups.find(sinfo->output_ports[i]);
              if (it != out_groups.end())
                out_refs[i] = it->second;
            }

            v.job = [this, sinfo, algo, hist_w,
                     in_refs = std::move(in_refs),
                     out_refs = std::move(out_refs)]() {

              // ── 功能 1：周期节点输入取样 helper ──
              // collect_window：hist 端口 → 最近 w 帧（尾部 = 最新；不足 w 有多少给多少，不补空帧）
              auto collect_window = [this](const std::string &pn, size_t w)
                  -> std::vector<Message> {
                std::vector<Message> frames;
                frames.reserve(w);

                util::TBBMap<std::deque<Message>>::const_accessor a;
                if (message_hist_.find(a, pn)) {
                  const auto &h = a->second; // 按 timestamp 升序，尾部 = 最新真实帧
                  const size_t take = std::min(w, h.size());
                  for (size_t j = 0; j < take; ++j)
                    frames.push_back(h[h.size() - take + j]); // 拷贝 Message（shared_ptr 保活）
                }
                return frames;
              };

              // read_latest：未声明 hist 的周期输入 → 最新一帧；槽空 → 空 Message（插件侧需容错）
              auto read_latest = [this](const std::string &pn) -> Message {
                Message m;

                util::TBBMap<std::deque<Message>>::const_accessor a;
                if (message_hist_.find(a, pn) && !a->second.empty())
                  m = a->second.back();

                return m;
              };

              // ── 功能 2：按端口序打包输入 array ──
              auto pack_inputs = [this, sinfo, hist_w, collect_window, read_latest, &in_refs]() {
                std::vector<Message> inputs(sinfo->input_ports.size());
                for (size_t i = 0; i < inputs.size(); ++i) {
                  const std::string &pn = sinfo->input_ports[i];

                  // 按**端口**三选一（不再按节点类型分流）：hist 声明 → 窗口读；有绑定边 →
                  // 边帧；两者皆无 → 历史槽最新一帧。三者互斥由 check_topology ④⑥ 保证。
                  if (hist_w.count(pn)) {
                    // hist 端口：最近 N 帧窗口。线格式 = vector<Message>（只拷句柄、payload 共享），
                    // 插件侧由 AlgoFunc::decode_input_ 翻成 History<E> 只读视图
                    Message m;
                    *m.p_mutable<std::vector<Message>>() = collect_window(pn, hist_w.at(pn));
                    inputs[i] = std::move(m);
                  } else if (!in_refs[i].empty()) {
                    // 绑定边（event 端口）：预解析帧（单写者 → 至多一条）
                    inputs[i] = in_refs[i][0].get();
                  } else {
                    // 无绑定边且未声明 hist：字段历史槽最新一帧（空槽 → 空 Message，插件侧容错）
                    inputs[i] = read_latest(pn);
                  }
                }
                return inputs;
              };

              // ── 功能 3：execute + record_exec ──
              auto execute_and_time = [this, sinfo, algo](std::vector<Message> &inputs) {
                std::vector<Message> outputs(sinfo->output_ports.size());

                const auto _t0 = std::chrono::steady_clock::now();

                algo->execute(inputs, outputs);

                record_exec(sinfo->name,
                            std::chrono::duration<double, std::micro>(
                                std::chrono::steady_clock::now() - _t0).count());

                return outputs;
              };

              // ── 功能 4：路由输出 ──
              auto route_outputs = [this, sinfo, &out_refs](std::vector<Message> &outputs) {
                for (size_t i = 0; i < outputs.size(); ++i) {
                  for (auto &e: out_refs[i])
                    e.get() = outputs[i]; // 写全部下游绑定边（生产者消费者共享帧）
                  const std::string &pn = sinfo->output_ports[i];
                  const auto cap_it = mesg_hist_cap.find(pn); // 锁外执行：const 查找避 operator[] 并发写 UB
                  if (cap_it != mesg_hist_cap.end() && cap_it->second > 0)
                    record_mesg(pn, outputs[i]);
                }
              };

              auto inputs = pack_inputs();
              auto outputs = execute_and_time(inputs);
              route_outputs(outputs);
            };
          });
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
      for (const auto &id: ids)
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
      hyper_start_ms = fins::util::now_ms(); // 新配置展开起点 = 当前真实时钟（勿残留上次回绕后的起点）
      mesg_hist_cap.clear(); // 只清容量表（新配置重算）；message_hist_ 历史槽跨重建保留不清（同 exec_us_hist_）
      pred_left_.clear(); // 就绪增量状态：空配置早退也一致清空
      in_degree_.clear();
      nonblocking_in_.clear(); // 非阻塞边记账随图重建重算
      blocking_succ_.clear();
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

      const auto nodes = pipeline.nodes; // 入参快照（拷贝，防外部改）
      const auto so_ctx = library.so_ctx;
      if (nodes.empty()) {
        ++graph_version;
        return;
      } // 空配置 → 空图（幂等；结构已清空，同样发失效信号）

      // ① 端口索引：producers/consumers + 一跳邻居
      std::map<std::string, std::vector<std::string>> producers, consumers;
      std::map<std::string, std::set<std::string>> in_producers, out_consumers;
      build_port_index(nodes, producers, consumers, in_producers, out_consumers);

      // ② 超周期：标称 HP = lcm(显式周期节点)；event 抽稀倍数不整除它时由 ⑤ 拓宽
      hyper_period_ms = build_hyper_period(nodes);

      // ③ 拓扑序：BFS 从源展开
      const auto topo = build_topo_order(nodes, in_producers, out_consumers);

      // ④ 实例化：by_id/by_info
      std::map<std::string, std::shared_ptr<AlgoBase>> by_id;
      std::map<std::string, const NodeInfo *> by_info;
      build_instances(nodes, so_ctx, by_id, by_info);

      // ⑤ 支配周期 + 实例数（domi_port = 声明 event 的节点的支配端口，⑥ 定阻塞边用）
      std::map<std::string, double> period_final;
      std::map<std::string, size_t> node_count;
      std::map<std::string, std::string> domi_port;
      build_dominance(topo, by_info, producers, hyper_period_ms, period_final, node_count, domi_port);

      // ⑥ 建顶点 {id}:{k}
      build_vertex(dag, nodes, period_final, node_count);

      // ⑦ seq 连续边 + 绑定边
      build_edge(nodes, node_count, domi_port);

      // ⑦.5 时间链：同步时间点顶点 + 挂靠边（图编辑好后钉时间约束）
      bind_sync(nodes, node_count);

      // ⑨ 就绪增量初始化：入度基准（build_pred）→ 初始就绪（seed_ready，私有函数，见 bind_job 之后）
      build_pred();

      // ⑧ job 闭包
      bind_job(nodes, by_id, node_count);

      // ⑨b 初始就绪（seed_ready，私有函数，见 bind_job 之后）
      seed_ready();

      ++graph_version; // 结构重建完成 → makespan 结构缓存失效信号（须在全部建图步骤后）

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
     *        自动对齐新起点；字段历史窗口数据槽跨周期保留不清；abs_deadline 由 update_abs_deadline 滚动
     *        校正，非本函数职责。
     * @retval 无
     */
    void rollover_hp() {
      // 超周期起点推进到绝对网格上的下一未来边界（ceil(delta/H)·H；delta=距本拍起点已过 ms）。
      // 早完工（delta<H）→ +H：下一边界在完工之后，tp 睡到边界才放 → 周期任务不提前释放；
      // 过载（delta≥H）→ 见下：默认严格等下一拍；FINS_ROLLOVER_LATE_REANCHOR=1 时提前到这一拍。
      const double hp_ = hyper_period_ms;
      if (hp_ > 0.0) {
        const double delta = fins::util::now_ms() - hyper_start_ms;
        double k = 1.0; // 至少推进一拍
        if (delta > 0.0) { // ceil(delta/H)，保证下一拍 ≥ now 的最近边界
          const double d = delta / hp_;
          k = std::floor(d);
          if (d > k)
            k += 1.0;
        }
#if FINS_ROLLOVER_LATE_REANCHOR
        // 过载（k≥2，排空晚于边界）→ “提前到这一拍”：不以完工时刻跳往未来边界空等，而是立刻以
        // “此刻”为新起点重启一拍（相位重置到完工时刻），之后按新节拍 now+j·H 继续——不漏掉空闲
        // 空洞、流水不断；代价是每次过载相位相对原绝对网格漂移一次。早完工（k==1）仍对齐下一拍。
        if (k >= 2.0) {
          FINS_LOG_WARN(
              "[rollover_hp] 超周期过载：排空晚于边界，提前到这一拍（此刻重锚，跳过 {} 个漏拍，新起点 {:.1f}ms）",
              (long long) (k - 1.0), delta);
          hyper_start_ms = fins::util::now_ms(); // 相位重置到“这一拍”完工时刻
        } else {
          hyper_start_ms += hp_; // 未过载：仍严格等下一拍
        }
#else
        // 过载 → “严格等下一拍”：跳过已错过的整拍、对齐下一未来网格边界（相位不漂移，但空等 + 丢拍）
        if (k >= 2.0)
          FINS_LOG_WARN("[rollover_hp] 超周期过载：排空晚于边界，跳过 {} 个释放拍，下一边界对齐 {:.1f}ms",
                        (long long) (k - 1.0), hyper_start_ms + k * hp_);
        hyper_start_ms += k * hp_;
#endif
      } else {
        hyper_start_ms = fins::util::now_ms(); // 无显式周期（理论上不进回绕）保持旧行为
      }

#if FINS_CAL_WCET
      update_wcet_estimation();
#endif


#if FINS_CAL_MAKESPAN
      if (const double makespan = makespan_updater(dag, num_worker); makespan > hyper_period_ms)
        FINS_LOG_WARN("[rollover_hp] 超周期过载：makespan={:.2f}ms > hyper_period={:.2f}ms", makespan, hyper_period_ms);
#endif

#if FINS_STATIC_PRIORITY
      for (auto &item: ready_.data())
        item.prio = priority_updater(dag, dag.vertex(item.id), num_worker);
      ready_.rebuild(); // 按最新 prio 重建堆（O(n)）
#endif

      ++done_gen_; // 世代化 clear：O(1) 重置，免释放 done_ 的 unordered_map 节点（原 std::set::clear 每顶点一次释放）
      done_count_ = 0;
      ready_.clear(); // 图静止时应空，防御清（LazyMaxHeap 为 vector，clear 保容量 O(1)）
      ready_seq_ = 0; // 入队序号随就绪集重置（新周期从头计序）
      tp_released_ = 0; // tp 全部重新释放（tp_order_ 不清——同一批时间点按原序重放）

      // pred_left 重置回入度基准：两 map 键集相同且均按键有序 → 锁步遍历，O(n) 免逐顶点 at() 查找
      {
        auto it_deg = in_degree_.begin();
        for (auto &[id, pl]: pred_left_) {
          pl = it_deg->second;
          ++it_deg;
        }
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
      if (ready_.empty())
        return nullptr;

#if FINS_DYNAMIC_PRIORITY
      update_abs_deadline();

      for (auto &item: ready_.data())
        item.prio = priority_updater(dag, dag.vertex(item.id), num_worker);
#endif

      ready_.rebuild(); // 按最新 prio 重建堆（O(n)）

      const ReadyItem item = ready_.pop_max(); // 取 prio 最高者；相等按 seq FIFO（O(log n)
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
      if (tp_released_ >= tp_order_.size())
        return nullptr; // 空配置/一次性图/已全部释放

      const std::string id = tp_order_[tp_released_++]; // 按预排顺序取下一个（每 tp 恰一次，游标前移天然防重）
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
     * @retval bool 本完成是否**新入队了就绪后继**（≥1）——装配点据此决定是否 notify_all 唤醒空闲
     *         worker：只有新增就绪才值得唤醒（空闲 worker 仅在就绪堆为空时存在；叶子/无后继的
     *         完成不新增 → 不空唤醒全池，减惊群与锁抖动）。
     */
    std::unordered_map<std::string, uint64_t>
        done_; // 顶点 id → 完成世代号（世代化 clear：rollover O(1) 重置，免释放节点）
    uint64_t done_gen_{0}; // 当前世代号（rollover/expand 递增；done_[id]==gen ⇒ 本世代已完成）
    size_t done_count_{0}; // 本世代已完成顶点数（is_hp_done 用；rollover 归零）
    bool trigger_workload_ready(const std::string &id) {
      bool enqueued = false;
      { // 幂等防御（世代化 done_：本世代已完成 → 跳过；正常每顶点每超周期恰完成一次）
        auto it = done_.find(id);
        if (it != done_.end() && it->second == done_gen_)
          return false; // 本代已处理过 → 无新增
        if (it == done_.end())
          done_.emplace(id, done_gen_);
        else
          it->second = done_gen_;
        ++done_count_;
      }

      // 只沿阻塞边递减：非阻塞边（event 非支配端口）的消费者不该因本顶点完成而减少等待，
      // 否则会提前就绪（与 build_pred 的扣减口径必须成对）
      const auto cs = blocking_succ_.find(id);
      if (cs == blocking_succ_.end())
        return enqueued;
      for (const auto &s: cs->second) {
        auto it = pred_left_.find(s);
        if (it == pred_left_.end() || it->second == 0)
          continue; // 未知/已就绪 → 跳过（防重复递减）
        if (--it->second == 0 && s.rfind("tp:", 0) != 0) { // 减到 0 = 恰好一次就绪
          ready_.push({s, ready_seq_++, 0}); // 推刚就绪的后继 s（勿推已完成前序 id）
          enqueued = true;
        }
      }
      return enqueued;
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
        if (!v.job)
          return;
        double start = hyper_start_ms;
        if (period > 0 && now > start + period)
          start += std::floor((now - start) / period) * period;
        v.ddl = start + (double) (v.k + 1) * v.deadline;
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
        if (!v.job)
          return;
        if (v.id.rfind("tp:", 0) != 0)
          TBBMAP_READ(exec_us_hist_, v.name,
                      [&](const auto &hist) { // 键 = 算法键（record_exec 用 info.name；v.id = {name}:{k} 对不上）
                        if (!hist.empty() && wcet_updater) {
                          std::deque<double> vals; // 统计槽只要用时序列（wcet_updater 槽签名 deque<double>）
                          for (const auto &s: hist)
                            vals.push_back(s.us);
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
      j["hyper_start_ms"] = hyper_start_ms;
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
      dag.for_each_edge([&](const std::string &from, const std::string &to, const std::string &tag, const Message &m) {
        const auto nb = nonblocking_in_.find(to);
        j["edges"].push_back({
            {"from", from},
            {"to", to},
            {"tag", tag},
            // false = 非阻塞边（event 非支配端口：只传帧，不计入就绪等待）；单写者约束下
            // (to, tag) 唯一标识一条入边，故查 tag 集合即可
            {"blocking", !(nb != nonblocking_in_.end() && nb->second.count(tag))},
            {"message", {{"has_frame", m.frame != nullptr}, {"type", m.type_name}}},
        });
      });

      return j;
    }
  };
  inline PrecedenceGraph graph_g;

  // 其他全局单例（PluginLoader/RPCListener/ThreadPool）：其 hpp 均 include g_state 拿全局对象，
  // 故反向 include 会成循环依赖——此处不定义，由业务代码在运行时 .instance() 初始化。

} // namespace fins::rt
