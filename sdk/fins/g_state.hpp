/*******************************************************************************
 * Copyright (c) 2026.
 * IWIN-FINS Lab, Shanghai Jiao Tong University.
 *******************************************************************************/

#pragma once

// ============================================================================
// g_state — 全局运行时状态（进程级共享对象）
// ============================================================================
//
// 内部逻辑：
//   集中存放跨组件共享的运行时状态，全部为独立全局对象：
//   - pipeline_g.cache（Pipeline 成员，配置双缓存 util::DoubleBuff<nlohmann::json>）：对**原始配置
//     JSON** 双缓存——write() 缓冲 RPC 收到的 JSON（不解析）、read() 运行份（主线程调度循环图静止
//     commit 后消费）。
//     解析统一在 Pipeline 完成（全局 pipeline_g 单份，见下），parse_pipeline/check_topology
//     两函数职责分离：
//       parse_pipeline(config)   — 拆封 script 顶层 + 触发 NodeInfo 构造（构造器自解析：逐节点格式
//                                  校验 + 字段抽取 + config_cache 取值表），无返回写 nodes；
//                                  违反抛异常（主线程调度循环图静止时调，充当第一级 json 格式审查）；
//       check_topology()         — 第二级图结构审查（无参读 nodes：单写者 + 源周期必填，
//                                  主线程调度循环图静止时 parse 后、expand_hp 前显式调用）。
//     NodeInfo 是 Pipeline 的一部分，定义在 Pipeline 结构体内（见本文件下方）。
//   - pipeline_g（全局单份解析态）：主线程调度循环图静止时从 JSON 缓冲份 parse_pipeline +
//     check_topology 填充，expand_hp(pipeline_g, library_g) 读它建图。RPC 不再解析（只存 JSON + 置位），
//     解析态单份即可。
//     双缓存：RPC 线程写 write() 份 JSON（**不打断 worker**）→ 置 pending；
//     主线程调度循环检测图静止 + 置位后 commit（active 指向新 JSON）→ parse 到 pipeline_g →
//     expand_hp 重建运行图。
//   - library_g（带壳 Library：算法定位表 so_ctx）：唯一算法定位数据源——expand_hp() 遍历
//     so_ctx 各 loaded_keys 找 [name:version]；装配点经 PluginLoader on_library_add/modify/delete
//     回调注入维护（SET/ERASE 由装配点回调完成）。
//   - graph_g（单份运行图，struct PrecedenceGraph）：当前调度推进所依据的数据流图 + 调度依据。
//     **重建时机 = 图本轮全部顶点执行完（is_hp_done() 图静止）+ 有新配置（pending）**——
//     主线程调度循环图静止时统一判断：有配置 expand_hp 就地重建 / 有超周期
//     rollover_hp 回绕 / 否则等待（图更新本就必须静止，故无双份图副本）。
//     PrecedenceGraph：dag = DirectedAcyclicGraph<Workload, Message>——job 实例级 precedence 图：
//     顶点 = job 实例 Workload（命名 {id}:{k}，k = 超周期内实例序号），边 = 绑定边
//     （Message 槽，共享帧；consumer job 读展开时静态绑定的 producer job 输出，
//     无 latest-value 隐式语义）。它既是数据流图也是调度依据：就绪判定/选最优先就绪
//     由框架内置（grab_ready_workload 读顶点权值 + 前序依赖）。
//     expand_hp(pipeline, library) 是唯一建图入口（输入显式入参，不隐式读全局）：
//     结构分析（端口名索引 + 一跳邻居 + 全周期 HP=lcm）→ 逐节点实例化（内置 delay/ringbuf
//     或遍历 so_ctx 定位 → C 工厂构造 + configure 注入参数）→ Replication（job 实例 {id}:{k}
//     + 连续 job precedence 边）→ 绑定（同名端口直连：consumer job k 读其时段 [k·T_c,(k+1)·T_c)
//     内 producer 最新已完成帧：整数式 ((k+1)·Np-1)/Nc，快 producer→慢 consumer 绑最后帧；
//     单写者约束：同名输出端口多 producer 由 Pipeline::check_topology 抛异常拒绝）。
//     Loop（显式端口补充定义）：节点 config 顶层 "loop" = {"step": {端口: 迭代步}} /
//     {"timer": {端口: 观测周期}}——loop 端口不参与拓扑依赖/支配继承/绑定边（无自环），
//     反馈走运行时数据槽 message_hist_（最近 w 帧滑动窗口，job 闭包聚合 vector<Message> 进
//     inputs[端口]；容量 = producer 节点 NodeInfo.cap（config 顶层 "cap"，默认 10），回绕不清
//     满丢最旧、跨重建保留；窗口未满 0 值占位）。
//     执行耗时统计：每节点环形队列（exec_us_hist_，容量 exec_hist_cap 可配）仅记 execute 耗时
//     （steady_clock）；expand_hp 重建保留不清（跨配置延续）。
//   - 调度运行时（带锁事务在装配点）：**主线程调度循环 + 完成事件驱动，无轮询 tick**——
//     PrecedenceGraph 只留无锁纯图原语（expand_hp/grab_ready_workload/is_hp_done/rollover_hp，
//     调用方持公开成员 mtx 调用）；**带锁逻辑由装配点写**：
//     worker = thread_pool on_execute 回调（装配点写单步事务）：mtx 锁内 grab_ready_workload() 纯拉取
//     （**不回绕**）——「前序全 Finished + Pending」中 priority 最大者置 Running + 返回
//     该顶点 Workload*（图内顶点指针，含 id/job）→ 锁外执行 w->job()（**纯执行业务**：打包输入 → execute →
//     路由输出，不包完成事件）→ 回锁直做完成事件（w->state = Finished + cv.notify_all()）→ 无可拉 cv.wait；
//     wcet/priority 回调槽由 main 主线程调度循环每轮持锁集中更新；
//     主线程 = main() 装配完 worker 后跑装配点调度循环（持 mtx），集中做 precedence graph 的
//     **超周期启停判断 + 回调槽集中更新**：图静止（is_hp_done()，超周期"停"）→ 有配置 commit + parse + expand_hp /
//     有超周期 rollover_hp 回绕 / 否则等待（cv.wait_for 1ms 超时兜底）。
//     源节点（无入边）启动/重建/回绕后自然被 grab_ready_workload 拉取（无需显式 kick）；
//     stopped 置位 → 主线程循环返回 + on_execute 回调返回 false → worker 退出。
//   - core_usages_g / mem_usage_g（系统指标）保留。
//
// 资源消耗：
//   - 建图（expand_hp）：每节点 1 个 AlgoBase 实例 + 1 个 Job 闭包 + 1 份 json config；
//     实例化遍历 so_ctx（O(so 数 × 每 so 算法数)）；边 = Message 槽（共享帧，O(1)）
//   - so_ctx 每 so 1 个 Plugin（dlopen handle，实例经删除器持引用保活）
//   - 运行图单份（graph_g，无图双缓冲）；配置 JSON 双缓存 2 份 nlohmann::json（原始配置，不解析）
//     + 解析态 pipeline_g 单份（NodeInfo 表，主线程调度循环图静止时 parse 填充）
//   - 调度：0 额外线程——主线程即 main（跑装配点调度循环，无独立调度线程、无轮询 tick）；
//     锁在 PrecedenceGraph 实例内（公开成员 mtx，装配点直接持有：on_execute 回调单步事务 +
//     主线程调度循环）+ cv + 原子停止位 stopped + 原子待应用标志 pending；图静止时 expand_hp
//     重建/回绕在主线程循环持锁内完成；阻塞时 worker 等 cv、main 等 wait_for 均不忙等
//   - 推进（grab_ready_workload）：每拉取 O(候选顶点数 + Σ 其入边) 找最优先就绪；图静止判定 O(顶点数)
//   - Loop 数据槽：每反馈端口 1 个定长滑动窗口 deque（容量 = producer 节点 NodeInfo.cap，
//     默认 10 可配；回绕不清、满丢最旧、expand_hp 重建保留）
//
// 对外接口：
//   pipeline_g.cache.write() = json             — 存原始配置 JSON 到缓冲份（RPC handler 调，不解析）
//   pipeline_g.parse_pipeline(json)             — 第一级 json 格式审查 + 拆封写 pipeline_g.nodes
//                                                 （主线程调度循环图静止时调；违反抛异常兜底日志）
//   pipeline_g.check_topology()                 — 第二级图结构审查（读 pipeline_g.nodes：单写者 + 源周期必填）
//   graph_g.pending = true + cv.notify_all()    — 置"新配置待应用" + 唤醒主线程调度循环（RPC/测试存
//                                                 JSON 到缓冲份后调；主线程循环检测图静止 + pending →
//                                                 commit + parse + expand_hp 消费）
//   graph_g.pending.load() / pending = false    — 只读观察 / 主线程调度循环消费新配置后清除
//   library_g.so_ctx                                — 算法定位表（装配点回调维护；expand_hp 经入参 library 定位算法）
//   graph_g                                     — 单份运行图（worker on_execute 拉取执行 / 主线程循环
//                                                 expand_hp 重建 / 测试读/推进）
//   graph_g.mtx / cv                            — 调度串行锁 + 就绪条件变量（公开成员；装配点持 mtx：
//                                                 on_execute 回调 + 主线程循环调用下述无锁原语）
//   graph_g.stopped = true + cv.notify_all()    — 置停止位唤醒（装配点退出前调 → 主线程循环返回 +
//                                                 on_execute 回调返回 false → 退出）
//   graph_g.stopped.load()                      — 只读观察是否已请求停止（on_execute 回调每轮检查）
//   graph_g.grab_ready_workload()                        — 无锁原语：拉最优先就绪（置 Running；返回图内顶点
//                                                 Workload*（含 id/job）或 nullptr；**不回绕**——回绕/expand_hp 由主线程
//                                                 循环做；装配点锁外 w->job() 后回锁直做完成事件）
//   graph_g.is_hp_done()                          — 无锁原语：图静止判定（全部 job 顶点 Finished）
//   graph_g.rollover_hp()                 — 无锁原语：超周期回绕（hyper_start_ms 平移 + 顶点重置 Pending）
//   graph_g.expand_hp(pipeline, library)           — 无锁原语：建图唯一入口（显式入参；主线程循环图静止时调）
//   graph_g.set_wcet_updater(cb) / set_priority_updater(cb)
//                                               — 装配点注入回调槽（main 主线程调度循环每轮持 mtx 集中更新）
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
#include <queue>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "algo/algo_base.hpp"
#include "algo/algo_utility.hpp"
#include "form.hpp"
#include "mesg/mesg.hpp"
#include "third_party/json.hpp"
#include "utils/logger.hpp"

namespace fins::rt {

  /// .so 加载上下文（library_g.so_ctx 的元素）：dlopen 一个 .so 后解析出的 C 符号工厂。
  /// **构造 = 装载**（dlopen + dlsym 解析 C 符号 + 填 loaded_keys，失败抛异常并清理 handle）；
  /// **析构 = dlclose 物理卸载**（引用计数归零才触发——算法实例删除器持 shared_ptr<Plugin>
  /// 保活期间库不卸载）；take_keys() 供删除/替换路径显式取走 keys（析构不能返回值）。
  /// 装配点经 PluginLoader::on_library_add/modify/delete 回调维护 library_g.so_ctx（SET/ERASE），
  /// 本 struct 只提供"装载 / 卸载"的库机制，不感知全局表。
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

    /// 构造 = 装载：dlopen(path) + dlsym 解析 5 个 C 工厂符号 + 枚举本 so 全部算法填
    /// loaded_keys（{name}:{version}，即 library_g.so_ctx 的定位键）。
    /// 输入：path = .so 文件路径（绝对/相对均按 dlopen 规则）。无返回值——失败抛
    /// std::runtime_error（dlopen 失败 → dlerror 文本；缺必需符号 → "Missing required C-symbols"），
    /// 已打开的 handle 在 catch 内 dlclose 清理（构造抛 → 析构不调用 → 防泄漏）。
    /// 调用方：PluginLoader 的 on_library_add/on_library_modify 直接 make_shared<Plugin>(path)。
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

    /// 析构 = 物理卸载：dlclose 释放句柄。**引用计数归零才触发**——算法实例删除器持
    /// shared_ptr<Plugin> 保活，实例未全销毁期间库不卸载（保活语义见 expand_hp()）。
    ~Plugin() { if (handle) dlclose(handle); handle = nullptr; }

    /// 显式取走 keys（删除/替换路径装配点调用）：move 走 loaded_keys 返回并清空——
    /// 析构不能返回值，单独保留供删除时回收 [name:version] 定位键。
    std::vector<std::string> take_keys() { return std::move(loaded_keys); }
  };
  /// so 上下文表（独立全局）：[so_path] → Plugin。唯一算法定位数据源——
  /// expand_hp() 遍历本表各 loaded_keys 找 [name:version]。装配点经 PluginLoader
  /// on_library_add/modify/delete 回调注入维护（回调载荷含 Plugin，SET/ERASE 由装配点做）。
  struct Library{
    util::TBBMap<std::shared_ptr<Plugin>> so_ctx;
  };
  inline Library library_g;

  // ==========================================================================
  // 节点解析态 = Pipeline::NodeInfo（Pipeline::parse 的产物，**Pipeline 的一部分**，
  // 定义在下方 Pipeline 结构体内，命名 NodeInfo）——解析态从命名空间级移入 Pipeline：
  // input_ports/output_ports（端口名数组）/config_cache（位置式值表，顺序 = config
  // "parameters" 数组元素顺序 = AlgoFunc 配置段相对序号）/loop_timer+loop_step（两个 map）/
  // period/wcet/deadline 全在 Pipeline::NodeInfo；图侧/运行时不再接触原始 JSON。
  // ==========================================================================

  /// Pipeline — dataflow 配置（解析态；全局单份 pipeline_g，主线程调度循环图静止时从 JSON 缓冲份 parse 填充）
  ///
  /// 双缓存对象 = 原始配置 JSON（本类成员 cache = DoubleBuff<nlohmann::json>，write/read 见下方），
  /// 本类只是其解析产物、单份即可：主线程调度循环图静止 + pending 时 commit JSON →
  /// parse 到 pipeline_g → expand_hp(pipeline_g, library_g) 重建运行图。收益：
  ///     ① 热切换不撕裂：读方永远读到一份完整写完的 JSON，不会撞见"写了一半"的中间态；
  ///     ② 写配置不打断 reader：写非激活份 JSON 期间，worker 照常跑旧运行图；
  ///     ③ pending = true（RPC 存 JSON 后直写成员）+ cv.notify_all() 唤醒主线程调度循环：图静止 +
  ///        pending 即 commit + parse + expand_hp 重建——**收包/解析/建图都不在 RPC handler 内**
  ///        （RPC 只存 JSON + pending 置位；parse/check_topology/expand_hp 由主线程调度循环
  ///        检测图静止后做，见头部装配模板）。
  ///
  /// parse 可通过的标准形式（合法 pipeline 配置）：
  ///   每个节点对象的字段：
  ///     必填   "name"         string    算法名（[name:version] 为 so 表定位键）
  ///     必填   "version"      string    算法版本（定位键）
  ///     必填   "id"           string    节点在图中的标识（图中唯一，建边/调度定位）
  ///     可选   "parameters"   [{"name": string 必填且不重复, "value": any}, ...]
  ///     可选   "inputs"       [端口名, ...]    string 数组（入边：同名端口直连，
  ///                                             顺序 = AlgoFunc 参数顺序）
  ///     可选   "outputs"      [端口名, ...]    string 数组（出边：同名端口直连）
  ///     可选   "wcet"         number   最坏执行时间（缺省 1）
  ///     可选   "deadline"     number   相对截止时间（缺省 = wcet）
  ///     可选   "period"       number   执行周期：inputs 为空（源头节点，无输入驱动）时必填
  ///     可选   "priority"     integer  调度优先级（缺省 0）
  ///     可选   "loop"         object   显式端口补充定义（Loop 反馈，2026-08-25 落地）：
  ///                                    {"step": {端口: 迭代步}} / {"timer": {端口: 观测周期}}。
  ///                                    parse 不额外校验该键；Loop 语义由 expand_hp 消费——
  ///                                    loop 端口不参与拓扑依赖/支配继承/绑定边（无自环边），
  ///                                    反馈走运行时数据槽 message_hist_（键 = 端口名，最近 w 帧
  ///                                    滑动窗口，容量 = producer 节点 NodeInfo.cap，见 expand_hp；
  ///                                    loop 端口名须等于节点自身某输出端口名）。
  ///   违反任一条即抛 std::invalid_argument（收包 handler 转 HTTP 400）；全部通过返回 true。
  ///   最小合法示例（同名端口直连：cam 输出 "cam_feed" → disp 输入 "cam_feed"）：
  ///     { "nodes": [
  ///         { "id": "cam",  "name": "cam",  "version": "1.0.0", "period": 100,
  ///           "outputs": ["cam_feed"] },
  ///         { "id": "disp", "name": "disp", "version": "1.0.0",
  ///           "inputs":  ["cam_feed"] } ] }
  /// 解析产物 = pipeline.nodes（每节点 1 个 NodeInfo 解析态，定义见下方 Pipeline::NodeInfo）。
  /// 图侧建边/绑定/loop/支配周期全从本结构读，图域不再接触原始 JSON。
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

    /// 逐节点自解析：全部结构校验 + 字段抽取（Pipeline::parse 只拆封顶层后逐个调用本构造器）。
    /// at = "nodes[i]." 上下文串（错误消息定位用）。parameters **位置式取值表**
    /// config_cache（只取 p["value"]，名字丢弃——顺序 = config "parameters" 数组元素顺序 =
    /// AlgoFunc 配置段相对序号，见 algo_func.hpp 头注释顺序保证链）。
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
  struct Pipeline {
    /// 原始数据
    util::DoubleBuff<nlohmann::json> cache;

    /// 解析产物：每节点 1 个 NodeInfo 解析态（字段见上方——Pipeline 的一部分）。parse 无返回、直接写
    /// 本成员；expand_hp() 只读本表（图侧/运行时不再接触原始 JSON）。实例化出的
    /// 具体算法实例由图侧 expand_hp 局部 by_id 持有，不替换本表——Workload 是纯数据。
    std::vector<NodeInfo> nodes;

    /// 第一级：拆封 script 顶层 + 触发 NodeInfo 构造（逐节点格式校验在构造器内），无返回，
    /// 直接写 nodes；格式违反抛异常（收包前调充当 json 格式审查）。跨节点图结构合法性
    /// 不在此处，由第二级 check_topology 审查。
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

    /// 第二级图结构审查（无参，读本实例 nodes——第一级 parse 已把 script 拆到 NodeInfo，
    /// 逐节点格式校验已过，这里只查跨节点图结构合法性；违反抛异常）。主线程调度循环
    /// 图静止时在 parse_pipeline 与 expand_hp 之间显式调用（对全局 pipeline_g）：
    ///   ① 单写者约束：同名输出端口至多一个生产者（数据流语义）；多写者直接拒绝，否则图侧
    ///      expand_hp 绑定边对每个 producer 都建边、闭包读哪条取决于遍历顺序（不确定）。
    ///   ② 源周期：无输入节点（input_ports 空）无上游驱动，须主动周期执行，必填 period。
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
    /// RPC 写入端串行锁访问：多 /update 并发写 pipeline_g.cache.write() JSON 份不撕裂；
    /// main/worker 侧（commit/read 消费）不持本锁。装配点 handler 用法：
    ///   std::lock_guard lk(pipeline_g.wr_lock());
    std::mutex &wr_lock() { return wr_mtx_; }
  };
  inline Pipeline pipeline_g;

  /// 图顶点 = 正常 job 实例 + 多维权值 + 生命周期状态（调度依据）。
  ///   job 非空 = 有执行体闭包，闭包只执行（打包输入 → execute → 路由输出），**不含完成事件**；
  ///   **不碰 state**——Running 由 grab_ready_workload() 拉取时置、Finished 由 on_execute 回调事务
  ///   回锁直做置（完成事件在装配点事务）；回绕重置回 Pending。见 PrecedenceGraph。
  ///   （框架不再有虚拟源/历史顶点——跨周期数据滞留由用户显式接内置工具算法承担。）
  /// Ready（可执行）由前序推导：job 的全部前序顶点 Finished → Ready（无释放时刻；
  /// 就绪判定框架内置：on_execute 回调经 grab_ready_workload() 拉取最优先就绪，见 PrecedenceGraph）。
  /// priority = 调度优先级（grab_ready_workload 选最优先就绪的读源；expand_hp ⑥ 从 NodeInfo 填初值，
  /// priority_updater_ 运行时更新——优先级只存图顶点，无并行结构）。
  /// 延迟 = 用户算法节点（AlgoBase 派生，函数体内做纯延迟——如内置 delay 算法按
  /// 阻塞 sleep_for 真延迟，占 worker；时长 offset 由后续超周期展开分析注入，非 JSON），
  /// 框架不建延迟顶点、无时间门控。
  inline util::TBBMap<float> core_usages_g{};
  inline std::atomic<float> mem_usage_g{};

  // ── 调度运行时（独立全局，无类壳）────────────────────────────
  /// Job = 可执行任务（数据处理函数 + 参数打成的 lambda）。g_state 独有定义——
  /// thread_pool.hpp 同命名空间同型，重定义会冲突，故只在 g_state 定义、线程池不定义。
  // using Job = std::function<void()>;

  /// 调度推进串行锁 = PrecedenceGraph **公开成员 mtx**（expand_hp/grab_ready_workload/is_hp_done/
  /// rollover_hp 串行化；装配点直接持有——on_execute 回调单步事务 + 主线程
  /// 调度循环在此锁内调用上述无锁原语）——worker 完成事件推进、缓冲份 expand_hp、图静止交换互不
  /// 并发；cv 关联该成员锁。std::mutex 不可重入：持锁期间勿再 lock()；
  /// spin_rw_mutex 不可重入，勿在持锁期间再嵌套 TBB accessor 遍历。

  /// 调度停止位 / 就绪条件变量 / 新配置待应用标志——均为 **PrecedenceGraph 公开成员**
  /// （mtx/cv/stopped/pending，见下；外部回调槽 wcet_updater_/priority_updater_ 仍 private，
  /// 经 set_wcet_updater/set_priority_updater 注入），全局不再有裸调度状态：
  ///   装配点直接读写成员（无薄封装函数）：
  ///     graph_g.stopped = true + cv.notify_all()   — 置停止位唤醒：主线程循环返回 + on_execute 返回 false；
  ///     graph_g.pending = true + cv.notify_all()   — 置待应用标志：RPC/测试存 JSON 后唤醒主线程调度循环；
  ///     graph_g.pending.load() / pending = false   — 只读观察 / 主线程调度循环消费新配置后清除；
  ///     graph_g.stopped.load()                     — 只读观察停止位（on_execute 回调每轮检查）；
  ///     graph_g.mtx / cv                           — 调度串行锁 + 就绪条件变量（wait/notify 直接持用）；
  ///     graph_g.set_wcet_updater(cb)               — 装配点注入 wcet 重估回调（main 主线程调度循环每轮持 mtx 集中调用）；
  ///     graph_g.set_priority_updater(cb)           — 装配点注入优先级更新回调（同上）。

  /// PrecedenceGraph — 数据流图 + 调度依据（单份运行图 graph_g；图更新本就必须图静止，无双缓冲）
  ///
  /// 内部逻辑：
  ///   dag = DirectedAcyclicGraph<Workload, Message>——job 实例级 precedence 图：
  ///     顶点 = job 实例（Workload{执行体闭包 + 多维权值 + 四态状态}），命名 {id}:{k}，
  ///           k = 超周期内实例序号（0..Nx-1，Nx = HP/T）。
  ///     边 = 绑定边（Message 槽，共享帧）——consumer job 读的是展开时静态绑定的
  ///           producer job 输出，无 latest-value 隐式语义。
  ///   展开 = 从源逐步 + 一跳邻居判定 + 全周期 Verucchi（HP = lcm(显式配置 period)）：
  ///     支配周期 + Replication：显式周期节点 Nx = HP/T，job 顶点 {x}:{k}（k=0..Nx-1，
  ///     连续 job 加 precedence 边 {x}:{k}→{x}:{k+1} 保证同节点实例串行）；未配置 period
  ///     的后级继承最短周期前级（支配原则，多前级取最短），Nx = HP/继承周期，v.period
  ///     为真实周期；孤立未配置 → 1。
  ///     无释放时刻/相位：就绪只看前序边（前序全 Finished → Ready，就绪判定框架内置，
  ///     grab_ready_workload 找最优先就绪，见下方）。
  ///     绑定规则：时段内最新已完成帧整数式——consumer job k 绑 producer 中
  ///     ((k+1)·Np-1)/Nc 号 job（向下取整天然在 [0,Np-1]；同速率一一对应，快 producer→
  ///     慢 consumer 绑最后帧）；恒有边（始终绑到 producer 某 job）——跨周期数据滞留由
  ///     用户显式接内置工具算法（delay/ringbuf）承担。
  ///     Loop（显式端口补充定义）：节点 config 顶层 "loop" = {"step": {端口: 迭代步}} /
  ///     {"timer": {端口: 观测周期}}——loop 端口不参与拓扑依赖/支配继承/绑定边（无自环）；
  ///     反馈走运行时数据槽 message_hist_[端口]（最近 w 帧滑动窗口，容量 = producer 节点
  ///     NodeInfo.cap（config 顶层 "cap"，默认 10），回绕不清、满丢最旧、跨重建保留；跨多超周期
  ///     依赖可表达），job 闭包聚合 std::vector<Message>
  ///     放入 inputs[端口]。恒长 w 契约：窗口未满（刚启动/超周期初期）开头补空 Message
  ///     0 值占位（frame=nullptr，算法须判断 .frame 跳过/当 0，勿 sub——sub 空帧抛异常）。
  ///     step = 迭代步 N / timer = ceil(观测周期/节点周期)。
  ///     顶点 = 正常 job 实例 {id}:{k}（见 Workload）——执行体闭包（job 非空），闭包只执行
  ///     不碰 state（Running 由 grab_ready_workload 拉取置、Finished 由 on_execute 回调事务回锁直做）；
  ///     周期回绕重置回 Pending。
  ///   既是数据流图也是调度依据——**装配点持锁驱动，无轮询 tick**：
  ///     PrecedenceGraph 只留无锁纯图逻辑原语，锁/条件变量为公开成员 mtx/cv，由装配点
  ///     直接持有调用：worker = thread_pool on_execute 回调内写带锁单步事务——锁内
  ///     grab_ready_workload() 拉最优先就绪（置 Running）→ 锁外 w->job() 执行 → 回锁直做
  ///     完成事件（w->state = Finished → notify_all）→ 无可拉
  ///     cv.wait()；main 主线程 = 持锁调度循环，集中做**超周期启停判断 + 每轮集中更新回调槽**：
  ///     图静止（is_hp_done()，停）→ 有配置 commit+parse+expand_hp / 有超周期 rollover_hp
  ///     回绕 / 否则 wait_for 等待；
  ///     源节点（无入边）启动/重建/回绕后自然被 grab_ready_workload 拉取，无需显式 kick。
  ///
  ///   expand_hp(pipeline, library) 唯一建图入口（输入显式入参：pipeline = 解析态 Pipeline、
  ///   library = 算法定位 Library（含 so_ctx），由调用方传 pipeline_g / library_g，不隐式读全局）。
  ///   单函数一步建图：① 结构分析（端口名索引 + 一跳邻居）→ ② 超周期 HP=lcm → ③ 拓扑序 →
  ///   ④ 实例化（内置工具算法 delay/ringbuf 或 so 表插件）→ ⑤ 支配周期继承/Replication →
  ///   ⑥ 建顶点 {id}:{k}（权值 + abs_deadline 按序排 + priority）→ ⑦ 建边（seq 连续边 +
  ///   同名端口绑定边，时段内最新帧整数式 ((k+1)·Np-1)/Nc，恒有边）→ ⑧ 填 job 执行体闭包
  ///   （按 NodeInfo端口序打包 array，AlgoBase execute；闭包捕获 this = graph_g（单份运行图），
  ///   完成事件由装配点回锁直做落到正确图）。
  ///
  ///   带锁事务全在装配点（本类无成员事务/自锁包装，方法全为无锁原语，调用方持 mtx）：
  ///     on_execute 回调（worker 单步）：锁内 grab_ready_workload() 拉最优先就绪（置 Running + 返回
  ///     该顶点 Workload*）→ 锁外 w->job() 执行 → 回锁直做完成事件（w->state = Finished + notify）；
  ///     无可拉 cv.wait()；stopped.load() 为 true 返回 false 退出。**不回绕**——rollover_hp()
  ///     （hyper_start_ms += hyper_period、job 顶点重置 Pending + abs_deadline 平移、seq 边不闭合
  ///     回绕后 {id}:{0} 重新被拉取）与 expand_hp 判断统一由 main 主线程调度循环在图静止时做。
  ///     主线程调度循环：图静止（is_hp_done()，本轮超周期执行完而非 wall-clock 到点）→ 有配置
  ///     commit+parse+check_topology+expand_hp / 有超周期 rollover_hp 回绕 / 否则 wait_for 等待；
  ///     每轮持锁集中更新 wcet/priority 回调槽（wcet_updater(graph_g) / priority_updater(graph_g)）。
  ///
  /// 资源消耗：
  ///   每节点 1 个 AlgoBase 实例（全部 job 实例共享，连续 job precedence 边保证串行）
  ///   + 每 job 实例 1 个 Job 闭包 + 1 个顶点；边 = 绑定边 Message 槽（共享帧，O(1)）。
  ///   Loop 数据槽：每反馈端口名 1 个定长滑动窗口 deque（容量 = producer 节点 NodeInfo.cap，
  ///     默认 10 可配；回绕不清、满丢最旧、expand_hp 重建保留）；TBBMap accessor 按端口锁（只锁
  ///     本端口历史槽字段，loop 聚合读 / record_mesg 写可能并发，不同端口互不阻塞）。
  ///   执行耗时统计：每节点 1 个 deque<double>（容量 exec_hist_cap 可配，默认 100，环形满丢最旧）；
  ///   每次 execute 2 次 steady_clock 读（纳秒级）；expand_hp 重建保留不清（跨配置延续）；
  ///   TBBMap accessor 按节点锁（只锁本节点字段；record_exec 写与 wcet 重估读 const_accessor 并发安全）。
  ///   建图 O(节点数 × so 表定位 + Σ Nx × 入边 producer 数)；grab_ready_workload 每拉取
  ///   O(候选顶点数 + Σ 其入边) 找最优先就绪，图静止判定 O(顶点数)；回调槽每轮集中更新 O(节点数)。
  ///
  /// 对外接口（无锁原语——方法不碰锁，调用方须持 mtx；带锁事务在装配点：
  /// on_execute 回调 + 主线程调度循环；调度状态 mtx/cv/stopped/pending 为公开成员直读直写）：
  ///   dag                     — job 实例级图（顶点={id}:{k}；边=绑定边 Message 槽；测试轮询用 dag.size()）
  ///   hyper_period            — 超周期长度（ms；<=0 不周期回绕）
  ///   hyper_start_ms          — 当前超周期起点（ms；rollover_hp 回绕维护）
  ///   mtx / cv                — 调度串行锁 + 就绪条件变量（公开成员；装配点持 mtx 调用下述
  ///                              无锁原语、cv.wait/notify；std::mutex 不可重入）
  ///   stopped / pending       — 调度停止位 + 新配置待应用标志（公开成员 atomic；装配点直写：
  ///                              stopped=true+pending=true 均须配 cv.notify_all() 唤醒）
  ///   expand_hp(pipeline, library) — 建图唯一入口（无锁原语）：Pipeline 解析态 + Library 定位表 →
  ///                               job 实例顶点 + 绑定边，一步构建（显式入参；主线程调度循环图静止
  ///                               时 parse 到 pipeline_g 后持 mtx 调用）
  ///   grab_ready_workload()            — 拉最优先就绪顶点（置 Running；返回图内顶点 Workload*（含 id/job）
  ///                              或 nullptr；不回绕——回绕/expand_hp 由主线程调度循环做；on_execute 回调事务用，
  ///                              锁外 w->job() 后回锁直做完成事件 w->state = Finished + cv.notify_all()）
  ///   is_hp_done()              — 图静止判定（全部 job 顶点 Finished；主线程调度循环超周期启停前提）
  ///   rollover_hp()     — 超周期回绕（hyper_start_ms 平移 + 顶点重置 Pending + abs_deadline 平移；
  ///                              主线程调度循环图静止时调用）
  ///   set_wcet_updater(cb) / set_priority_updater(cb)
  ///                          — 装配点注入回调槽（main 主线程调度循环每轮持 mtx 集中更新；槽仍 private）
  ///   exec_us_hist_           — 算法执行耗时统计（按节点环形队列）：节点 id → 最近
  ///                              exec_hist_cap 次 execute 耗时（us；job 闭包 record_exec 写）
  ///   exec_hist_cap            — 环形队列容量（可配，默认 100）
  struct Workload {
    /// 生命周期状态（标记"走到哪一步"，非"是什么"——顶点类别由 job 是否非空区分）。
    enum class State { Pending, Ready, Running, Finished };
    State state{State::Pending};  // 生命周期：拉取（grab_ready_workload）置 Running → 装配点回锁直做置 Finished；回绕重置 Pending

    std::string id{};  // 顶点名（格式 {节点id}:{k}，如 cam:0/cam:1）——expand_hp ⑥ 建顶点时填 vtx（同 dag 的 map 键）；
                       // grab_ready_workload 返回的 Workload* 含 id，装配点执行后回锁直做完成事件用

    int priority{0};
    float period{0};
    float deadline{1};            // 相对截止期（ms；缺省 = wcet）
    float abs_deadline{0};        // 绝对截止期（ms；派生：超周期起点 + (k+1)·deadline，按序排）
    float wcet{1};                // 最坏执行时间（ms；缺省 1）

    std::function<void()> job;                 // 执行体（闭包捕获实例 + 节点配置，执行时现查边取帧/发布）
  };
  struct PrecedenceGraph {
    // ── public：图数据 + 无锁原语（方法不碰锁，前提调用方持 mtx；带锁事务在装配点
    //    on_execute 回调 / 主线程调度循环）──
    util::DirectedAcyclicGraph<Workload, Message> dag;  // 顶点带权、边=Message 槽

    float hyper_period{0};       // 超周期长度（ms）
    float hyper_start_ms{0};    // 当前超周期起点（update 维护）

    // ── 历史数据统计（loop 反馈滑动窗口，按输出端口名）
    std::map<std::string, size_t> mesg_hist_cap{};   // 滑动窗口容量（expand_hp 填充：loop 反馈输出端口 → producer 节点 NodeInfo.cap，默认 10 可配；expand_hp 重建时清空重算；运行时只读无并发写）
    util::TBBMap<std::deque<Message>> message_hist_;  // 运行时：输出端口名 → 最近 cap 帧滑动窗口（loop 反馈历史槽；
    void record_mesg(const std::string &id, const Message& mesg) {
      util::TBBMap<std::deque<Message>>::accessor a;
      message_hist_.insert(a, id);      // 无则默认构造插入、有则定位（持写锁，仅本端口）
      auto &q = a->second;
      if (q.size() >= mesg_hist_cap[id]) q.pop_front();  // 满丢最旧（环形；cap 运行时只读，调用方已 guard >0）
      q.push_back(mesg);
    }

    // ── 算法执行耗时统计（按节点环形队列，仅 execute 耗时）
    std::map<std::string, size_t> exec_hist_cap{};   // 环形队列容量（可配：每节点保留最近 N 次 execute 耗时；当前无填充 → 缺省 0，既有语义保持）
    util::TBBMap<std::deque<double>> exec_us_hist_;  // 节点 id → 最近 execute 耗时（us；TBBMap accessor 按节点锁，只锁本节点字段）
    void record_exec(const std::string &id, double us) {
      util::TBBMap<std::deque<double>>::accessor a;
      exec_us_hist_.insert(a, id);
      auto &q = a->second;
      const size_t cap = exec_hist_cap.count(id) ? exec_hist_cap.at(id) : 100;  // 缺省 100（未填充时勿取 operator[]→0，否则 pop_front 空 deque = UB）
      if (q.size() >= cap) q.pop_front();  // 满丢最旧（环形）
      q.push_back(us);
    }

    // ── 调度状态公开成员（装配点直接读写：worker on_execute 回调 / main 主线程调度循环
    //    持 mtx 调用下述无锁原语；std::mutex 不可重入——持锁期间勿再 lock()，会死锁）──
    std::mutex mtx;
    std::condition_variable cv;
    std::atomic<bool> stopped{false};
    std::atomic<bool> pending{false};

    /// 建图唯一入口（**无锁原语，前提调用方持 mtx**）：把
    /// NodeInfo 解析态列表（Pipeline::parse 产物，每节点 1 个 NodeInfo 解析态，承载解析好的
    /// 结构化状态）展开成 job 实例级完整图（全量重建）。
    /// **输入全部显式入参**：pipeline = 解析态 Pipeline（调用方传全局 pipeline_g 或局部
    /// Pipeline）、library = 算法定位 Library（调用方传 library_g，实例化 [name:version] 用）——
    /// 不隐式依赖任何全局（图侧无 JSON）。
    /// 由 main 主线程调度循环持 mtx、检测图静止 is_hp_done() + pending 后调用：
    /// commit JSON 缓冲份 → pipeline_g.parse_pipeline + check_topology → expand_hp(pipeline_g, library_g)
    /// 就地重建（图更新必须静止）。
    /// 单函数一步建图（①结构分析→②超周期→③拓扑序→④实例化→⑤支配周期/实例数→
    /// ⑥建顶点→⑦建边→⑧填 job 闭包），中间量全为局部（不占成员、无 BuildCtx）。
    void expand_hp(const Pipeline &pipeline, const Library &library) {
      dag.clear();
      hyper_period = 0;
      hyper_start_ms = 0;   // 新配置展开回到新超周期起点（勿残留上次回绕后的起点）
      mesg_hist_cap.clear();   // 只清容量表（新配置重算）；message_hist_ 历史槽跨重建保留不清（同 exec_us_hist_）

      auto nodes = pipeline.nodes;
      auto so_ctx = library.so_ctx;

      if (nodes.empty()) return;        // 空配置 → 空图（幂等，parse 已产空表）

      // ① 结构分析：端口名生产者/消费者索引 + 一跳邻居拓扑（节点级，判定 Multi-hop/Fork/Join）。
      // 单写者约束已在 Pipeline::check_topology 校验（同名输出端口多 producer 拒绝）；这里只建索引
      // 供拓扑/继承周期用。loop 端口（loop_timer/loop_step 中）跳过索引（反馈 producer 是自身，无绑定边）。
      std::map<std::string, std::vector<std::string>> producers, consumers;   // 端口名 → 节点
      for (const auto &info : nodes) {
        for (const auto &pn : info.output_ports)
          producers[pn].push_back(info.id);
        for (const auto &pn : info.input_ports)
          if (!info.loop_timer.count(pn) && !info.loop_step.count(pn))   // loop 端口：不构成消费者（无自环绑定边）
            consumers[pn].push_back(info.id);
      }
      std::map<std::string, std::set<std::string>> in_producers, out_consumers;  // 节点 → 一跳邻居
      for (const auto &info : nodes) {
        for (const auto &pn : info.input_ports)
          if (!info.loop_timer.count(pn) && !info.loop_step.count(pn))   // loop 端口：反馈 producer 是自身，不构成拓扑依赖
            for (const auto &p : producers[pn])
              in_producers[info.id].insert(p);
        for (const auto &pn : info.output_ports)
          for (const auto &c : consumers[pn])
            out_consumers[info.id].insert(c);
      }

      // ② 超周期：HP = lcm(全部周期节点的 period)（无周期节点 → hyper_period=0 不回绕）
      {
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
        hyper_period = any_periodic ? (float)hp : 0.0f;
      }

      // ③ 拓扑序（生产者先于消费者，BFS 从源展开；环未覆盖的节点补入末尾——Loop 待扩充）
      std::vector<std::string> topo;
      {
        std::map<std::string, size_t> indeg;
        std::deque<std::string> q;
        for (const auto &info : nodes) {
          indeg[info.id] = in_producers[info.id].size();
          if (in_producers[info.id].empty()) q.push_back(info.id);
        }
        while (!q.empty()) {
          const std::string id = q.front(); q.pop_front();
          topo.push_back(id);
          for (const auto &c : out_consumers[id])
            if (--indeg[c] == 0) q.push_back(c);
        }
        for (const auto &info : nodes)
          if (std::find(topo.begin(), topo.end(), info.id) == topo.end()) topo.push_back(info.id);
      }

      // ④ 实例化：为每个 NodeInfo 构造具体算法实例（内置 delay/ringbuf 或 so 表（入参 so_ctx）
      // 插件 C 工厂）。配置注入 = 遍历 info.config_cache 逐个 configure（位置式解码，顺序 =
      // config "parameters" 数组元素值顺序 = AlgoFunc 配置段相对序号）：AlgoFunc 解码写
      // configs_ 类型化帧（execute 零解析）；ringbuf 位置式 depth/back（第 0/1 位）；
      // delay 的 offset（阻塞时长）由后续超周期展开/调度阶段分析注入、非 JSON 配置——
      // 本阶段无装配点注入 → DelayAlgo offset_ 恒 0（纯透传）。
      // 实例不替换 pipeline.nodes（NodeInfo 是纯数据）——全部 job 实例共享 1 个（连续
      // job precedence 边保证串行），由局部 by_id（节点 → 具体实例）持有。
      std::map<std::string, std::shared_ptr<AlgoBase>> by_id;   // 节点 → 具体算法实例（⑧ 填 job 闭包用）
      std::map<std::string, const NodeInfo *> by_info; // 节点 → NodeInfo（⑤ 反查用）
      for (const auto &info : nodes) {
        const std::string &nm = info.name;
        const std::string key = nm + ":" + info.version;

        // 先查内置工具算法（algo_utility.hpp：delay/ringbuf 数据滞留二件套，框架提供、无需插件），
        // name 直接命中即构造；查不到再走插件 so 表。
        std::shared_ptr<AlgoBase> algo;
        if      (nm == "delay")   algo = std::make_shared<DelayAlgo>();
        else if (nm == "ringbuf") algo = std::make_shared<RingBufferAlgo>();
        else {
          // 遍历 so 表（显式入参 so_ctx）定位算法（[name:version] 落在哪个 so 的 loaded_keys）
          std::shared_ptr<Plugin> pctx;
          for (const auto &[so_path, c] : so_ctx) {
            bool found = false;
            for (const auto &k : c->loaded_keys)
              if (k == key) { pctx = c; found = true; break; }
            if (found) break;
          }
          if (!pctx) throw std::runtime_error("Unregistered algorithm name in map: " + key);

          // C 工厂实例化；shared_ptr 删除器持 ctx → 实例存活期间库不卸载
          algo = std::shared_ptr<AlgoBase>(
              pctx->create_algo(key.c_str()),
              [pctx](AlgoBase *p) { if (pctx->destroy_plugin && p) pctx->destroy_plugin(p); });
        }
        // 配置注入：顺序 = info.config_cache（位置式值表，顺序 = config "parameters" 数组元素
        // 值顺序 = AlgoFunc 配置段相对序号）——AlgoFunc 位置式解码写 configs_（execute 零解析）；
        // ringbuf 位置式 depth/back（第 0/1 位）；delay 的 offset（阻塞时长）由后续超周期展开/
        // 调度阶段分析注入、非 JSON 配置——本阶段无装配点注入 → DelayAlgo offset_ 恒 0（纯透传）。
        for (const auto &v : info.config_cache)
          algo->configure("", v);

        by_id[info.id] = std::move(algo);
        by_info[info.id] = &info;
      }

      // ⑤ 支配周期 + Replication：逐节点定最终执行周期 + job 实例数。
      // 支配原则：后级执行周期默认与前级严格一致——未显式配置 period 的节点继承前级周期
      // （多前级继承最短周期前级）；只有显式配置了 period 才与前级不同（multi-hop 速率变化）。
      // HP 只统计显式配置周期（② 的 lcm），继承周期不计入（但继承值必为某显式周期，整除 HP）。
      std::map<std::string, float> period_final;   // 节点 → 最终执行周期（显式或继承前级）
      std::map<std::string, size_t> node_count;    // 节点 → job 实例数（Replication 展开）
      for (const auto &id : topo) {
        const auto *info = by_info.at(id);
        float T = info->period;                    // 显式配置周期（0 = 未配置 → 走继承）
        if (T <= 0) {
          // 未配置 → 继承前级：多前级取最短周期前级（topo 序保证前级已定最终周期）
          std::string trig;
          float best = 0;
          for (const auto &pn : info->input_ports)
            if (!info->loop_timer.count(pn) && !info->loop_step.count(pn))   // loop 端口：反馈 producer 是自身，不参与继承
              for (const auto &p : producers[pn]) {
                const float pt = period_final.count(p) ? period_final[p] : 0;
                if (trig.empty() || pt < best) { trig = p; best = pt; }
              }
          T = (!trig.empty() && period_final.count(trig)) ? period_final[trig] : 0.0f;
        }
        period_final[id] = T;
        node_count[id] = (T > 0) ? (size_t)(hyper_period / T + 0.5f) : 1;
      }

      // ⑥ 建顶点：每节点展开 node_count 个 job 实例顶点 {id}:{k}（k=0..N-1），载荷 = attrs 基础
      // （period/deadline/wcet）+ abs_deadline 按 job 序排 = 超周期起点 + (k+1)·deadline（无 release/相位）。
      for (const auto &info : nodes) {
        const size_t N = node_count.at(info.id);
        for (size_t k = 0; k < N; ++k) {
          Workload v;
          v.period       = period_final.at(info.id);
          v.deadline     = info.deadline;   // parse 已折到 wcet（缺省 = wcet）
          v.wcet         = info.wcet;
          v.priority     = info.priority;   // grab_ready_workload 选最优先就绪的初值（priority_updater_ 运行时更新）
          v.abs_deadline = hyper_start_ms + (float)(k + 1) * info.deadline;
          dag.add_node(info.id + ":" + std::to_string(k), std::move(v));
        }
      }

      // ⑦ 建边：seq 连续边（同节点实例串行）+ 同名端口直连绑定边。
      //   seq 边：{id}:{k} → {id}:{k+1}，边名 "seq:"+id（Replication 多实例串行——job 实例按
      //   超周期序逐个执行，保证同节点内串行）。
      for (const auto &info : nodes) {
        const size_t N = node_count.at(info.id);
        for (size_t k = 0; k + 1 < N; ++k)
          dag.add_edge(info.id + ":" + std::to_string(k), info.id + ":" + std::to_string(k + 1),
                       "seq:" + info.id, Message{});
      }
      //   绑定边：consumer 每输入端口 pn 绑输出 pn 的唯一 producer 节点（单写者已由 check_topology
      //   保证唯一），整数式 pk=((k+1)·Np-1)/Nc 连 producer:{pk} → consumer:{k}（时段内最新已完成帧，
      //   同速率一一对应；快 producer→慢 consumer 绑末帧；慢 producer→快 consumer 共享帧；恒有边）。
      //   loop 端口（loop_timer/loop_step 中）无绑定边——反馈走运行时数据槽 message_hist_（滑动窗口）。
      std::map<std::string, std::string> producer_of;   // 输出端口名 → producer 节点 id（首生产者）
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

      // ⑧ 填 job 执行体闭包：按 NodeInfo端口序打包 array（inputs[i] ↔ 输入端口序[i]、outputs[i] ↔
      // 输出端口序[i]），AlgoBase execute——配置已建图期注入实例（AlgoFunc configs_ 类型化帧，
      // execute 零解析）。闭包捕获稳定的解析态（shared_ptr<const Pipeline::NodeInfo>，每节点 1 份按 k
      // 共享）+ 具体算法实例（by_id），执行时 Running → 逐输入端口
      // 取绑定边帧（loop 端口从数据槽 message_hist_ 聚合历史帧，pub<vector<Message>> 进对应下标）
      // → execute → 按输出端口序路由 outputs[i] 写下游绑定边 + record_mesg 维护 loop 滑动窗口
      // → Finished。运行时不再接触原始 JSON。
      // hist 长度（写清楚）：mesg_hist_cap[输出端口] = 该端口作为 loop 反馈被消费时 producer 节点的
      // NodeInfo.cap（config 顶层 "cap"，默认 10）。只对 loop 反馈输出端口建历史槽（有消费者才存）；
      // 单写者约束保证每输出端口唯一 producer → 直接赋值（非 max）。message_hist_ 跨重建保留不清。
      for (const auto &info : nodes) {
        for (const auto &[port, _] : info.loop_step)
          mesg_hist_cap[port] = info.cap;
        for (const auto &[port, _] : info.loop_timer)
          mesg_hist_cap[port] = info.cap;
      }
      for (const auto & info : nodes) {
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

    /// 所有 job 顶点是否全部 Finished（无 Pending/Ready/Running 的执行体）——图静止判定。
    /// **无锁原语，前提调用方持 mtx**（range 遍历只读 state，无嵌套 accessor）；
    /// 主线程调度循环持锁调本原语。
    bool is_hp_done() {
      bool idle = true;
      dag.for_each_vertex([&](const std::string &, const Workload &w) {
        if (w.job && w.state != Workload::State::Finished) idle = false;
      });
      return idle;
    }

    /// 超周期回绕（**无锁原语，前提调用方持 mtx**；主线程调度循环图静止时调用）：
    /// hyper_start_ms += hyper_period、job 顶点重置 Pending + abs_deadline 平移一个超周期
    /// （seq 边不闭合，回绕后 {id}:{0} 由 grab_ready_workload 重新拉取）。loop 数据槽跨周期保留不清。
    void rollover_hp() {
      hyper_start_ms += hyper_period;
      dag.for_each_vertex([&](const std::string &, Workload &v) {
        if (v.job) v.state = Workload::State::Pending;  // 有执行体 → 重置待执行
        v.abs_deadline += hyper_period;
      });
    }

    /// 拉取一个最优先就绪的顶点并置 Running（**无锁原语，前提调用方持 mtx**；装配点
    /// on_execute 回调事务内部调用）。
    /// 两趟扫描规避 TBBMap range 遍历锁内嵌套 accessor 死锁（spin_rw_mutex 不可重入）：
    /// ① for_each_vertex（range 遍历持锁）只收集候选 id（Pending + 有执行体），不嵌套 accessor；
    /// ② 逐个候选查前序（in_nodes，const_accessor）全 Finished + priority 最大者。
    /// 无就绪返回 nullptr；有则 mutate_vertex 置 Running + 返回图内顶点指针（含 id/job；
    /// id = expand_hp ⑥ 填的 vtx 名）——装配点锁外执行 w->job() 后回锁直做完成事件
    /// （w->state = Finished + cv.notify_all()，见 on_execute 回调）。
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
  };

  /// 外部回调槽（装配点经 set_wcet_updater 注入；main 主线程调度循环每轮持 mtx 集中更新）。
  inline std::function<void(PrecedenceGraph&)> wcet_updater = nullptr;
  /// 外部回调槽（装配点经 set_priority_updater 注入；main 主线程调度循环每轮持 mtx 集中更新）。
  inline std::function<void(PrecedenceGraph&)> priority_updater = nullptr;

  /// 单份运行图（独立全局）。带锁事务在装配点：worker = thread_pool on_execute 回调（mtx 锁内
  /// grab_ready_workload + 锁外执行 + 回锁直做完成事件 + cv.wait）；main 主线程 = 持锁调度循环——
  /// 每轮集中更新 wcet/priority 回调槽；图静止 is_hp_done() → 有配置 pending 则
  /// pipeline_g.cache.commit() + pipeline_g.parse_pipeline(read()) + check_topology +
  /// expand_hp（pending = false + notify）、有超周期则 rollover_hp 回绕、否则 wait_for
  /// （图更新本就必须静止，故无双缓冲）。
  /// 声明晚于 PrecedenceGraph（依赖完整类型）；job 闭包捕获的 this = graph_g 本实例，
  /// 完成事件由装配点回锁直做落到正确图。
  inline PrecedenceGraph graph_g;

  // 其他的全局单例，单例依赖全局信息，这里无法 include。
  // 依赖方向：这几个单例的 hpp 都 include g_state 拿全局对象；g_state 反向
  // include 会成循环依赖，故此处仅注释，由业务代码在运行时 .instance() 初始化。
  //   PluginLoader        — 依赖 library_g.so_ctx（算法定位表）：loader 只做库机制，
  //                          on_library_add/modify 回调载荷 (so_path, shared_ptr<Plugin>)
  //                          （Plugin 已构造装载）、on_library_delete 载荷 (so_path)——
  //                          本表 SET/ERASE 由装配点回调完成；expand_hp() 经入参 library 定位算法。
  //                          init(plugin_dir) 定目录 + start(num_threads)（唯一参数 =
  //                          文件事件回调派发线程数）起监听。
  //   RPCListener         — 不依赖 g_state：路由回调由装配点注入
  //                          （pipeline_g.cache.write() = json 存缓冲份 + graph_g.pending = true +
  //                          graph_g.cv.notify_all()；parse/check/expand_hp 由 main 主线程调度循环图静止时做）
  //                          init(端点) 定监听/上报端点 + start(num_threads)（唯一参数 =
  //                          HTTP 请求并发线程池大小）启动。
  //   ThreadPool          — 不依赖 g_state：on_execute 装配取任务回调（bool：true 继续取、
  //                          false 退出）；停止由装配点 graph_g.stopped = true + graph_g.cv.notify_all()
  //                          + pool.stop() 协作（见 thread_pool.hpp 停止约定）
  // auto &watcher = PluginLoader::instance();
  // auto &listener = RPCListener::instance();
  // auto &threadPool = ThreadPool::instance();

} // namespace fins::rt
