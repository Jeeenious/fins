/*******************************************************************************
 * priority_updater.hpp — DAG 顶点调度优先级策略：静态 + 动态，自包含多策略
 *
 * 函数槽（include/g_state.hpp）：priority_updater = std::function<int(DAG&, const Workload&)>
 *   —— 顶点 → 调度优先级（int，**值越大越优先**，对齐就绪最大堆）。grab 决策点现算。
 *
 * 本头文件提供两类策略 + 一行装配选择器（对外无复杂数据结构）：
 *   · 静态（字段）：prio_fifo / prio_rm / prio_dm / prio_sjf / prio_ljf / prio_density
 *   · 静态（图结构）：prio_depth / prio_height —— 结构缓存按 version 失效（同 makespan）
 *   · 动态（grab 现算）：prio_edf / prio_llf —— 内部读 now_ms，用 ddl−now 归一化防 int32 溢出
 *   · make_priority(Policy, version_of) → 与槽签名一致的 std::function，装配点一行选策略
 *
 * 数值约定：返回 int，越大越优先；时间量统一 µs 精度（ms×1000 取整）；相对量（ddl−now）
 * 避免绝对时间戳（steady_clock 巨量级）溢出 int32。
 *
 * 依赖：include/g_state.hpp（Workload/Message）、include/utils/form.hpp（DAG）、
 *       include/utils/time.hpp（now_ms）。
 ******************************************************************************/
#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <functional>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "../include/utils/form.hpp"
#include "../include/utils/time.hpp"
#include "../include/g_state.hpp"

namespace fins::sched {

using Dag = fins::util::DirectedAcyclicGraph<fins::rt::Workload, fins::rt::Message>;

// ============================================================================
// 静态策略 · 仅顶点字段（一次赋值即可；值越大越优先）
// ============================================================================

/// FIFO：恒 0 —— 与未注入 priority_updater 的纯 FIFO 行为等价（显式版本）。
inline int prio_fifo(const fins::rt::Workload &w) { (void)w; return 0; }

/// RM（Rate Monotonic，Liu & Layland 1973）：周期越短优先级越高。
inline int prio_rm(const fins::rt::Workload &w) { return -(int)std::llround(w.period * 1000.0); }

/// DM（Deadline Monotonic，Leung & Whitehead 1982）：相对截止期越短优先级越高
/// （RM 对截止期≠周期情形的推广；任意相对截止期下静态最优）。
inline int prio_dm(const fins::rt::Workload &w) { return -(int)std::llround(w.deadline * 1000.0); }

/// SJF（Shortest Job First）：wcet 越小优先级越高（最小化平均响应/周转）。
inline int prio_sjf(const fins::rt::Workload &w) { return -(int)std::llround(w.wcet * 1000.0); }

/// LJF（Longest Job First）：wcet 越大优先级越高（先做重活、尾部并行收尾）。
inline int prio_ljf(const fins::rt::Workload &w) { return (int)std::llround(w.wcet * 1000.0); }

/// HDF（Highest Density First）：密度 = wcet/截止期 越大越优先（负载/截止期比例最紧者先做）。
inline int prio_density(const fins::rt::Workload &w) {
  return (int)std::llround(w.wcet / std::max(1e-9, w.deadline) * 1e6);
}

// ============================================================================
// 静态策略 · 图结构（DAG 感知；结构缓存按 version 失效，同 makespan_updater）
// ============================================================================
namespace detail {

  /** @brief 优先级结构缓存：只缓存**纯结构量**——拓扑深度/高度是未加权量，只依赖边，
   *         跨 rollover 不变，仅随 expand_hp 重建（graph_version）变。 */
  struct PriorityStructure {
    uint64_t ver{0};
    std::unordered_map<std::string, uint32_t> idx;   // job 顶点 id → 稠密下标
    std::vector<uint32_t> order;                     // 拓扑序（稠密）
    std::vector<std::vector<uint32_t>> preds;        // 前驱（深度 DP）
    std::vector<std::vector<uint32_t>> succs;        // 后继（高度 DP）
    std::vector<uint32_t> depth;                     // 拓扑深度（源=0；未加权 → 纯结构可缓存）
    std::vector<uint32_t> height;                    // 到汇距离（汇=0）

    bool empty() const { return order.empty(); }

    void rebuild(Dag &dag) {
      idx.clear(); order.clear(); preds.clear(); succs.clear(); depth.clear(); height.clear();
      const auto is_job = [](const std::string &id) { return id.rfind("tp:", 0) != 0; };

      std::vector<std::string> ids;
      dag.for_each_vertex([&](const std::string &id, const fins::rt::Workload &) {
        if (is_job(id)) ids.push_back(id);
      });
      const size_t n = ids.size();
      if (n == 0) return;

      idx.reserve(n); order.reserve(n); preds.resize(n); succs.resize(n);
      for (size_t i = 0; i < n; ++i) idx.emplace(ids[i], (uint32_t)i);

      std::vector<uint32_t> indeg(n, 0);
      for (size_t i = 0; i < n; ++i) {
        const uint32_t u = (uint32_t)i;
        for (const auto &p : dag.in_nodes(ids[i])) {
          if (!is_job(p)) continue;
          preds[u].push_back(idx.at(p)); ++indeg[u];
        }
        for (const auto &o : dag.out_nodes(ids[i]))
          if (is_job(o)) succs[u].push_back(idx.at(o));
      }

      std::queue<uint32_t> q;                       // Kahn 拓扑序（纯结构，可缓存）
      for (uint32_t u = 0; u < n; ++u) if (indeg[u] == 0) q.push(u);
      while (!q.empty()) {
        const uint32_t u = q.front(); q.pop();
        order.push_back(u);
        for (const uint32_t v : succs[u]) if (--indeg[v] == 0) q.push(v);
      }

      depth.assign(n, 0);
      for (const uint32_t u : order)                // 深度：正向（前驱先于 u）
        for (const uint32_t p : preds[u])
          depth[u] = std::max(depth[u], depth[p] + 1);

      height.assign(n, 0);
      for (auto it = order.rbegin(); it != order.rend(); ++it)   // 高度：反向（后继晚于 u）
        for (const uint32_t s : succs[*it])
          height[*it] = std::max(height[*it], height[s] + 1);
    }
  };

  inline const PriorityStructure &priority_structure(Dag &dag, uint64_t version) {
    static PriorityStructure s;   // 单例：全局唯一 graph_g，main 线程持 mtx 调用
    if (s.ver != version) { s.rebuild(dag); s.ver = version; }
    return s;
  }

} // namespace detail

/// 拓扑深度优先：越深（下游越多）越优先 —— 尽早释放后继，缩短整图关键路径。
inline int prio_depth(Dag &dag, uint64_t version, const fins::rt::Workload &w) {
  const auto &s = detail::priority_structure(dag, version);
  const auto it = s.idx.find(w.id);
  return it == s.idx.end() ? 0 : (int)s.depth[it->second];
}

/// 到汇高度优先：越接近 sink 越优先 —— 收尾阶段让后续无依赖的顶点尽快完成。
inline int prio_height(Dag &dag, uint64_t version, const fins::rt::Workload &w) {
  const auto &s = detail::priority_structure(dag, version);
  const auto it = s.idx.find(w.id);
  return it == s.idx.end() ? 0 : (int)s.height[it->second];
}

// ============================================================================
// 动态策略 · grab 决策点现算（内部读 now_ms；用 ddl−now 归一化防 int32 溢出）
// ============================================================================

/// EDF（Earliest Deadline First，Liu & Layland 1973）：绝对截止期最早者优先。
/// ddl−now = 剩余截止期（ms → µs）；同一 grab 内所有就绪顶点共享同一时间轴 → 单调变换保序。
inline int prio_edf(const fins::rt::Workload &w) {
  return -(int)std::llround((w.ddl - fins::util::now_ms()) * 1000.0);
}

/// LLF（Least Laxity First，Dertouzos 1974；同 LST/MLF）：松弛度 = 剩余截止期 − 剩余执行，
/// 松弛越小越紧急。剩余执行未知 → 用 wcet 保守近似（视为尚未执行）。
inline int prio_llf(const fins::rt::Workload &w) {
  const double laxity = w.ddl - fins::util::now_ms() - w.wcet;
  return -(int)std::llround(laxity * 1000.0);
}

// ============================================================================
// 策略选择器（装配点一行选策略）
// ============================================================================
enum class Policy { FIFO, RM, DM, SJF, LJF, DENSITY, DEPTH, HEIGHT, EDF, LLF };

/** @brief 策略 → 装配函数槽（client 一行装配；version_of 供图结构策略现读结构版本号）。
 *  @param p          策略（Policy 枚举）
 *  @param version_of 结构版本号提供者（DEPTH/HEIGHT 用；其余策略不调用）。client 传
 *                    [&]{ return graph_g.graph_version; }
 *  @param workers_of 线程池 worker 数提供者（DEPTH/HEIGHT 用；其余策略不调用）。client 传
 *                    [&]{ return graph_g.num_workers; }
 *  @retval std::function<int(Dag&, const Workload&)> 与 g_state priority_updater 槽签名一致
 */
inline std::function<int(Dag &, const fins::rt::Workload &)> make_priority(
    Policy p,
    const std::function<uint64_t()> &version_of,
    const std::function<size_t()> &workers_of) {
  switch (p) {
    case Policy::FIFO:    return [](Dag &, const fins::rt::Workload &w) { return prio_fifo(w); };
    case Policy::RM:      return [](Dag &, const fins::rt::Workload &w) { return prio_rm(w); };
    case Policy::DM:      return [](Dag &, const fins::rt::Workload &w) { return prio_dm(w); };
    case Policy::SJF:     return [](Dag &, const fins::rt::Workload &w) { return prio_sjf(w); };
    case Policy::LJF:     return [](Dag &, const fins::rt::Workload &w) { return prio_ljf(w); };
    case Policy::DENSITY: return [](Dag &, const fins::rt::Workload &w) { return prio_density(w); };
    case Policy::DEPTH:   return [version_of](Dag &d, const fins::rt::Workload &w) { return prio_depth(d, version_of(), w); };
    case Policy::HEIGHT:  return [version_of](Dag &d, const fins::rt::Workload &w) { return prio_height(d, version_of(), w); };
    case Policy::EDF:     return [](Dag &, const fins::rt::Workload &w) { return prio_edf(w); };
    case Policy::LLF:     return [](Dag &, const fins::rt::Workload &w) { return prio_llf(w); };
  }
  return [](Dag &, const fins::rt::Workload &) { return 0; };   // 防御：未知策略 → FIFO
}

} // namespace fins::sched
