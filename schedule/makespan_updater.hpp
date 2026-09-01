/*******************************************************************************
 * makespan_updater.hpp — DAG makespan 上界估计：Graham's bound(1979) + Multi-Path Bound(2023)
 *
 * 内部逻辑：
 *   ① Graham's bound（Graham 1979）: R ≤ len(G) + ( vol(G) − len(G) ) / m
 *   ② Multi-Path Bound（He et al. 2023, arXiv:2310.15471 / TCAD'24）:
 *        R ≤ min_{ j∈[0,k] } [ len(G) + ( vol(G) − Σ_{i=0..j} len(λ_i) ) / (m − j) ]
 *      j=0 项即 Graham's bound，j≥1 更紧；路径表用贪心取最长路径（合法安全上界）。
 *   均只统计 job 顶点（跳过 "tp:" 时间点——其 wcet 是释放间隔，不是执行负载）。
 *
 * 结构缓存（核心优化）：
 *   len(G)/vol(G)/路径表都是顶点权(wcet)的函数，而 wcet 每轮自整定(FINS_CAL_WCET)会变 →
 *   **加权量必须每轮现算**；但拓扑序/前驱表/权重源指针是**纯结构量**，跨 rollover 不变 →
 *   只在结构版本号 version 变化时重建一次，之后每轮只跑数组版 DP（无字符串、无并发 find、
 *   无按值分配）。version = g_state.hpp 的 PrecedenceGraph::graph_version（expand_hp 重建后 ++）。
 *
 * 对外接口（对外仍是两个函数，无复杂数据结构暴露）：
 *   graham_makespan(dag, version, m) — Graham's bound（ms）
 *   mpb_makespan(dag, version, m)    — Multi-Path Bound（ms，支配 Graham；装配点默认用它）
 *
 * 依赖：include/g_state.hpp（Workload/Message/PrecedenceGraph）、include/utils/form.hpp（DAG）。
 ******************************************************************************/
#pragma once

#include <algorithm>
#include <cstdint>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "../include/utils/form.hpp"
#include "../include/g_state.hpp"

namespace fins::sched {

namespace detail {

  /** @brief makespan 结构缓存：只缓存**纯结构量**（与权重无关，跨 rollover 不变）。
   *         权重(wcet)每轮经 wsrc 指针现读——wcet 自整定在顶点对象原位改，指针恒有效。 */
  struct MakespanStructure {
    uint64_t ver{0};                                  // 命中版本号（结构缓存的失效信号）
    std::unordered_map<std::string, uint32_t> idx;    // job 顶点 id → 稠密下标
    std::vector<uint32_t> order;                      // 拓扑序（稠密下标）
    std::vector<std::vector<uint32_t>> preds;         // 每个顶点的 job 前驱（稠密下标）
    std::vector<const fins::rt::Workload *> wsrc;     // 稠密下标 → 顶点载荷指针（现读 wcet，零哈希）

    bool empty() const { return order.empty(); }

    /** @brief 从 dag 重建结构（仅结构量；权重源存指针、每轮现读）。 */
    void rebuild(fins::util::DirectedAcyclicGraph<fins::rt::Workload, fins::rt::Message> &dag) {
      idx.clear(); order.clear(); preds.clear(); wsrc.clear();
      const auto is_job = [](const std::string &id) { return id.rfind("tp:", 0) != 0; };

      std::vector<std::string> ids;
      dag.for_each_vertex([&](const std::string &id, const fins::rt::Workload &) {
        if (is_job(id)) ids.push_back(id);
      });
      const size_t n = ids.size();
      if (n == 0) return;

      idx.reserve(n); order.reserve(n); preds.resize(n); wsrc.resize(n);
      for (size_t i = 0; i < n; ++i) idx.emplace(ids[i], (uint32_t)i);

      std::vector<std::vector<uint32_t>> succ(n);   // 后继（仅 Kahn 用，用完即弃）
      std::vector<uint32_t> indeg(n, 0);
      for (size_t i = 0; i < n; ++i) {
        const std::string &id = ids[i];
        const uint32_t u = (uint32_t)i;
        wsrc[u] = &dag.vertex(id);
        for (const auto &p : dag.in_nodes(id)) {          // 前驱（仅 job 顶点间边）
          if (!is_job(p)) continue;
          const uint32_t v = idx.at(p);
          preds[u].push_back(v); ++indeg[u];
        }
        for (const auto &o : dag.out_nodes(id)) {         // 后继
          if (!is_job(o)) continue;
          succ[u].push_back(idx.at(o));
        }
      }

      std::queue<uint32_t> q;                             // Kahn 拓扑序（纯结构，可缓存）
      for (uint32_t u = 0; u < n; ++u) if (indeg[u] == 0) q.push(u);
      while (!q.empty()) {
        const uint32_t u = q.front(); q.pop();
        order.push_back(u);
        for (const uint32_t v : succ[u]) if (--indeg[v] == 0) q.push(v);
      }
    }
  };

  inline const MakespanStructure &ensure_structure(
      fins::util::DirectedAcyclicGraph<fins::rt::Workload, fins::rt::Message> &dag, uint64_t version) {
    static MakespanStructure s;   // 单例：全局唯一 graph_g，main 线程持 mtx 调用
    if (s.ver != version) { s.rebuild(dag); s.ver = version; }
    return s;
  }

  /** @brief 现读权重（wcet 字段单位 ms，直接用）。 */
  inline std::vector<double> read_weights(const MakespanStructure &s) {
    std::vector<double> w(s.order.size());
    for (size_t i = 0; i < s.order.size(); ++i) w[i] = s.wsrc[i]->wcet;
    return w;
  }

  /** @brief 数组版 len(G)/vol(G)：沿缓存拓扑序 + 前驱表，纯数组读写（无字符串/并发 find）。 */
  inline void len_vol(const MakespanStructure &s, const std::vector<double> &w, double &lenG, double &vol) {
    const size_t n = s.order.size();
    std::vector<double> cp(n, 0.0);
    lenG = 0.0; vol = 0.0;
    for (const uint32_t u : s.order) {
      double best = 0.0;
      for (const uint32_t p : s.preds[u]) best = std::max(best, cp[p]);
      cp[u] = best + w[u];
      vol += w[u];
      lenG = std::max(lenG, cp[u]);
    }
  }

} // namespace detail

  /** @brief Graham's bound（1979）：R ≤ len(G) + (vol(G) − len(G)) / m。
   *  @param dag     当前 precedence graph（job 实例级 DAG）
   *  @param version 图结构版本号（PrecedenceGraph::graph_version；结构缓存的失效信号）
   *  @param m       worker 数（并行核数）
   *  @retval double makespan 上界（ms；无 job 顶点返回 0）
   */
  inline double graham_makespan(fins::util::DirectedAcyclicGraph<fins::rt::Workload, fins::rt::Message> &dag,
                                uint64_t version, int m) {
    const auto &s = detail::ensure_structure(dag, version);
    if (s.empty()) return 0.0;
    const auto w = detail::read_weights(s);
    double lenG, vol;
    detail::len_vol(s, w, lenG, vol);
    return lenG + (vol - lenG) / std::max(1, m);
  }

  /** @brief Multi-Path Bound makespan 上界（He et al. 2023）；j=0 项即 Graham's bound，j≥1 更紧。
   *         结构缓存命中时每轮只跑数组版 DP + 贪心（无字符串/并发 find/按值分配）。
   *  @param dag     当前 precedence graph（job 实例级 DAG）
   *  @param version 图结构版本号（PrecedenceGraph::graph_version；结构缓存的失效信号）
   *  @param m       worker 数（并行核数）
   *  @retval double makespan 上界（ms；无 job 顶点返回 0）
   */
  inline double mpb_makespan(fins::util::DirectedAcyclicGraph<fins::rt::Workload, fins::rt::Message> &dag,
                             uint64_t version, int m) {
    const auto &s = detail::ensure_structure(dag, version);
    if (s.empty()) return 0.0;
    const auto w = detail::read_weights(s);
    double lenG, vol;
    detail::len_vol(s, w, lenG, vol);

    const size_t n = s.order.size();
    std::vector<double> path_len;
    std::vector<uint8_t> removed(n, 0);
    size_t removed_cnt = 0;
    const int max_paths = std::max(1, std::min(m, (int)n));

    // 贪心取互不相交最长路径（首条 = 关键路径）；数组版，每轮复用 cp2/parent 缓冲
    std::vector<double> cp2(n, 0.0);
    std::vector<int32_t> parent(n, -1);
    for (int k = 0; k < max_paths && removed_cnt < n; ++k) {
      double best = 0.0;
      int32_t best_end = -1;
      for (const uint32_t u : s.order) {
        if (removed[u]) { cp2[u] = 0.0; continue; }   // 已移除：清旧值防下轮误用（其后续顶点不再引用）
        double bestp = 0.0;
        int32_t bp = -1;
        for (const uint32_t p : s.preds[u]) {
          if (removed[p]) continue;
          if (cp2[p] > bestp) { bestp = cp2[p]; bp = (int32_t)p; }
        }
        cp2[u] = bestp + w[u];
        parent[u] = bp;
        if (cp2[u] > best) { best = cp2[u]; best_end = (int32_t)u; }
      }
      if (best <= 0.0) break;
      path_len.push_back(best);
      for (int32_t cur = best_end; cur != -1; cur = parent[cur]) {
        if (removed[(size_t)cur]) break;   // 保险：路径顶点应均未移除（无环）
        removed[(size_t)cur] = 1; ++removed_cnt;
      }
    }

    // 公式 (9)：min_j [ lenG + (vol − Σ_{i≤j} lenλ_i) / (m − j) ]
    double bound = lenG + vol / std::max(1, m);   // 兜底（无路径 = 宽松 Graham 项）
    double covered = 0.0;
    for (size_t j = 0; j < path_len.size(); ++j) {
      covered += path_len[j];
      const int denom = m - (int)j;
      if (denom <= 0) break;
      bound = std::min(bound, lenG + (vol - covered) / denom);
    }
    return bound;
  }

} // namespace fins::sched
