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
 * 对外接口：
 *   graham_makespan(dag, m) — Graham's bound（ms）
 *   mpb_makespan(dag, m)    — Multi-Path Bound（ms，支配 Graham；装配点默认用它）
 *
 * 依赖：include/g_state.hpp（Workload/Message）、include/utils/form.hpp（DAG）。
 ******************************************************************************/
#pragma once

#include <algorithm>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "../include/utils/form.hpp"
#include "../include/g_state.hpp"

namespace fins::sched {

  /** @brief Graham's bound（1979）：R ≤ len(G) + (vol(G) − len(G)) / m。
   *  @param dag 当前 precedence graph（job 实例级 DAG）
   *  @param m   worker 数（并行核数）
   *  @retval double makespan 上界（ms；无 job 顶点返回 0）
   */
  inline double graham_makespan(fins::util::DirectedAcyclicGraph<fins::rt::Workload, fins::rt::Message> &dag, int m) {
    const auto is_job = [](const std::string &id) { return id.rfind("tp:", 0) != 0; };

    // job 顶点集 + 拓扑序（Kahn，仅 job 顶点间边）
    std::vector<std::string> ids;
    dag.for_each_vertex([&](const std::string &id, const fins::rt::Workload &) {
      if (is_job(id)) ids.push_back(id);
    });
    if (ids.empty()) return 0.0;

    std::map<std::string, size_t> indeg;
    std::map<std::string, std::vector<std::string>> succ;
    for (const auto &id : ids) {
      for (const auto &p : dag.in_nodes(id)) if (is_job(p)) ++indeg[id];
      for (const auto &o : dag.out_nodes(id)) if (is_job(o)) succ[id].push_back(o);
    }
    std::vector<std::string> order;
    {
      std::map<std::string, size_t> deg = indeg;
      std::queue<std::string> q;
      for (const auto &id : ids) if (deg[id] == 0) q.push(id);
      while (!q.empty()) {
        const std::string u = q.front(); q.pop();
        order.push_back(u);
        for (const auto &v : succ[u]) if (--deg[v] == 0) q.push(v);
      }
    }

    // len(G) 关键路径 + vol(G)（拓扑序 DP）
    double lenG = 0.0, vol = 0.0;
    std::map<std::string, double> cp;
    for (const auto &id : order) {
      double best = 0.0;
      for (const auto &p : dag.in_nodes(id)) if (is_job(p)) best = std::max(best, cp[p]);
      cp[id] = best + dag.vertex(id).wcet;
      vol += dag.vertex(id).wcet;
      lenG = std::max(lenG, cp[id]);
    }

    return lenG + (vol - lenG) / std::max(1, m);
  }

  /** @brief Multi-Path Bound makespan 上界（He et al. 2023）；j=0 项即 Graham's bound，j≥1 更紧。
   *  @param dag 当前 precedence graph（job 实例级 DAG）
   *  @param m   worker 数（并行核数）
   *  @retval double makespan 上界（ms；无 job 顶点返回 0）
   */
  inline double mpb_makespan(fins::util::DirectedAcyclicGraph<fins::rt::Workload, fins::rt::Message> &dag, int m) {
    const auto is_job = [](const std::string &id) { return id.rfind("tp:", 0) != 0; };

    // job 顶点集 + 拓扑序（Kahn，仅 job 顶点间边）
    std::vector<std::string> ids;
    dag.for_each_vertex([&](const std::string &id, const fins::rt::Workload &) {
      if (is_job(id)) ids.push_back(id);
    });
    const size_t n = ids.size();
    if (n == 0) return 0.0;

    std::map<std::string, size_t> indeg;
    std::map<std::string, std::vector<std::string>> succ;
    for (const auto &id : ids) {
      for (const auto &p : dag.in_nodes(id)) if (is_job(p)) ++indeg[id];
      for (const auto &o : dag.out_nodes(id)) if (is_job(o)) succ[id].push_back(o);
    }
    std::vector<std::string> order;
    {
      std::map<std::string, size_t> deg = indeg;
      std::queue<std::string> q;
      for (const auto &id : ids) if (deg[id] == 0) q.push(id);
      while (!q.empty()) {
        const std::string u = q.front(); q.pop();
        order.push_back(u);
        for (const auto &v : succ[u]) if (--deg[v] == 0) q.push(v);
      }
    }

    // len(G) 关键路径 + vol(G)（拓扑序 DP）
    double lenG = 0.0, vol = 0.0;
    std::map<std::string, double> cp;
    for (const auto &id : order) {
      double best = 0.0;
      for (const auto &p : dag.in_nodes(id)) if (is_job(p)) best = std::max(best, cp[p]);
      cp[id] = best + dag.vertex(id).wcet;
      vol += dag.vertex(id).wcet;
      lenG = std::max(lenG, cp[id]);
    }

    // 贪心取互不相交最长路径（generalized path list；首条 = 关键路径）
    std::vector<double> path_len;
    std::set<std::string> removed;
    const int max_paths = std::max(1, std::min(m, (int)n));
    for (int k = 0; k < max_paths && removed.size() < n; ++k) {
      std::map<std::string, double> cp2;
      std::map<std::string, std::string> parent;
      double best = 0.0;
      std::string best_end;
      for (const auto &id : order) {
        if (removed.count(id)) continue;
        double bestp = 0.0;
        std::string bp;
        for (const auto &p : dag.in_nodes(id)) {
          if (!is_job(p) || removed.count(p)) continue;
          if (cp2[p] > bestp) { bestp = cp2[p]; bp = p; }
        }
        cp2[id] = bestp + dag.vertex(id).wcet;
        parent[id] = bp;
        if (cp2[id] > best) { best = cp2[id]; best_end = id; }
      }
      if (best <= 0.0) break;
      path_len.push_back(best);
      for (std::string cur = best_end; !cur.empty(); cur = parent[cur]) removed.insert(cur);
    }

    // 公式 (9)：min_j [ lenG + (vol − Σ_{i≤j} lenλ_i) / (m − j) ]
    double bound = lenG + vol / std::max(1, m);   // 兜底
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
