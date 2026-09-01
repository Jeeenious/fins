/*******************************************************************************
 * wcet_updater.hpp — 基于执行历史的 wcet 估计：High-Water-Mark(最高水位) + p 分位数
 *
 * 函数槽签名（include/g_state.hpp）：wcet_updater = std::function<double(std::deque<double>)>
 *   —— 输入某顶点(算法)最近 N 次 execute 耗时（us，exec_us_hist_ 环形队列，键 = 算法键），
 *      输出估计 wcet（ms，直接覆盖 v.wcet 字段）。µs→ms 换算在函数内完成。
 *
 * 本头文件只提供两个自包含函数（无任何复杂数据结构）：
 *   wcet_hwm(hist, margin)          — 最高水位：max(hist)·(1+margin)，安全上界（不低于观测峰值）
 *   wcet_pquantile(hist, p, margin) — p 分位数·(1+margin)，统计估计，抗尖峰（忽略顶部 (1−p) 稀有超时）
 *
 * 调用点：PrecedenceGraph::update_wcet_estimation()（rollover_hp 内 #if FINS_CAL_WCET）。
 * 依赖：仅标准库（<deque>/<algorithm>/<vector>），不依赖 g_state/form。
 ******************************************************************************/
#pragma once

#include <algorithm>
#include <deque>
#include <vector>

namespace fins::sched {

  /** @brief 最高水位 wcet 估计：wcet = max(hist)·(1+margin)。绝不低估任何已观测到的最坏执行。
   *  @param hist   最近 execute 耗时序列（us；非空，空返回 0）
   *  @param margin 安全裕度（0.2 = 在观测峰值上放 20%）
   *  @retval double 估计 wcet（ms）
   */
  inline double wcet_hwm(const std::deque<double> &hist, double margin = 0.2) {
    if (hist.empty()) return 0.0;
    const double peak = *std::max_element(hist.begin(), hist.end());
    return peak * (1.0 + margin) / 1000.0;   // us → ms
  }

  /** @brief p 分位数 wcet 估计：wcet = q_p(hist)·(1+margin)，q_p 用排序 + 线性插值。
   *         抗尖峰：顶部 (1−p) 的稀有超时(调度抖动/缓存冷)不计入，主体分布 p 分位 + 裕度兜瞬态。
   *  @param hist   最近 execute 耗时序列（us；非空，空返回 0）
   *  @param p      分位数（0.99 = 99% 分位；p≥1 退化到最高水位）
   *  @param margin 安全裕度（0.2）
   *  @retval double 估计 wcet（ms）
   */
  inline double wcet_pquantile(const std::deque<double> &hist, double p = 0.99, double margin = 0.2) {
    if (hist.empty()) return 0.0;
    if (p >= 1.0) return wcet_hwm(hist, margin);
    std::vector<double> v(hist.begin(), hist.end());
    std::sort(v.begin(), v.end());
    const double idx = p * (double)(v.size() - 1);
    const size_t lo = (size_t)idx;
    const size_t hi = std::min(lo + 1, v.size() - 1);
    const double q = v[lo] + (v[hi] - v[lo]) * (idx - (double)lo);   // 线性插值
    return q * (1.0 + margin) / 1000.0;   // us → ms
  }

} // namespace fins::sched
