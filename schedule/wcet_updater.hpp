/*******************************************************************************
 * wcet_updater.hpp — 基于执行历史的 wcet 估计：High-Water-Mark(最高水位) + p 分位数
 *
 * 函数槽签名（include/g_state.hpp）：wcet_updater = std::function<double(std::deque<double>)>
 *   —— 输入某顶点(算法)最近 N 次 execute 耗时（us，exec_us_hist_ 环形队列，键 = 算法键），
 *      输出估计 wcet（ms，直接覆盖 v.wcet 字段）。µs→ms 换算在函数内完成。
 *
 * 状态模式（同 priority/makespan）：
 *   WcetMethod 枚举 + make_wcet_updater(method, margin, p) → 与槽签名一致的 std::function，
 *   装配点一行选方法。底层自包含函数（对外无复杂数据结构）：
 *     wcet_hwm(hist, margin)          — 最高水位：max(hist)·(1+margin)，安全上界
 *     wcet_pquantile(hist, p, margin) — p 分位数·(1+margin)，统计估计，抗尖峰
 *
 * 调用点：PrecedenceGraph::update_wcet_estimation()（rollover_hp 内 #if FINS_CAL_WCET）。
 * 依赖：仅标准库（<deque>/<algorithm>/<vector>/<functional>），不依赖 g_state/form。
 ******************************************************************************/
#pragma once

#include <algorithm>
#include <deque>
#include <functional>
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

  /** @brief wcet 估计方法（装配点一行选）。 */
  enum class WcetMethod { HWM, PQUANTILE };

  /** @brief 方法 → 装配函数槽（与 wcet_updater 槽签名 std::function<double(std::deque<double>)> 一致）。
   *  @param method 估计方法
   *  @param margin 安全裕度（缺省 0.2）
   *  @param p      p 分位数（仅 PQUANTILE 用；缺省 0.99）
   *  @retval std::function<double(std::deque<double>)> 槽函数（µs 历史 → ms wcet）
   */
  inline std::function<double(std::deque<double>)> make_wcet_updater(
      WcetMethod method, double margin = 0.2, double p = 0.99) {
    switch (method) {
      case WcetMethod::HWM:       return [margin](std::deque<double> h) { return wcet_hwm(h, margin); };
      case WcetMethod::PQUANTILE: return [margin, p](std::deque<double> h) { return wcet_pquantile(h, p, margin); };
    }
    return [](std::deque<double>) { return 0.0; };   // 防御：未知方法 → 0（无历史语义）
  }

} // namespace fins::sched
