/*******************************************************************************
 * tracing.hpp — 实时调度 tracing：worker 生命周期事件流（唤醒/释放/执行/结束/休眠）
 *
 * 内部逻辑：
 *   全局 TBB 并发队列 trace_q：[tid] → 该线程的事件序列（deque<TraceEvent>）。
 *   每条事件 = 类型 + 单调时刻（µs）+ 核心号 + 附加标识（job 顶点 id / tp id，可空）。
 *   各线程只写自己的槽（trace_record(kind, tag) 自动以当前线程 OS 线程号为键）→ 无跨线程竞争；
 *   导出 trace_export(path) 写 CSV（tid,seq,kind,t_us,cpu,tag）后清空队列。
 *   本文件不自带开关——由调用方（装配点）按需调用；不调用则零开销。
 *
 * 事件类型（trace_kind_name）：
 *   WAKE     唤醒：worker 从 cv.wait 醒来（含首轮进入循环 = 线程启动）
 *   RELEASE  释放：grab_ready_workload 取到 job（tag = 顶点 id，如 cam:0）
 *   EXECUTE  执行：job 闭包开始（锁外）
 *   END      结束：job 闭包返回（锁外）
 *   SLEEP    休眠：worker 进 cv.wait_for 前
 *
 * 对外接口：
 *   trace_record(kind, tag)       追加一条事件到当前线程队列（键 = 当前线程 tid；自动记 cpu）
 *   trace_export(path)            导出 CSV（tid,seq,kind,t_us,cpu,tag）+ 清空队列
 *   trace_now_us()                当前单调时刻（µs，相对 trace 起始基准，steady_clock）
 *   trace_tid()                   当前线程 OS 线程号（gettid，thread_local 缓存）
 *   trace_cpu()                   当前所在 CPU 核心号（sched_getcpu）
 ******************************************************************************/
#pragma once

#include <chrono>
#include <cstdint>
#include <deque>
#include <fstream>
#include <sched.h>   // sched_getcpu（core id）
#include <string>
#include <unistd.h>
#include <sys/syscall.h>

#include "form.hpp"

namespace fins::util {

  /** @brief 事件类型。 */
  enum class TraceKind : uint8_t {
    WAKE      = 0,        // 唤醒
    RELEASE   = 1,        // 释放（job 取到）
    EXECUTE   = 2,        // 执行（job 开始）
    COMPLETE  = 3,        // 结束（job 完成）
    FINISHED  = 4,        // 完成（job 完成，置 done + 传播 pred_left + 入 ready）
    SLEEP     = 5,        // 休眠（进 cv.wait）
  };

  inline const char *trace_kind_name(TraceKind k) {
    switch (k) {
      case TraceKind::WAKE:      return "wake";
      case TraceKind::RELEASE:   return "release";
      case TraceKind::EXECUTE:   return "execute";
      case TraceKind::COMPLETE:  return "complete";
      case TraceKind::FINISHED:  return "finished";
      case TraceKind::SLEEP:     return "sleep";
    }
    return "?";
  }

  /** @brief 单条事件：类型 + 单调时刻（µs）+ 核心号 + 附加标识（job 顶点 id，可空）。
   *  线程号即 trace_q 键（CSV 第一列），事件内不再冗余存 tid。 */
  struct TraceEvent {
    TraceKind kind;
    int64_t t_us;
    int cpu;          // 记录事件时所在 CPU 核心号（sched_getcpu）
    std::string tag;
  };

  /** @brief 全局 trace 队列：[wid] → 事件序列（inline 变量跨翻译单元共享一份）。 */
  inline TBBMap<std::deque<TraceEvent>> trace_q;

  /** @brief trace 起始基准（µs，steady_clock 自系统启动；函数局部 static 一次初始化，
   *  跨翻译单元/worker 共享同一基准 → 各 wid 时刻同源可比较）。 */
  inline int64_t trace_base_us() {
    static const int64_t base = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    return base;
  }

  /** @brief 当前单调时刻（µs，相对 trace 起始基准）。int64 无溢出风险：
   *  相对值天然小；即使记绝对 epoch µs，也要数百世纪才达 int64 上限（约 9.2e18）。 */
  inline int64_t trace_now_us() {
    return std::chrono::duration_cast<std::chrono::microseconds>(
               std::chrono::steady_clock::now().time_since_epoch()).count()
           - trace_base_us();
  }

  /** @brief 当前线程的 OS 线程号（gettid，Linux）。thread_local 缓存——每线程首次调用
   *  一次 syscall，之后直接取缓存，零重复开销。用于把 trace 事件和
   *  `top -H` / `perf` / `/proc/<pid>/task/<tid>` 等 OS 观测对上号。
   * @retval int64_t 当前线程 tid（syscall 失败返回 0） */
  inline int64_t trace_tid() {
    static thread_local const auto tid = static_cast<int64_t>(::syscall(SYS_gettid));
    return tid;
  }

  /** @brief 当前线程所在的 CPU 核心号（sched_getcpu，vDSO 级开销）。worker 绑核后
   *  恒等于绑定的核；未绑/可迁移线程反映实际运行核——用于核对绑核是否生效、检测迁移。
   * @retval int 核心号（失败返回 -1） */
  inline int trace_cpu() {
    return ::sched_getcpu();
  }

  /** @brief 追加一条事件到该 wid 队列（TBB accessor 按 wid 锁，各 worker 无竞争）。
   * @param kind 事件类型
   * @param tag  附加标识（job 顶点 id，可空）
   * @retval 无
   */
  inline void trace_record(TraceKind kind, const std::string &tag = "") {
    TBBMAP_UPDATE(trace_q, std::to_string(trace_tid()), [&](auto &q) { q.push_back({kind, trace_now_us(), trace_cpu(), tag}); });
  }

  /** @brief 导出全部线程的事件流为 CSV（tid,seq,kind,t_us,cpu,tag），导出后清空队列。
   * @param path CSV 输出路径（相对 cwd 或绝对）
   * @retval 无
   */
  inline void trace_export(const std::string &path) {
    std::ofstream ofs(path);
    ofs << "tid,seq,kind,t_us,cpu,tag\n";
    for (auto &kv : trace_q.range()) {
      const std::string &tid = kv.first;
      const auto &q = kv.second;
      size_t seq = 0;
      for (const auto &e : q)
        ofs << tid << ','
            << (seq++) << ','
            << trace_kind_name(e.kind) << ','
            << e.t_us << ','
            << e.cpu << ','
            << e.tag << '\n';
    }
    ofs.close();
    trace_q.clear();
  }

} // namespace fins::util
