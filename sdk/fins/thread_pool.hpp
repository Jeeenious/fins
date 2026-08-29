#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#pragma once

// ============================================================================
// ThreadPool — 绑核自调度线程池（单例）
// ============================================================================
//
// 内部逻辑：
//   n 个 worker 线程，worker i 绑定 core (i+1)（跳过 core 0，留给系统中断/管理）。
//   每个 worker 的自调度循环：
//     while (!stop_.load()) { if (!cb_) break; if (!cb_(idx)) break; }
//   每轮调用装配点注册的取任务回调 cb_（bool），由它自己完成"取任务 + 执行"：
//   典型装配 = 装配点写带锁单步事务（g_state.hpp 的 PrecedenceGraph 无锁原语 + 公开成员锁）：
//   持 mtx：stopped.load() 检查 → grab_ready_workload() 锁内纯拉取（**不回绕**）从运行图拉取
//   该顶点 Workload*（含 id/job，grab 置 Running）→ 锁外 w->job() 执行 → 回锁直做完成事件
//   （w->state = Finished + notify_all）；无可拉 cv.wait()；
//   **worker 只拉取执行，不做任何启停判断**——图静止回绕（rollover_hp）与新配置重建
//   （commit JSON → parse pipeline_g → expand_hp）全由 main 主线程调度循环集中判断（见 g_state.hpp）。
//   cb_ 返回 false（无更多工作，通常停止）→ worker 退出循环。cb_ 为空（未注册）时直接退出。
//   本组件不感知任何图/任务队列、不引用 g_state，也不知道任务类型——拉取对象是运行图顶点
//   Workload（g_state.hpp 定义，job 成员 = 执行体闭包），装配点经 on_execute 回调事务
//   grab_ready_workload 从图拉取，worker 经 cb_ 取到 Workload 后调用其 job()。本文件不定义 Job 别名
//   （与 g_state.hpp 的 `using Job = std::function<void()>` 同命名空间同型，重定义会冲突）。
//
// 停止约定（解耦后线程池不触碰 g_state 的调度停止位，由装配点置）：
//   worker 阻塞在 cb_ 内的 cv.wait（on_execute 锁循环）上时，若只置 pool 的 stop_，
//   wait 不会返回，join 将永不返回。正确停止顺序：
//     1. 装配点置调度停止位并唤醒：graph_g.stopped = true; graph_g.cv.notify_all();
//        （g_state.hpp）——on_execute 回调检查 stopped.load() 为 true 返回 false，cb_ 据此退出循环；
//     2. 再调 pool.stop()——置 stop_ 并 join 回收 worker（worker 检查 stop_ 退出循环）。
//   若只调 pool.stop() 而不先唤醒 on_execute 回调的 wait，worker 仍阻塞在 wait 上，join 永不返回。
//
// 调度模式（非抢占、完成事件驱动 + 任务拉取）：
//   worker 自调度且非抢占——执行完当前任务后才回到 cb_ 取下一个任务，绝不中途打断
//   正在执行的任务。任务的优先级选择在 DAG 侧完成（on_execute 回调事务内 grab_ready_workload()
//   锁内拉取时选 priority 最大就绪才返回），线程池本身不做调度决策。
//   start() 会等待全部 worker 绑定核并进入就绪态后才返回，保证投递任务时所有 worker
//   已在线，避免后启动的 worker 抢不到任务。
//
// worker 单任务生命周期（时间线，供计时/测量口径参考）：
//   main() 调度循环 push 入队(t0) ── worker pop 取到 ── j() 回调开始(t_now) ── 执行 ── 回调返回
//   · 纯排队延迟  = t_now - t0     ：任务在就绪队列里等 worker 的时长（任务视角）
//   · 相邻任务间隔 = 本次 t_now - 上次回调返回时刻 ：worker 干完上一个到开工下一个
//     的间隔，含 pop 等待 + 唤醒/切换开销（worker 视角）
//
// 绑核说明：
//   worker i 通过 pthread_setaffinity_np 绑定到 core (i+1)，跳过 core 0（留给系统中断 /
//   管理任务）。绑核保证该 worker 线程只在本核运行，不会被系统调度到其他核。
//   注意：绑核只约束本进程线程，其他进程的线程仍可能被调度进这些核。
//   如需彻底隔离（"避免 core 被其他任务占用"），配合内核 isolcpus 预留核心，
//   或将 worker 提升为 SCHED_FIFO 实时调度类。
//
// 资源消耗：
//   - 线程：n 个 worker = n 个线程，各占 1 个核（core 1..n）
//   - 空闲时 worker 阻塞在取任务回调内的就绪条件变量上（ready_cv().wait），不忙等、不占 CPU
//   - 任务由运行图持有（on_execute 回调事务 grab_ready_workload 拉取），无独立任务队列
//   - 回调由 worker 串行执行，任务体自身开销由业务决定
//   - 停止：1 个 atomic 停止位（stop_），join 时不新增线程
//
// 对外接口：
//   instance()                          — 单例
//   on_execute(cb)                    — 注册取任务回调（bool，须在 start() 前）；
//                                          cb: (int wid) -> bool，wid = 线程序号（worker i →
//                                          wid=i，可用于按线程独立统计/绑核信息）；返回 true 继续
//                                          取下一任务、false（无更多工作，通常停止）退出；
//                                          典型装配 = 装配点写带锁单步事务（on_execute 回调内
//                                          持 graph_g.mtx：stopped.load() 检查 → grab_ready_workload 锁内
//                                          拉取（返回 Workload*）→ 锁外执行 → 回锁直做完成事件
//                                          （w->state = Finished + notify）→ 无可拉
//                                          cv.wait；stopped.load() 为 true 返回 false 退出），
//                                          锁/停止检测/等待经公开成员（mtx/stopped/cv）由回调自持
//                                          （worker 不做启停判断，见装配示例）
//   start(num_workers)                  — 启动 n 个 worker 绑 core 1..n；阻塞至全部就绪
//   stop()                              — 置停止位并回收 worker 线程（须先置调度停止位并唤醒
//                                          on_execute 回调的 wait，见停止约定）；析构时自动调用
//   装配示例（业务方 main 中）：
//     ThreadPool::instance().on_execute([](int /*wid*/) -> bool {
//       std::unique_lock lk(graph_g.mtx);
//       for (;;) {
//         if (graph_g.stopped.load()) return false;    // 停止 → 退出 worker 循环
//         if (Workload *w = graph_g.grab_ready_workload()) {   // 锁内拉取（置 Running，返回图内顶点指针，不回绕）
//           lk.unlock(); w->job(); lk.lock();          // 锁外执行 → 回锁
//           w->state = Workload::State::Finished;      // 直做完成事件（改图内真实顶点）
//           graph_g.cv.notify_all();
//           return true;
//         }
//         graph_g.cv.wait(lk);                         // 无可拉 → 等完成/回绕/expand_hp/停止
//       }
//     });
//     ThreadPool::instance().start(2);
//     // main 主线程调度循环（装配点）：图静止 → 新配置 expand_hp / 超周期回绕 / 等待（见 g_state.hpp）
//     // 退出：graph_g.stopped = true; graph_g.cv.notify_all(); ThreadPool::instance().stop();
// ============================================================================

#include <pthread.h>
#include <sched.h>

#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "utils/logger.hpp"

namespace fins::rt {
  /// 绑核自调度线程池（单例）：worker 经装配点注册的取任务回调取任务并执行，
  /// 不依赖 g_state（不引用任务队列 / 停止位，见停止约定）。
  class ThreadPool {
  private:
    std::vector<std::thread> workers_;
    std::condition_variable ready_cv_;
    std::mutex ready_mtx_;
    int workers_ready_ = 0;
    /// 取任务回调：worker 每轮调它完成"取任务 + 执行"。由装配点在 main 中通过
    /// on_execute() 注册（典型：装配点写带锁单步事务——持 graph_g.mtx：stopped.load() 检查 →
    /// grab_ready_workload 拉取 → 锁外执行 → 回锁直做完成事件 → 无可拉 cv.wait；线程池不直接引用图/任务源）。
    /// 参数 = 线程序号 wid（worker i → wid=i）；返回 bool：true 继续取下一任务；
    /// false = 无更多工作（停止）→ worker 退出循环。
    std::function<bool(int)> cb_;
    /// 线程池自身的停止位
    /// worker 每轮检查，置位后退出循环。
    /// 调度停止位（PrecedenceGraph 公开成员 stopped）仍由装配点经 graph_g.stopped = true +
    /// graph_g.cv.notify_all() 置并唤醒，两者分工见头部"停止约定"。
    std::atomic<bool> stop_{false};

  public:
    static ThreadPool &instance() {
      static ThreadPool inst;
      return inst;
    }

    /// 注册取任务回调（bool，须在 start() 前调用）：worker 循环每轮调用 cb()
    /// 一次，由回调自己完成"取任务 + 执行"（典型：装配点写带锁单步事务——持 graph_g.mtx：
    /// stopped.load() 检查 → grab_ready_workload 拉取 → 锁外执行 → 回锁直做完成事件 → 无可拉 cv.wait）。
    /// 回调参数 = 线程序号 wid（worker i → wid=i，供按线程独立统计用）；返回 true 继续取下一任务；
    /// 返回 false = 无更多工作（停止）→ worker 退出循环。
    void on_execute(std::function<bool(int)> cb) {
      cb_ = std::move(cb);
    }

    /// 启动 num_workers 个 worker，worker i 绑定 core (i+1)。
    /// 阻塞直到全部 worker 绑定核并进入就绪态（随后在取任务回调上等待任务），
    /// 保证返回后投递的任务能被所有 worker 公平竞争，而非被先启动的 worker 独占。
    void start(int num_workers) {
      if (!workers_.empty()) {
        FINS_LOG_WARN("[ThreadPool] already started, ignore.");
        return;
      }
      {
        std::unique_lock lk(ready_mtx_);
        workers_ready_ = 0;
        for (int i = 0; i < num_workers; ++i)
          workers_.emplace_back([this, i] { working(i); });
        ready_cv_.wait(lk, [this, num_workers] { return workers_ready_ >= num_workers; });
      }
    }

    /// 置停止位并回收 worker 线程（join 已退出的 worker）。只负责本类线程与自己的
    /// 停止位，不置调度停止位——调度停止位须由装配点在调用前
    /// graph_g.stopped = true; graph_g.cv.notify_all(); 置位并唤醒，否则 worker 仍阻塞在
    /// 取任务回调内的 cv.wait 上，join 永不返回（见头部"停止约定"）。析构时自动调用。
    void stop() {
      stop_ = true;
      for (auto &w : workers_)
        if (w.joinable()) w.join();
      workers_.clear();
    }

  private:
    ThreadPool() = default;
    /// 析构自清理：stop() 置停止位并 join 回收 worker；调度停止位须由装配点在退出前
    /// 置位并唤醒（graph_g.stopped = true + graph_g.cv.notify_all()），否则 worker 阻塞在
    /// 取任务回调内的 cv.wait 上，join 永不返回。
    ~ThreadPool() {
      stop();
    }
    ThreadPool(const ThreadPool &) = delete;
    ThreadPool &operator=(const ThreadPool &) = delete;

    void working(int idx) {
      bind_core(idx + 1);  // 跳过 core 0
      FINS_LOG_INFO("[ThreadPool] worker {} -> core {}", idx, idx + 1);

      {  // 宣布就绪：start() 据此得知所有 worker 已绑定核、即将在取任务回调上等待
        std::lock_guard lk(ready_mtx_);
        ++workers_ready_;
      }
      ready_cv_.notify_all();

      while (!stop_.load()) {
        if (!cb_) break;      // 空回调（未注册）直接退出
        try {
          if (!cb_(idx)) break;  // 回调返回 false → 无更多工作 → 退出（idx = 线程序号）
        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[ThreadPool] worker {} callback threw: {}", idx, e.what());
        } catch (...) {
          FINS_LOG_ERROR("[ThreadPool] worker {} callback threw unknown exception", idx);
        }
      }
    }

    static void bind_core(int core) {
      cpu_set_t set;
      CPU_ZERO(&set);
      CPU_SET(core, &set);
      if (pthread_setaffinity_np(pthread_self(), sizeof(set), &set) != 0)
        FINS_LOG_WARN("[ThreadPool] bind core {} failed: {}", core, strerror(errno));
    }
  };

} // namespace fins::rt
