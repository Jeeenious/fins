#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#pragma once

// ============================================================================
// ThreadPool — 绑核自调度线程池（单例）
// ============================================================================

#include <pthread.h>
#include <sched.h>

#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <cstring>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "utils/logger.hpp"

namespace fins::rt {
  /// 设置当前线程名（comm）。仅影响可观测性（sched_switch prev_comm/next_comm、top -H、
  /// ps -T、perf、gdb），失败（如名字超长 / 权限）只告警，不影响调度正确性。
  static void set_thread_name(const std::string &name) {
    if (pthread_setname_np(pthread_self(), name.c_str()) != 0)
      FINS_LOG_WARN("[ThreadPool] set thread name '{}' failed: {}", name, strerror(errno));
  }

  static void bind_core(int core) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    if (pthread_setaffinity_np(pthread_self(), sizeof(set), &set) != 0)
      FINS_LOG_WARN("[ThreadPool] bind core {} failed: {}", core, strerror(errno));
  }

  /// 实时调度类（SCHED_FIFO）：worker 绑 1..num_workers、非 worker 线程（主循环/计时/组件）绑
  /// 控制核（见 example/client.cpp 开头的 bind_core，新线程继承创建者掩码）→ 同核竞争只剩外部
  /// 进程，RT 保证 worker 的忙等自旋不被同核 CFS 线程打断。需 CAP_SYS_NICE；失败 warn 降级为
  /// 普通 CFS 调度（不影响运行）。
  static void set_realtime(int priority) {
    struct sched_param sp{};
    sp.sched_priority = priority;
    if (pthread_setschedparam(pthread_self(), SCHED_FIFO, &sp) != 0)
      FINS_LOG_WARN("[ThreadPool] set SCHED_FIFO prio {} failed: {}", priority, strerror(errno));
  }

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
    std::function<bool()> cb_;
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
    /// 返回 false = 无更多工作（停止）→ worker 退出循环。
    void on_execute(std::function<bool()> cb) {
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
      // ★ 线程名（comm）：内核 sched_switch 的 prev_comm/next_comm 读的就是它。
      set_thread_name("fins_worker/" + std::to_string(idx+1));
      bind_core(idx+1);  // 跳过 core 0
      set_realtime(50);    // SCHED_FIFO 实时优先级，工作线程可以主动让出核心，其他线程临时使用核心允许，但工作线程有任务时将抢占
      FINS_LOG_INFO("[ThreadPool] worker {} -> core {}", idx, idx + 1);

      {  // 宣布就绪：start() 据此得知所有 worker 已绑定核、即将在取任务回调上等待
        std::lock_guard lk(ready_mtx_);
        ++workers_ready_;
      }
      ready_cv_.notify_all();

      while (!stop_.load()) {
        if (!cb_) break;      // 空回调（未注册）直接退出
        try {
          if (!cb_()) break;  // 回调返回 false → 无更多工作 → 退出（idx = 线程序号）
        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[ThreadPool] worker {} callback threw: {}", idx, e.what());
        } catch (...) {
          FINS_LOG_ERROR("[ThreadPool] worker {} callback threw unknown exception", idx);
        }
      }
    }
  };

} // namespace fins::rt
