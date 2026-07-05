/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// shared_states.hpp

#pragma once

#include <atomic>
#include <cstdint>
#include <map>
#include <string>
#include <vector>

namespace fins {

  // ============== 全局运行状态 ==============

  enum class Running_State { PAUSE = 0, RUN };

  /** 调度策略：决定线程池中任务的优先级排序方式 */
  enum class SchedulePolicy {
    RMS,   // Rate Monotonic: 周期越短优先级越高
    EDF,   // Earliest Deadline First: 截止时间越近优先级越高
  };

  inline std::atomic<Running_State> g_running_state{Running_State::PAUSE};
  inline std::atomic<SchedulePolicy> g_schedule_policy{SchedulePolicy::RMS};

  inline void set_running_state(Running_State state) { g_running_state.store(state, std::memory_order_relaxed); }
  inline Running_State get_running_state() { return g_running_state.load(std::memory_order_relaxed); }
  inline void set_schedule_policy(SchedulePolicy p) { g_schedule_policy.store(p, std::memory_order_relaxed); }
  inline SchedulePolicy get_schedule_policy() { return g_schedule_policy.load(std::memory_order_relaxed); }

  // ============== 算法流程图 (Pipeline) ==============

  /**
   * 算法任务 t_i
   * t_i := { ch_I^1, ..., ch_I^m, ch_O^1, ..., ch_O^n }
   */
  struct Algorithm {
    std::string id;               // 算法类名 (NodeMeta::name)

    std::vector<std::string> inputs;
    std::vector<std::string> outputs;

    int64_t period_ms = -1;                          // p(t): 执行周期(ms), -1 = 事件驱动(从上游继承)
  };

  struct Pipeline {
    std::vector<Algorithm> algorithms; //todo tbb

    void clear() { algorithms.clear(); }
    bool empty() const { return algorithms.empty(); }
    bool is_lcm() const { }//todo
    bool is_acy() const { }//todo
    // todo add algo
    // todo remove algo
  };

  // ============== 前序图 (Precedence Graph) ==============

  /**
   * 算法实例 v_i: HP 超周期内的单次执行
   *
   * 生命周期: 就绪→入队→出队→启动→完成→记录
   */
  struct AlgoInstance {
    std::string id;                 // t_i ID
    int instance_id = 0;            // 实例序号 (0-based)

    int64_t period_ms = -1;
    int64_t absolute_deadline_ms = -1;
    int64_t worst_case_execution_time_ms = -1;

    // --- 运行时动态更新 ---
    int64_t ready_ns = 0;
    int64_t enqueue_ns = 0;
    int64_t dequeue_ns = 0;
    int64_t start_ns = 0;
    int64_t done_ns = 0;
    float actual_us = 0.0f;
    bool completed = false;
  };

  /**
   * 前序图 G = {V, E}
   *
   * 线程池 + 调度策略 (RMS/EDF) 决定实例的执行顺序。
   * 策略只影响优先级排序，不改变 work-conserving 条件。
   */
  struct PrecedenceGraph {
    int64_t hyperperiod_ns = 0;
    std::vector<AlgoInstance> instances;
    std::vector<std::pair<int, int>> precedence;

    void clear() {
      hyperperiod_ns = 0;
      instances.clear();
      precedence.clear();
    }
    bool empty() const { return instances.empty(); }

    float total_volume_us() const;
    float critical_path_us() const;

    void record_ready(int idx, int64_t ns);
    void record_enqueue(int idx, int64_t ns);
    void record_dequeue(int idx, int64_t ns);
    void record_start(int idx, int64_t ns);
    void record_done(int idx, int64_t ns, float actual_us);
  };

} // namespace fins
