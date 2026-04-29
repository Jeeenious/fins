/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// shared_states.hpp

#pragma once

#include <atomic>

namespace fins {

  enum class Running_State {
    PAUSE = 0,
    RUN,
  };

  enum class Process_Strategy { SERIAL, PARALLEL, POOL };
  enum class Schedule_Strategy { FCFS, BALANCE_FPS, BALANCE_DELAY };

  inline std::atomic<Running_State> g_running_state{Running_State::PAUSE};
  inline std::atomic<Process_Strategy> g_process_strategy{Process_Strategy::POOL};
  inline std::atomic<Schedule_Strategy> g_schedule_strategy{Schedule_Strategy::FCFS};

  inline void set_running_state(Running_State state) { g_running_state.store(state, std::memory_order_relaxed); }

  inline Running_State get_running_state() { return g_running_state.load(std::memory_order_relaxed); }

  inline void set_process_strategy(Process_Strategy strategy) {
    g_process_strategy.store(strategy, std::memory_order_relaxed);
  }

  inline Process_Strategy get_process_strategy() { return g_process_strategy.load(std::memory_order_relaxed); }

  inline void set_schedule_strategy(Schedule_Strategy strategy) {
    g_schedule_strategy.store(strategy, std::memory_order_relaxed);
  }

  inline Schedule_Strategy get_schedule_strategy() { return g_schedule_strategy.load(std::memory_order_relaxed); }
} // namespace fins