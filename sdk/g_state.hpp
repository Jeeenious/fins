/*******************************************************************************
 * Copyright (c) 2026.
 * IWIN-FINS Lab, Shanghai Jiao Tong University.
 *******************************************************************************/

#pragma once

#include "form.hpp"
#include "third_party/json.hpp"

namespace fins::rt {
  inline util::TBBMap<float> core_usages{};
  inline std::atomic<float> mem_usage{};

  inline util::TBBMap<util::TBBQueue<float>> mesg_aoi_history;
  inline util::TBBMap<util::TBBQueue<float>> algo_wcet_history; // ms
  
  inline util::TBBMap<std::string> algo_version; // algo --> version
  inline util::TBBMap<float> algo_period; // ms or -1
  inline util::TBBMap<float> mesg_aoi_predict;
  inline util::TBBMap<float> algo_wcet_predict;

  enum class JobState {
    PENDING,    // 等待中：已创建，但尚未被调度器激活（资源在排队）
    READY,      // 就绪：所有输入准备就绪，可以立即触发
    WAITING,    // 运行中：正在执行 (on_loop)
    RUNNING,    // 已挂起：调度器主动暂停（如为了频率同步）
    COMPLETED,  // 已完成：单次任务结束，等待下一次循环
    ERROR       // 异常：逻辑崩溃或超时
  };
  // scheduling
  inline util::TBBMap<JobState> job_state;
  inline util::TBBMap<float> job_rate;
  inline util::TBBMap<float> job_ddl;
  inline util::TBBMap<int> job_priority;

} // namespace fins