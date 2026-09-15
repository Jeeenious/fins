/*******************************************************************************
 * Copyright (c) 2024-2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

#pragma once

// ============================================================================
// time — 时间工具：单调时间接口
// ============================================================================
//   仅保留 now_ms()：单调时钟（steady_clock）从 epoch 以来的流逝（ms），不随
//   校时跳变，用于时间间隔/排期基准。其余墙上时间（Time/now()/to_string）、
//   延迟（latency_*）、转换（to_seconds/to_microseconds/...）、线程 CPU 时间、
//   ROS2 桥接（from_ros_time/to_ros_time）当前代码库均无引用，已清理。
// ============================================================================

#include <chrono>

namespace fins::util {
  /// 单调时钟从 epoch 以来的流逝（ms，steady_clock 单调、不随校时跳变）——时间间隔/排期基准。
  inline double now_ms() {
    return std::chrono::duration<double, std::milli>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
  }

  /// 单调时钟从 epoch 以来的流逝（µs，steady_clock 单调、不随校时跳变）——时间间隔/排期基准。
  inline double now_us() {
    return std::chrono::duration<double, std::micro>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
  }
} // namespace fins::util
