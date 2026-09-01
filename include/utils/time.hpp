/*******************************************************************************
 * Copyright (c) 2024-2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

#pragma once

// ============================================================================
// time — 时间工具：区分「墙上时间」与「单调时间」两类时钟
// ============================================================================
//
// 内部逻辑：
//   本文件提供两类时钟的时间接口，用途不同、类型上不可混用：
//   - 墙上时间（system_clock）：Time / now() / to_string()——记录"发生时刻"，
//     可格式化为 YYYY-MM-DD HH:MM:SS.ffffff（operator<</to_string 走 localtime_r），
//     也用于跨进程/跨机对表（epoch 数值）。缺点：随 NTP 校时/手动改时间跳变。
//   - 单调时间（steady_clock）：SteadyTime / steady_now() / latency_*()——只保证
//     递增不跳变，用于测量时间间隔（延迟/耗时）。延迟测量必须用它，否则校时会让
//     间隔跳变甚至为负。两套时钟 epoch 不同，不允许直接相减（编译器会拒绝）。
//   历史教训：latency_* 原本基于 system_clock（旧 now()-acq_time），文件头曾有
//   todo"墙上时间计算的延迟不可靠"，本版已改为 steady_clock。
//
// 资源消耗：
//   - 纯内联函数，无运行期状态；每次调用读时钟（vDSO/syscall），纳秒级开销
//   - get_thread_cpu_time_ns() 读 CLOCK_THREAD_CPUTIME_ID（每线程一次 syscall）
//
// 对外接口：
//   墙上：now() / zero() / to_seconds / to_microseconds / to_nanoseconds /
//         from_seconds / operator<< / to_string
//   单调：now_ms() / steady_now() / latency_sec / latency_ms / latency_us（参数为 SteadyTime）
//   其他：get_thread_cpu_time_ns()；ROS2 桥接（FINS_HAS_ROS2 时启用）
// ============================================================================

#include <chrono>
#include <iomanip>
#include <logger.hpp>
#include <sstream>
#include <string>
#include <ctime>

#if __has_include(<rclcpp/rclcpp.hpp>)
  #include <rclcpp/rclcpp.hpp>
  #include <builtin_interfaces/msg/time.hpp>
  #define FINS_HAS_ROS2 1
#endif

namespace fins::util {
  using sys_clock = std::chrono::system_clock;
  using Time = std::chrono::time_point<sys_clock, std::chrono::nanoseconds>;

  inline Time now() {
    return std::chrono::time_point_cast<std::chrono::nanoseconds>(sys_clock::now());
  }

  inline constexpr Time zero() {
    return Time{std::chrono::nanoseconds{0}};
  }

  using steady_clock = std::chrono::steady_clock;
  using SteadyTime = std::chrono::time_point<steady_clock, std::chrono::nanoseconds>;

  inline SteadyTime steady_now() {
    return std::chrono::time_point_cast<std::chrono::nanoseconds>(steady_clock::now());
  }

  /// 单调时钟从 epoch 以来的流逝（ms，steady_clock 单调、不随校时跳变）——时间间隔/排期基准；
  /// 注意与墙上时钟（Time/now()）epoch 不同，不可直接混用。
  inline double now_ms() {
    return std::chrono::duration<double, std::milli>(steady_now().time_since_epoch()).count();
  }

  inline double latency_sec(const SteadyTime &acq_time) {
    auto current = steady_now();
    if (acq_time == SteadyTime{}) return 0.0;
    return std::chrono::duration<double>(current - acq_time).count();
  }

  inline double latency_ms(const SteadyTime &acq_time) {
    return latency_sec(acq_time) * 1000.0;
  }

  inline double latency_us(const SteadyTime &acq_time) {
    return latency_sec(acq_time) * 1e6;
  }

  inline double to_seconds(const Time &ts) {
    return std::chrono::duration<double>(ts.time_since_epoch()).count();
  }

  inline int64_t to_microseconds(const Time &ts) {
    return std::chrono::duration_cast<std::chrono::microseconds>(ts.time_since_epoch()).count();
  }

  inline int64_t to_nanoseconds(const Time &ts) {
    return ts.time_since_epoch().count();
  }

  inline Time from_seconds(double sec) {
    auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::duration<double>(sec));
    return Time(dur);
  }

  inline int64_t get_thread_cpu_time_ns() {
    struct timespec ts{};
    if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts) == 0) {
      return static_cast<int64_t>(ts.tv_sec) * 1000000000LL + ts.tv_nsec;
    }
    return 0;
  }
  
#ifdef FINS_HAS_ROS2
  // ROS2 时间戳是墙上时间（epoch 数值），与 Time（system_clock）同源，可直接互转。
  inline Time from_ros_time(const builtin_interfaces::msg::Time& ros_msg) {
    std::chrono::nanoseconds dur(ros_msg.sec * 1000000000LL + ros_msg.nanosec);
    return Time(dur);
  }

  inline Time from_ros_time(const rclcpp::Time& ros_time) {
    return Time(std::chrono::nanoseconds(ros_time.nanoseconds()));
  }

  inline builtin_interfaces::msg::Time to_ros_msg_time(const Time& acq_time) {
    auto ns_total = acq_time.time_since_epoch().count();
    builtin_interfaces::msg::Time msg;
    msg.sec = static_cast<int32_t>(ns_total / 1000000000LL);
    msg.nanosec = static_cast<uint32_t>(ns_total % 1000000000LL);
    return msg;
  }

  inline rclcpp::Time to_ros_time(const Time& acq_time) {
    return rclcpp::Time(acq_time.time_since_epoch().count(), RCL_SYSTEM_TIME);
  }
#endif // FINS_HAS_ROS2

  inline std::ostream &operator<<(std::ostream &os, const fins::util::Time &ts) {
    // 强转为 time_t 以便调用传统时间函数
    auto time_t_val = fins::util::sys_clock::to_time_t(ts);
    const auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(ts.time_since_epoch()) % std::chrono::seconds(1);

    std::tm tm_val{};
    localtime_r(&time_t_val, &tm_val);

    // 输出格式化：年-月-日 时:分:秒.纳秒(9位自动补零)
    os << std::put_time(&tm_val, "%Y-%m-%d %H:%M:%S")
       << '.' << std::setfill('0') << std::setw(9) << ns.count();
    return os;
  }

  inline std::string to_string(const fins::util::Time &ts) {
    std::ostringstream oss;
    // 显式拉取上面定义的通用流重载，确保万无一失
    oss << ts;
    return oss.str();
  }
} // namespace fins::util