/*******************************************************************************
 * Copyright (c) 2024-2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

#pragma once

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

  inline double latency_sec(const Time &acq_time) {
    auto current = now();
    if (acq_time == zero()) return 0.0;
    return std::chrono::duration<double>(current - acq_time).count();
  }
  
  inline double latency_ms(const Time &acq_time) {
    return latency_sec(acq_time) * 1000.0;
  }

  inline double latency_us(const Time &acq_time) {
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
  inline AcqTime from_ros_time(const builtin_interfaces::msg::Time& ros_msg) {
    std::chrono::nanoseconds dur(ros_msg.sec * 1000000000LL + ros_msg.nanosec);
    return AcqTime(dur);
  }

  inline AcqTime from_ros_time(const rclcpp::Time& ros_time) {
    return AcqTime(std::chrono::nanoseconds(ros_time.nanoseconds()));
  }

  inline builtin_interfaces::msg::Time to_ros_msg_time(const AcqTime& acq_time) {
    auto ns_total = acq_time.time_since_epoch().count();
    builtin_interfaces::msg::Time msg;
    msg.sec = static_cast<int32_t>(ns_total / 1000000000LL);
    msg.nanosec = static_cast<uint32_t>(ns_total % 1000000000LL);
    return msg;
  }

  inline rclcpp::Time to_ros_time(const AcqTime& acq_time) {
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
} // namespace std