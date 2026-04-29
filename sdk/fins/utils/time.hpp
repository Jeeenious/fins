/*******************************************************************************
 * Copyright (c) 2024-2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// utils/time.hpp

#pragma once

#include <chrono>
#include <fins/utils/logger.hpp>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <time.h>

#if __has_include(<rclcpp/rclcpp.hpp>)
  #include <rclcpp/rclcpp.hpp>
  #include <builtin_interfaces/msg/time.hpp>
  #define FINS_HAS_ROS2 1
#endif

namespace fins {
  using sys_clock = std::chrono::system_clock;
  using AcqTime = std::chrono::time_point<sys_clock, std::chrono::nanoseconds>;

  inline AcqTime now() { 
    return std::chrono::time_point_cast<std::chrono::nanoseconds>(sys_clock::now()); 
  }

  inline constexpr AcqTime zero() { 
    return AcqTime{std::chrono::nanoseconds{0}}; 
  }

  inline double latency_sec(const AcqTime &acq_time) {
    auto current = now();
    if (acq_time == zero()) return 0.0;
    return std::chrono::duration<double>(current - acq_time).count();
  }
  
  inline double latency_ms(const AcqTime &acq_time) {
    return latency_sec(acq_time) * 1000.0;
  }

  inline double latency_us(const AcqTime &acq_time) {
    return latency_sec(acq_time) * 1e6;
  }

  inline double to_seconds(const AcqTime &ts) {
    return std::chrono::duration<double>(ts.time_since_epoch()).count();
  }

  inline int64_t to_microseconds(const AcqTime &ts) {
    return std::chrono::duration_cast<std::chrono::microseconds>(ts.time_since_epoch()).count();
  }

  inline int64_t to_nanoseconds(const AcqTime &ts) {
    return ts.time_since_epoch().count();
  }

  inline AcqTime from_seconds(double sec) {
    auto dur = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::duration<double>(sec));
    return AcqTime(dur);
  }

  inline int64_t get_thread_cpu_time_ns() {
    struct timespec ts;
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
}

namespace std {
  inline std::ostream &operator<<(std::ostream &os, const fins::AcqTime &ts) {
    auto time_t_val = fins::sys_clock::to_time_t(ts);
    auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(ts.time_since_epoch()) % std::chrono::seconds(1);
    
    std::tm tm_val;
    localtime_r(&time_t_val, &tm_val);

    os << std::put_time(&tm_val, "%Y-%m-%d %H:%M:%S") 
       << '.' << std::setfill('0') << std::setw(9) << ns.count();
    return os;
  }

  inline std::string to_string(const fins::AcqTime &ts) {
    std::ostringstream oss;
    oss << ts;
    return oss.str();
  }
}