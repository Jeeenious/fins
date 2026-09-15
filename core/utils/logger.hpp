/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// utils/logger.hpp

#pragma once

#include <atomic>
#include <chrono>
#include <ctime>
#include <fmt/chrono.h>
#include <fmt/color.h>
#include <fmt/core.h>
#include <mutex>

#define FINS_LEVEL_DEBUG 0
#define FINS_LEVEL_INFO  1
#define FINS_LEVEL_WARN  2
#define FINS_LEVEL_ERROR 3
#define FINS_LEVEL_OFF   4

#ifndef FINS_LOG_LEVEL
#define FINS_LOG_LEVEL FINS_LEVEL_INFO
#endif

namespace fins {

  enum class LogLevel { DEBUG = 0, INFO, WARN, ERROR, OFF };

  class Logger {
  public:
    static Logger &get() {
      static Logger instance;
      return instance;
    }

    void set_node_terminal_enabled(bool enabled) { 
      if (enabled) setenv("FINS_NODE_TERMINAL_LOG", "1", 1);
      else setenv("FINS_NODE_TERMINAL_LOG", "0", 1);
    }

    bool is_node_terminal_enabled() const { 
      const char* env = getenv("FINS_NODE_TERMINAL_LOG");
      if (env) {
        return std::string(env) == "1";
      }
      return true; // 默认开启
    }

    template<typename... Args>
    void log(LogLevel level, fmt::format_string<Args...> fmt, Args &&...args) {
      auto now = std::chrono::system_clock::now();

      fmt::text_style style;
      const char *level_tag = "";

      switch (level) {
        case LogLevel::DEBUG:
          style = fmt::bg(fmt::color::cyan) | fmt::fg(fmt::color::white);
          level_tag = " DEBUG ";
          break;
        case LogLevel::INFO:
          style = fmt::bg(fmt::color::green) | fmt::fg(fmt::color::white);
          level_tag = " INFO  ";
          break;
        case LogLevel::WARN:
          style = fmt::bg(fmt::color::yellow) | fmt::fg(fmt::color::black);
          level_tag = " WARN  ";
          break;
        case LogLevel::ERROR:
          style = fmt::bg(fmt::color::red) | fmt::fg(fmt::color::white);
          level_tag = " ERROR ";
          break;
        default:
          break;
      }

      std::lock_guard<std::mutex> lock(mutex_);
      auto tp = std::chrono::system_clock::to_time_t(now);
      struct tm tm_info;
      localtime_r(&tp, &tm_info);
      auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
      fmt::print(fmt::fg(fmt::color::gray), "[{:%H:%M:%S}.{:03d}] ", tm_info, ms.count());
      fmt::print(style, "{}", level_tag);
      fmt::print(" ");
      fmt::print(fmt, std::forward<Args>(args)...);
      fmt::print("\n");
      fflush(stdout);
    }

  private:
    Logger() = default;
    ~Logger() = default;
    Logger(const Logger &) = delete;
    Logger &operator=(const Logger &) = delete;

    std::mutex mutex_;
  };

} // namespace fins

#if FINS_LOG_LEVEL <= FINS_LEVEL_DEBUG
    #define FINS_LOG_DEBUG(...) fins::Logger::get().log(fins::LogLevel::DEBUG, __VA_ARGS__)
#else
    #define FINS_LOG_DEBUG(...) do {} while(0)
#endif

#if FINS_LOG_LEVEL <= FINS_LEVEL_INFO
    #define FINS_LOG_INFO(...)  fins::Logger::get().log(fins::LogLevel::INFO, __VA_ARGS__)
#else
    #define FINS_LOG_INFO(...)  do {} while(0)
#endif

#if FINS_LOG_LEVEL <= FINS_LEVEL_WARN
    #define FINS_LOG_WARN(...)  fins::Logger::get().log(fins::LogLevel::WARN, __VA_ARGS__)
#else
    #define FINS_LOG_WARN(...)  do {} while(0)
#endif

#if FINS_LOG_LEVEL <= FINS_LEVEL_ERROR
    #define FINS_LOG_ERROR(...) fins::Logger::get().log(fins::LogLevel::ERROR, __VA_ARGS__)
#else
    #define FINS_LOG_ERROR(...) do {} while(0)
#endif