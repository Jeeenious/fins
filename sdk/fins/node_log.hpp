/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// node_log.hpp

#pragma once

#include <atomic>
#include <chrono>
#include <deque>
#include <mutex>
#include <string>
#include <vector>

#if __cplusplus >= 202002L
#include <source_location>
#endif

#include <fmt/chrono.h>
#include <fmt/color.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/printf.h>
#include <fins/utils/logger.hpp>

namespace fins {

  /**
   * @brief 节点日志级别 / Node log level
   * @details 控制节点日志输出的详细程度。级别从低到高依次为 DEBUG、INFO、WARN、ERROR、OFF。
   *          OFF 表示完全禁用日志输出。
   *
   *          Controls the verbosity of node log output. Levels from low to high
   *          are DEBUG, INFO, WARN, ERROR, OFF. OFF disables all log output.
   */
  enum class NodeLogLevel { DEBUG = 0, INFO = 1, WARN = 2, ERROR = 3, OFF = 4 };

#if __cplusplus >= 202002L
  struct FmtWithLoc {
    std::string_view fmt_str;
    std::source_location loc;

    template<typename T>
    consteval FmtWithLoc(const T &s, std::source_location l = std::source_location::current()) : fmt_str(s), loc(l) {}
  };
#else
  struct FmtWithLoc {
    std::string_view fmt_str;
    std::string file;
    uint32_t line;

    template<typename T>
    constexpr FmtWithLoc(const T &s, const char *f = "", uint32_t l = 0) : fmt_str(s), file(f), line(l) {}
  };
#endif

  struct LogEntry {
    double timestamp;
    std::string level;
    std::string message;
    std::string file;
    uint32_t line;
  };

  inline std::atomic<NodeLogLevel> &get_log_level_ref() {
    static std::atomic<NodeLogLevel> level{NodeLogLevel::INFO};
    return level;
  }

  /// @brief 获取全局日志级别 / Get global log level
  inline NodeLogLevel get_log_level() { return get_log_level_ref().load(std::memory_order_relaxed); }

  /// @brief 设置全局日志级别 / Set global log level
  inline void set_node_log_level(NodeLogLevel level) { get_log_level_ref().store(level, std::memory_order_relaxed); }

  /**
   * @brief 节点日志器 / Node logger
   * @details 每个节点通过 Node::logger 成员访问此日志器。支持 4 级日志：
   *          debug、info、warn、error。使用 fmt 库格式化消息，支持 sprintf 风格（`f` 后缀版本）。
   *
   *          Each node accesses this logger via the Node::logger member. Supports
   *          4 log levels: debug, info, warn, error. Uses fmt library for message
   *          formatting, with sprintf-style variants (`f` suffix).
   *
   * @par 示例 / Example
   * @code
   * logger->debug("Entering process loop, iteration {}", i);
   * logger->info("Model loaded successfully from {}", path);
   * logger->warn("Confidence {:.2f} below threshold", score);
   * logger->error("Failed to open file: {}", filename);
   * @endcode
   */
  class NodeLogger {
  public:
    NodeLogger() = default;

  private:
    void print_to_terminal(const std::string &level, const std::string &msg) {
      if (!fins::Logger::get().is_node_terminal_enabled()) return;

      auto now = std::chrono::system_clock::now();
      fmt::text_style style;
      const char *level_tag = "";

      if (level == "DEBUG") {
        style = fmt::fg(fmt::color::cyan);
        level_tag = "DBG";
      } else if (level == "INFO") {
        style = fmt::fg(fmt::color::green);
        level_tag = "INF";
      } else if (level == "WARN") {
        style = fmt::fg(fmt::color::yellow) | fmt::emphasis::bold;
        level_tag = "WRN";
      } else if (level == "ERROR") {
        style = fmt::fg(fmt::color::red) | fmt::emphasis::bold;
        level_tag = "ERR";
      }

      static std::mutex terminal_mutex;
      std::lock_guard<std::mutex> lock(terminal_mutex);
      auto tp = std::chrono::system_clock::to_time_t(now);
      struct tm tm_info;
      localtime_r(&tp, &tm_info);
      auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
      fmt::print(fmt::fg(fmt::color::gray), "[{:%H:%M:%S}.{:03d}] ", tm_info, ms.count());
      fmt::print(style, "[{}] ", level_tag);
      fmt::print("{}\n", msg);
      fflush(stdout);
    }

  public:
#if __cplusplus >= 202002L
    void log_impl(const std::string &level, const std::string &msg, const std::source_location &loc) {
      print_to_terminal(level, msg);
      auto now =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
              .count() /
          1000.0;

      std::lock_guard<std::mutex> lock(mutex_);
      if (logs_.size() >= 200)
        logs_.pop_front();
      logs_.push_back({now, level, msg, loc.file_name(), loc.line()});
    }
#else
    void log_impl(const std::string &level, const std::string &msg, const char *file = "", uint32_t line = 0) {
      print_to_terminal(level, msg);
      auto now =
          std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now().time_since_epoch())
              .count() /
          1000.0;

      std::lock_guard<std::mutex> lock(mutex_);
      if (logs_.size() >= 200)
        logs_.pop_front();
      logs_.push_back({now, level, msg, file, line});
    }
#endif

#if __cplusplus >= 202002L
    /**
     * @brief 输出 INFO 级别日志（fmt::format 风格） / Output INFO level log (fmt::format style)
     * @tparam Args 格式化参数类型 / Format argument types
     * @param f 格式字符串 + 源码位置 / Format string + source location
     * @param args 格式化参数 / Format arguments
     */
    template<typename... Args>
    void info(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::INFO) {
        log_impl("INFO", fmt::vformat(f.fmt_str, fmt::make_format_args(args...)), f.loc);
      }
    }

    /// @brief 输出 INFO 日志（sprintf 风格） / Output INFO log (sprintf style)
    template<typename... Args>
    void infof(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::INFO) {
        log_impl("INFO", fmt::sprintf(f.fmt_str.data(), std::forward<Args>(args)...), f.loc);
      }
    }

    /// @brief 输出 DEBUG 日志（fmt::format 风格） / Output DEBUG log (fmt::format style)
    template<typename... Args>
    void debug(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::DEBUG) {
        log_impl("DEBUG", fmt::vformat(f.fmt_str, fmt::make_format_args(args...)), f.loc);
      }
    }

    /// @brief 输出 DEBUG 日志（sprintf 风格） / Output DEBUG log (sprintf style)
    template<typename... Args>
    void debugf(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::DEBUG) {
        log_impl("DEBUG", fmt::sprintf(f.fmt_str.data(), std::forward<Args>(args)...), f.loc);
      }
    }

    /// @brief 输出 WARN 日志（fmt::format 风格） / Output WARN log (fmt::format style)
    template<typename... Args>
    void warn(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::WARN) {
        log_impl("WARN", fmt::vformat(f.fmt_str, fmt::make_format_args(args...)), f.loc);
      }
    }

    /// @brief 输出 WARN 日志（sprintf 风格） / Output WARN log (sprintf style)
    template<typename... Args>
    void warnf(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::WARN) {
        log_impl("WARN", fmt::sprintf(f.fmt_str.data(), std::forward<Args>(args)...), f.loc);
      }
    }

    /// @brief 输出 ERROR 日志（fmt::format 风格） / Output ERROR log (fmt::format style)
    template<typename... Args>
    void error(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::ERROR) {
        log_impl("ERROR", fmt::vformat(f.fmt_str, fmt::make_format_args(args...)), f.loc);
      }
    }

    /// @brief 输出 ERROR 日志（sprintf 风格） / Output ERROR log (sprintf style)
    template<typename... Args>
    void errorf(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::ERROR) {
        log_impl("ERROR", fmt::sprintf(f.fmt_str.data(), std::forward<Args>(args)...), f.loc);
      }
    }
#else
    /// @brief 输出 INFO 日志（fmt::format 风格，C++17） / Output INFO log (fmt::format style, C++17)
    template<typename... Args>
    void info(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::INFO) {
        log_impl("INFO", fmt::vformat(f.fmt_str, fmt::make_format_args(args...)), f.file.c_str(), f.line);
      }
    }

    /// @brief 输出 INFO 日志（sprintf 风格，C++17） / Output INFO log (sprintf style, C++17)
    template<typename... Args>
    void infof(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::INFO) {
        log_impl("INFO", fmt::sprintf(f.fmt_str.data(), std::forward<Args>(args)...), f.file.c_str(), f.line);
      }
    }

    /// @brief 输出 DEBUG 日志（fmt::format 风格，C++17） / Output DEBUG log (fmt::format style, C++17)
    template<typename... Args>
    void debug(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::DEBUG) {
        log_impl("DEBUG", fmt::vformat(f.fmt_str, fmt::make_format_args(args...)), f.file.c_str(), f.line);
      }
    }

    /// @brief 输出 DEBUG 日志（sprintf 风格，C++17） / Output DEBUG log (sprintf style, C++17)
    template<typename... Args>
    void debugf(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::DEBUG) {
        log_impl("DEBUG", fmt::sprintf(f.fmt_str.data(), std::forward<Args>(args)...), f.file.c_str(), f.line);
      }
    }

    /// @brief 输出 WARN 日志（fmt::format 风格，C++17） / Output WARN log (fmt::format style, C++17)
    template<typename... Args>
    void warn(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::WARN) {
        log_impl("WARN", fmt::vformat(f.fmt_str, fmt::make_format_args(args...)), f.file.c_str(), f.line);
      }
    }

    /// @brief 输出 WARN 日志（sprintf 风格，C++17） / Output WARN log (sprintf style, C++17)
    template<typename... Args>
    void warnf(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::WARN) {
        log_impl("WARN", fmt::sprintf(f.fmt_str.data(), std::forward<Args>(args)...), f.file.c_str(), f.line);
      }
    }

    /// @brief 输出 ERROR 日志（fmt::format 风格，C++17） / Output ERROR log (fmt::format style, C++17)
    template<typename... Args>
    void error(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::ERROR) {
        log_impl("ERROR", fmt::vformat(f.fmt_str, fmt::make_format_args(args...)), f.file.c_str(), f.line);
      }
    }

    /// @brief 输出 ERROR 日志（sprintf 风格，C++17） / Output ERROR log (sprintf style, C++17)
    template<typename... Args>
    void errorf(FmtWithLoc f, Args &&...args) {
      if (get_log_level() <= NodeLogLevel::ERROR) {
        log_impl("ERROR", fmt::sprintf(f.fmt_str.data(), std::forward<Args>(args)...), f.file.c_str(), f.line);
      }
    }
#endif

    std::vector<LogEntry> get_and_clear_logs() {
      std::lock_guard<std::mutex> lock(mutex_);
      std::vector<LogEntry> result(logs_.begin(), logs_.end());
      logs_.clear();
      return result;
    }

  private:
    std::deque<LogEntry> logs_;
    std::mutex mutex_;
  };

} // namespace fins