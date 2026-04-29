/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// system_monitor.hpp

#pragma once

#include <algorithm>
#include <chrono>
#include <fstream>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>
#include <fins/utils/logger.hpp>

namespace fins {
  struct SystemStats {
    double cpu_usage_percent = 0.0;
    double cpu_temperature_c = 0.0;
    double mem_usage_percent = 0.0;
    double mem_used_mb = 0.0;
    double mem_total_mb = 0.0;
  };

  class SystemMonitor {
  public:
    static double get_cpu_usage(int sample_duration_ms = 100) {
      try {
        auto stats1 = read_cpu_stats();
        if (stats1.empty())
          return 0.0;

        std::this_thread::sleep_for(std::chrono::milliseconds(sample_duration_ms));

        auto stats2 = read_cpu_stats();
        if (stats2.empty())
          return 0.0;

        const unsigned long long total_time1 = std::accumulate(stats1.begin(), stats1.end(), 0ULL);
        const unsigned long long total_time2 = std::accumulate(stats2.begin(), stats2.end(), 0ULL);

        const unsigned long long idle_time1 = stats1[3];
        const unsigned long long idle_time2 = stats2[3];

        const double delta_total = static_cast<double>(total_time2 - total_time1);
        const double delta_idle = static_cast<double>(idle_time2 - idle_time1);

        if (delta_total <= 0.0)
          return 0.0;

        const double usage_percent = (1.0 - (delta_idle / delta_total)) * 100.0;
        return std::clamp(usage_percent, 0.0, 100.0);
      } catch (const std::exception &e) {
        FINS_LOG_ERROR("[SystemMonitor] Error getting CPU usage: {}", e.what());
        return 0.0;
      }
    }

    static double get_cpu_temperature() {
      try {
        std::ifstream temp_file("/sys/class/thermal/thermal_zone0/temp");
        if (!temp_file.is_open()) {
          return 0.0;
        }

        std::string raw_temp;
        temp_file >> raw_temp;

        double temp_milli = std::stod(raw_temp);
        return temp_milli / 1000.0;
      } catch (...) {
        return 0.0;
      }
    }

    static SystemStats get_system_stats(int cpu_sample_ms = 100) {
      SystemStats stats;

      stats.cpu_usage_percent = get_cpu_usage(cpu_sample_ms);
      stats.cpu_temperature_c = get_cpu_temperature();

      try {
        std::ifstream meminfo_file("/proc/meminfo");
        if (!meminfo_file.is_open()) {
          throw std::runtime_error("Could not open /proc/meminfo");
        }

        std::string line;
        long long mem_total = -1, mem_available = -1, mem_free = -1, buffers = -1, cached = -1;

        while (std::getline(meminfo_file, line)) {
          std::stringstream ss(line);
          std::string key;
          long long value;
          ss >> key >> value;

          if (key == "MemTotal:")
            mem_total = value;
          else if (key == "MemAvailable:")
            mem_available = value;
          else if (key == "MemFree:")
            mem_free = value;
          else if (key == "Buffers:")
            buffers = value;
          else if (key == "Cached:")
            cached = value;
        }

        if (mem_total > 0) {
          if (mem_available == -1) {
            mem_available =
                (mem_free != -1 && buffers != -1 && cached != -1) ? (mem_free + buffers + cached) : mem_free;
          }

          stats.mem_total_mb = static_cast<double>(mem_total) / 1024.0;
          const long long mem_used = mem_total - mem_available;
          stats.mem_used_mb = static_cast<double>(mem_used) / 1024.0;
          stats.mem_usage_percent = (static_cast<double>(mem_used) / static_cast<double>(mem_total)) * 100.0;
        }
      } catch (const std::exception &e) {
        FINS_LOG_ERROR("[SystemMonitor] Error getting memory info: {}", e.what());
      }
      
      return stats;
    }

  private:
    static std::vector<unsigned long long> read_cpu_stats() {
      std::ifstream stat_file("/proc/stat");
      if (!stat_file.is_open()) {
        throw std::runtime_error("Could not open /proc/stat");
      }

      std::string line;
      if (!std::getline(stat_file, line))
        return {};

      std::stringstream ss(line);
      std::string cpu_label;
      ss >> cpu_label;

      if (cpu_label != "cpu")
        return {};

      std::vector<unsigned long long> stats;
      unsigned long long value;
      while (ss >> value) {
        stats.push_back(value);
      }

      return stats;
    }
  };

} // namespace fins