/*******************************************************************************
 * Copyright (c) 2024.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// aoi_analyzer.hpp

#pragma once

#include <algorithm>
#include <array>
#include <atomic>
#include <cmath>
#include <fins/utils/time.hpp>
#include <vector>

namespace fins {

  constexpr size_t ANALYZER_BUFFER_CAPACITY = 100;
  constexpr int64_t WINDOW_US = 1000000;
  constexpr double DEFAULT_VIOLATION_THRESHOLD_MS = 200.0;

  struct PipeMetrics {
    double avg_aoi_ms;
    double peak_aoi_ms;
    double sys_delay_ms;
    double violation_prob;
    double fps;
    size_t count;
    double time_window_s;
  };

  class AoIAnalyzer {
  private:
    struct Record {
      std::atomic<int64_t> arrival_us;
      std::atomic<int64_t> generation_us;
      Record() : arrival_us(0), generation_us(0) {}
    };

    std::array<Record, ANALYZER_BUFFER_CAPACITY> buffer_;
    std::atomic<size_t> head_{0};
    std::atomic<int64_t> violation_threshold_us_;
    std::atomic<bool> has_record_{false};

  public:
    AoIAnalyzer() { set_violation_threshold_ms(DEFAULT_VIOLATION_THRESHOLD_MS); }

    void set_violation_threshold_ms(double ms) { violation_threshold_us_.store(static_cast<int64_t>(ms * 1000.0)); }

    void record_send(const AcqTime &acq_time) {
      size_t idx = head_.fetch_add(1, std::memory_order_relaxed) % ANALYZER_BUFFER_CAPACITY;
      has_record_.store(true, std::memory_order_release);

      int64_t gen_us = to_microseconds(acq_time);
      int64_t arr_us = to_microseconds(fins::now());

      buffer_[idx].generation_us.store(gen_us, std::memory_order_release);
      buffer_[idx].arrival_us.store(arr_us, std::memory_order_release);
    }

    bool has_record() const {
      return has_record_.load(std::memory_order_acquire);
    }

    PipeMetrics get_metrics() {
      PipeMetrics metrics = {0.0, 0.0, 0.0, 0.0, 0.0, 0, 0.0};

      int64_t now_us = to_microseconds(fins::now());
      int64_t limit_us = now_us - WINDOW_US;

      size_t current_head = head_.load(std::memory_order_acquire);
      std::vector<std::pair<int64_t, int64_t>> samples;
      samples.reserve(ANALYZER_BUFFER_CAPACITY);

      for (size_t i = 0; i < ANALYZER_BUFFER_CAPACITY; ++i) {
        size_t idx = (current_head - 1 - i + ANALYZER_BUFFER_CAPACITY) % ANALYZER_BUFFER_CAPACITY;
        int64_t arr = buffer_[idx].arrival_us.load(std::memory_order_acquire);
        int64_t gen = buffer_[idx].generation_us.load(std::memory_order_acquire);

        if (arr == 0 || arr < limit_us)
          break;
        if (arr > now_us)
          continue;

        samples.push_back({arr, gen});
      }

      metrics.count = samples.size();
      if (metrics.count == 0)
        return metrics;

      std::reverse(samples.begin(), samples.end());

      double total_sys_delay_us = 0;
      double integral_aoi_area = 0;
      double total_violation_time_us = 0;
      double max_peak_us = 0;

      int64_t thresh_us = violation_threshold_us_.load();

      int64_t t_prev = samples[0].first;
      int64_t s_prev = samples[0].second;

      for (const auto &sample: samples) {
        int64_t t_curr = sample.first;
        int64_t s_curr = sample.second;

        total_sys_delay_us += (t_curr - s_curr);

        if (t_curr > t_prev) {
          int64_t h_start = t_prev - s_prev;
          int64_t h_end = t_curr - s_prev;
          int64_t dt = t_curr - t_prev;

          integral_aoi_area += 0.5 * (static_cast<double>(h_start) + h_end) * dt;

          if (h_start > thresh_us) {
            total_violation_time_us += dt;
          } else if (h_end > thresh_us) {
            total_violation_time_us += (h_end - thresh_us);
          }

          if (h_end > max_peak_us)
            max_peak_us = h_end;
        }

        t_prev = t_curr;
        s_prev = s_curr;
      }

      if (now_us > t_prev) {
        int64_t h_start = t_prev - s_prev;
        int64_t h_end = now_us - s_prev;
        int64_t dt = now_us - t_prev;

        integral_aoi_area += 0.5 * (static_cast<double>(h_start) + h_end) * dt;

        if (h_start > thresh_us) {
          total_violation_time_us += dt;
        } else if (h_end > thresh_us) {
          total_violation_time_us += (h_end - thresh_us);
        }

        if (h_end > max_peak_us)
          max_peak_us = h_end;
      }

      double duration_us = static_cast<double>(now_us - samples[0].first);
      if (duration_us < 1.0)
        duration_us = 1.0;

      metrics.fps = (metrics.count / duration_us) * 1000000.0;
      metrics.avg_aoi_ms = (integral_aoi_area / duration_us) / 1000.0;
      metrics.sys_delay_ms = (total_sys_delay_us / metrics.count) / 1000.0;
      metrics.peak_aoi_ms = max_peak_us / 1000.0;

      metrics.violation_prob = total_violation_time_us / duration_us;

      metrics.time_window_s = duration_us / 1000000.0;

      has_record_.store(false, std::memory_order_release);

      return metrics;
    }
  };

} // namespace fins