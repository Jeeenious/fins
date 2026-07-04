/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// step.hpp

#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <sched.h>
#include <pthread.h>
#include <sys/resource.h>
#include <unistd.h>

#include <fins/msg.hpp>
#include <fins/node.hpp>
#include <fins/pipe.hpp>
#include <fins/thread_manager.hpp>
#include <fins/utils/time.hpp>
#include <fins/utils/performance_recorder.hpp>

namespace fins {

  class Step : public std::enable_shared_from_this<Step> {
  private:
    std::string id_;
    std::shared_ptr<INode> node_;
    NodeMeta cached_meta_;

    std::map<int, std::vector<std::string>> pipes_in_;
    std::map<int, std::vector<std::string>> pipes_out_;
    std::mutex pipes_mutex_;

    std::vector<std::thread> input_threads_;
    std::atomic<bool> running_{false};
    std::atomic<bool> shutdown_requested_{false};

    ScheduleInfo schedule_info_;
    bool schedule_configured_{false};
    std::atomic<bool> is_busy_{false};

  public:
    Step(const std::string &id, std::shared_ptr<INode> node) : id_(id), node_(node) {
      if (node) {
        cached_meta_ = node->get_meta(); 
      }

      node_->set_publisher([this](int port_index, AnyMsg msg) { this->publish(port_index, msg); });

      node_->set_connection_checker([this](int port_index) -> bool {
        std::lock_guard<std::mutex> lock(pipes_mutex_);
        auto it = pipes_out_.find(port_index);
        return (it != pipes_out_.end() && !it->second.empty());
      });
    }

    ~Step() { stop_threads(); }

    std::string get_id() const { return id_; }

    NodeMeta get_node_meta() const { 
      return cached_meta_; 
    }

    void run() {
      if (running_ || !node_)
        return;
      running_ = true;
      shutdown_requested_ = false;

      node_->run();
      start_threads();
    }

    void pause() {
      if (!running_)
        return;
      running_ = false;

      stop_threads();
      if (node_) {
        node_->pause();
      }
    }

    void reset() { 
      if (node_) {
        node_->reset(); 
      }
    }
    bool is_running() const { return running_; }
    void add_input_pipe(int port, const std::string &pipe_id) {
      std::lock_guard<std::mutex> lock(pipes_mutex_);
      FINS_PIPE_FACTORY.get_pipe(pipe_id);

      auto &pipes = pipes_in_[port];
      if (std::find(pipes.begin(), pipes.end(), pipe_id) == pipes.end()) {
        pipes.push_back(pipe_id);
      }
    }

    void add_output_pipe(int port, const std::string &pipe_id) {
      std::lock_guard<std::mutex> lock(pipes_mutex_);
      FINS_PIPE_FACTORY.get_pipe(pipe_id);

      auto &pipes = pipes_out_[port];
      if (std::find(pipes.begin(), pipes.end(), pipe_id) == pipes.end()) {
        pipes.push_back(pipe_id);
      }
    }

    void remove_all_pipes() {
      std::lock_guard<std::mutex> lock(pipes_mutex_);
      pipes_in_.clear();
      pipes_out_.clear();
    }

    void update_node_parameter(const std::string &name, const std::string &value) {
      if (node_) {
        node_->update_parameter(name, value);
      }
    }

    std::shared_ptr<INode> get_node() const { return node_; }

    void inject_node(std::shared_ptr<INode> new_node) {
      std::lock_guard<std::mutex> lock(pipes_mutex_);
      
      if (node_) {
        node_->pause();
      }

      node_ = new_node;

      if (node_) {
        cached_meta_ = node_->get_meta(); 
        
        node_->set_publisher([this](int port_index, AnyMsg msg) { 
          this->publish(port_index, msg); 
        });
        node_->set_connection_checker([this](int port_index) -> bool {
          std::lock_guard<std::mutex> lock(pipes_mutex_);
          auto it = pipes_out_.find(port_index);
          return (it != pipes_out_.end() && !it->second.empty());
        });
      }
    }

    std::string get_port_description(int port_index) const {
      if (port_index >= 0 && static_cast<size_t>(port_index) < cached_meta_.inputs.size()) {
        const auto& port_info = cached_meta_.inputs[port_index];
        return port_info.name;
      }
      return "unknown";
    }

    std::vector<LogEntry> get_logs() {
      if (node_) {
        return node_->get_logs();
      }
      return {};
    }

    void set_schedule(const ScheduleInfo &info) {
      schedule_info_ = info;
      schedule_configured_ = true;
      
      apply_schedule_policy();
    }

    const ScheduleInfo& get_schedule() const {
      return schedule_info_;
    }

    bool has_schedule() const {
      return schedule_configured_;
    }

  private:
    void publish(int port_index, AnyMsg msg) {
      std::vector<std::string> target_pipes;
      {
        std::lock_guard<std::mutex> lock(pipes_mutex_);
        if (pipes_out_.find(port_index) == pipes_out_.end())
          return;
        target_pipes = pipes_out_[port_index];
      }

      for (const auto &pid: target_pipes) {
        auto pipe = FINS_PIPE_FACTORY.get_pipe(pid);
        if (pipe) {
          pipe->push(msg);
        }
      }
    }

    void start_threads() {
      auto meta = node_->get_meta();
      size_t input_count = meta.inputs.size();

      shutdown_requested_ = false;

      for (size_t i = 0; i < input_count; ++i) {
        input_threads_.emplace_back([this, i]() { this->input_listener_loop(i); });
      }
    }

    void stop_threads() {
      shutdown_requested_ = true;

      {
        std::lock_guard<std::mutex> lock(pipes_mutex_);
        for (auto &pair: pipes_in_) {
          for (auto &pid: pair.second) {
            auto p = FINS_PIPE_FACTORY.find_pipe(pid);
            if (p) {
              p->notify();
            }
          }
        }
      }

      for (auto &t: input_threads_) {
        if (t.joinable())
          t.join();
      }
      input_threads_.clear();
    }

    void apply_schedule_policy() {
      if (!schedule_configured_) {
        return;
      }

      pthread_t self = pthread_self();

      if (schedule_info_.priority == SchedulePriority::Urgent || 
          schedule_info_.priority == SchedulePriority::High) {
        
        struct sched_param param;
        param.sched_priority = (schedule_info_.priority == SchedulePriority::Urgent) ? 50 : 30;
        
        int result = pthread_setschedparam(self, SCHED_FIFO, &param);
        if (result != 0) {
          FINS_LOG_WARN("[Step {}] Failed to set SCHED_FIFO (errno={}), falling back to nice value. "
                        "Priority: {}", id_, result, 
                        (schedule_info_.priority == SchedulePriority::Urgent) ? "Urgent" : "High");
          
          int nice_val = (schedule_info_.priority == SchedulePriority::Urgent) ? -20 : -10;
          setpriority(PRIO_PROCESS, 0, nice_val);
        }
      } else {
        struct sched_param param;
        param.sched_priority = 0;
        pthread_setschedparam(self, SCHED_OTHER, &param);
        
        int nice_val;
        if (schedule_info_.priority == SchedulePriority::Medium) {
          nice_val = 0;
        } else {
          nice_val = 10;
        }
        setpriority(PRIO_PROCESS, 0, nice_val);
      }

      FINS_LOG_INFO("[Step {}] Schedule policy applied: priority={}, queue={}", id_,
                    (schedule_info_.priority == SchedulePriority::Urgent) ? "Urgent" :
                    (schedule_info_.priority == SchedulePriority::High) ? "High" :
                    (schedule_info_.priority == SchedulePriority::Medium) ? "Medium" : "Low",
                    (schedule_info_.queue == ScheduleQueue::FCFS) ? "FCFS" : "LGFS");
    }

    bool should_drop_message() const {
      if (schedule_info_.queue == ScheduleQueue::LGFS && is_busy_.load(std::memory_order_relaxed)) {
        return true;
      }
      return false;
    }

  private:
    struct PipeEntry {
      int port;
      std::string pid;
      std::shared_ptr<Pipe> ptr;
    };

    static constexpr uint64_t WINDOW_MS = 100;
    static constexpr float UP_THRESHOLD = 0.85f;

    std::atomic<bool> force_pool_{false};

    void input_listener_loop(int port_index) {
      char t_name[16];
      snprintf(t_name, sizeof(t_name), "f_in_%d_%s", port_index, id_.c_str());
      pthread_setname_np(pthread_self(), t_name);

      apply_schedule_policy();

      std::vector<PipeEntry> active_pipes;
      {
        std::lock_guard<std::mutex> lock(pipes_mutex_);
        if (pipes_in_.count(port_index)) {
          for (const auto &pid: pipes_in_[port_index]) {
            auto p = FINS_PIPE_FACTORY.find_pipe(pid);
            if (p)
              active_pipes.push_back({port_index, pid, p});
          }
        }
      }

      if (active_pipes.empty()) {
        return;
      }

      auto window_start = std::chrono::steady_clock::now();
      uint64_t active_us_sum = 0;

      while (!shutdown_requested_) {
        bool worked = false;
        AnyMsg msg;
        auto loop_start = std::chrono::steady_clock::now();

#ifndef FINS_LIGHT_SCHEDULER
        if (!force_pool_.load(std::memory_order_relaxed)) {
          auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(loop_start - window_start).count();
          if (elapsed >= WINDOW_MS) {
            float occupancy = (float) active_us_sum / (elapsed * 1000.0f);
            if (occupancy > UP_THRESHOLD) {
              force_pool_.store(true, std::memory_order_release);
              FINS_LOG_INFO("[Step {}] Thread occupancy {:.2f}% exceeded threshold. Locked to THREAD_POOL mode.", id_,
                            occupancy * 100);
            }
            active_us_sum = 0;
            window_start = loop_start;
          }
        }
#endif

        for (auto &entry: active_pipes) {
          if (schedule_configured_ && schedule_info_.queue == ScheduleQueue::LGFS) {
            AnyMsg tmp_msg;
            if (!entry.ptr->try_pop(tmp_msg)) {
              continue;
            }
            msg = tmp_msg;
          } else {
            auto msg_opt = entry.ptr->pop_wait();
            if (!msg_opt)
              continue;
            msg = *msg_opt;
          }

          if (should_drop_message()) {
            FINS_LOG_DEBUG("[Step {}] LGFS: Dropping message for {}", id_, get_port_description(entry.port));
            continue;
          }

          auto t_recv = fins::now(); 
          worked = true;
          const AnyMsg &msg_ref = msg;

          if (schedule_configured_ && schedule_info_.queue == ScheduleQueue::LGFS) {
            is_busy_.store(true, std::memory_order_relaxed);
          }

#ifdef FINS_LIGHT_SCHEDULER
          node_->on_input(entry.port, msg_ref);
#else
          if (!force_pool_.load(std::memory_order_relaxed)) {

            auto t_start = std::chrono::steady_clock::now();
            auto cpu_start = get_thread_cpu_time_ns();
            node_->on_input(entry.port, msg_ref);
            auto cpu_end = get_thread_cpu_time_ns();
            auto t_end = std::chrono::steady_clock::now();

            active_us_sum += std::chrono::duration_cast<std::chrono::microseconds>(t_end - t_start).count();
            
            auto t_comp = fins::now();
            FINS_PERF_MONITOR.push_record({
              id_, 
              entry.port, 
              get_port_description(entry.port),
              to_nanoseconds(msg.acq_time),
              to_nanoseconds(t_recv),
              to_nanoseconds(t_comp),
              (cpu_end - cpu_start),
              (int64_t)pthread_self()
            });
          } else {
            auto self = shared_from_this();
            FINS_THREAD_MANAGER.enqueue(
                id_, entry.pid, entry.port, 
                [self, p = entry.port, msg, t_recv](size_t) { 
                  auto c_start = get_thread_cpu_time_ns();
                  self->node_->on_input(p, msg);
                  auto c_end = get_thread_cpu_time_ns();
                  
                  auto t_comp = fins::now();
                  FINS_PERF_MONITOR.push_record({
                    self->id_, p, 
                    self->get_port_description(p),
                    to_nanoseconds(msg.acq_time),
                    to_nanoseconds(t_recv),
                    to_nanoseconds(t_comp),
                    (c_end - c_start),
                    (int64_t)pthread_self()
                  });
                },
                [](size_t) {}, msg.acq_time);
          }
#endif

          if (schedule_configured_ && schedule_info_.queue == ScheduleQueue::LGFS) {
            is_busy_.store(false, std::memory_order_relaxed);
          }
        }
      }
    }
  };
} // namespace fins