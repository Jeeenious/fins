/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// thread_manager.hpp

#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

#include <fins/shared_states.hpp>
#include <fins/utils/logger.hpp>
#include <fins/utils/time.hpp>

namespace fins {

  using TaskType = std::function<void(size_t)>;

  enum class Priority { Urgent = 0, High = 1, Medium = 2, Low = 3 };
  enum class QueueStrategy { FCFS, LGFS };

  struct TaskWrapper {
    std::string step_id;
    std::string pipe_id;
    int port;
    TaskType task;
    TaskType drop;
    AcqTime acq_time;
    std::chrono::steady_clock::time_point enqueue_time;

    TaskWrapper(std::string sid, std::string pid, int p, TaskType t, TaskType d, AcqTime et) :
        step_id(std::move(sid)), pipe_id(std::move(pid)), port(p), task(std::move(t)), drop(std::move(d)), acq_time(et),
        enqueue_time(std::chrono::steady_clock::now()) {}
  };

  struct PipeConfig {
    Priority priority = Priority::Medium;
    QueueStrategy strategy = QueueStrategy::FCFS;
    size_t capacity = 64;
  };

  struct ThreadMetrics {
    size_t total_queue_length = 0;
    std::map<std::string, size_t> pipe_queue_lengths;
    double avg_wait_time_ms = 0.0;
    size_t dropped_tasks_count = 0;
    double utilization = 0.0;
  };

  class ThreadManager {
  private:
    static constexpr size_t K_THREAD_POOL_SIZE = 4;

    struct Worker {
      int id;
      std::thread thread;
      std::mutex mtx;
      std::condition_variable cv;
      std::deque<std::shared_ptr<TaskWrapper>> queue;
      std::atomic<bool> running{false};
      std::atomic<bool> stop_flag{false};

      size_t processed_count = 0;
    };

    std::vector<std::unique_ptr<Worker>> workers_;
    std::atomic<bool> stop_global_{false};
    std::atomic<size_t> dropped_tasks_count_{0};

    std::map<std::string, PipeConfig> pipe_configs_;
    std::mutex config_mutex_;

  public:
    static ThreadManager &get_instance() {
      static ThreadManager instance;
      return instance;
    }

    ThreadManager() {
      workers_.reserve(K_THREAD_POOL_SIZE);
      for (size_t i = 0; i < K_THREAD_POOL_SIZE; ++i) {
        auto w = std::make_unique<Worker>();
        w->id = static_cast<int>(i);
        workers_.push_back(std::move(w));
      }
    }

    ~ThreadManager() { shutdown(); }

    void set_urgent_threads(size_t) {}
    void set_high_threads(size_t) {}
    void set_medium_threads(size_t) {}
    void set_low_threads(size_t) {}

    void set_pipe_config(const std::string &pipe_id, Priority p, QueueStrategy s, size_t capacity = 64) {
      std::lock_guard<std::mutex> lock(config_mutex_);
      pipe_configs_[pipe_id] = {p, s, capacity};
    }

    void start() {
      if (stop_global_)
        return;

      for (auto &w: workers_) {
        {
          std::lock_guard<std::mutex> lk(w->mtx);
          if (w->running)
            continue;
          w->running = true;
          w->stop_flag = false;
        }
        w->thread = std::thread(&ThreadManager::worker_loop, this, w.get());
      }

      FINS_LOG_INFO("[ThreadManager] Started fixed thread pool with {} threads.", K_THREAD_POOL_SIZE);
    }

    void shutdown() {
      stop_global_ = true;

      for (auto &w: workers_) {
        {
          std::lock_guard<std::mutex> lk(w->mtx);
          w->stop_flag = true;
        }
        w->cv.notify_all();
      }

      for (auto &w: workers_) {
        if (w->thread.joinable()) {
          w->thread.join();
        }
        w->running = false;
      }
    }

    void enqueue(const std::string &step_id, const std::string &pipe_id, int port, TaskType task, TaskType drop,
                 AcqTime acq_time) {

      if (stop_global_)
        return;

      if (get_running_state() == Running_State::PAUSE) {
        dropped_tasks_count_++;
        if (drop)
          drop(0);
        return;
      }

      size_t thread_idx = calculate_affinity_index(step_id);
      Worker *target_worker = workers_[thread_idx].get();

      auto wrapper = std::make_shared<TaskWrapper>(step_id, pipe_id, port, std::move(task), std::move(drop), acq_time);

      {
        std::unique_lock<std::mutex> lock(target_worker->mtx);

        if (target_worker->queue.size() > 2048) {
          dropped_tasks_count_++;
          FINS_LOG_WARN("Queue full for step {}, dropping task", step_id);
          if (wrapper->drop)
            wrapper->drop(0);
          return;
        }

        target_worker->queue.push_back(wrapper);
      }

      target_worker->cv.notify_one();
    }

    ThreadMetrics get_metrics() {
      ThreadMetrics metrics;
      for (const auto &w: workers_) {
        std::lock_guard<std::mutex> lock(w->mtx);
        size_t qs = w->queue.size();
        metrics.total_queue_length += qs;
        if (qs > 0 || w->running) {
          metrics.pipe_queue_lengths["Thread_" + std::to_string(w->id)] = qs;
        }
      }
      metrics.dropped_tasks_count = dropped_tasks_count_;
      return metrics;
    }

  private:
    size_t calculate_affinity_index(const std::string &step_id) {
      std::hash<std::string> hasher;
      return hasher(step_id) % K_THREAD_POOL_SIZE;
    }

    void worker_loop(Worker *me) {
      std::string thread_name = "fins_w_" + std::to_string(me->id);
#ifdef __linux__
      pthread_setname_np(pthread_self(), thread_name.substr(0, 15).c_str());
#endif

#ifdef __linux__
      int num_cores = std::thread::hardware_concurrency();
      if (num_cores > 0) {
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(me->id % num_cores, &cpuset);
        int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0) {
          FINS_LOG_ERROR("[ThreadManager] Error setting thread affinity: {}", rc);
        }
      }
#endif

      while (true) {
        std::shared_ptr<TaskWrapper> task;

        {
          std::unique_lock<std::mutex> lock(me->mtx);
          me->cv.wait(lock, [me, this] { return me->stop_flag || stop_global_ || !me->queue.empty(); });

          if ((me->stop_flag || stop_global_) && me->queue.empty()) {
            return;
          }

          if (!me->queue.empty()) {
            task = me->queue.front();
            me->queue.pop_front();
          }
        }

        if (task) {
          double t_start = std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();

          try {
            task->task(0);
          } catch (const std::exception &e) {
            FINS_LOG_ERROR("[ThreadManager] Exception in step {}: {}", task->step_id, e.what());
          }

          double t_end = std::chrono::duration<double>(std::chrono::system_clock::now().time_since_epoch()).count();

          int current_cpu = me->id;
#ifdef __linux__
          current_cpu = sched_getcpu();
#endif

          me->processed_count++;
        }
      }
    }
  };

#define FINS_THREAD_MANAGER fins::ThreadManager::get_instance()
} // namespace fins