/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// pipe.hpp

#pragma once

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>

#include "spsc_queue.hpp"

#include <fins/analysis/aoi_analyzer.hpp>
#include <fins/msg.hpp>

namespace fins {

  const int PIPE_CAPACITY = 32;

  class Pipe {
  public:
    explicit Pipe(const std::string &id = "") : id_(id), started_(true), consumer_waiting_(false) {}

    ~Pipe() { stop(); }

    void push(const AnyMsg &msg) {
      if (!started_.load(std::memory_order_relaxed))
        return;
      analyzer_.record_send(msg.acq_time);

      if (!queue_.push(msg)) {
        AnyMsg temp;
        if (queue_.pop(temp)) {
          queue_.push(msg);
        }
      }

      if (consumer_waiting_.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> lock(mutex_);
        cond_.notify_one();
      }
    }

    std::optional<AnyMsg> pop_wait() {
      AnyMsg msg;
      if (queue_.pop(msg))
        return msg;

      std::unique_lock<std::mutex> lock(mutex_);

      consumer_waiting_.store(true, std::memory_order_release);

      cond_.wait(lock, [this] { return !queue_.empty() || !started_.load(std::memory_order_acquire); });

      consumer_waiting_.store(false, std::memory_order_release);

      if (queue_.pop(msg))
        return msg;
      return std::nullopt;
    }

    bool try_pop(AnyMsg &out_msg) { return queue_.pop(out_msg); }

    PipeMetrics get_metrics() { return analyzer_.get_metrics(); }
    bool has_record() const { return analyzer_.has_record(); }

    void stop() {
      started_.store(false);
      std::lock_guard<std::mutex> lock(mutex_);
      cond_.notify_all();
    }

    void notify() {
      started_.store(false, std::memory_order_release);
      std::lock_guard<std::mutex> lock(mutex_);
      cond_.notify_all();
    }

    void reset() {
      AnyMsg temp;
      while (queue_.pop(temp)) {
      }
      started_.store(true, std::memory_order_release);
    }

    bool is_empty() { return queue_.empty(); }

    std::string get_id() const { return id_; }

  private:
    std::string id_;

    fins::SPSCQueue<AnyMsg, PIPE_CAPACITY> queue_;

    std::mutex mutex_;
    std::condition_variable cond_;

    std::atomic<bool> started_;
    std::atomic<bool> consumer_waiting_;

    AoIAnalyzer analyzer_;
  };

  class PipeFactory {
  public:
    static PipeFactory &get_instance() {
      static PipeFactory instance;
      return instance;
    }

    PipeFactory(const PipeFactory &) = delete;
    PipeFactory &operator=(const PipeFactory &) = delete;

    std::shared_ptr<Pipe> get_pipe(const std::string &id) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = pipes_.find(id);
      if (it == pipes_.end()) {
        auto pipe = std::make_shared<Pipe>(id);
        pipes_[id] = pipe;
        return pipe;
      }
      return it->second;
    }

    std::shared_ptr<Pipe> find_pipe(const std::string &id) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = pipes_.find(id);
      if (it != pipes_.end()) {
        return it->second;
      }
      return nullptr;
    }

    void remove_pipe(const std::string &id) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (pipes_.count(id)) {
        pipes_[id]->stop();
        pipes_.erase(id);
      }
    }

    void clear() {
      std::lock_guard<std::mutex> lock(mutex_);
      for (auto &[id, pipe]: pipes_) {
        pipe->stop();
      }
      pipes_.clear();
    }

    void stop_all() {
      std::lock_guard<std::mutex> lock(mutex_);
      for (auto &[id, pipe]: pipes_) {
        pipe->stop();
      }
    }

    std::map<std::string, std::shared_ptr<Pipe>> get_all_pipes() {
      std::lock_guard<std::mutex> lock(mutex_);
      return pipes_;
    }

  private:
    PipeFactory() = default;
    ~PipeFactory() { clear(); }

    std::map<std::string, std::shared_ptr<Pipe>> pipes_;
    std::mutex mutex_;
  };

#define FINS_PIPE_FACTORY (PipeFactory::get_instance())
} // namespace fins