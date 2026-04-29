/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// spsc_queue.hpp

#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <vector>

namespace fins {

  template<typename T, size_t Capacity>
  class SPSCQueue {
  public:
    SPSCQueue() : head_(0), tail_(0) {}

    SPSCQueue(const SPSCQueue &) = delete;
    SPSCQueue &operator=(const SPSCQueue &) = delete;

    bool push(const T &item) {
      const size_t current_head = head_.load(std::memory_order_relaxed);
      const size_t next_head = (current_head + 1) % (Capacity + 1);

      if (next_head == tail_.load(std::memory_order_acquire)) {
        return false;
      }

      buffer_[current_head] = item;
      head_.store(next_head, std::memory_order_release);
      return true;
    }

    bool pop(T &item) {
      const size_t current_tail = tail_.load(std::memory_order_relaxed);

      if (current_tail == head_.load(std::memory_order_acquire)) {
        return false;
      }

      item = buffer_[current_tail];
      tail_.store((current_tail + 1) % (Capacity + 1), std::memory_order_release);
      return true;
    }

    bool empty() const { return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire); }

    size_t read_available() const {
      size_t head = head_.load(std::memory_order_relaxed);
      size_t tail = tail_.load(std::memory_order_relaxed);
      if (head >= tail)
        return head - tail;
      return (Capacity + 1) - (tail - head);
    }

  private:
    std::array<T, Capacity + 1> buffer_;

    alignas(64) std::atomic<size_t> head_;
    alignas(64) std::atomic<size_t> tail_;
  };

} // namespace fins