/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// service/service_manager.cpp

#include "service_manager.hpp"
#include <fins/utils/logger.hpp>
#include <stdexcept>
#include <pthread.h>

namespace fins {

  ServiceManager &ServiceManager::get_instance() {
    static ServiceManager instance;
    return instance;
  }

  ServiceManager::ServiceManager() : stop_(false) {
    worker_thread_ = std::thread(&ServiceManager::worker_loop, this);
    // FINS_LOG_INFO("[ServiceManager] Service Manager started.");
  }

  ServiceManager::~ServiceManager() {
    stop_ = true;
    cv_.notify_all();
    if (worker_thread_.joinable()) {
      worker_thread_.join();
    }
    // FINS_LOG_INFO("[ServiceManager] Service Manager stopped.");
  }

  void ServiceManager::register_service(const std::string &topic, ServiceCallback cb, std::type_index inputs_id,
                                        std::type_index outputs_id) {
    std::lock_guard<std::mutex> lock(map_mutex_);
    if (services_.find(topic) != services_.end()) {
      FINS_LOG_WARN("[ServiceManager] Overwriting existing service on topic: {}", topic);
    }
    services_[topic] = {cb, inputs_id, outputs_id};
  }

  std::future<std::any> ServiceManager::call_service(const std::string &topic, std::vector<std::any> args,
                                                     std::type_index req_inputs_id, std::type_index req_outputs_id) {
    auto promise = std::make_shared<std::promise<std::any>>();
    std::future<std::any> future = promise->get_future();

    {
      std::lock_guard<std::mutex> lock(queue_mutex_);
      if (stop_) {
        promise->set_exception(std::make_exception_ptr(std::runtime_error("ServiceManager is stopping")));
        return future;
      }
      tasks_.push_back({topic, std::move(args), promise, req_inputs_id, req_outputs_id});
    }

    cv_.notify_one();
    return future;
  }

  void ServiceManager::worker_loop() {
    pthread_setname_np(pthread_self(), "fins_service");
    while (!stop_) {
      Task task;
      {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });

        if (stop_ && tasks_.empty())
          return;

        task = std::move(tasks_.front());
        tasks_.pop_front();
      }

      ServiceEntry entry;
      bool found = false;
      {
        std::lock_guard<std::mutex> lock(map_mutex_);
        auto it = services_.find(task.topic);
        if (it != services_.end()) {
          entry = it->second;
          found = true;
        }
      }

      if (!found) {
        FINS_LOG_ERROR("[ServiceManager] Service not found: {}", task.topic);
        try {
          throw std::runtime_error("Service not found: " + task.topic);
        } catch (...) {
          task.promise->set_exception(std::current_exception());
        }
        continue;
      }

      if (entry.input_type_id != task.input_check || entry.output_type_id != task.output_check) {
        FINS_LOG_ERROR("[ServiceManager] Type mismatch for service: {}. Check client/server signatures.", task.topic);
        try {
          throw std::runtime_error("Service type mismatch for " + task.topic);
        } catch (...) {
          task.promise->set_exception(std::current_exception());
        }
        continue;
      }

      try {
        std::any result = entry.callback(task.args);
        task.promise->set_value(result);
      } catch (const std::exception &e) {
        FINS_LOG_ERROR("[ServiceManager] Exception during execution of service {}: {}", task.topic, e.what());
        task.promise->set_exception(std::current_exception());
      } catch (...) {
        FINS_LOG_ERROR("[ServiceManager] Unknown exception during execution of service {}", task.topic);
        task.promise->set_exception(std::current_exception());
      }
    }
  }

} // namespace fins