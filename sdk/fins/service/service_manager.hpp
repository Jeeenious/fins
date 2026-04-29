/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// service/service_manager.hpp

#pragma once

#include <any>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <typeindex>
#include <vector>

#include <fins/macros.hpp>

namespace fins {

  class FINS_API ServiceManager {
  public:
    using ServiceCallback = std::function<std::any(const std::vector<std::any> &)>;

    static ServiceManager &get_instance();

    ServiceManager(const ServiceManager &) = delete;
    ServiceManager &operator=(const ServiceManager &) = delete;

    void register_service(const std::string &topic, ServiceCallback cb, std::type_index inputs_id,
                          std::type_index outputs_id);

    std::future<std::any> call_service(const std::string &topic, std::vector<std::any> args,
                                       std::type_index req_inputs_id, std::type_index req_outputs_id);

  private:
    ServiceManager();
    ~ServiceManager();

    struct ServiceEntry {
      ServiceCallback callback;
      std::type_index input_type_id = std::type_index(typeid(void));
      std::type_index output_type_id = std::type_index(typeid(void));
    };

    struct Task {
      std::string topic;
      std::vector<std::any> args;
      std::shared_ptr<std::promise<std::any>> promise;
      std::type_index input_check = std::type_index(typeid(void));
      std::type_index output_check = std::type_index(typeid(void));
    };

    void worker_loop();

  private:
    std::map<std::string, ServiceEntry> services_;
    std::mutex map_mutex_;

    std::deque<Task> tasks_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;

    std::thread worker_thread_;
    std::atomic<bool> stop_;
  };

} // namespace fins

#define FINS_SERVICE_MANAGER ServiceManager::get_instance()