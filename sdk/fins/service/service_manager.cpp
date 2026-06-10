/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// service/service_manager.cpp

#include "service_manager.hpp"
#include <fins/utils/logger.hpp>
#include <stdexcept>

namespace fins {

  ServiceManager &ServiceManager::get_instance() {
    static ServiceManager instance;
    return instance;
  }

  ServiceManager::ServiceManager() {}
  ServiceManager::~ServiceManager() {}

  void ServiceManager::register_service(const std::string &topic, ServiceCallback cb, std::type_index inputs_id,
                                        std::type_index outputs_id) {
    std::lock_guard<std::mutex> lock(map_mutex_);
    if (services_.find(topic) != services_.end()) {
      FINS_LOG_WARN("[ServiceManager] Overwriting existing service on topic: {}", topic);
    }
    services_[topic] = {cb, inputs_id, outputs_id, nullptr};
  }

  void ServiceManager::register_service_handler(const std::string &topic, std::unique_ptr<ServiceHandler> handler,
                                                std::type_index inputs_id, std::type_index outputs_id) {
    std::lock_guard<std::mutex> lock(map_mutex_);
    if (services_.find(topic) != services_.end()) {
      FINS_LOG_WARN("[ServiceManager] Overwriting existing service on topic: {}", topic);
    }
    services_[topic] = {nullptr, inputs_id, outputs_id, std::move(handler)};
  }

  std::future<std::any> ServiceManager::call_service(const std::string &topic, std::vector<std::any> args,
                                                     std::type_index req_inputs_id, std::type_index req_outputs_id) {
    auto promise = std::make_shared<std::promise<std::any>>();
    std::future<std::any> future = promise->get_future();

    std::lock_guard<std::mutex> lock(map_mutex_);
    auto it = services_.find(topic);
    if (it == services_.end()) {
      FINS_LOG_ERROR("[ServiceManager] Service not found: {}", topic);
      promise->set_exception(std::make_exception_ptr(std::runtime_error("Service not found: " + topic)));
      return future;
    }

    const auto &entry = it->second;
    if (entry.input_type_id != req_inputs_id || entry.output_type_id != req_outputs_id) {
      FINS_LOG_ERROR("[ServiceManager] Type mismatch for service: {}. Check client/server signatures.", topic);
      promise->set_exception(std::make_exception_ptr(std::runtime_error("Service type mismatch for " + topic)));
      return future;
    }

    try {
      std::any result;
      if (entry.handler) {
        result = entry.handler->invoke(args.data(), args.size());
      } else if (entry.callback) {
        result = entry.callback(args);
      } else {
        throw std::runtime_error("Service entry has no handler or callback: " + topic);
      }
      promise->set_value(result);
      FINS_LOG_INFO("[ServiceManager] Service {} executed successfully", topic);
    } catch (const std::exception &e) {
      FINS_LOG_ERROR("[ServiceManager] Exception during execution of service {}: {}", topic, e.what());
      promise->set_exception(std::current_exception());
    } catch (...) {
      FINS_LOG_ERROR("[ServiceManager] Unknown exception during execution of service {}", topic);
      promise->set_exception(std::current_exception());
    }

    return future;
  }

} // namespace fins
