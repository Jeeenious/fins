/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// studio.hpp

#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

#include <fins/shared_states.hpp>
#include <fins/step.hpp>
#include <fins/utils/logger.hpp>

namespace fins {

  class Studio {
  public:
    static Studio &GetInstance() {
      static Studio instance;
      return instance;
    }

    Studio(const Studio &) = delete;
    Studio &operator=(const Studio &) = delete;

    std::shared_ptr<Step> add_step(std::shared_ptr<INode> node, const std::string &id) {
      std::lock_guard<std::mutex> lock(mutex_);

      if (steps_.find(id) != steps_.end()) {
        throw std::runtime_error("Step ID " + id + " already exists.");
      }

      auto step = std::make_shared<Step>(id, node);
      steps_[id] = step;

      auto meta = node->get_meta();
      FINS_LOG_DEBUG("[Studio] Added node [{}] type: {}", id, meta.name);

      return step;
    }

    void remove_step(const std::string &id) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (steps_.find(id) == steps_.end())
        return;

      auto step = steps_[id];
      step->pause();
      step->remove_all_pipes();
      steps_.erase(id);

      topology_order_.erase(std::remove(topology_order_.begin(), topology_order_.end(), id), topology_order_.end());
    }

    void set_topology_order(const std::vector<std::string> &order) {
      std::lock_guard<std::mutex> lock(mutex_);
      topology_order_ = order;
    }

    void add_pipe(const std::string &from_id, const std::string &to_id, int from_port, int to_port,
                  std::string pipe_id = "") {
      std::lock_guard<std::mutex> lock(mutex_);

      if (!steps_.count(from_id) || !steps_.count(to_id)) {
        throw std::invalid_argument("Node ID not found.");
      }

      auto from_step = steps_[from_id];
      auto to_step = steps_[to_id];

      auto from_meta = from_step->get_node_meta();
      auto to_meta = to_step->get_node_meta();

      if (static_cast<size_t>(from_port) >= from_meta.outputs.size())
        throw std::out_of_range("Output port index out of range");
      if (static_cast<size_t>(to_port) >= to_meta.inputs.size())
        throw std::out_of_range("Input port index out of range");

      std::string out_type = from_meta.outputs[from_port].type;
      std::string in_type = to_meta.inputs[to_port].type;

      if (out_type != in_type) {
        throw std::runtime_error("Type mismatch: " + from_id + ":" + std::to_string(from_port) + "(" + out_type +
                                 ") -> " + to_id + ":" + std::to_string(to_port) + "(" + in_type + ")");
      }

      if (pipe_id.empty()) {
        static std::atomic<int> pipe_seq{0};
        pipe_id = "pipe_" + std::to_string(pipe_seq++);
      }

      from_step->add_output_pipe(from_port, pipe_id);
      to_step->add_input_pipe(to_port, pipe_id);

      FINS_LOG_DEBUG("[Studio] Pipe connected: {} ({}:{} -> {}:{})", pipe_id, from_id, from_port, to_id, to_port);
    }

    std::vector<std::string> get_all_step_ids() {
      std::lock_guard<std::mutex> lock(mutex_);
      std::vector<std::string> ids;
      for (const auto &[id, step]: steps_) {
        ids.push_back(id);
      }
      return ids;
    }

    std::shared_ptr<Step> get_step(const std::string &id) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (steps_.find(id) != steps_.end()) {
        return steps_[id];
      }
      return nullptr;
    }

    void set_step_parameter(const std::string &step_id, const std::string &param_name, const std::string &value) {

      std::lock_guard<std::mutex> lock(mutex_);
      auto it = steps_.find(step_id);
      if (it == steps_.end()) {
        FINS_LOG_ERROR("[Studio] Error: Step not found: {}", step_id);
        return;
      }

      FINS_LOG_DEBUG("[Studio] Setting parameter for {}: {} = {}", step_id, param_name, value);

      it->second->update_node_parameter(param_name, value);
    }

    void set_step_client_topic(const std::string &step_id, const std::string &key, const std::string &topic) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = steps_.find(step_id);
      if (it == steps_.end()) {
        FINS_LOG_ERROR("[Studio] Error: Step not found: {}", step_id);
        return;
      }

      it->second->set_client_topic(key, topic);
    }

    void set_step_server_topic(const std::string &step_id, const std::string &key, const std::string &topic) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = steps_.find(step_id);
      if (it == steps_.end()) {
        FINS_LOG_ERROR("[Studio] Error: Step not found: {}", step_id);
        return;
      }

      it->second->set_server_topic(key, topic);
    }

    void set_step_commander_topic(const std::string &step_id, const std::string &key, const std::string &topic) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = steps_.find(step_id);
      if (it == steps_.end()) {
        FINS_LOG_ERROR("[Studio] Error: Step not found: {}", step_id);
        return;
      }

      it->second->set_commander_topic(key, topic);
    }

    void set_step_actor_topic(const std::string &step_id, const std::string &key, const std::string &topic) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = steps_.find(step_id);
      if (it == steps_.end()) {
        FINS_LOG_ERROR("[Studio] Error: Step not found: {}", step_id);
        return;
      }

      it->second->set_actor_topic(key, topic);
    }

    void set_step_schedule(const std::string &step_id, const ScheduleInfo &schedule) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto it = steps_.find(step_id);
      if (it == steps_.end()) {
        FINS_LOG_ERROR("[Studio] Error: Step not found: {}", step_id);
        return;
      }

      it->second->set_schedule(schedule);
      FINS_LOG_DEBUG("[Studio] Set schedule for {}: priority={}, queue={}", step_id,
                     (schedule.priority == SchedulePriority::Urgent) ? "Urgent" :
                     (schedule.priority == SchedulePriority::High) ? "High" :
                     (schedule.priority == SchedulePriority::Medium) ? "Medium" : "Low",
                     (schedule.queue == ScheduleQueue::FCFS) ? "FCFS" : "LGFS");
    }

    void run() {
      std::lock_guard<std::mutex> lock(mutex_);
      set_running_state(Running_State::RUN);
      FINS_LOG_INFO("[Studio] Starting nodes...");

      if (!topology_order_.empty()) {
        for (const auto &id: topology_order_) {
          auto it = steps_.find(id);
          if (it != steps_.end()) {
            FINS_LOG_INFO("[Studio] Starting node: {}...", id);
            it->second->run();
          }
        }
      } else {
        for (auto &[id, step]: steps_) {
          FINS_LOG_INFO("[Studio] Starting node: {}...", id);
          step->run();
        }
      }
    }

    bool is_running() const { return get_running_state() == Running_State::RUN; }

    void pause() {
      std::lock_guard<std::mutex> lock(mutex_);
      set_running_state(Running_State::PAUSE);
      FINS_LOG_DEBUG("[Studio] Pausing...");

      if (!topology_order_.empty()) {
        for (auto it = topology_order_.rbegin(); it != topology_order_.rend(); ++it) {
          auto step_it = steps_.find(*it);
          if (step_it != steps_.end()) {
            if (step_it->second->is_running()) {
              FINS_LOG_INFO("[Studio] Pausing node: {}...", *it);
              step_it->second->pause();
            }
          }
        }
      } else {
        for (auto &[id, step]: steps_) {
          if (step->is_running()) {
            FINS_LOG_INFO("[Studio] Pausing node: {}...", id);
            step->pause();
          }
        }
      }
    }

    void reset() {
      std::lock_guard<std::mutex> lock(mutex_);
      FINS_PIPE_FACTORY.clear();
      for (auto &[id, step]: steps_) {
        step->reset();
      }
    }

    void clear() {
      std::map<std::string, std::shared_ptr<Step>> steps_to_destroy;

      {
        std::lock_guard<std::mutex> lock(mutex_);
        set_running_state(Running_State::PAUSE);

        // FINS_LOG_INFO("[Studio] Stopping all nodes in reverse topological order...");

        FINS_PIPE_FACTORY.stop_all();

        if (!topology_order_.empty()) {
          for (auto it = topology_order_.rbegin(); it != topology_order_.rend(); ++it) {
            auto step_it = steps_.find(*it);
            if (step_it != steps_.end()) {
              if (step_it->second->is_running()) {
                FINS_LOG_INFO("[Studio] Stopping node: {}...", *it);
                step_it->second->pause();
              }
            }
          }
        } else {
          for (auto &[id, step]: steps_) {
            if (step->is_running()) {
              FINS_LOG_INFO("[Studio] Stopping node: {}...", id);
              step->pause();
            }
          }
        }

        // FINS_LOG_INFO("[Studio] All nodes stopped. Moving steps for safe destruction...");

        steps_to_destroy = std::move(steps_);
        steps_.clear();
        topology_order_.clear();
      }

      if (!steps_to_destroy.empty()) {
        // FINS_LOG_INFO("[Studio] Executing destructors of {} nodes...", steps_to_destroy.size());
        for (auto it = steps_to_destroy.begin(); it != steps_to_destroy.end(); ) {
          FINS_LOG_INFO("[Studio] Destroying node: {}...", it->first);
          it = steps_to_destroy.erase(it);
        }
      }

      FINS_PIPE_FACTORY.clear();
      // FINS_LOG_INFO("[Studio] Studio cleanup complete.");
    }

  private:
    Studio() = default;
    ~Studio() { steps_.clear(); }

    std::map<std::string, std::shared_ptr<Step>> steps_;
    std::vector<std::string> topology_order_;
    std::mutex mutex_;
  };

#define FINS_STUDIO (fins::Studio::GetInstance())

} // namespace fins