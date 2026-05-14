/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 *******************************************************************************/

// server.hpp

#pragma once

#include <atomic>
#include <chrono>
#include <fstream>
#include <iostream>
#include <mutex>
#include <pthread.h>
#include <stdexcept>
#include <string>
#include <thread>

#include <fins/third_party/httplib.h>
#include <fins/third_party/json.hpp>

#include <fins/analysis/system_monitor.hpp>
#include <fins/nodelib.hpp>
#include <fins/server/parameter_server.hpp>
#include <fins/studio.hpp>
#include <fins/thread_manager.hpp>
#include <fins/utils/logger.hpp>

namespace fins {

  using json = nlohmann::json;

  class AgentServer {
  public:
    AgentServer(NodeLib &node_lib) : node_lib_(node_lib) { 
      setup_routes(); 
      node_lib_.set_on_reloaded([this]() {
        FINS_LOG_INFO("[AgentServer] Plugin reloaded. Re-registering with new capabilities...");
        httplib::Client client(orchestrator_server_);
        registerAgent(client);
      });
    }

    ~AgentServer() { stop(); }

    void start(const std::string &agent_id, const std::string &agent_ip, int agent_port) {
      if (is_running_) {
        FINS_LOG_WARN("[AgentServer] Warning: Server is already running.");
        return;
      }

      agent_id_ = agent_id;
      agent_ip_ = agent_ip;
      agent_port_ = agent_port;

      server_thread_ = std::thread([this, agent_port]() {
        FINS_LOG_INFO("[AgentServer] Starting HTTP server on port {}...", agent_port);
        server_.listen("0.0.0.0", agent_port);
      });

      stop_monitoring_ = false;
      monitoring_thread_ = std::thread(&AgentServer::monitoringLoop, this);

      is_running_ = true;
      FINS_LOG_INFO("[AgentServer] Agent '{}' started. Reporting to {}", agent_id_, orchestrator_server_);
    }

    void connect(const std::string &orchestrator_server) { orchestrator_server_ = orchestrator_server; }

    void stop() {
      if (!is_running_)
        return;

      FINS_LOG_INFO("[AgentServer] Stopping...");

      stop_monitoring_ = true;
      if (monitoring_thread_.joinable()) {
        monitoring_thread_.join();
      }

      server_.stop();
      if (server_thread_.joinable()) {
        server_thread_.join();
      }

      is_running_ = false;
      FINS_LOG_INFO("[AgentServer] Stopped.");
    }

  private:
    void setup_routes() {
      server_.new_task_queue = [] { return new httplib::ThreadPool(4); };

      server_.Post("/load_dataflow", [&](const httplib::Request &req, httplib::Response &res) {
        FINS_LOG_DEBUG("[AgentServer] /load_dataflow received: {}", std::to_string(req.body));
        try {
          json dataflow = json::parse(req.body);
          FINS_LOG_INFO("[AgentServer] /load_dataflow: Received config.");
          
          FINS_STUDIO.clear();
          node_lib_.load_json(dataflow.dump());

          res.set_content(json{{"status", "success"}, {"message", "Dataflow loaded."}}.dump(), "application/json");
        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[AgentServer] /load_dataflow Error: {}", e.what());
          res.status = 500;
          res.set_content(json{{"status", "error"}, {"message", e.what()}}.dump(), "application/json");
        }
      });

      server_.Post("/apply_parameters", [&](const httplib::Request &req, httplib::Response &res) {
        try {
          json params = json::parse(req.body);
          std::string yaml_str = params["content"].get<std::string>();
          fins::param_server().load_string(yaml_str);
          res.set_content(json{{"status", "success"}, {"message", "Parameters applied."}}.dump(), "application/json");
        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[AgentServer] /apply_parameters Error: {}", e.what());
          res.status = 500;
          res.set_content(json{{"status", "error"}, {"message", e.what()}}.dump(), "application/json");
        }
      });

      server_.Get("/get_status", [&](const httplib::Request &, httplib::Response &res) {
        bool running = FINS_STUDIO.is_running();
        std::string state_str = running ? "RUNNING" : "STOPPED";
        res.set_content(json{{"status", state_str}}.dump(), "application/json");
      });

      server_.Post("/set_status", [&](const httplib::Request &req, httplib::Response &res) {
        try {
          json body = json::parse(req.body);
          std::string new_state = body["state"];
          if (new_state == "RUNNING") {
            FINS_LOG_INFO("[AgentServer] State -> RUNNING");
            FINS_STUDIO.run();
          } else if (new_state == "STOPPED") {
            FINS_LOG_INFO("[AgentServer] State -> STOPPED");
            FINS_STUDIO.pause();
          } else {
            throw std::invalid_argument("Invalid state. Use 'RUNNING' or 'STOPPED'.");
          }
          res.set_content(json{{"status", "success"}, {"message", "State updated."}}.dump(), "application/json");
        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[AgentServer] /set_status Error: {}", e.what());
          res.status = 400;
          res.set_content(json{{"status", "error"}, {"message", e.what()}}.dump(), "application/json");
        }
      });

      server_.Post("/reset", [&](const httplib::Request &, httplib::Response &res) {
        FINS_STUDIO.reset();
        res.set_content(json{{"status", "success"}, {"message", "Studio reset."}}.dump(), "application/json");
      });

      server_.Get("/get_params_template", [&](const httplib::Request &, httplib::Response &res) {
        try {
          json response;

          response["template_yaml"] = fins::param_server().dump_template_yaml();
          response["current_yaml"] = fins::param_server().dump_active_yaml();

          res.set_content(response.dump(), "application/json");
        } catch (const std::exception &e) {
          res.status = 500;
          res.set_content(json{{"status", "error"}, {"message", e.what()}}.dump(), "application/json");
        }
      });

      server_.Get("/plugin_status", [&](const httplib::Request &, httplib::Response &res) {
          auto state = node_lib_.get_load_state();
          json j;
          switch (state) {
              case NodeLib::LoadState::IDLE: j["state"] = "IDLE"; break;
              case NodeLib::LoadState::LOADING: j["state"] = "LOADING"; break;
              case NodeLib::LoadState::COMPLETE: j["state"] = "COMPLETE"; break;
              case NodeLib::LoadState::ERROR: j["state"] = "ERROR"; break;
          }
          res.set_content(j.dump(), "application/json");
      });

    }

    void monitoringLoop() {
      pthread_setname_np(pthread_self(), "fins_telemetry");

      httplib::Client orchestrator_client(orchestrator_server_);
      orchestrator_client.set_connection_timeout(2, 0);

      registerAgent(orchestrator_client);

      auto last_report_time = std::chrono::steady_clock::now();

#ifdef FINS_ARM_ARCH
      const double monitor_interval_s = 5.0;
#else
      const double monitor_interval_s = 1.0;
#endif

      while (!stop_monitoring_) {
        auto now = std::chrono::steady_clock::now();
        if (std::chrono::duration_cast<std::chrono::seconds>(now - last_report_time).count() >= monitor_interval_s) {
          reportTelemetry(orchestrator_client);
          last_report_time = now;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
      }
    }

    void registerAgent(httplib::Client &client) {
      json payload;
      payload["agent_id"] = agent_id_;
      payload["agent_ip"] = agent_ip_;
      payload["agent_port"] = agent_port_;
      json capabilites = node_lib_.get_capabilities();
      payload["capabilities"] = capabilites;
      FINS_LOG_DEBUG("[AgentServer] Agent capabilities: {}", capabilites.dump());

      if (auto res = client.Post("/register_agent", payload.dump(), "application/json")) {
        if (res->status != 200) {
          FINS_LOG_ERROR("[AgentServer] Registration failed: {}", res->status);
        }
      } else {
        FINS_LOG_ERROR("[AgentServer] Connection failed: {}", to_string(res.error()));
      }

      FINS_LOG_INFO("[AgentServer] Registration completed.");
    }

    json get_node_metrics() {
      json node_metrics_json = json::object();
      auto step_ids = FINS_STUDIO.get_all_step_ids();
      for (const std::string &id: step_ids) {
        auto step = FINS_STUDIO.get_step(id);
        if (step) {
          auto logs = step->get_logs();

          json logs_json = json::array();
          for (const auto &log: logs) {
            logs_json.push_back({{"timestamp", log.timestamp},
                                 {"level", log.level},
                                 {"message", log.message},
                                 {"file", log.file},
                                 {"line", log.line}});
          }

          if (!logs_json.empty()) {
            node_metrics_json[id]["logs"] = logs_json;
          }
        }
      }
      return node_metrics_json;
    }

    json get_pipe_metrics() {
      json pipe_metrics_json = json::object();

      auto all_pipes = FINS_PIPE_FACTORY.get_all_pipes();

      for (const auto &[pipe_id, pipe]: all_pipes) {
        if (!pipe->has_record())
          continue;
        PipeMetrics metrics = pipe->get_metrics();

        pipe_metrics_json[pipe_id] = {{"avg_aoi_ms", metrics.avg_aoi_ms},
                                      {"peak_aoi_ms", metrics.peak_aoi_ms},
                                      {"sys_delay_ms", metrics.sys_delay_ms},
                                      {"violation_prob", metrics.violation_prob},
                                      {"fps", metrics.fps},
                                      {"count", metrics.count},
                                      {"time_window_s", metrics.time_window_s}};
      }
      return pipe_metrics_json;
    }

    void reportTelemetry(httplib::Client &client) {
      json payload;
      payload["agent_id"] = agent_id_;
      payload["agent_ip"] = agent_ip_;
      payload["agent_port"] = agent_port_;
      payload["is_running"] = FINS_STUDIO.is_running();
      payload["timestamp"] = std::chrono::system_clock::now().time_since_epoch().count();

      auto system_stats = SystemMonitor::get_system_stats();
      auto thread_metrics = FINS_THREAD_MANAGER.get_metrics();

      payload["system_metrics"] = {{"cpu_usage_percent", system_stats.cpu_usage_percent},
                                   {"memory_usage_percent", system_stats.mem_usage_percent},
                                   {"memory_used_mb", system_stats.mem_used_mb},
                                   {"memory_total_mb", system_stats.mem_total_mb},
                                   {"cpu_temperature_c", system_stats.cpu_temperature_c},
                                   {"queue_length", thread_metrics.total_queue_length},
                                   {"dropped_tasks_count", thread_metrics.dropped_tasks_count},
                                   {"thread_pool_utilization", thread_metrics.utilization}};

      if (FINS_STUDIO.is_running()) {
        payload["node_metrics"] = get_node_metrics();
        payload["pipe_metrics"] = get_pipe_metrics();
      }

      if (auto res = client.Post("/report_telemetry", payload.dump(), "application/json")) {
        if (res->status != 200) {
          FINS_LOG_ERROR("[AgentServer] Telemetry report failed: {}", res->status);
        }
      } else {
        FINS_LOG_ERROR("[AgentServer] Telemetry connection failed.");
      }
    }

  private:
    NodeLib &node_lib_;
    std::string agent_id_, agent_ip_;
    int agent_port_;
    std::string orchestrator_server_;

    httplib::Server server_;
    std::thread server_thread_;
    std::thread monitoring_thread_;
    std::atomic<bool> is_running_{false};
    std::atomic<bool> stop_monitoring_{false};
  };

} // namespace fins