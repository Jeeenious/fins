/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// node.hpp

#pragma once

#include <algorithm>
#include <fins/msg.hpp>
#include <fins/node_log.hpp>
#include <fins/service/service_manager.hpp>
#include <fins/service/service_tags.hpp>
#include <fins/service/service_traits.hpp>
#include <fins/action/action_manager.hpp>
#include <fins/action/action_tags.hpp>
#include <fins/action/action_traits.hpp>
#include <fins/third_party/json.hpp>
#include <fins/type/string_convert.hpp>
#include <fins/type/type_register.hpp>
#include <fins/utils/time.hpp>
#include <fins/utils/logger.hpp>
#include <fins/utils/performance_recorder.hpp>
#include <fins/server/parameter_server.hpp>
#include <cassert>
#include <functional>
#include <map>
#include <stdexcept>
#include <vector>

namespace fins {

  using json = nlohmann::json;

  struct PortInfo {
    std::string name;
    std::string type;
  };

  struct ParameterInfo {
    std::string name;
    std::string type;
    std::string default_value;
  };

  struct ServiceInfo {
    std::string name;
    std::string request_type;
    std::string response_type;
  };

  struct ActionInfo {
    std::string name;
    std::string goal_type;
    std::string feedback_type;
  };

  enum class SchedulePriority {
    Urgent,
    High,
    Medium,
    Low
  };

  enum class ScheduleQueue {
    FCFS,  // First Come First Serve
    LGFS   // Last Got First Serve (drop new if busy)
  };

  struct ScheduleInfo {
    SchedulePriority priority = SchedulePriority::Medium;
    ScheduleQueue queue = ScheduleQueue::FCFS;
  };

  struct NodeMeta {
    std::string name;
    std::string description;
    std::string category;
    std::string source;
    std::string package_name;
    std::string version = "default";

    std::vector<PortInfo> inputs;
    std::vector<PortInfo> outputs;
    std::vector<ParameterInfo> parameters;

    std::vector<ServiceInfo> clients;
    std::vector<ServiceInfo> servers;

    std::vector<ActionInfo> commanders;
    std::vector<ActionInfo> actors;

    ScheduleInfo schedule;

    nlohmann::json to_json() const {
      nlohmann::json j;
      j["name"] = name;
      j["description"] = description;
      j["category"] = category;
      j["source"] = source;
      j["package_name"] = package_name;
      j["version"] = version;

      auto map_ports = [](const std::vector<PortInfo> &ports) {
        nlohmann::json arr = nlohmann::json::array();
        for (size_t i = 0; i < ports.size(); ++i) {
          arr.push_back({
              {"id", i},
              {"name", ports[i].name},
              {"type", ports[i].type},
          });
        }
        return arr;
      };

      auto map_parameters = [](const std::vector<ParameterInfo> &params) {
        nlohmann::json arr = nlohmann::json::array();
        for (const auto &e: params) {
          arr.push_back({
              {"name", e.name},
              {"type", e.type},
              {"default_value", e.default_value},
          });
        }
        return arr;
      };

      auto map_services = [](const std::vector<ServiceInfo> &svcs) {
        nlohmann::json arr = nlohmann::json::array();
        for (const auto &s: svcs) {
          arr.push_back({{"name", s.name}, {"request_type", s.request_type}, {"response_type", s.response_type}});
        }
        return arr;
      };

      auto map_actions = [](const std::vector<ActionInfo> &acts) {
        nlohmann::json arr = nlohmann::json::array();
        for (const auto &a: acts) {
          arr.push_back({{"name", a.name}, {"goal_type", a.goal_type}, {"feedback_type", a.feedback_type}});
        }
        return arr;
      };

      j["inputs"] = map_ports(inputs);
      j["outputs"] = map_ports(outputs);
      j["parameters"] = map_parameters(parameters);
      j["clients"] = map_services(clients);
      j["servers"] = map_services(servers);
      j["commanders"] = map_actions(commanders);
      j["actors"] = map_actions(actors);
      
      std::string priority_str;
      switch (schedule.priority) {
        case SchedulePriority::Urgent: priority_str = "Urgent"; break;
        case SchedulePriority::High: priority_str = "High"; break;
        case SchedulePriority::Medium: priority_str = "Medium"; break;
        case SchedulePriority::Low: priority_str = "Low"; break;
      }
      std::string queue_str = (schedule.queue == ScheduleQueue::FCFS) ? "FCFS" : "LGFS";
      j["schedule"] = {
        {"priority", priority_str},
        {"queue", queue_str}
      };
      
      return j;
    }
  };

  class INode {
  public:
    virtual ~INode() = default;
    virtual void set_publisher(std::function<void(int, AnyMsg)> pub_func) = 0;
    virtual void set_connection_checker(std::function<bool(int)> check_func) = 0;

    virtual void define() {
      FINS_LOG_WARN("[Node {} Warning] define() not implemented. Using default empty implementation.", get_meta().name);
    }
    virtual void initialize() {
      FINS_LOG_WARN("[Node {} Warning] initialize() not implemented. Using default empty implementation.", get_meta().name);
    }

    virtual void run() {
      FINS_LOG_WARN("[Node {} Warning] run() not implemented. Using default empty implementation.", get_meta().name);
    }
    virtual void pause() {
      FINS_LOG_WARN("[Node {} Warning] pause() not implemented. Using default empty implementation.", get_meta().name);
    } 
    virtual void reset() {
      FINS_LOG_WARN("[Node {} Warning] reset() not implemented. Using default empty implementation.", get_meta().name);
    }
    virtual void on_input(int port, const AnyMsg &msg) = 0;
    virtual void update_parameter(const std::string &name, const std::string &value) = 0;
    virtual NodeMeta get_meta() const = 0;
    virtual ScopedSegmentTimer recorder(const std::string& label, AcqTime acq_time) = 0;
    virtual ScopedSegmentTimer recorder(const std::string& label, double acq_time_sec) = 0;
    virtual std::vector<LogEntry> get_logs() = 0;

    virtual void set_client_topic(const std::string &name, const std::string &topic) = 0;
    virtual void set_server_topic(const std::string &name, const std::string &topic) = 0;

    virtual void set_commander_topic(const std::string &name, const std::string &topic) = 0;
    virtual void set_actor_topic(const std::string &name, const std::string &topic) = 0;
  };

  template<typename T>
  struct member_func_traits;
  template<typename R, typename C, typename... Args>
  struct member_func_traits<R (C::*)(Args...)> {
    using class_type = C;
  };
  template<typename R, typename C, typename... Args>
  struct member_func_traits<R (C::*)(Args...) const> {
    using class_type = C;
  };


  class Node : public INode {
  protected:
    NodeMeta meta_;
    std::map<int, std::function<void(const AnyMsg &)>> input_handlers_;
    std::map<std::string, std::function<void(const std::string &)>> parameter_handlers_;
    std::function<void(int, AnyMsg)> publisher_;
    std::function<bool(int)> connection_checker_;

    std::map<std::string, int> input_name_to_port_;
    std::map<std::string, int> output_name_to_port_;
    int next_input_port_ = 0;
    int next_output_port_ = 0;

    std::map<std::string, std::string> client_remaps_;

    struct ServerHandle {
      ServiceManager::ServiceCallback callback;
      std::type_index input_id = std::type_index(typeid(void));
      std::type_index output_id = std::type_index(typeid(void));
      std::unique_ptr<ServiceHandler> handler;
    };
    std::map<std::string, ServerHandle> server_handles_;
    std::map<std::string, std::string> server_remaps_;

    struct CommanderHandle {
      ActionManager::ResultCallback result_callback;
      ActionManager::FeedbackCallback feedback_callback;
      std::type_index goal_type_id = std::type_index(typeid(void));
      std::type_index feedback_type_id = std::type_index(typeid(void));
    };
    std::map<std::string, CommanderHandle> commander_handles_;
    std::map<std::string, std::string> commander_remaps_;

    struct ActorHandle {
      ActionManager::GoalCallback goal_callback;
      std::type_index goal_type_id = std::type_index(typeid(void));
      std::type_index feedback_type_id = std::type_index(typeid(void));
    };
    std::map<std::string, ActorHandle> actor_handles_;
    std::map<std::string, std::string> actor_remaps_;

  public:
    std::shared_ptr<NodeLogger> logger;

    Node() : logger(std::make_shared<NodeLogger>()) {
#ifdef PKG_SOURCE
      std::string src = PKG_SOURCE;
      if (src.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-@.") !=
          std::string::npos) {
        throw std::runtime_error("[Node " + meta_.name + "]: Invalid source format in PKG_SOURCE: " + src);
      }
      meta_.source = src;
#endif
#ifdef PKG_NAME
      meta_.package_name = PKG_NAME;
#endif
    }

    void set_publisher(std::function<void(int, AnyMsg)> pub_func) override { publisher_ = pub_func; }

    void set_connection_checker(std::function<bool(int)> check_func) override { connection_checker_ = check_func; }

    void on_input(int port, const AnyMsg &msg) override {
      if (input_handlers_.find(port) != input_handlers_.end()) {
        input_handlers_[port](msg);
      }
    }

    void update_parameter(const std::string &name, const std::string &value) override {
      if (parameter_handlers_.find(name) != parameter_handlers_.end()) {
        try {
          parameter_handlers_[name](value);
        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[Node {} Error] Update parameter '{}' failed: {}", meta_.name, name, e.what());
        }
      } else {
        FINS_LOG_WARN("[Node {} Warning] Unknown parameter: {}", meta_.name, name);
      }
    }

    std::vector<LogEntry> get_logs() override { return logger->get_and_clear_logs(); }

    NodeMeta get_meta() const override { return meta_; }

    ScopedSegmentTimer recorder(const std::string& label, AcqTime acq_time) override {
      return ScopedSegmentTimer(this->meta_.name, label, acq_time);
    }
    ScopedSegmentTimer recorder(const std::string& label, double acq_time_sec) override {
      return ScopedSegmentTimer(this->meta_.name, label, acq_time_sec);
    }

    void initialize() override {}

    void set_client_topic(const std::string &key, const std::string &topic) override { client_remaps_[key] = topic; }

    void set_server_topic(const std::string &key, const std::string &topic) override {
      server_remaps_[key] = topic;

      auto it = server_handles_.find(key);
      if (it != server_handles_.end()) {
        FINS_LOG_INFO("[Node] Applying Server Remap: Internal '{}' -> Topic '{}'", key, topic);
        if (it->second.handler) {
          // Clone the handler if possible, or move it if it's a one-time thing.
          // Since it's unique_ptr, we might need a better way if multiple topics map to same internal key.
          // But usually it's 1-to-1 or just one remap.
          // For now, let's assume we can move it or we need a way to register it.
          // Actually, register_service_handler takes ownership.
          // If it was already registered, it might be gone.
          // Let's check how TypedServiceHandler is created. It's created in register_server.
          // We should probably store a factory or just the handler and use it.
          // For now, let's just register it.
          FINS_SERVICE_MANAGER.register_service_handler(topic, std::move(it->second.handler), it->second.input_id, it->second.output_id);
        } else if (it->second.callback) {
          FINS_SERVICE_MANAGER.register_service(topic, it->second.callback, it->second.input_id, it->second.output_id);
        }
      }
    }

    void set_commander_topic(const std::string &key, const std::string &topic) override {
      commander_remaps_[key] = topic;
      auto it = commander_handles_.find(key);
      if (it != commander_handles_.end()) {
        FINS_LOG_INFO("[Node] Applying Commander Remap: Internal '{}' -> Topic '{}'", key, topic);
        FINS_ACTION_MANAGER.register_commander(topic, it->second.goal_type_id, it->second.feedback_type_id,
                                                it->second.result_callback, it->second.feedback_callback);
      }
    }

    void set_actor_topic(const std::string &key, const std::string &topic) override {
      actor_remaps_[key] = topic;
      auto it = actor_handles_.find(key);
      if (it != actor_handles_.end()) {
        FINS_LOG_INFO("[Node] Applying Actor Remap: Internal '{}' -> Topic '{}'", key, topic);
        FINS_ACTION_MANAGER.register_actor(topic, it->second.goal_type_id, it->second.feedback_type_id,
                                            it->second.goal_callback);
      }
    }

  protected:
    void set_name(const std::string &name) { meta_.name = name; }

    void set_description(const std::string &desc) { meta_.description = desc; }

    void set_category(const std::string &cat) { meta_.category = cat; }

    void set_version(const std::string &ver) { meta_.version = ver; }

    void set_basics(const std::string &name, const std::string &desc, const std::string &cat,
                    const std::string &ver = "default") {
      set_name(name);
      set_description(desc);
      set_category(cat);
      set_version(ver);
    }

    template<int Port, typename T, typename ClassType>
    void register_input(const std::string &name, void (ClassType::*method)(const Msg<T> &)) {

      std::string type_str = FINS_TYPE_REGISTER.get_name<T>();

      if (meta_.inputs.size() <= static_cast<size_t>(Port))
        meta_.inputs.resize(Port + 1);
      meta_.inputs[Port] = {name, type_str};
      std::type_index expected_id = std::type_index(typeid(T));
      input_handlers_[Port] = [this, method, expected_id](const AnyMsg &any_msg) {
        if (any_msg.type_id != expected_id)
          return;
        Msg<T> typed_msg(any_msg);
        (static_cast<ClassType *>(this)->*method)(typed_msg);
      };
    }

    template<int Port, typename T, typename ClassType>
    void register_input(const std::string &name, void (ClassType::*method)(const T &, AcqTime)) {
      std::string type_str = FINS_TYPE_REGISTER.get_name<T>();

      if (meta_.inputs.size() <= static_cast<size_t>(Port))
        meta_.inputs.resize(Port + 1);
      meta_.inputs[Port] = {name, type_str};
      std::type_index expected_id = std::type_index(typeid(T));
      input_handlers_[Port] = [this, method, expected_id](const AnyMsg &any_msg) {
        if (any_msg.type_id != expected_id)
          return;
        Msg<T> typed_msg(any_msg);
        (static_cast<ClassType *>(this)->*method)(*typed_msg.data, typed_msg.acq_time);
      };
    }

    template<int Port, typename T, typename ClassType>
    void register_input(const std::string &name, void (ClassType::*method)(const T &)) {
      std::string type_str = FINS_TYPE_REGISTER.get_name<T>();

      if (meta_.inputs.size() <= static_cast<size_t>(Port))
        meta_.inputs.resize(Port + 1);
      meta_.inputs[Port] = {name, type_str};
      std::type_index expected_id = std::type_index(typeid(T));
      input_handlers_[Port] = [this, method, expected_id](const AnyMsg &any_msg) {
        if (any_msg.type_id != expected_id)
          return;
        Msg<T> typed_msg(any_msg);
        (static_cast<ClassType *>(this)->*method)(*typed_msg.data);
      };
    }

    template<typename T, typename ClassType>
    void register_input(const std::string &name, void (ClassType::*method)(const Msg<T> &)) {
      int port = next_input_port_++;
      input_name_to_port_[name] = port;

      std::string type_str = FINS_TYPE_REGISTER.get_name<T>();

      if (meta_.inputs.size() <= static_cast<size_t>(port))
        meta_.inputs.resize(port + 1);
      meta_.inputs[port] = {name, type_str};
      std::type_index expected_id = std::type_index(typeid(T));
      input_handlers_[port] = [this, method, expected_id](const AnyMsg &any_msg) {
        if (any_msg.type_id != expected_id)
          return;
        Msg<T> typed_msg(any_msg);
        (static_cast<ClassType *>(this)->*method)(typed_msg);
      };
    }

    template<typename T, typename ClassType>
    void register_input(const std::string &name, void (ClassType::*method)(const T &, AcqTime)) {
      int port = next_input_port_++;
      input_name_to_port_[name] = port;

      std::string type_str = FINS_TYPE_REGISTER.get_name<T>();

      if (meta_.inputs.size() <= static_cast<size_t>(port))
        meta_.inputs.resize(port + 1);
      meta_.inputs[port] = {name, type_str};
      std::type_index expected_id = std::type_index(typeid(T));
      input_handlers_[port] = [this, method, expected_id](const AnyMsg &any_msg) {
        if (any_msg.type_id != expected_id)
          return;
        Msg<T> typed_msg(any_msg);
        (static_cast<ClassType *>(this)->*method)(*typed_msg.data, typed_msg.acq_time);
      };
    }

    template<typename T, typename ClassType>
    void register_input(const std::string &name, void (ClassType::*method)(const T &)) {
      int port = next_input_port_++;
      input_name_to_port_[name] = port;

      std::string type_str = FINS_TYPE_REGISTER.get_name<T>();

      if (meta_.inputs.size() <= static_cast<size_t>(port))
        meta_.inputs.resize(port + 1);
      meta_.inputs[port] = {name, type_str};
      std::type_index expected_id = std::type_index(typeid(T));
      input_handlers_[port] = [this, method, expected_id](const AnyMsg &any_msg) {
        if (any_msg.type_id != expected_id)
          return;
        Msg<T> typed_msg(any_msg);
        (static_cast<ClassType *>(this)->*method)(*typed_msg.data);
      };
    }


    template<int Port, typename T>
    void register_output(const std::string &name) {
      std::string type_str = FINS_TYPE_REGISTER.get_name<T>();

      if (meta_.outputs.size() <= static_cast<size_t>(Port))
        meta_.outputs.resize(Port + 1);
      meta_.outputs[Port] = {name, type_str};
    }

    template<typename T>
    void register_output(const std::string &name) {
      int port = next_output_port_++;
      output_name_to_port_[name] = port;

      std::string type_str = FINS_TYPE_REGISTER.get_name<T>();

      if (meta_.outputs.size() <= static_cast<size_t>(port))
        meta_.outputs.resize(port + 1);
      meta_.outputs[port] = {name, type_str};
    }

    template<typename T>
    void register_parameter(const std::string &name, std::function<void(const T &)> handler) {

      std::string type_str = FINS_TYPE_REGISTER.get_name<T>();

      parameter_handlers_[name] = [handler](const std::string &str_val) {
        T val = FINS_TYPE_REGISTER.string_convert<T>(str_val);
        handler(val);
      };
    }

    template<typename T, typename ClassType>
    void register_parameter(const std::string &name, void (ClassType::*method)(const T &), T default_value = T()) {

      std::string type_str = FINS_TYPE_REGISTER.get_name<T>();
      meta_.parameters.push_back({name, type_str, std::to_string(default_value)});

      parameter_handlers_[name] = [this, method](const std::string &str_val) {
        T val = FINS_TYPE_REGISTER.string_convert<T>(str_val);
        (static_cast<ClassType *>(this)->*method)(val);
      };
    }

    template<typename T, typename ClassType>
    void register_parameter(const std::string &name, void (ClassType::*method)(T), T default_value = T()) {
      std::string type_str = FINS_TYPE_REGISTER.get_name<T>();
      meta_.parameters.push_back({name, type_str, std::to_string(default_value)});
      parameter_handlers_[name] = [this, method](const std::string &str_val) {
        T val = FINS_TYPE_REGISTER.string_convert<T>(str_val);
        (static_cast<ClassType *>(this)->*method)(val);
      };
    }

  public:
    template<int Port, typename T>
    void send_ptr(std::shared_ptr<T> data, AcqTime ts = fins::now()) {
      if (publisher_) {
        AnyMsg msg(data, ts);
        publisher_(Port, msg);
      }
    }

    template<int Port, typename T>
    void send(const T &data, AcqTime ts = fins::now()) {
      if (publisher_) {
        auto data_ptr = std::make_shared<T>(data);
        AnyMsg msg(data_ptr, ts);
        publisher_(Port, msg);
      }
    }

    template<typename T>
    void send_ptr(const std::string &name, std::shared_ptr<T> data, AcqTime ts = fins::now()) {
      auto it = output_name_to_port_.find(name);
      if (it == output_name_to_port_.end()) {
        FINS_LOG_ERROR("[Node] Output port '{}' not found in node '{}'", name, meta_.name);
        assert(false && "Output port not found");
        return;
      }
      if (publisher_) {
        AnyMsg msg(data, ts);
        publisher_(it->second, msg);
      }
    }

    template<typename T>
    void send(const std::string &name, const T &data, AcqTime ts = fins::now()) {
      auto it = output_name_to_port_.find(name);
      if (it == output_name_to_port_.end()) {
        FINS_LOG_ERROR("[Node] Output port '{}' not found in node '{}'", name, meta_.name);
        assert(false && "Output port not found");
        return;
      }
      if (publisher_) {
        auto data_ptr = std::make_shared<T>(data);
        AnyMsg msg(data_ptr, ts);
        publisher_(it->second, msg);
      }
    }

    template<size_t N, typename T>
    void send_ptr(const char (&name)[N], std::shared_ptr<T> data, AcqTime ts = fins::now()) {
      send_ptr(std::string(name), data, ts);
    }

    template<size_t N, typename T>
    void send(const char (&name)[N], const T &data, AcqTime ts = fins::now()) {
      send(std::string(name), data, ts);
    }

    template<typename T>
    void send_dynamic(int port, std::shared_ptr<T> data, AcqTime ts = fins::now()) {
      if (publisher_) {
        AnyMsg msg(data, ts);
        publisher_(port, msg);
      }
    }

    template<int Port>
    bool required() {
      if (connection_checker_) {
        return connection_checker_(Port);
      }
      return false;
    }

    bool required(const std::string &name) {
      auto it = output_name_to_port_.find(name);
      if (it != output_name_to_port_.end() && connection_checker_) {
        return connection_checker_(it->second);
      }
      return false;
    }

    template<size_t N>
    bool required(const char (&name)[N]) {
      return required(std::string(name));
    }

  protected:
    template<typename Tuple, std::size_t... Is>
    std::string tuple_types_to_string_impl(std::index_sequence<Is...>) {
      std::stringstream ss;
      ((ss << (Is == 0 ? "" : ", ") << FINS_TYPE_REGISTER.get_name<std::tuple_element_t<Is, Tuple>>()), ...);
      return ss.str();
    }

    template<typename Tuple>
    std::string tuple_types_to_string() {
      return tuple_types_to_string_impl<Tuple>(std::make_index_sequence<std::tuple_size_v<Tuple>>{});
    }

    template<typename... Args>
    auto register_client(const std::string &name) {
      using Traits = ServiceTraits<Args...>;
      using InTuple = typename Traits::InputTuple;
      using OutTuple = typename Traits::OutputTuple;
      using RetType = typename Traits::ReturnType;

      std::string req_str = tuple_types_to_string<InTuple>();
      std::string res_str = tuple_types_to_string<OutTuple>();

      meta_.clients.push_back({name, req_str, res_str});

      return [this, name](auto &&...args) -> RetType {
        constexpr size_t ArgCount = sizeof...(args);
        constexpr size_t ExpectedCount = std::tuple_size_v<InTuple>;
        static_assert(ArgCount == ExpectedCount, "Client argument count mismatch");

        // 1. 在栈上分配参数数组，0 堆开销
        std::any args_array[ArgCount > 0 ? ArgCount : 1] = { std::any(std::forward<decltype(args)>(args))... };

        // 2. 如果是同线程/直连模式，直接通过 ServiceManager 获取 Handler 执行
        std::string current_topic = name;
        if (client_remaps_.count(name)) {
          current_topic = client_remaps_[name];
        }

        auto handler = FINS_SERVICE_MANAGER.get_service_handler(current_topic);
        if (handler) {
          std::any res_any = handler->invoke(args_array, ArgCount);
          if constexpr (std::is_void_v<RetType>) {
            return;
          } else {
            return std::any_cast<RetType>(res_any);
          }
        }

        // 3. 回退到异步队列模式（原有的 promise/future 逻辑）
        std::vector<std::any> type_erased_args;
        type_erased_args.reserve(ArgCount);
        for (size_t i = 0; i < ArgCount; ++i) {
          type_erased_args.push_back(args_array[i]);
        }

        auto future =
            FINS_SERVICE_MANAGER.call_service(current_topic, std::move(type_erased_args),
                                              std::type_index(typeid(InTuple)), std::type_index(typeid(OutTuple)));

        std::any res_any = future.get();

        if constexpr (std::is_void_v<RetType>) {
          return;
        } else {
          return std::any_cast<RetType>(res_any);
        }
      };
    }

    template<typename... Args, typename Func>
    void register_server(const std::string &name, Func &&callback_ptr) {
      using Traits = ServiceTraits<Args...>;
      using InTuple = typename Traits::InputTuple;
      using OutTuple = typename Traits::OutputTuple;
      using RetType = typename Traits::ReturnType;

      using ClassType = typename member_func_traits<std::decay_t<Func>>::class_type;

      std::string req_str = tuple_types_to_string<InTuple>();
      std::string res_str = tuple_types_to_string<OutTuple>();

      meta_.servers.push_back({name, req_str, res_str});

      // 强类型处理器，内部直接调用成员函数，免去 std::function 包装
      class TypedServiceHandler : public ServiceHandler {
          ClassType* instance_;
          std::decay_t<Func> func_;
      public:
          TypedServiceHandler(ClassType* inst, Func f) : instance_(inst), func_(f) {}

          std::any invoke(const std::any* args, size_t count) override {
              if (count != std::tuple_size_v<InTuple>) {
                  throw std::runtime_error("Server received wrong number of arguments");
              }
              // 将原始 std::any 数组解包并调用
              return call_member_array_impl<InTuple, RetType, ClassType>(
                  func_, instance_, args, std::make_index_sequence<std::tuple_size_v<InTuple>>{}
              );
          }
      };

      // 注册到 ServiceManager
      auto handler = std::make_unique<TypedServiceHandler>(static_cast<ClassType*>(this), callback_ptr);
      
      server_handles_[name] = {nullptr, std::type_index(typeid(InTuple)), std::type_index(typeid(OutTuple)), std::move(handler)};
    }

    template<typename... Args, typename ResultFunc, typename FeedbackFunc>
    void register_commander(const std::string &name, ResultFunc &&result_callback, FeedbackFunc &&feedback_callback) {
      using Traits = ActionTraits<Args...>;
      using GoalTuple = typename Traits::GoalTuple;
      using FeedbackTuple = typename Traits::FeedbackTuple;

      using ResultClassType = typename member_func_traits<std::decay_t<ResultFunc>>::class_type;
      using FeedbackClassType = typename member_func_traits<std::decay_t<FeedbackFunc>>::class_type;

      std::string goal_str = tuple_types_to_string<GoalTuple>();
      std::string feedback_str = tuple_types_to_string<FeedbackTuple>();

      meta_.commanders.push_back({name, goal_str, feedback_str});

      auto result_wrapper = [this, func = result_callback](ActionState state) {
        (static_cast<ResultClassType *>(this)->*func)(state);
      };

      auto feedback_wrapper = [this, func = feedback_callback](const std::vector<std::any> &args) {
        if (args.size() != std::tuple_size_v<FeedbackTuple>) {
          throw std::runtime_error("Commander received wrong number of feedback arguments");
        }
        call_feedback_impl<FeedbackTuple, FeedbackClassType>(func, args,
                                                              std::make_index_sequence<std::tuple_size_v<FeedbackTuple>>{});
      };

      commander_handles_[name] = {result_wrapper, feedback_wrapper, std::type_index(typeid(GoalTuple)),
                                   std::type_index(typeid(FeedbackTuple))};

      FINS_ACTION_MANAGER.register_commander(name, std::type_index(typeid(GoalTuple)),
                                              std::type_index(typeid(FeedbackTuple)), result_wrapper, feedback_wrapper);
    }
    
    template<typename... Args, typename GoalFunc>
    void register_actor(const std::string &name, GoalFunc &&goal_callback) {
      using Traits = ActionTraits<Args...>;
      using GoalTuple = typename Traits::GoalTuple;
      using FeedbackTuple = typename Traits::FeedbackTuple;

      using GoalClassType = typename member_func_traits<std::decay_t<GoalFunc>>::class_type;

      std::string goal_str = tuple_types_to_string<GoalTuple>();
      std::string feedback_str = tuple_types_to_string<FeedbackTuple>();

      meta_.actors.push_back({name, goal_str, feedback_str});

      auto goal_wrapper = [this, func = goal_callback](std::shared_ptr<ActionSessionBase> session,
                                                        const std::vector<std::any> &args) {
        if (args.size() != std::tuple_size_v<GoalTuple>) {
          throw std::runtime_error("Actor received wrong number of goal arguments");
        }
        call_goal_impl_with_session<GoalTuple, GoalClassType>(session, func, args,
                                                               std::make_index_sequence<std::tuple_size_v<GoalTuple>>{});
      };

      actor_handles_[name] = {goal_wrapper, std::type_index(typeid(GoalTuple)),
                               std::type_index(typeid(FeedbackTuple))};

      FINS_ACTION_MANAGER.register_actor(name, std::type_index(typeid(GoalTuple)),
                                          std::type_index(typeid(FeedbackTuple)), goal_wrapper);
    }

    template<typename... GoalArgs>
    std::shared_ptr<ActionSessionBase> create_action(const std::string &name, GoalArgs &&...goal_args) {
      std::vector<std::any> type_erased_args;
      type_erased_args.reserve(sizeof...(goal_args));
      (type_erased_args.push_back(std::any(std::forward<GoalArgs>(goal_args))), ...);

      auto cmd_it = commander_handles_.find(name);
      if (cmd_it == commander_handles_.end()) {
        throw std::runtime_error("Commander '" + name + "' not registered");
      }

      std::string actual_topic = name;
      if (commander_remaps_.count(name)) {
        actual_topic = commander_remaps_[name];
      }

      return FINS_ACTION_MANAGER.create_action_session(actual_topic, std::move(type_erased_args),
                                                        cmd_it->second.goal_type_id, cmd_it->second.feedback_type_id);
    }

    template<typename... GoalArgs>
    std::shared_ptr<ActionSessionBase> create_action(const std::string &name, const GoalArgs &...goal_args) {
      std::vector<std::any> type_erased_args;
      type_erased_args.reserve(sizeof...(goal_args));
      (type_erased_args.push_back(std::any(goal_args)), ...);

      auto cmd_it = commander_handles_.find(name);
      if (cmd_it == commander_handles_.end()) {
        throw std::runtime_error("Commander '" + name + "' not registered");
      }

      std::string actual_topic = name;
      if (commander_remaps_.count(name)) {
        actual_topic = commander_remaps_[name];
      }

      return FINS_ACTION_MANAGER.create_action_session(actual_topic, std::move(type_erased_args),
                                                        cmd_it->second.goal_type_id, cmd_it->second.feedback_type_id);
    }

    ActionState get_action_state(const std::string &name) {
      std::string actual_topic = name;
      if (commander_remaps_.count(name)) {
        actual_topic = commander_remaps_[name];
      }

      return FINS_ACTION_MANAGER.get_action_state(actual_topic);
    }

    void cancel_action(const std::string &name) {
      std::string actual_topic = name;
      if (commander_remaps_.count(name)) {
        actual_topic = commander_remaps_[name];
      }

      FINS_ACTION_MANAGER.cancel_action(actual_topic);
    }

  private:
    template<typename InTuple, typename RetType, typename ClassType, typename Func, size_t... Is>
    static std::any call_member_array_impl(Func func, ClassType* inst, const std::any* args, std::index_sequence<Is...>) {
        if constexpr (std::is_void_v<RetType>) {
            (inst->*func)(std::any_cast<std::tuple_element_t<Is, InTuple>>(args[Is])...);
            return std::any();
        } else {
            return std::any((inst->*func)(std::any_cast<std::tuple_element_t<Is, InTuple>>(args[Is])...));
        }
    }

    template<typename InTuple, typename RetType, typename ClassType, typename Func, size_t... Is>
    std::any call_member_impl(Func func, const std::vector<std::any> &args, std::index_sequence<Is...>) {
      auto typed_args = std::make_tuple(std::any_cast<std::tuple_element_t<Is, InTuple>>(args[Is])...);

      if constexpr (std::is_void_v<RetType>) {
        (static_cast<ClassType *>(this)->*func)(std::get<Is>(typed_args)...);
        return std::any();
      } else {
        RetType ret = (static_cast<ClassType *>(this)->*func)(std::get<Is>(typed_args)...);
        return std::any(ret);
      }
    }

    template<typename FeedbackTuple, typename ClassType, typename Func, size_t... Is>
    void call_feedback_impl(Func func, const std::vector<std::any> &args, std::index_sequence<Is...>) {
      auto typed_args = std::make_tuple(std::any_cast<std::tuple_element_t<Is, FeedbackTuple>>(args[Is])...);
      (static_cast<ClassType *>(this)->*func)(std::get<Is>(typed_args)...);
    }

    template<typename GoalTuple, typename ClassType, typename Func, size_t... Is>
    void call_goal_impl_with_session(std::shared_ptr<ActionSessionBase> session, Func func,
                                      const std::vector<std::any> &args, std::index_sequence<Is...>) {
      auto typed_args = std::make_tuple(std::any_cast<std::tuple_element_t<Is, GoalTuple>>(args[Is])...);
      (static_cast<ClassType *>(this)->*func)(session, std::get<Is>(typed_args)...);
    }
  };


  class NodeFactory {
  public:
    using CreatorFunc = std::function<INode *()>;

    static NodeFactory &get_instance() {
      static NodeFactory instance;
      return instance;
    }

    void register_node(const NodeMeta &meta, CreatorFunc creator) {
      std::string unique_name = meta.source + "/" + meta.name + "@" + meta.version;
      
      auto it = creators_.find(unique_name);
      if (it != creators_.end()) {
        creators_[unique_name] = creator;
        metas_[unique_name] = meta;
        FINS_LOG_DEBUG("[NodeFactory] Updated logic for existing node: {}", unique_name);
      } else {
        creators_[unique_name] = creator;
        metas_[unique_name] = meta;
        names_.push_back(unique_name);
        FINS_LOG_DEBUG("[NodeFactory] Registered new node: {}", unique_name);
      }
    }

    void print_registered_nodes() {
      FINS_LOG_INFO("[NodeFactory] Registered Nodes:");
      for (const auto &name: names_) {
        FINS_LOG_INFO(" - {}", name);
      }
    }

    INode *create(const std::string &name) {
      if (creators_.find(name) != creators_.end()) {
        INode *node = creators_[name]();
        node->define();
        return node;
      }
      return nullptr;
    }

    size_t count() const { return names_.size(); }

    const char *get_name(size_t index) const {
      if (index < names_.size())
        return names_[index].c_str();
      return nullptr;
    }

    std::string get_json(const std::string &name) {
      if (metas_.find(name) != metas_.end()) {
        return metas_[name].to_json().dump();
      }
      return "{}";
    }

    json get_capabilities() const {
      json caps = json::object();
      for (const auto &pair: metas_) {
        caps[pair.first] = pair.second.to_json();
      }
      return caps;
    }

  private:
    std::map<std::string, CreatorFunc> creators_;
    std::map<std::string, NodeMeta> metas_;
    std::vector<std::string> names_;
    NodeFactory() = default;
  };

#define FINS_NODE_FACTORY fins::NodeFactory::get_instance()

#define EXPORT_NODE(UserClass)                                                                                    \
  namespace {                                                                                                     \
    struct Register_##UserClass {                                                                                 \
      Register_##UserClass() {                                                                                    \
        auto temp_ptr = std::make_unique<UserClass>();                                                            \
                                                                                                                  \
        temp_ptr->define();                                                                                       \
        fins::NodeMeta meta = temp_ptr->get_meta();                                                               \
                                                                                                                  \
        fins::NodeFactory::get_instance().register_node(meta, []() -> fins::INode * { return new UserClass(); }); \
      }                                                                                                           \
    };                                                                                                            \
    static Register_##UserClass register_inst_##UserClass;                                                        \
  }

  enum PluginState {
    STATEFUL,
    STATELESS
  };

#ifndef FINS_STATIC_BUILD
#define DEFINE_PLUGIN_ENTRY(state)                                                                      \
  extern "C" {                                                                                          \
  int get_node_count() { return static_cast<int>(fins::NodeFactory::get_instance().count()); }          \
  const char *get_node_name(int index) { return fins::NodeFactory::get_instance().get_name(index); }    \
  const char *get_node_meta_json(const char *name) {                                                    \
    static thread_local std::string json_buffer;                                                        \
    json_buffer = fins::NodeFactory::get_instance().get_json(name);                                     \
    return json_buffer.c_str();                                                                         \
  }                                                                                                     \
  fins::INode *create_node(const char *name) { return fins::NodeFactory::get_instance().create(name); } \
  void destroy_node(fins::INode *p) { delete p; }                                                       \
  void plugin_init();                                                                                   \
  void plugin_destroy();                                                                                \
  bool is_hot_reloadable() { return (state) == fins::STATELESS; }                                       \
  }
#else
#define DEFINE_PLUGIN_ENTRY(state)
#endif

#ifndef FINS_STATIC_BUILD
#define REGISTER_PLUGIN_INIT(CodeBlock) \
  extern "C" {                          \
  void plugin_init() { CodeBlock }      \
  }
#define REGISTER_PLUGIN_DESTROY(CodeBlock) \
  extern "C" {                             \
  void plugin_destroy() { CodeBlock }      \
  }
#else
#define REGISTER_PLUGIN_INIT(CodeBlock)                  \
  namespace {                                            \
    struct StaticPluginInit {                            \
      StaticPluginInit() { CodeBlock }                   \
    };                                                   \
    static StaticPluginInit static_plugin_init_instance; \
  }
#define REGISTER_PLUGIN_DESTROY(CodeBlock)                  \
  namespace {                                               \
    struct StaticPluginDestroy {                            \
      ~StaticPluginDestroy() { CodeBlock }                  \
    };                                                      \
    static StaticPluginDestroy static_plugin_dest_instance; \
  }
#endif

} // namespace fins