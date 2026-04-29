/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// nodelib.hpp

#pragma once

#include <array>
#include <chrono>
#include <dlfcn.h>
#include <filesystem>
#include <fins/studio.hpp>
#include <fins/third_party/json.hpp>
#include <fins/thread_manager.hpp>
#include <fins/utils/fs.hpp>
#include <fins/utils/logger.hpp>
#include <condition_variable>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <set>
#include <sstream>
#include <string>
#include <sys/inotify.h>
#include <thread>
#include <vector>
#include <wordexp.h>
#include <pthread.h>

namespace fins {

  namespace fs = std::filesystem;
  using json = nlohmann::json;

  class NodeLib {
  private:
    std::function<void()> on_reloaded_callback_;

  public:
    NodeLib() {
      try {
        std::string runtime_dir = expand_user("~/.fins/runtime/");
        if (fs::exists(runtime_dir)) {
          FINS_LOG_INFO("[NodeLib] Cleaning up runtime directory: {}", runtime_dir);
          fs::remove_all(runtime_dir);
        }
        fs::create_directories(runtime_dir);
      } catch (const std::exception &e) {
        FINS_LOG_ERROR("[NodeLib] Failed to cleanup runtime directory: {}", e.what());
      }
    }
    ~NodeLib() {
      stop_monitor_ = true;
      if (monitor_thread_.joinable()) {
        monitor_thread_.join();
      }
      for (auto &ctx: contexts_) {
        if (ctx->plugin_destroy) {
          try {
            FINS_LOG_INFO("[NodeLib] Destroying plugin: {}...", ctx->path);
            ctx->plugin_destroy();
            FINS_LOG_INFO("[NodeLib] Plugin destroyed: {}.", ctx->path);
          } catch (const std::exception &e) {
            FINS_LOG_ERROR("[NodeLib] Plugin destruction failed in {}: {}", ctx->path, e.what());
          }
        }
        if (ctx->handle) {
          dlclose(ctx->handle);
          ctx->handle = nullptr;
        }
      }
    }

    void load_from_executable_dir() {
      try {
        fs::path exe_path = fs::read_symlink("/proc/self/exe");
        load_directory(exe_path.parent_path().string());
      } catch (...) {
      }
    }

    void load_directory(const std::string &dir_path) {
      std::string expanded_path = expand_user(dir_path);
      if (!fs::exists(expanded_path))
        return;
      FINS_LOG_INFO("[NodeLib] Scanning: {}", expanded_path);

      if (watch_dir_.empty()) {
        watch_dir_ = expanded_path;
        start_monitor();
      }

      std::vector<fs::path> plugins;
      for (const auto &entry: fs::directory_iterator(expanded_path)) {
        if (entry.path().extension() == ".so" &&
            entry.path().filename().string().find("fins_shared") == std::string::npos) {
          plugins.push_back(entry.path());
        }
      }

      if (plugins.empty())
        return;

      size_t success_count = 0;
      size_t fail_count = 0;

      for (size_t i = 0; i < plugins.size(); ++i) {
        const auto &path = plugins[i];
        float progress = (float)(i + 1) / plugins.size();
        int bar_width = 20;
        int pos = bar_width * progress;

        std::stringstream ss;
        ss << "[";
        for (int j = 0; j < bar_width; ++j) {
          if (j < pos)
            ss << "=";
          else if (j == pos)
            ss << ">";
          else
            ss << " ";
        }
        ss << "](" << success_count + fail_count << "/" << plugins.size()
           << ") | " << path.filename().string();

        FINS_LOG_INFO("[NodeLib] {}", ss.str());

        try {
          load(path.string());
          success_count++;
        } catch (const std::exception &e) {
          fail_count++;
          FINS_LOG_ERROR("[NodeLib] Error: {}", e.what());
        }
      }
      FINS_LOG_INFO("[NodeLib] Load complete: {} success, {} fail.", success_count, fail_count);
    }

    bool load_plugin(const std::string &path) {
      try {
        load(path);
        return true;
      } catch (const std::exception &e) {
        FINS_LOG_ERROR("[NodeLib] Error: {}", e.what());
        return false;
      }
    }

    void load(const std::string &path) {
      for (const auto &ctx: contexts_)
        if (ctx->path == path)
          return;

      auto ctx = std::make_shared<PluginContext>();
      ctx->original_path = path;
      std::string runtime_path = setup_runtime_path(path);
      ctx->path = runtime_path;
      
      ctx->handle = dlopen(runtime_path.c_str(), RTLD_LAZY | RTLD_NODELETE);
      if (!ctx->handle)
        ctx->handle = dlopen(runtime_path.c_str(), RTLD_LAZY);
      if (!ctx->handle) {
        const char *error_msg = dlerror();
        std::string detail = error_msg ? error_msg : "Unknown dlopen error";
        FINS_LOG_ERROR("[NodeLib] dlopen failed for {}. Error: {}", runtime_path, detail);
        throw std::runtime_error("Failed to load: " + runtime_path + " | Reason: " + detail);
      }

      ctx->plugin_init = (InitFunc) dlsym(ctx->handle, "plugin_init");
      if (ctx->plugin_init) {
        try {
          ctx->plugin_init();
        } catch (const std::exception &e) {
          dlclose(ctx->handle);
          throw std::runtime_error("Plugin initialization failed in " + path + ": " + e.what());
        }
      }

      ctx->plugin_destroy = (InitFunc) dlsym(ctx->handle, "plugin_destroy");
      ctx->is_hot_reloadable = (IsReloadableFunc) dlsym(ctx->handle, "is_hot_reloadable");

      ctx->get_count = (GetCountFunc) dlsym(ctx->handle, "get_node_count");
      ctx->get_name = (GetNameFunc) dlsym(ctx->handle, "get_node_name");
      ctx->get_meta_json = (GetMetaJsonFunc) dlsym(ctx->handle, "get_node_meta_json");
      ctx->create_node = (CreateFunc) dlsym(ctx->handle, "create_node");
      ctx->destroy_node = (DestroyFunc) dlsym(ctx->handle, "destroy_node");

      if (!ctx->get_count || !ctx->get_name || !ctx->create_node) {
        if (ctx->plugin_destroy) {
          ctx->plugin_destroy();
        }
        dlclose(ctx->handle);
        throw std::runtime_error("Invalid interface in " + path);
      }

      int count = ctx->get_count();
      if (count > 0) {
        for (int i = 0; i < count; ++i) {
          std::string name = ctx->get_name(i);
          registry_[name] = ctx;

          try {
            capabilities_cache_[name] = json::parse(ctx->get_meta_json(name.c_str()));
          } catch (...) {
          }
        }
        contexts_.push_back(ctx);
      } else {
        if (ctx->plugin_destroy) {
          ctx->plugin_destroy();
        }
        dlclose(ctx->handle);
      }
    }

#ifndef FINS_STATIC_BUILD
    json get_capabilities() const { return capabilities_cache_; }
#else
    json get_capabilities() const { return FINS_NODE_FACTORY.get_capabilities(); }
#endif
    void print_capabilities() const {
      json caps = get_capabilities();
      if (caps.is_null() || caps.empty()) {
        FINS_LOG_INFO("[NodeLib] No capabilities available.");
        return;
      }

      std::stringstream ss;
      ss << "NodeLib Capabilities:\n";
      for (const auto &item: caps.items()) {
        ss << "  - " << item.key() << ": " << item.value().dump(2) << "\n";
      }
      FINS_LOG_INFO("[NodeLib] {}", ss.str());
    }

    std::shared_ptr<INode> create_step(const std::string &node_class_name, const std::string &step_id, bool auto_init = true) {
      INode *raw_ptr = NodeFactory::get_instance().create(node_class_name);

      if (raw_ptr) {
        std::shared_ptr<INode> node(raw_ptr, [](INode *p) { delete p; });
        FINS_STUDIO.add_step(node, step_id);
        if (auto_init) node->initialize();
        return node;
      }

      auto it = registry_.find(node_class_name);
      if (it == registry_.end()) {
        FINS_LOG_ERROR("[NodeLib] Node class '{}' not found.", node_class_name);
        return nullptr;
      }

      auto &ctx = it->second;
      raw_ptr = ctx->create_node(node_class_name.c_str());
      if (!raw_ptr)
        return nullptr;

      auto destroyer = ctx->destroy_node;
      std::shared_ptr<INode> node(raw_ptr, [destroyer](INode *p) {
        if (destroyer)
          destroyer(p);
      });

      FINS_STUDIO.add_step(node, step_id);

      if (auto_init) node->initialize();
      return node;
    }

    void load_json_file(const std::string &file_path) {
      std::ifstream file(file_path);
      if (!file.is_open())
        throw std::runtime_error("Cannot open JSON config file: " + file_path);
      std::string str((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
      load_json(str);
    }

    void load_json(const std::string &json_str) {
      FINS_LOG_INFO("[NodeLib] Received new dataflow. Clearing previous execution graph...");
      FINS_STUDIO.clear();

      json root = json::parse(json_str);
      if (!root.contains("nodes") || !root["nodes"].is_array()) {
        throw std::runtime_error("JSON must contain 'nodes' array.");
      }

      std::map<std::string, std::string> node_id_to_full_key_;
      std::map<std::string, std::vector<std::string>> adj;
      std::map<std::string, int> in_degree;
      std::vector<std::string> node_ids;

      // ==========================================
      // Pass 1: Create Nodes, Set Params, Set Services, Set Actions
      // ==========================================
      for (const auto &node : root["nodes"]) {
        // 1. Basic Info
        std::string id = node["id"].get<std::string>();
        node_ids.push_back(id);
        in_degree[id] = 0;

        std::string node_name = node["name"].get<std::string>(); // New Format: 'name'
        
        std::string source = node.value("source", "workspace");
        std::string version = node.value("version", "default");

        std::string full_key = source + "/" + node_name + "@" + version;
        node_id_to_full_key_[id] = full_key;

        // 2. Create Node (defer initialization)
        auto node_ptr = create_step(full_key, id, false);
        if (!node_ptr) {
          throw std::runtime_error("Failed to create node '" + full_key + "' (ID: " + id + ")");
        }

        // 3. Parameters (New Format: Array of objects)
        if (node.contains("parameters") && node["parameters"].is_array()) {
          for (const auto &param : node["parameters"]) {
            if (param.contains("name") && param.contains("value")) {
              std::string p_name = param["name"].get<std::string>();
              std::string p_val = param["value"].is_string() 
                                  ? param["value"].get<std::string>() 
                                  : param["value"].dump();
              
              FINS_STUDIO.set_step_parameter(id, p_name, p_val);
            }
          }
        }

        // 4. Clients (Service Remapping)
        if (node.contains("clients") && node["clients"].is_array()) {
          for (const auto &client : node["clients"]) {
            if (client.contains("name") && client.contains("topic")) {
              FINS_STUDIO.set_step_client_topic(id, client["name"].get<std::string>(),
                                         client["topic"].get<std::string>());
            }
          }
        }

        // 5. Servers (Service Remapping)
        if (node.contains("servers") && node["servers"].is_array()) {
          for (const auto &server : node["servers"]) {
            if (server.contains("name") && server.contains("topic")) {
              FINS_STUDIO.set_step_server_topic(id, server["name"].get<std::string>(),
                                         server["topic"].get<std::string>());
            }
          }
        }

        // 6. Commanders (Action Remapping)
        if (node.contains("commanders") && node["commanders"].is_array()) {
          for (const auto &commander : node["commanders"]) {
            if (commander.contains("name") && commander.contains("topic")) {
              FINS_STUDIO.set_step_commander_topic(id, commander["name"].get<std::string>(),
                                                    commander["topic"].get<std::string>());
            }
          }
        }

        // 7. Actors (Action Remapping)
        if (node.contains("actors") && node["actors"].is_array()) {
          for (const auto &actor : node["actors"]) {
            if (actor.contains("name") && actor.contains("topic")) {
              FINS_STUDIO.set_step_actor_topic(id, actor["name"].get<std::string>(),
                                                actor["topic"].get<std::string>());
            }
          }
        }
      }

      // ==========================================
      // Pass 2: Connect Pipes (Input/Output Connections) & Build Dependency Graph
      // ==========================================
      for (const auto &node : root["nodes"]) {
        std::string dest_id = node["id"].get<std::string>();
        std::string dest_full_key = node_id_to_full_key_.at(dest_id);

        if (node.contains("inputs") && node["inputs"].is_object()) {
          for (const auto &[input_port_name, input_info] : node["inputs"].items()) {
            
            if (input_info.contains("connect")) {
              std::string conn_str = input_info["connect"].get<std::string>();
              
              auto [src_id, src_out_name] = split_conn(conn_str);
              
              if (input_info.contains("schedule") && input_info["schedule"].is_string()) {
                ScheduleInfo schedule_info;
                std::string schedule_str = input_info["schedule"].get<std::string>();
                
                size_t priority_start = schedule_str.find("PRIORITY:") + 9;
                size_t priority_end = schedule_str.find(";", priority_start);
                if (priority_end == std::string::npos) priority_end = schedule_str.length();
                std::string priority_str = schedule_str.substr(priority_start, priority_end - priority_start);
                
                if (priority_str == "Urgent") {
                  schedule_info.priority = SchedulePriority::Urgent;
                } else if (priority_str == "High") {
                  schedule_info.priority = SchedulePriority::High;
                } else if (priority_str == "Medium") {
                  schedule_info.priority = SchedulePriority::Medium;
                } else if (priority_str == "Low") {
                  schedule_info.priority = SchedulePriority::Low;
                }
                
                size_t queue_start = schedule_str.find("QUEUE:") + 6;
                size_t queue_end = schedule_str.find(";", queue_start);
                if (queue_end == std::string::npos) queue_end = schedule_str.length();
                std::string queue_str = schedule_str.substr(queue_start, queue_end - queue_start);
                
                if (queue_str == "FCFS") {
                  schedule_info.queue = ScheduleQueue::FCFS;
                } else if (queue_str == "LGFS") {
                  schedule_info.queue = ScheduleQueue::LGFS;
                }
                
                FINS_STUDIO.set_step_schedule(dest_id, schedule_info);
              }
              
              if (node_id_to_full_key_.find(src_id) == node_id_to_full_key_.end()) {
                 throw std::runtime_error("Source node ID not found: " + src_id);
              }
              std::string src_full_key = node_id_to_full_key_.at(src_id);

              int in_port_idx = find_port_idx(dest_full_key, input_port_name, true);
              if (in_port_idx == -1)
                throw std::runtime_error("Input port '" + input_port_name + "' not found in " + dest_full_key);

              int out_port_idx = find_port_idx(src_full_key, src_out_name, false);
              if (out_port_idx == -1)
                throw std::runtime_error("Output port '" + src_out_name + "' not found in " + src_full_key);

              // Create Pipe
              std::string pipe_id = src_id + "/" + src_out_name + "->" + dest_id + "/" + input_port_name;
              FINS_STUDIO.add_pipe(src_id, dest_id, out_port_idx, in_port_idx, pipe_id);

              // Build graph for topological sort (src -> dest)
              adj[src_id].push_back(dest_id);
              in_degree[dest_id]++;
            }
          }
        }
      }

      // ==========================================
      // Pass 3: Topological Initialization
      // ==========================================
      std::vector<std::string> init_order;
      std::queue<std::string> q;
      for (const auto &id : node_ids) {
        if (in_degree[id] == 0) q.push(id);
      }

      while (!q.empty()) {
        std::string u = q.front(); q.pop();
        init_order.push_back(u);
        if (adj.count(u)) {
          for (const auto &v : adj[u]) {
            if (--in_degree[v] == 0) q.push(v);
          }
        }
      }

      bool has_cycle = init_order.size() < node_ids.size();
      if (has_cycle) {
        FINS_LOG_WARN("[NodeLib] Cycle detected in node topology! Using default order for some nodes.");
        // Add remaining nodes to init_order
        std::set<std::string> visited(init_order.begin(), init_order.end());
        for (const auto &id : node_ids) {
          if (visited.find(id) == visited.end()) {
            init_order.push_back(id);
          }
        }
      }

      FINS_LOG_INFO("[NodeLib] Initializing {} nodes in topological order...", init_order.size());
      auto start_init = std::chrono::high_resolution_clock::now();
      FINS_STUDIO.set_topology_order(init_order);
      for (size_t i = 0; i < init_order.size(); ++i) {
        const auto &id = init_order[i];
        auto step = FINS_STUDIO.get_step(id);
        if (step) {
          float progress = (float)(i + 1) / init_order.size() * 100.0f;
          FINS_LOG_INFO("[NodeLib] [{:3.0f}%] Initializing {} ({})", progress, id, node_id_to_full_key_[id]);
          step->get_node()->initialize();
        }
      }
      auto end_init = std::chrono::high_resolution_clock::now();
      double duration = std::chrono::duration<double>(end_init - start_init).count();
      FINS_LOG_INFO("[NodeLib] All nodes initialized in {:.3f} seconds. Starting execution...", duration);

      if (root.contains("pipes") && root["pipes"].is_array()) {
        for (const auto &pipe : root["pipes"]) {
          if (pipe.contains("from") && pipe.contains("to")) {
          }
        }
      }
    }

    json inspect_plugin(const std::string &path) {
      json report;
      report["file_path"] = path;
      report["status"] = "UNKNOWN";
      report["nodes"] = json::array();
      report["warnings"] = json::array();
      report["dependencies"] = "";
      report["error"] = "";

      std::string file_info = exec_cmd("file -b " + path);
      if (file_info.find("x86-64") != std::string::npos)
        report["architecture"] = "x86_64";
      else if (file_info.find("aarch64") != std::string::npos)
        report["architecture"] = "aarch64";
      else
        report["architecture"] = "unknown (" + file_info.substr(0, file_info.find(',')) + ")";

      report["dependencies"] = exec_cmd("ldd " + path);

      std::string nm_out = exec_cmd("nm -uC " + path + " 2>/dev/null");
      std::istringstream nm_stream(nm_out);
      std::string line;
      std::vector<std::string> ignore_warnings = {"w _ITM_deregisterTMCloneTable", "w _ITM_registerTMCloneTable",
                                                  "U __FRAME_END__", "w __gmon_start__"};
      while (std::getline(nm_stream, line)) {
        if (!line.empty()) {
          size_t start = line.find_first_not_of(" \t");
          if (start != std::string::npos) {
            line = line.substr(start);
          }
          bool ignore = false;
          for (const auto &ignore_msg: ignore_warnings) {
            if (line == ignore_msg) {
              ignore = true;
              break;
            }
          }
          if (!ignore) {
            report["warnings"].push_back(line);
          }
        }
      }

      auto start_time = std::chrono::high_resolution_clock::now();
      void *handle = nullptr;

      try {
        handle = dlopen(path.c_str(), RTLD_LAZY | RTLD_LOCAL);
        if (!handle) {
          const char *err = dlerror();
          throw std::runtime_error(err ? err : "Unknown dlopen error");
        }

        auto get_count = (GetCountFunc) dlsym(handle, "get_node_count");
        auto get_name = (GetNameFunc) dlsym(handle, "get_node_name");
        auto get_meta = (GetMetaJsonFunc) dlsym(handle, "get_node_meta_json");
        auto create_node = (CreateFunc) dlsym(handle, "create_node");

        if (!get_count || !get_name || !create_node) {
          throw std::runtime_error("failed to find node entry, forget DEFINE_PLUGIN_ENTRY()?");
        }

        int count = get_count();
        for (int i = 0; i < count; ++i) {
          std::string name = get_name(i);
          json node_info;
          try {
            if (get_meta) {
              node_info = json::parse(get_meta(name.c_str()));
            } else {
              node_info = "N/A";
            }
          } catch (const std::exception &e) {
            node_info["metadata_error"] = e.what();
          }
          report["nodes"].push_back(node_info);
        }

        report["status"] = "VALID";

      } catch (const std::exception &e) {
        report["status"] = "ERROR";
        report["error"] = e.what();
      }

      if (handle) {
        dlclose(handle);
      }

      auto end_time = std::chrono::high_resolution_clock::now();
      report["load_time_ms"] = std::chrono::duration<double, std::milli>(end_time - start_time).count();

      return report;
    }

  private:
    int find_port_idx(const std::string &unique_name, const std::string &port_name, bool is_input) {
      json caps = get_capabilities();
      if (!caps.contains(unique_name))
        return -1;
      const auto &meta = caps[unique_name];

      std::string key = is_input ? "inputs" : "outputs";
      if (!meta.contains(key))
        return -1;

      for (const auto &port: meta[key]) {
        if (port["name"] == port_name) {
          return port["id"].get<int>();
        }
      }
      return -1;
    }

    std::string get_file_md5(const std::string &path) {
      std::string cmd = "md5sum " + path + " | awk '{print $1}'";
      std::string res = exec_cmd(cmd);
      if (res.size() > 32)
        res = res.substr(0, 32);
      while (!res.empty() && (res.back() == '\n' || res.back() == '\r'))
        res.pop_back();
      return res;
    }

    std::string setup_runtime_path(const std::string &path) {
      std::string runtime_dir = expand_user("~/.fins/runtime/");
      fs::create_directories(runtime_dir);

      std::string md5 = get_file_md5(path);
      fs::path p(path);
      std::string stem = p.stem().string();
      std::string ext = p.extension().string();

      std::string new_path = runtime_dir + stem + "_" + md5 + ext;
      if (!fs::exists(new_path)) {
        fs::copy_file(path, new_path, fs::copy_options::overwrite_existing);
      }
      return new_path;
    }

    std::pair<std::string, std::string> split_conn(const std::string &s) {
      size_t pos = s.find('/');
      if (pos == std::string::npos)
        throw std::runtime_error("Invalid connection: " + s);
      return {s.substr(0, pos), s.substr(pos + 1)};
    }

    typedef int (*GetCountFunc)();
    typedef const char *(*GetNameFunc)(int);
    typedef const char *(*GetMetaJsonFunc)(const char *);
    typedef INode *(*CreateFunc)(const char *);
    typedef void (*DestroyFunc)(INode *);
    typedef void (*InitFunc)();
    typedef bool (*IsReloadableFunc)();

    struct PluginContext {
      void *handle = nullptr;
      std::string path;
      std::string original_path;
      GetCountFunc get_count = nullptr;
      GetNameFunc get_name = nullptr;
      GetMetaJsonFunc get_meta_json = nullptr;
      CreateFunc create_node = nullptr;
      DestroyFunc destroy_node = nullptr;
      InitFunc plugin_init = nullptr;
      InitFunc plugin_destroy = nullptr;
      IsReloadableFunc is_hot_reloadable = nullptr;
    };

    std::vector<std::shared_ptr<PluginContext>> contexts_;
    std::map<std::string, std::shared_ptr<PluginContext>> registry_;

    json capabilities_cache_;

    std::string watch_dir_;
    std::thread monitor_thread_;
    std::atomic<bool> stop_monitor_{false};

    void start_monitor() {
      stop_monitor_ = false;
      monitor_thread_ = std::thread([this]() {
        pthread_setname_np(pthread_self(), "fins_inotify");
        int fd = inotify_init();
        if (fd < 0) return;
        int wd = inotify_add_watch(fd, watch_dir_.c_str(), IN_CLOSE_WRITE | IN_MOVED_TO);
        if (wd < 0) return;

        char buffer[4096];
        while (!stop_monitor_) {
          struct timeval tv = {1, 0};
          fd_set fds;
          FD_ZERO(&fds);
          FD_SET(fd, &fds);
          if (select(fd + 1, &fds, NULL, NULL, &tv) <= 0) continue;

          int length = read(fd, buffer, sizeof(buffer));
          if (length <= 0) continue;

          for (int i = 0; i < length; ) {
            struct inotify_event *event = (struct inotify_event *) &buffer[i];
            if (event->len && (event->mask & (IN_CLOSE_WRITE | IN_MOVED_TO))) {
              std::string filename = event->name;
              if (filename.size() > 3 && filename.substr(filename.size() - 3) == ".so") {
                FINS_LOG_INFO("[NodeLib] Plugin change detected: {}", filename);
                std::string full_path = watch_dir_ + "/" + filename;
                std::this_thread::sleep_for(std::chrono::milliseconds(200)); 
                reload_plugin_process(full_path);
              }
            }
            i += sizeof(struct inotify_event) + event->len;
          }
        }
        close(fd);
      });
    }

    void reload_plugin_process(const std::string &path) {
      auto normalize = [](std::string p) {
        while (p.find("//") != std::string::npos) p.replace(p.find("//"), 2, "/");
        return p;
      };
      std::string norm_target = normalize(path);

      std::shared_ptr<PluginContext> old_ctx = nullptr;
      for (auto &ctx : contexts_) {
        if (normalize(ctx->original_path) == norm_target) {
          old_ctx = ctx;
          break;
        }
      }

      if (!old_ctx) {
        fs::path p(path);
        std::string filename = p.filename().string();
        for (auto &ctx : contexts_) {
          if (fs::path(ctx->original_path).filename().string() == filename) {
            old_ctx = ctx;
            FINS_LOG_INFO("[NodeLib] Resolved plugin via filename match: {}", filename);
            break;
          }
        }
      }

      if (!old_ctx) {
        FINS_LOG_INFO("[NodeLib] New plugin detected: {}. Loading...", norm_target);
        try {
          load(norm_target);
          if (on_reloaded_callback_) {
            on_reloaded_callback_();
          }
        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[NodeLib] Failed to load new plugin {}: {}", norm_target, e.what());
        }
        return;
      }

      std::set<std::string> nodes_owned_by_this_plugin;
      int count = old_ctx->get_count ? old_ctx->get_count() : 0;
      for (int i = 0; i < count; ++i) {
        nodes_owned_by_this_plugin.insert(old_ctx->get_name(i));
      }

      std::vector<std::string> affected_steps;
      auto all_ids = FINS_STUDIO.get_all_step_ids();
      for (const auto &id : all_ids) {
        auto step = FINS_STUDIO.get_step(id);
        if (!step) continue;

        auto meta = step->get_node_meta(); 
        std::string full_key = meta.source + "/" + meta.name + "@" + meta.version;
        if (nodes_owned_by_this_plugin.count(full_key)) {
          affected_steps.push_back(id);
        }
      }

      FINS_LOG_INFO("[NodeLib] Hard-reloading plugin: {} ({} steps affected)", 
                   fs::path(path).filename().string(), affected_steps.size());

      bool is_stateless = old_ctx->is_hot_reloadable ? old_ctx->is_hot_reloadable() : true;
      if (is_stateless) {
        for (const auto &id : affected_steps) {
          auto step = FINS_STUDIO.get_step(id);
          FINS_LOG_INFO("[NodeLib] Killing old instance: {}", id);
          
          step->pause();
          step->inject_node(nullptr);
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        try {
          if (old_ctx->plugin_destroy) {
            old_ctx->plugin_destroy();
          }
          if (old_ctx->handle) {
            FINS_LOG_INFO("[NodeLib] Unloading SO handle: {}", old_ctx->path);
            dlclose(old_ctx->handle);
            old_ctx->handle = nullptr;
          }

          std::string runtime_path = setup_runtime_path(path);
          FINS_LOG_INFO("[NodeLib] Loading new version from: {}", runtime_path);
          
          void* new_handle = dlopen(runtime_path.c_str(), RTLD_NOW | RTLD_LOCAL);
          if (!new_handle) {
            FINS_LOG_ERROR("[NodeLib] dlopen failed: {}", dlerror());
            return;
          }

          old_ctx->handle = new_handle;
          old_ctx->path = runtime_path;
          
          old_ctx->get_count = (GetCountFunc)dlsym(new_handle, "get_node_count");
          old_ctx->get_name = (GetNameFunc)dlsym(new_handle, "get_node_name");
          if (!old_ctx->get_name) old_ctx->get_name = (GetNameFunc) dlsym(new_handle, "get_node_name");
          old_ctx->get_meta_json = (GetMetaJsonFunc) dlsym(new_handle, "get_node_meta_json");
          old_ctx->create_node = (CreateFunc)dlsym(new_handle, "create_node");
          old_ctx->destroy_node = (DestroyFunc)dlsym(new_handle, "destroy_node");
          old_ctx->plugin_init = (InitFunc)dlsym(new_handle, "plugin_init");
          old_ctx->plugin_destroy = (InitFunc)dlsym(new_handle, "plugin_destroy");

          if (old_ctx->plugin_init) old_ctx->plugin_init();

          int new_count = old_ctx->get_count ? old_ctx->get_count() : 0;
          for (int i = 0; i < new_count; ++i) {
            std::string node_name = old_ctx->get_name(i);
            registry_[node_name] = old_ctx;
            try {
              if (old_ctx->get_meta_json) {
                capabilities_cache_[node_name] = json::parse(old_ctx->get_meta_json(node_name.c_str()));
              }
            } catch (...) {}
          }

          for (const auto &id : affected_steps) {
            auto step = FINS_STUDIO.get_step(id);
            auto meta = step->get_node_meta();
            std::string full_key = meta.source + "/" + meta.name + "@" + meta.version;

            FINS_LOG_INFO("[NodeLib] Spawning new instance for: {}", id);
            auto raw_ptr = FINS_NODE_FACTORY.create(full_key);
            if (raw_ptr) {
                auto destroyer = old_ctx->destroy_node;
                std::shared_ptr<INode> new_node(raw_ptr, [destroyer](INode *p){ if(destroyer) destroyer(p); });
                
                step->inject_node(new_node);
                new_node->initialize();
                step->run();
                FINS_LOG_INFO("[NodeLib] Step {} restored with new logic.", id);
            }
          }
          if (on_reloaded_callback_) on_reloaded_callback_();

        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[NodeLib] Reload critical failure: {}", e.what());
        }
      } else {
        FINS_LOG_WARN("[NodeLib] Stateful plugin change detected. Reload deferred until graph stop.");
      }
    }

  public:
    void set_on_reloaded(std::function<void()> cb) { on_reloaded_callback_ = cb; }

  private:
    std::string exec_cmd(const std::string &cmd) {
      std::array<char, 128> buffer;
      std::string result;
      std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);
      if (!pipe) {
        return "exec_cmd failed";
      }
      while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
        result += buffer.data();
      }
      return result;
    }
  };
} // namespace fins