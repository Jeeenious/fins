/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 *******************************************************************************/

// main.cpp

#include <fins/agent/server.hpp>
#include <fins/node_log.hpp>
#include <fins/thread_manager.hpp>
#include <fins/utils/performance_recorder.hpp>
#include <getopt.h>
#include <iostream>
#include <string>
#include <vector>
#include <csignal>
#include <atomic>

namespace {
  std::atomic<bool> g_running(true);
  void signal_handler(int) {
    g_running = false;
  }
}

void print_usage(const char *prog_name) {
  std::cout << "Usage: " << prog_name << " [options]\n"
            << "Options:\n"
            << "  --threads-urgent <n>    Set urgent priority thread pool size (default: 4)\n"
            << "  --threads-high <n>      Set high priority thread pool size (default: 4)\n"
            << "  --threads-medium <n>    Set medium priority thread pool size (default: 4)\n"
            << "  --threads-low <n>       Set low priority thread pool size (default: 4)\n"
            << "  --log-level <level>     Set node log level (0=DEBUG, 1=INFO, 2=WARN, 3=ERROR, 4=OFF) (default: 1)\n"
            << "  --plugin <file>         Load specified .so plugin file (can be repeated)\n"
            << "  --load-all              Load all plugins from ~/.fins/install/ (ignores --plugin)\n"
            << "  --webui <url>           Connect to WebUI URL (e.g. http://localhost:8080)\n"
            << "  --name <agent_name>     Set agent name (default: agent-any)\n"
            << "  --ip <agent_ip>         Set agent IP binding (default: 0.0.0.0)\n"
            << "  --port <agent_port>     Set agent listening port (default: 9090)\n"
            << "  -h, --help              Show this help message\n";
}

int main(int argc, char **argv) {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  int urgent_threads = 2;
  int high_threads = 2;
  int medium_threads = 0;
  int low_threads = 0;
  int log_level = 1; // INFO
  bool load_all = false;
  std::vector<std::string> plugins;
  std::string webui_url = "http://localhost:8080";
  std::string agent_name = "agent";
  std::string agent_ip = "0.0.0.0";
  int agent_port = 9090;

  struct option long_options[] = {{"threads-urgent", required_argument, 0, 'u'},
                                  {"threads-high", required_argument, 0, 'H'},
                                  {"threads-medium", required_argument, 0, 'm'},
                                  {"threads-low", required_argument, 0, 'l'},
                                  {"log-level", required_argument, 0, 'L'},
                                  {"plugin", required_argument, 0, 'p'},
                                  {"load-all", no_argument, 0, 'A'},
                                  {"webui", required_argument, 0, 'w'},
                                  {"name", required_argument, 0, 'n'},
                                  {"ip", required_argument, 0, 'I'},
                                  {"port", required_argument, 0, 'P'},
                                  {"help", no_argument, 0, 'h'},
                                  {0, 0, 0, 0}};

  int opt;
  int option_index = 0;
  while ((opt = getopt_long(argc, argv, "u:H:m:l:L:p:Aw:n:I:P:h", long_options, &option_index)) != -1) {
    switch (opt) {
      case 'u':
        urgent_threads = std::stoi(optarg);
        break;
      case 'H':
        high_threads = std::stoi(optarg);
        break;
      case 'm':
        medium_threads = std::stoi(optarg);
        break;
      case 'l':
        low_threads = std::stoi(optarg);
        break;
      case 'L':
        log_level = std::stoi(optarg);
        break;
      case 'p':
        plugins.push_back(optarg);
        break;
      case 'A':
        load_all = true;
        break;
      case 'w':
        webui_url = optarg;
        break;
      case 'n':
        agent_name = optarg;
        break;
      case 'I':
        agent_ip = optarg;
        break;
      case 'P':
        agent_port = std::stoi(optarg);
        break;
      case 'h':
        print_usage(argv[0]);
        return 0;
      default:
        print_usage(argv[0]);
        return 1;
    }
  }

  // Set Log Level
  if (log_level < 0)
    log_level = 0;
  if (log_level > 4)
    log_level = 4;
  fins::set_node_log_level(static_cast<fins::NodeLogLevel>(log_level));

  // Set Thread Pool
  FINS_THREAD_MANAGER.set_urgent_threads(urgent_threads);
  FINS_THREAD_MANAGER.set_high_threads(high_threads);
  FINS_THREAD_MANAGER.set_medium_threads(medium_threads);
  FINS_THREAD_MANAGER.set_low_threads(low_threads);
  FINS_THREAD_MANAGER.start();

  FINS_PERF_MONITOR.start();

  fins::NodeLib lib;

  // Load plugins
  if (load_all) {
    FINS_LOG_INFO("[Agent] Loading all plugins from ~/.fins/install/");
    lib.load_directory("~/.fins/install/");
  } else {
    for (const auto &p: plugins) {
      lib.load_plugin(p);
    }
  }

  fins::AgentServer server(lib);

  FINS_LOG_INFO("[Agent] Connecting to WebUI: {}", webui_url);
  server.connect(webui_url);

  FINS_LOG_INFO("[Agent] Starting agent '{}' on {}:{}", agent_name, agent_ip, agent_port);
  server.start(agent_name, agent_ip, agent_port);

  while (g_running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  FINS_LOG_INFO("[Agent] Shutting down agent...");
  server.stop();
  FINS_PERF_MONITOR.stop();
  FINS_STUDIO.clear();
  FINS_THREAD_MANAGER.shutdown();

  return 0;
}