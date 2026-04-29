/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// agent.cpp

#define FINS_LOG_LEVEL 1

#include <fins/agent/server.hpp>
#include <fins/utils/performance_recorder.hpp>
#include <csignal>

namespace {
  std::atomic<bool> g_running(true);
  void signal_handler(int) {
    g_running = false;
  }
}

int main() {
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);

  set_node_log_level(fins::NodeLogLevel::DEBUG);

  FINS_THREAD_MANAGER.start();

  FINS_PERF_MONITOR.start();

  fins::NodeLib lib;
  lib.load_directory("~/.fins/install/");
  // lib.print_capabilities();

  fins::AgentServer server(lib);

  server.connect("http://localhost:8080");
  server.start("agent", "0.0.0.0", 1896);

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