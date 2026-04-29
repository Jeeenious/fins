/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// static_agent.cpp

#include <fins/agent/server.hpp>
#include <fins/utils/network.hpp>
#include "static_workspace/hello_world.hpp"
#ifdef FINS_HAS_ZENOH
#include "static_workspace/zenoh_node.hpp"
#endif

int main() {
  set_node_log_level(fins::NodeLogLevel::DEBUG);

  fins::NodeLib lib;
  fins::NodeFactory::get_instance().print_registered_nodes();
  fins::AgentServer server(lib);

  server.connect("http://192.168.1.100:8090");
  FINS_LOG_INFO("Local IP Address: {}", fins::utils::get_local_ip());
  server.start("agent-static", fins::utils::get_local_ip(), 9091);

  while (true) {
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  return 0;
}