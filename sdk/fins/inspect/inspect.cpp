/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// inspect.cpp

#define FINS_LOG_LEVEL 4

#include <iostream>
#include <string>
#include <vector>
#include <memory>

#include <fins/node_log.hpp>
#include <fins/nodelib.hpp>

void print_usage(const char *prog_name) {
  std::cout << "Usage: " << prog_name << " [options] <plugin_file1> [plugin_file2] ...\n"
            << "Options:\n"
            << "  -h, --help              Show this help message\n"
            << "\n"
            << "Description:\n"
            << "  Inspects FINS shared object (.so) plugins and outputs metadata in JSON format.\n";
}

int main(int argc, char **argv) {
  std::vector<std::string> plugins;

  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "-h" || arg == "--help") {
      print_usage(argv[0]);
      return 0;
    } else {
      plugins.push_back(arg);
    }
  }

  if (plugins.empty()) {
    FINS_LOG_ERROR("[Inspect] No plugin files specified.");
    print_usage(argv[0]);
    return 1;
  }

  fins::NodeLib lib;
  fins::json result_array = fins::json::array();

  for (const auto &p : plugins) {
    if (!std::filesystem::exists(p)) {
        fins::json error_obj;
        error_obj["file_path"] = p;
        error_obj["status"] = "ERROR";
        error_obj["error"] = "File not found";
        result_array.push_back(error_obj);
        continue;
    }

    fins::json report = lib.inspect_plugin(p);
    result_array.push_back(report);
  }

  std::cout << result_array.dump(4) << std::endl;

  return 0;
}