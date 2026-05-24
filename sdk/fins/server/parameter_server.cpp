/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// parameter_server.cpp

#include "parameter_server.hpp"
#include <sstream>

namespace fins {

  ParameterServer &ParameterServer::get_instance() {
    static ParameterServer instance;
    return instance;
  }

  ParameterServer::ParameterServer() {}

  std::string ParameterServer::trim(const std::string &str) {
    size_t first = str.find_first_not_of(" \t\r\n");
    if (std::string::npos == first)
      return "";
    size_t last = str.find_last_not_of(" \t\r\n");
    return str.substr(first, (last - first + 1));
  }

  std::string ParameterServer::strip_quotes(const std::string &str) {
    std::string s = trim(str);
    if (s.size() >= 2) {
      if ((s.front() == '"' && s.back() == '"') || (s.front() == '\'' && s.back() == '\'')) {
        return s.substr(1, s.size() - 2);
      }
    }
    return s;
  }

  bool ParameterServer::load_file(const std::string &path) {
    if (!std::filesystem::exists(path)) {
      FINS_LOG_WARN("[ParameterServer] Parameter file does not exist: {}", path);
      return false;
    }
    std::ifstream file(path);
    if (!file.is_open()) {
      FINS_LOG_WARN("[ParameterServer] Failed to open parameter file: {}", path);
      return false;
    }
    std::stringstream buffer;
    buffer << file.rdbuf();
    return load_string(buffer.str());
  }

  bool ParameterServer::load_string(const std::string &str) {
    std::lock_guard<std::mutex> lock(mutex_);
    FINS_LOG_INFO("[ParameterServer] Loading parameters.");
    // FINS_LOG_INFO("[ParameterServer] Parameter string (first 50 chars): {}",
    //               str.substr(0, std::min<size_t>(50, str.size())));

    std::istringstream stream(str);

    std::string line;
    std::vector<std::pair<int, std::string>> scope_stack;
    std::string pending_multiline_value = "";
    std::string pending_full_key = "";
    bool in_multiline_mode = false;

    int line_num = 0;
    while (std::getline(stream, line)) {
      line_num++;

      size_t comment_pos = line.find('#');
      if (comment_pos != std::string::npos) {
        line = line.substr(0, comment_pos);
      }
      std::string trimmed_line = trim(line);
      if (trimmed_line.empty())
        continue;

      if (in_multiline_mode) {
        pending_multiline_value += " " + trimmed_line;
        if (trimmed_line.find(']') != std::string::npos) {
          if (params_.find(pending_full_key) == params_.end()) {
            params_order_.push_back(pending_full_key);
          }
          params_[pending_full_key] = pending_multiline_value;
          in_multiline_mode = false;
          pending_multiline_value = "";
          pending_full_key = "";
        }
        continue;
      }

      int indent = 0;
      while (static_cast<size_t>(indent) < line.length() && line[indent] == ' ') {
        indent++;
      }

      size_t colon_pos = line.find(':');
      if (colon_pos == std::string::npos)
        continue;

      std::string key = trim(line.substr(0, colon_pos));
      std::string value = trim(line.substr(colon_pos + 1));

      while (!scope_stack.empty() && indent <= scope_stack.back().first) {
        scope_stack.pop_back();
      }
      scope_stack.push_back({indent, key});

      std::string full_key = "";
      for (size_t i = 0; i < scope_stack.size(); ++i) {
        full_key += scope_stack[i].second;
        if (i < scope_stack.size() - 1)
          full_key += ".";
      }

      if (value.empty()) {
        continue;
      }

      if (value.find('[') != std::string::npos && value.find(']') == std::string::npos) {
        in_multiline_mode = true;
        pending_full_key = full_key;
        pending_multiline_value = value;
      } else {
        if (params_.find(full_key) == params_.end()) {
          params_order_.push_back(full_key);
        }
        params_[full_key] = value;
      }
    }

    FINS_LOG_INFO("[ParameterServer] Loaded {} parameters.", params_.size());
    return true;
  }

} // namespace fins
