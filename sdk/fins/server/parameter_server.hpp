/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 *******************************************************************************/

// parameter_server.hpp

#pragma once

#include <algorithm>
#include <cctype>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <sstream>
#include <string>
#include <vector>

#include <filesystem>
#include <fins/utils/logger.hpp>
#include <fins/macros.hpp>
#include <fins/type/type_register.hpp>

namespace fins {

  struct YamlNode {
    std::map<std::string, YamlNode> children;
    std::vector<std::string> child_order;
    std::string value;
    std::string type_info;
    bool is_leaf = false;

    void insert(const std::string &full_key, const std::string &val, const std::string &type = "") {
      std::size_t pos = full_key.find('.');
      if (pos == std::string::npos) {
        if (children.find(full_key) == children.end()) {
          child_order.push_back(full_key);
        }
        children[full_key].value = val;
        children[full_key].type_info = type;
        children[full_key].is_leaf = true;
      } else {
        std::string head = full_key.substr(0, pos);
        std::string tail = full_key.substr(pos + 1);
        if (children.find(head) == children.end()) {
          child_order.push_back(head);
        }
        children[head].insert(tail, val, type);
      }
    }

    void dump(std::stringstream &ss, int indent = 0) const {
      for (const auto &name : child_order) {
        auto it = children.find(name);
        if (it == children.end()) continue;
        const auto &node = it->second;
        
        ss << std::string(indent * 2, ' ') << name << ":";
        if (node.is_leaf) {
          ss << " " << node.value;
          if (!node.type_info.empty()) ss << " # type: " << node.type_info;
          ss << "\n";
        } else {
          ss << "\n";
          node.dump(ss, indent + 1);
        }
      }
    }
  };

  class FINS_API ParameterServer {
  public:
    static ParameterServer &get_instance();

    ParameterServer(const ParameterServer &) = delete;
    ParameterServer &operator=(const ParameterServer &) = delete;

    bool load_string(const std::string &str);

    bool load_file(const std::string &path);

    template<typename T>
    T get(const std::string &key) const {
      return get_impl<T>(key, nullptr);
    }

    template<typename T>
    T get(const std::string &key, const T &default_value) const {
      return get_impl<T>(key, &default_value);
    }

    std::string get(const std::string &key, const char *default_value) const {
      std::string def_val(default_value);
      return get_impl<std::string>(key, &def_val);
    }

    std::string dump_active_yaml() const {
      std::lock_guard<std::mutex> lock(mutex_);
      YamlNode root;
      for (const auto &key : params_order_) {
        auto it = params_.find(key);
        if (it != params_.end()) {
          root.insert(key, it->second);
        }
      }
      std::stringstream ss;
      root.dump(ss);
      return ss.str();
    }

    std::string dump_template_yaml() const {
      std::lock_guard<std::mutex> lock(mutex_);
      YamlNode root;
      for (const auto &key : requested_order_) {
        auto it = requested_entries_.find(key);
        if (it != requested_entries_.end()) {
          root.insert(key, it->second.default_val, it->second.type_name);
        }
      }
      std::stringstream ss;
      root.dump(ss);
      return ss.str();
    }

  private:
    template<typename T>
    T get_impl(const std::string &key, const T *default_value_ptr) const {
      if (default_value_ptr) {
        record_requirement(key, *default_value_ptr);
      }

      std::lock_guard<std::mutex> lock(mutex_);

      auto it = params_.find(key);
      if (it == params_.end()) {
        size_t first_dot = key.find('.');
        if (first_dot != std::string::npos) {
          std::string sub_key = key.substr(first_dot + 1);
          for (auto const &[k, v]: params_) {
            if (k.length() >= sub_key.length() &&
                k.compare(k.length() - sub_key.length(), sub_key.length(), sub_key) == 0) {
              if (!default_value_ptr) {
                static bool warned = false;
                if (!warned) {
                  FINS_LOG_WARN("[ParameterServer] Resolved '{}' using suffix matching to '{}'", key, k);
                  warned = true;
                }
              }
              return convert<T>(v, key);
            }
          }
        }
        if (default_value_ptr) {
          FINS_LOG_WARN("[ParameterServer] Parameter not found: {}. Using default value.", key);
          return *default_value_ptr;
        }
        return T();
      }
      return convert<T>(it->second, key);
    }

    struct ParamRequirement {
      std::string default_val;
      std::string type_name;
    };
    mutable std::map<std::string, ParamRequirement> requested_entries_;
    mutable std::vector<std::string> requested_order_;

    template<typename T>
    void record_requirement(const std::string &key, const T &default_val) const {
      std::lock_guard<std::mutex> lock(mutex_);
      if (requested_entries_.find(key) == requested_entries_.end()) {
        requested_entries_[key] = { to_raw_string(default_val), FINS_TYPE_REGISTER.get_name<T>() };
        requested_order_.push_back(key);
      }
    }

    template<typename T>
    std::string to_raw_string(const T &val) const {
      if constexpr (std::is_same_v<T, std::string>) return val;
      else if constexpr (std::is_same_v<T, bool>) return val ? "true" : "false";
      else if constexpr (std::is_floating_point_v<T>) {
        return fmt::format("{:g}", val);
      } else {
        using namespace std;
        return to_string(val);
      }
    }

  private:
    ParameterServer();
    ~ParameterServer() = default;

    std::map<std::string, std::string> params_;
    std::vector<std::string> params_order_;
    mutable std::mutex mutex_;

    static std::string trim(const std::string &str);

    static std::string strip_quotes(const std::string &str);

    template<typename T>
    T convert(const std::string &val, const std::string &key) const {
      (void) val;
      (void) key;
      FINS_LOG_ERROR("[ParameterServer] No conversion available for type. Key: {}", key);
      return T();
    }
  };

  template<>
  inline std::string ParameterServer::convert<std::string>(const std::string &val, const std::string &key) const {
    (void) key;
    return strip_quotes(val);
  }

  template<>
  inline bool ParameterServer::convert<bool>(const std::string &val, const std::string &key) const {
    (void) key;
    std::string v = val;
    std::transform(v.begin(), v.end(), v.begin(), ::tolower);
    return (v == "true" || v == "1" || v == "on");
  }

  template<>
  inline int ParameterServer::convert<int>(const std::string &val, const std::string &key) const {
    try {
      return std::stoi(val);
    } catch (...) {
      FINS_LOG_WARN("[ParameterServer] Failed to convert to int: {} = {}", key, val);
      return 0;
    }
  }

  template<>
  inline long ParameterServer::convert<long>(const std::string &val, const std::string &key) const {
    try {
      return std::stol(val);
    } catch (...) {
      FINS_LOG_WARN("[ParameterServer] Failed to convert to long: {} = {}", key, val);
      return 0L;
    }
  }

  template<>
  inline long long ParameterServer::convert<long long>(const std::string &val, const std::string &key) const {
    try {
      return std::stoll(val);
    } catch (...) {
      FINS_LOG_WARN("[ParameterServer] Failed to convert to long long: {} = {}", key, val);
      return 0LL;
    }
  }

  template<>
  inline unsigned int ParameterServer::convert<unsigned int>(const std::string &val, const std::string &key) const {
    try {
      return static_cast<unsigned int>(std::stoul(val));
    } catch (...) {
      FINS_LOG_WARN("[ParameterServer] Failed to convert to unsigned int: {} = {}", key, val);
      return 0U;
    }
  }

  template<>
  inline unsigned long ParameterServer::convert<unsigned long>(const std::string &val, const std::string &key) const {
    try {
      return std::stoul(val);
    } catch (...) {
      FINS_LOG_WARN("[ParameterServer] Failed to convert to unsigned long: {} = {}", key, val);
      return 0UL;
    }
  }

  template<>
  inline unsigned long long ParameterServer::convert<unsigned long long>(const std::string &val,
                                                                         const std::string &key) const {
    try {
      return std::stoull(val);
    } catch (...) {
      FINS_LOG_WARN("[ParameterServer] Failed to convert to unsigned long long: {} = {}", key, val);
      return 0ULL;
    }
  }

  template<>
  inline float ParameterServer::convert<float>(const std::string &val, const std::string &key) const {
    try {
      return std::stof(val);
    } catch (...) {
      FINS_LOG_WARN("[ParameterServer] Failed to convert to float: {} = {}", key, val);
      return 0.0f;
    }
  }

  template<>
  inline double ParameterServer::convert<double>(const std::string &val, const std::string &key) const {
    try {
      return std::stod(val);
    } catch (...) {
      FINS_LOG_WARN("[ParameterServer] Failed to convert to double: {} = {}", key, val);
      return 0.0;
    }
  }

  template<>
  inline std::vector<double> ParameterServer::convert<std::vector<double>>(const std::string &val,
                                                                           const std::string &key) const {
    std::vector<double> res;
    std::string s = val;

    size_t start = s.find('[');
    size_t end = s.find(']');
    if (start == std::string::npos || end == std::string::npos) {
      FINS_LOG_WARN("[ParameterServer] Invalid vector format: {}", key);
      return res;
    }
    s = s.substr(start + 1, end - start - 1);

    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
      std::string clean_item = trim(item);
      if (!clean_item.empty()) {
        try {
          res.push_back(std::stod(clean_item));
        } catch (...) {
        }
      }
    }
    return res;
  }

  template<>
  inline std::vector<int> ParameterServer::convert<std::vector<int>>(const std::string &val,
                                                                     const std::string &key) const {
    std::vector<int> res;
    std::string s = val;

    size_t start = s.find('[');
    size_t end = s.find(']');
    if (start == std::string::npos || end == std::string::npos) {
      FINS_LOG_WARN("[ParameterServer] Invalid vector format: {}", key);
      return res;
    }
    s = s.substr(start + 1, end - start - 1);

    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, ',')) {
      std::string clean_item = trim(item);
      if (!clean_item.empty()) {
        try {
          res.push_back(std::stoi(clean_item));
        } catch (...) {
          FINS_LOG_WARN("[ParameterServer] Failed to convert vector element to int: {}", clean_item);
        }
      }
    }
    return res;
  }

  inline ParameterServer &param_server() { return ParameterServer::get_instance(); }

  class ParamLoader {
  public:
    ParamLoader(const std::string &prefix = "") : prefix_(prefix) {
      if (!prefix_.empty() && prefix_.back() != '.') {
        prefix_ += ".";
      }
    }

    template<typename T>
    T get(const std::string &key, const T &default_val) const {
      return param_server().get<T>(prefix_ + key, default_val);
    }

    std::string get(const std::string &key, const char *default_val) const {
      return param_server().get<std::string>(prefix_ + key, std::string(default_val));
    }

    // Same as get but named load
    template<typename T>
    T load(const std::string &key, const T &default_val) const {
      return param_server().get<T>(prefix_ + key, default_val);
    }

    std::string load(const std::string &key, const char *default_val) const {
      return param_server().get<std::string>(prefix_ + key, std::string(default_val));
    }

    template<typename T>
    const ParamLoader &bind(const std::string &key, T &target, const T &default_val) const {
      target = param_server().get<T>(prefix_ + key, default_val);
      return *this;
    }

  private:
    std::string prefix_;
  };

} // namespace fins