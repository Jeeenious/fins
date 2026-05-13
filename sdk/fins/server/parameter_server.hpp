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

namespace fins {

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

  private:
    template<typename T>
    T get_impl(const std::string &key, const T *default_value_ptr) const {
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

  private:
    ParameterServer();
    ~ParameterServer() = default;

    std::map<std::string, std::string> params_;
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