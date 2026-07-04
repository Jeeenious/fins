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
    std::string description;
    bool is_leaf = false;

    void insert(const std::string &full_key, const std::string &val, const std::string &type = "", const std::string &desc = "") {
      std::size_t pos = full_key.find('.');
      if (pos == std::string::npos) {
        if (children.find(full_key) == children.end()) {
          child_order.push_back(full_key);
        }
        children[full_key].value = val;
        children[full_key].type_info = type;
        children[full_key].description = desc;
        children[full_key].is_leaf = true;
      } else {
        std::string head = full_key.substr(0, pos);
        std::string tail = full_key.substr(pos + 1);
        if (children.find(head) == children.end()) {
          child_order.push_back(head);
        }
        children[head].insert(tail, val, type, desc);
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
          bool has_info = !node.type_info.empty() || !node.description.empty();
          if (has_info) {
            ss << " #";
            if (!node.type_info.empty()) ss << " type: " << node.type_info;
            if (!node.description.empty()) {
              if (!node.type_info.empty()) ss << ",";
              ss << " description: " << node.description;
            }
          }
          ss << "\n";
        } else {
          ss << "\n";
          node.dump(ss, indent + 1);
        }
      }
    }
  };

  /**
   * @brief 参数查询结果（流式 API） / Parameter query result (fluent API)
   * @details 通过 ParameterServer::get() 或 ParamLoader::get() 返回。
   *          支持链式调用设置约束：description、range、options 等。
   *          可隐式转换为 T 类型直接使用。
   *
   *          Returned by ParameterServer::get() or ParamLoader::get().
   *          Supports chained constraint settings: description, range, options, etc.
   *          Implicitly convertible to type T for direct use.
   *
   * @tparam T 参数类型 / Parameter type
   *
   * @par 示例 / Example
   * @code
   * fins::ParamLoader loader("MyNode");
   * double threshold = loader.get("threshold", 0.7)
   *     .with_description("检测阈值，范围 0-1 / Detection threshold, range 0-1")
   *     .within(0.0, 1.0);
   * @endcode
   */
  template<typename T>
  class ParamResult {
  public:
    ParamResult(const std::string &key, const T &val);

    operator T() const { return value_; }

    /**
     * @brief 设置参数描述 / Set parameter description
     * @param desc 描述文本 / Description text
     * @return 自身引用用于链式调用 / Self reference for chaining
     */
    ParamResult &with_description(const std::string &desc);

    /**
     * @brief 设置参数范围约束 / Set parameter range constraint
     * @param min_val 最小值 / Minimum value
     * @param max_val 最大值 / Maximum value
     * @return 自身引用用于链式调用 / Self reference for chaining
     */
    ParamResult &within(const T &min_val, const T &max_val);

    /**
     * @brief 设置参数上限 / Set parameter upper bound
     * @param max_val 最大值 / Maximum value
     * @return 自身引用用于链式调用 / Self reference for chaining
     */
    ParamResult &less_than(const T &max_val);

    /**
     * @brief 设置参数下限 / Set parameter lower bound
     * @param min_val 最小值 / Minimum value
     * @return 自身引用用于链式调用 / Self reference for chaining
     */
    ParamResult &greater_than(const T &min_val);

    /**
     * @brief 设置参数可选值列表 / Set parameter allowed values
     * @param options 可选值向量 / Vector of allowed values
     * @return 自身引用用于链式调用 / Self reference for chaining
     */
    ParamResult &one_of(const std::vector<T> &options);

    /**
     * @brief 标记参数须为整数 / Mark parameter must be integer
     * @return 自身引用用于链式调用 / Self reference for chaining
     */
    ParamResult &is_integer();

  private:
    std::string key_;
    T value_;
  };

  template<typename T>
  struct is_vector : std::false_type {};
  template<typename T, typename A>
  struct is_vector<std::vector<T, A>> : std::true_type {};

  /**
   * @brief 参数服务器（单例） / Parameter server (singleton)
   * @details 全局参数管理器，支持从 YAML 文件或字符串加载参数。
   *          通常通过 ParamLoader 间接使用，而非直接调用。
   *
   *          Global parameter manager supporting parameter loading from YAML files
   *          or strings. Typically used indirectly via ParamLoader.
   */
  class FINS_API ParameterServer {
  public:
    /// @brief 获取单例 / Get singleton instance
    static ParameterServer &get_instance();

    ParameterServer(const ParameterServer &) = delete;
    ParameterServer &operator=(const ParameterServer &) = delete;

    /**
     * @brief 从 YAML 字符串加载参数 / Load parameters from YAML string
     * @param str YAML 格式字符串 / YAML formatted string
     * @return 加载成功返回 true / true on success
     */
    bool load_string(const std::string &str);

    /**
     * @brief 从 YAML 文件加载参数 / Load parameters from YAML file
     * @param path 文件路径 / File path
     * @return 加载成功返回 true / true on success
     */
    bool load_file(const std::string &path);

    /**
     * @brief 查询参数（无默认值） / Query parameter (no default)
     * @tparam T 参数类型 / Parameter type
     * @param key 参数键（支持点分路径，如 "camera.exposure"） / Parameter key (dot-separated path, e.g. "camera.exposure")
     * @return ParamResult<T> 流式结果 / Fluent result
     */
    template<typename T>
    ParamResult<T> get(const std::string &key) const {
      return ParamResult<T>(key, get_impl<T>(key, nullptr));
    }

    /**
     * @brief 查询参数（带默认值） / Query parameter (with default)
     * @tparam T 参数类型 / Parameter type
     * @param key 参数键 / Parameter key
     * @param default_value 默认值 / Default value
     * @return ParamResult<T> 流式结果 / Fluent result
     */
    template<typename T>
    ParamResult<T> get(const std::string &key, const T &default_value) const {
      return ParamResult<T>(key, get_impl<T>(key, &default_value));
    }

    /// @brief 查询字符串参数（const char* 重载） / Query string parameter (const char* overload)
    ParamResult<std::string> get(const std::string &key, const char *default_value) const {
      std::string def_val(default_value ? default_value : "");
      return ParamResult<std::string>(key, get_impl<std::string>(key, &def_val));
    }

    void set_description(const std::string &key, const std::string &description) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (requested_entries_.find(key) != requested_entries_.end()) {
        requested_entries_[key].description = description;
      }
    }

    template<typename T>
    void set_min(const std::string &key, const T &min_val) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (requested_entries_.find(key) != requested_entries_.end()) {
        requested_entries_[key].min_val = to_raw_string(min_val);
        requested_entries_[key].has_min = true;
      }
    }

    template<typename T>
    void set_max(const std::string &key, const T &max_val) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (requested_entries_.find(key) != requested_entries_.end()) {
        requested_entries_[key].max_val = to_raw_string(max_val);
        requested_entries_[key].has_max = true;
      }
    }

    template<typename T>
    void set_options(const std::string &key, const std::vector<T> &options) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (requested_entries_.find(key) != requested_entries_.end()) {
        requested_entries_[key].options.clear();
        for (const auto &opt : options) {
          requested_entries_[key].options.push_back(to_raw_string(opt));
        }
      }
    }

    void set_is_integer(const std::string &key, bool is_int) {
      std::lock_guard<std::mutex> lock(mutex_);
      if (requested_entries_.find(key) != requested_entries_.end()) {
        requested_entries_[key].is_integer = is_int;
      }
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
          root.insert(key, it->second.default_val, it->second.type_name, it->second.description);
        }
      }
      std::stringstream ss;
      root.dump(ss);
      return ss.str();
    }

    std::string dump_template_json() const;

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
        } else {
          FINS_LOG_WARN("[ParameterServer] Parameter not found: {}. No default value provided! Returning default constructed value.", key);
          return T();
        }
      }
      return convert<T>(it->second, key);
    }

    struct ParamRequirement {
      std::string default_val;
      std::string type_name;
      std::string description;
      std::string min_val;
      std::string max_val;
      std::vector<std::string> options;
      bool has_min = false;
      bool has_max = false;
      bool is_integer = false;
    };
    mutable std::map<std::string, ParamRequirement> requested_entries_;
    mutable std::vector<std::string> requested_order_;

    template<typename T>
    void record_requirement(const std::string &key, const T &default_val) const {
      std::lock_guard<std::mutex> lock(mutex_);
      if (requested_entries_.find(key) == requested_entries_.end()) {
        requested_entries_[key] = { to_raw_string(default_val), FINS_TYPE_REGISTER.get_name<T>(), "", "", "", {}};
        requested_order_.push_back(key);
      }
    }

    template<typename T>
    std::string to_raw_string(const T &val) const {
      if constexpr (std::is_same_v<T, std::string>) return val;
      if constexpr (std::is_same_v<T, const char*>) return val ? std::string(val) : std::string("");
      else if constexpr (std::is_same_v<T, bool>) return val ? "true" : "false";
      else if constexpr (is_vector<T>::value) {
        using namespace std; 
        return to_string(val);
      }
      else if constexpr (std::is_floating_point_v<T>) {
        return fmt::format("{:g}", val);
      } else {
        return fmt::format("{}", val);
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

  template<typename T>
  ParamResult<T>::ParamResult(const std::string &key, const T &val) : key_(key), value_(val) {}

  template<typename T>
  ParamResult<T> &ParamResult<T>::with_description(const std::string &desc) {
    ParameterServer::get_instance().set_description(key_, desc);
    return *this;
  }

  template<typename T>
  ParamResult<T> &ParamResult<T>::within(const T &min_val, const T &max_val) {
    if (value_ < min_val || value_ > max_val) {
      FINS_LOG_WARN("[ParameterServer] Parameter '{}' value {} is out of range [{}, {}]", key_, value_, min_val, max_val);
    }
    ParameterServer::get_instance().set_min(key_, min_val);
    ParameterServer::get_instance().set_max(key_, max_val);
    return *this;
  }

  template<typename T>
  ParamResult<T> &ParamResult<T>::less_than(const T &max_val) {
    if (value_ > max_val) {
      FINS_LOG_WARN("[ParameterServer] Parameter '{}' value {} is greater than {}", key_, value_, max_val);
    }
    ParameterServer::get_instance().set_max(key_, max_val);
    return *this;
  }

  template<typename T>
  ParamResult<T> &ParamResult<T>::greater_than(const T &min_val) {
    if (value_ < min_val) {
      FINS_LOG_WARN("[ParameterServer] Parameter '{}' value {} is less than {}", key_, value_, min_val);
    }
    ParameterServer::get_instance().set_min(key_, min_val);
    return *this;
  }

  template<typename T>
  ParamResult<T> &ParamResult<T>::one_of(const std::vector<T> &options) {
    if (std::find(options.begin(), options.end(), value_) == options.end()) {
      std::stringstream ss;
      ss << "[";
      for (size_t i = 0; i < options.size(); ++i) {
        ss << options[i] << (i == options.size() - 1 ? "" : ", ");
      }
      ss << "]";
      FINS_LOG_WARN("[ParameterServer] Parameter '{}' value {} is not one of {}", key_, value_, ss.str());
    }
    ParameterServer::get_instance().set_options(key_, options);
    return *this;
  }

  template<typename T>
  ParamResult<T> &ParamResult<T>::is_integer() {
    if constexpr (!std::is_integral_v<T>) {
      FINS_LOG_WARN("[ParameterServer] Parameter '{}' is checked for is_integer() but its type is not integral", key_);
    }
    ParameterServer::get_instance().set_is_integer(key_, true);
    return *this;
  }

  inline ParameterServer &param_server() { return ParameterServer::get_instance(); }

  /**
   * @brief 参数加载器 / Parameter loader
   * @details 带命名空间前缀的参数访问器，避免键名冲突。
   *          构造函数传入的 prefix 会自动追加到所有查询键的前面。
   *
   *          Namespace-prefixed parameter accessor to avoid key name collisions.
   *          The prefix passed to the constructor is automatically prepended to
   *          all query keys.
   *
   * @par 示例 / Example
   * @code
   * fins::ParamLoader loader("MyNodeCategory");
   * double conf = loader.get("confidence", 0.5)
   *     .with_description("置信度阈值 / Confidence threshold")
   *     .within(0.0, 1.0);
   * @endcode
   */
  class ParamLoader {
  public:
    /**
     * @brief 构造参数加载器 / Construct parameter loader
     * @param prefix 命名空间前缀（自动追加 "."） / Namespace prefix (auto-appends ".")
     */
    ParamLoader(const std::string &prefix = "") : prefix_(prefix) {
      if (!prefix_.empty() && prefix_.back() != '.') {
        prefix_ += ".";
      }
    }

    /**
     * @brief 查询参数（无默认值，带前缀） / Query parameter (no default, prefixed)
     * @tparam T 参数类型 / Parameter type
     * @param key 参数键（相对前缀） / Parameter key (relative to prefix)
     * @return ParamResult<T>
     */
    template<typename T>
    ParamResult<T> get(const std::string &key) const {
      return param_server().get<T>(prefix_ + key);
    }

    /**
     * @brief 查询参数（带默认值，带前缀） / Query parameter (with default, prefixed)
     * @tparam T 参数类型 / Parameter type
     * @param key 参数键 / Parameter key
     * @param default_val 默认值 / Default value
     * @return ParamResult<T>
     */
    template<typename T>
    ParamResult<T> get(const std::string &key, const T &default_val) const {
      return param_server().get<T>(prefix_ + key, default_val);
    }

    /// @brief 查询字符串参数（const char* 重载，带前缀） / Query string parameter (const char* overload, prefixed)
    ParamResult<std::string> get(const std::string &key, const char *default_val) const {
      return param_server().get(prefix_ + key, default_val);
    }

    /**
     * @brief 加载参数（无默认值，get 的别名） / Load parameter (no default, alias for get)
     * @tparam T 参数类型 / Parameter type
     * @param key 参数键 / Parameter key
     * @return ParamResult<T>
     */
    template<typename T>
    ParamResult<T> load(const std::string &key) const {
      return param_server().get<T>(prefix_ + key);
    }

    /**
     * @brief 加载参数（带默认值，get 的别名） / Load parameter (with default, alias for get)
     * @tparam T 参数类型 / Parameter type
     * @param key 参数键 / Parameter key
     * @param default_val 默认值 / Default value
     * @return ParamResult<T>
     */
    template<typename T>
    ParamResult<T> load(const std::string &key, const T &default_val) const {
      return param_server().get<T>(prefix_ + key, default_val);
    }

    /// @brief 加载字符串参数（const char* 重载） / Load string parameter (const char* overload)
    ParamResult<std::string> load(const std::string &key, const char *default_val) const {
      return param_server().get(prefix_ + key, default_val);
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