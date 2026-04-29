/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 *******************************************************************************/

// type_register.hpp

#pragma once

#include <any>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <unordered_map>
#include <vector>
#include <fins/macros.hpp>

namespace fins {

  template<typename T>
  struct is_tuple : std::false_type {};
  template<typename... Args>
  struct is_tuple<std::tuple<Args...>> : std::true_type {};

  class FINS_API TypeRegister {
  public:
    static TypeRegister &instance();

    TypeRegister(const TypeRegister &) = delete;
    TypeRegister &operator=(const TypeRegister &) = delete;

    template<typename T>
    TypeRegister &register_type(std::string name) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto type_idx = std::type_index(typeid(T));
      entries_[type_idx].name = std::move(name);
      return *this;
    }

    template<typename T>
    [[nodiscard]] std::string get_name() const {
      if constexpr (is_tuple<T>::value) {
        return get_tuple_name_impl<T>(std::make_index_sequence<std::tuple_size_v<T>>{});
      }
      std::lock_guard<std::mutex> lock(mutex_);
      auto type_idx = std::type_index(typeid(T));
      auto it = entries_.find(type_idx);
      if (it != entries_.end() && !it->second.name.empty()) {
        return it->second.name;
      }
      return demangle(typeid(T).name());
    }

    template<typename T, typename Func>
    TypeRegister &string_convert_func(Func &&func) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto type_idx = std::type_index(typeid(T));
      entries_[type_idx].string_converter = [fn = std::forward<Func>(func)](const std::string &payload) -> std::any {
        return std::any(fn(payload));
      };
      return *this;
    }

    template<typename T>
    T string_convert(const std::string &str) {
      std::lock_guard<std::mutex> lock(mutex_);
      auto type_idx = std::type_index(typeid(T));
      auto it = entries_.find(type_idx);
      if (it != entries_.end() && it->second.string_converter) {
        try {
          return std::any_cast<T>(it->second.string_converter(str));
        } catch (const std::exception &e) {
          throw std::runtime_error("Conversion failed for " + get_name<T>() + ": " + std::string(e.what()));
        }
      }
      throw std::runtime_error("No converter for: " + demangle(typeid(T).name()));
    }

  private:
    TypeRegister();
    void register_basic_types();
    std::string demangle(const char *name) const;

    template<typename Tuple, std::size_t... Is>
    std::string get_tuple_name_impl(std::index_sequence<Is...>) const {
      std::string name = "tuple<";
      auto append = [&](const std::string &s, bool is_first) {
        if (!is_first)
          name += ", ";
        name += s;
      };
      ((append(this->get_name<std::tuple_element_t<Is, Tuple>>(), Is == 0)), ...);
      name += ">";
      return name;
    }

    struct Entry {
      std::string name;
      std::function<std::any(const std::string &)> string_converter;
    };

    mutable std::mutex mutex_;
    std::unordered_map<std::type_index, Entry> entries_;
  };

#define FINS_TYPE_REGISTER fins::TypeRegister::instance()

} // namespace fins