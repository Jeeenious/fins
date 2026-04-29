/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// function_tags.hpp

#pragma once

namespace fins {

  template<typename T>
  struct Input {
    const T &value;
    const Msg<T> &msg;
    Input(const Msg<T> &m) : value(*m.data), msg(m) {}
    const T *operator->() const { return &value; }
    const T &operator*() const { return value; }
    operator const T &() const { return value; }
    std::shared_ptr<const T> ptr() const { return std::const_pointer_cast<const T>(msg.data); }
  };

  template<typename T>
  struct Output {
    T value{};

    T *operator->() { return &value; }
    T &operator*() { return value; }
    
    template<typename U>
    Output& operator=(U&& new_val) {
      if constexpr (std::is_assignable_v<T&, U>) {
        value = std::forward<U>(new_val);
      } else {
        value.reset(new_val.get());
      }
      return *this;
    }

    Output& operator=(const T& new_val) {
      value = new_val;
      return *this;
    }
    Output& operator=(T&& new_val) {
      value = std::move(new_val);
      return *this;
    }
  };

  template<typename T>
  struct Parameter {
    const T &value;
    Parameter(const T &v) : value(v) {}

    operator const T &() const { return value; }
    const T &operator*() const { return value; }
    const T *operator->() const { return &value; }
  };

  template<typename T>
  struct function_traits;

  template<typename ClassType, typename ReturnType, typename... Args>
  struct function_traits<ReturnType (ClassType::*)(Args...) const> {
    using args_tuple = std::tuple<Args...>;
  };

  template<typename T>
  struct strip_wrapper;
  template<typename T>
  struct strip_wrapper<Input<T> &> {
    using type = T;
  };
  template<typename T>
  struct strip_wrapper<const Input<T> &> {
    using type = T;
  };
  template<typename T>
  struct strip_wrapper<Output<T> &> {
    using type = T;
  };
  template<typename T>
  struct strip_wrapper<Parameter<T> &> {
    using type = T;
  };
  template<typename T>
  struct strip_wrapper<const Parameter<T> &> {
    using type = T;
  };

  template<typename T>
  using strip_wrapper_t = typename strip_wrapper<T>::type;

  template<typename T>
  struct is_parameter : std::false_type {};
  template<typename T>
  struct is_parameter<Parameter<T> &> : std::true_type {};
  template<typename T>
  struct is_parameter<const Parameter<T> &> : std::true_type {};

  template<typename T>
  struct is_output : std::false_type {};
  template<typename T>
  struct is_output<Output<T> &> : std::true_type {};

} // namespace fins