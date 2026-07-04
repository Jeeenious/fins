/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// service/service_traits.hpp

#pragma once

#include <fins/service/service_tags.hpp>
#include <tuple>
#include <type_traits>

namespace fins {

  template<typename T>
  struct is_request : std::false_type {};
  template<typename... Ts>
  struct is_request<Request<Ts...>> : std::true_type {};

  template<typename T>
  struct is_response : std::false_type {};
  template<typename... Ts>
  struct is_response<Response<Ts...>> : std::true_type {};

  template<typename T>
  struct unwrap_type {
    using type = T;
  };
  template<typename... Ts>
  struct unwrap_type<Request<Ts...>> {
    using type = std::tuple<Ts...>;
  };
  template<typename... Ts>
  struct unwrap_type<Response<Ts...>> {
    using type = std::tuple<Ts...>;
  };
  template<typename T>
  using unwrap_type_t = typename unwrap_type<T>::type;

  template<template<typename> class Predicate, typename... Args>
  struct filter_types {
    using type = std::tuple<>;
  };

  template<template<typename> class Predicate, typename Head, typename... Tail>
  struct filter_types<Predicate, Head, Tail...> {
    using HeadTuple = std::conditional_t<Predicate<Head>::value, unwrap_type_t<Head>, std::tuple<>>;
    using TailTuple = typename filter_types<Predicate, Tail...>::type;
    using type = decltype(std::tuple_cat(std::declval<HeadTuple>(), std::declval<TailTuple>()));
  };

  template<typename Tuple>
  struct service_return_helper {
    using type = void;
  };

  template<typename T>
  struct service_return_helper<std::tuple<T>> {
    using type = T;
  };

  template<typename... Ts>
  struct service_return_helper<std::tuple<Ts...>> {
    using type = std::tuple<Ts...>;
  };


  template<typename... Args>
  struct ServiceTraits {
    using InputTuple = typename filter_types<is_request, Args...>::type;
    using OutputTuple = typename filter_types<is_response, Args...>::type;

    using ReturnType = typename service_return_helper<OutputTuple>::type;
  };

  template<typename Ret, typename... Args>
  struct ServiceTraits<Ret(Args...)> {
    using InputTuple = std::tuple<Args...>;
    using OutputTuple = std::conditional_t<std::is_void_v<Ret>, std::tuple<>, std::tuple<Ret>>;
    using ReturnType = Ret;
  };

} // namespace fins