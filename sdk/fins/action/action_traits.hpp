/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// action/action_traits.hpp

#pragma once

#include <fins/action/action_tags.hpp>
#include <tuple>
#include <type_traits>

namespace fins {

  template<typename T>
  struct is_goal : std::false_type {};
  
  template<typename... Ts>
  struct is_goal<Goal<Ts...>> : std::true_type {};

  template<typename T>
  struct is_feedback : std::false_type {};
  
  template<typename... Ts>
  struct is_feedback<Feedback<Ts...>> : std::true_type {};

  template<typename T>
  struct unwrap_action_type {
    using type = T;
  };
  
  template<typename... Ts>
  struct unwrap_action_type<Goal<Ts...>> {
    using type = std::tuple<Ts...>;
  };
  
  template<typename... Ts>
  struct unwrap_action_type<Feedback<Ts...>> {
    using type = std::tuple<Ts...>;
  };
  
  template<typename T>
  using unwrap_action_type_t = typename unwrap_action_type<T>::type;

  template<template<typename> class Predicate, typename... Args>
  struct filter_action_types {
    using type = std::tuple<>;
  };

  template<template<typename> class Predicate, typename Head, typename... Tail>
  struct filter_action_types<Predicate, Head, Tail...> {
    using HeadTuple = std::conditional_t<Predicate<Head>::value, unwrap_action_type_t<Head>, std::tuple<>>;
    using TailTuple = typename filter_action_types<Predicate, Tail...>::type;
    using type = decltype(std::tuple_cat(std::declval<HeadTuple>(), std::declval<TailTuple>()));
  };

  // Action Traits
  template<typename... Args>
  struct ActionTraits {
    using GoalTuple = typename filter_action_types<is_goal, Args...>::type;
    using FeedbackTuple = typename filter_action_types<is_feedback, Args...>::type;
  };

} // namespace fins
