/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// action/action.hpp

#pragma once

#include <fins/action/action_manager.hpp>
#include <fins/action/action_session.hpp>
#include <fins/action/action_tags.hpp>
#include <fins/action/action_traits.hpp>
#include <sstream>
#include <typeinfo>

namespace fins {

  // Helper function to convert tuple types to string
  template<typename Tuple, std::size_t... Is>
  inline std::string action_tuple_types_to_string_impl(std::index_sequence<Is...>) {
    std::stringstream ss;
    ((ss << (Is == 0 ? "" : ", ") << typeid(std::tuple_element_t<Is, Tuple>).name()), ...);
    return ss.str();
  }

  template<typename Tuple>
  inline std::string action_tuple_types_to_string() {
    return action_tuple_types_to_string_impl<Tuple>(std::make_index_sequence<std::tuple_size_v<Tuple>>{});
  }

} // namespace fins
