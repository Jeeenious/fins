/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// service/service_tags.hpp

#pragma once

#include <tuple>

namespace fins {
  template<typename... Ts>
  struct Request {
    using types = std::tuple<Ts...>;
  };
  template<typename... Ts>
  struct Response {
    using types = std::tuple<Ts...>;
  };
} // namespace fins