/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// action/action_tags.hpp

#pragma once

#include <tuple>

namespace fins {

  enum class ActionState {
    Accepted,   // 目标已接受
    Executing,  // 正在执行
    Succeeded,  // 成功完成
    Canceled,   // 已取消
    Canceling,  // 正在取消
    Aborted     // 已中止
  };

  template<typename... Ts>
  struct Goal {
    using types = std::tuple<Ts...>;
  };

  template<typename... Ts>
  struct Feedback {
    using types = std::tuple<Ts...>;
  };

} // namespace fins
