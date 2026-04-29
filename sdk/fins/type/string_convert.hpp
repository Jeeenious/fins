/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 *******************************************************************************/

// string_convert.hpp

#pragma once

#include <sstream>
#include <string>
#include <vector>

namespace std {

  inline std::string to_string(const std::string &str) { return str; }

  template<typename T>
  inline std::string to_string(const std::vector<T> &vec) {
    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < vec.size(); ++i) {
      ss << vec[i];
      if (i < vec.size() - 1) {
        ss << ", ";
      }
    }
    ss << "]";
    return ss.str();
  }

} // namespace std
