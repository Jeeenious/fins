/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// utils/fs.hpp

#pragma once

#include <string>
#include <cstdlib>
#include <filesystem>

namespace fins {

  namespace fs = std::filesystem;

  /**
   * @brief Expands the tilde (~) in a path string to the home directory.
   * 
   * @param path The path string to expand.
   * @return std::string The expanded path.
   */
  inline std::string expand_user(const std::string &path) {
    if (path.empty() || path[0] != '~') {
      return path;
    }

    const char *home = std::getenv("HOME");
    if (!home) {
      return path;
    }

    if (path.size() == 1) {
      return std::string(home);
    }

    if (path[1] == '/' || path[1] == '\\') {
      return std::string(home) + path.substr(1);
    }

    // ~user is not supported, just return as is or handle if needed
    return path;
  }

} // namespace fins
