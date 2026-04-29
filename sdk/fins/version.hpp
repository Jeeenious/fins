#pragma once

#define FINS_VERSION_MAJOR 0
#define FINS_VERSION_MINOR 1
#define FINS_VERSION_PATCH 0

#define FINS_VERSION_STRING "0.1.0"

namespace fins {

  struct Version {
    static constexpr int major = FINS_VERSION_MAJOR;
    static constexpr int minor = FINS_VERSION_MINOR;
    static constexpr int patch = FINS_VERSION_PATCH;
    static constexpr const char *string = FINS_VERSION_STRING;
  };

} // namespace fins
