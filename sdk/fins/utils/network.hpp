#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netinet/in.h>
#include <string>
#include <vector>

namespace fins {
  namespace utils {

    inline std::string get_local_ip() {
      struct ifaddrs *ifAddrStruct = nullptr;
      struct ifaddrs *ifa = nullptr;
      void *tmpAddrPtr = nullptr;
      std::string ip_address = "127.0.0.1";

      getifaddrs(&ifAddrStruct);

      for (ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next) {
        if (!ifa->ifa_addr)
          continue;

        if (ifa->ifa_addr->sa_family == AF_INET) {
          tmpAddrPtr = &((struct sockaddr_in *) ifa->ifa_addr)->sin_addr;
          char addressBuffer[INET_ADDRSTRLEN];
          inet_ntop(AF_INET, tmpAddrPtr, addressBuffer, INET_ADDRSTRLEN);

          std::string ifName = ifa->ifa_name;
          if (ifName != "lo") {
            ip_address = addressBuffer;
            break;
          }
        }
      }

      if (ifAddrStruct != nullptr)
        freeifaddrs(ifAddrStruct);
      return ip_address;
    }

  } // namespace utils
} // namespace fins