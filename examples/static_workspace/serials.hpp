/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// serials.hpp

#pragma once
#include <fcntl.h>
#include <fins/utils/logger.hpp>
#include <string>
#include <termios.h>
#include <unistd.h>
#include <vector>

namespace fins {
  class SerialPort {
  public:
    SerialPort() : fd_(-1) {}
    ~SerialPort() { close_port(); }

    bool open_port(const std::string &device, int baudrate) {
      fd_ = open(device.c_str(), O_RDWR | O_NOCTTY | O_NDELAY);
      if (fd_ == -1)
        return false;

      struct termios options;
      tcgetattr(fd_, &options);

      speed_t speed;
      switch (baudrate) {
        case 115200:
          speed = B115200;
          break;
        case 9600:
          speed = B9600;
          break;
        default:
          speed = B115200;
          break;
      }
      cfsetispeed(&options, speed);
      cfsetospeed(&options, speed);

      options.c_cflag &= ~PARENB;
      options.c_cflag &= ~CSTOPB;
      options.c_cflag &= ~CSIZE;
      options.c_cflag |= CS8;

      options.c_lflag &= ~(ICANON | ECHO | ECHOE | ISIG);
      options.c_oflag &= ~OPOST;
      options.c_iflag &= ~(IXON | IXOFF | IXANY | ICRNL);

      tcsetattr(fd_, TCSANOW, &options);
      return true;
    }

    int read_data(uint8_t *buffer, int size) {
      if (fd_ == -1)
        return -1;
      return read(fd_, buffer, size);
    }

    void close_port() {
      if (fd_ != -1) {
        close(fd_);
        fd_ = -1;
      }
    }

  private:
    int fd_;
  };
} // namespace fins