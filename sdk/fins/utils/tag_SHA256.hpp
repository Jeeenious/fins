/*******************************************************************************
 * Copyright (c) 2026.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/
#pragma once

#include <fstream>
#include <iomanip>
#include <openssl/evp.h>
#include <sstream>

namespace fins::util {
  /**
   * @brief
   */
  inline std::string calculate_file_sha256(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    if (!file) throw std::runtime_error("Cannot open file: " + path);

    // 1. 创建上下文对象
    EVP_MD_CTX* context = EVP_MD_CTX_new();
    // 2. 初始化：使用 SHA256 算法
    EVP_DigestInit_ex(context, EVP_sha256(), NULL);

    char buffer[1024 * 4];
    while (file.read(buffer, sizeof(buffer)) || file.gcount()) {
      // 3. 更新哈希
      EVP_DigestUpdate(context, buffer, file.gcount());
    }

    unsigned char result[EVP_MAX_MD_SIZE];
    unsigned int length = 0;
    // 4. 完成哈希计算
    EVP_DigestFinal_ex(context, result, &length);
    // 5. 销毁上下文
    EVP_MD_CTX_free(context);

    std::stringstream ss;
    for(unsigned int i = 0; i < length; ++i)
      ss << std::hex << std::setw(2) << std::setfill('0') << (int)result[i];
    return ss.str();
  }

}