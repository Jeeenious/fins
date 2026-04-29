/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 *******************************************************************************/

// type_register.cpp

#include "type_register.hpp"
#include <algorithm>
#include <iostream>
#include <sstream>
#include <vector>

#if defined(__GNUC__) || defined(__clang__)
#include <cxxabi.h>
#endif

namespace fins {

  TypeRegister &TypeRegister::instance() {
    static TypeRegister registry;
    return registry;
  }

  TypeRegister::TypeRegister() { register_basic_types(); }

  std::string TypeRegister::demangle(const char *name) const {
#if defined(__GNUC__) || defined(__clang__)
    int status = -1;
    std::unique_ptr<char, void (*)(void *)> res{abi::__cxa_demangle(name, NULL, NULL, &status), std::free};
    return (status == 0) ? res.get() : name;
#else
    return name;
#endif
  }

  void TypeRegister::register_basic_types() {
    // String
    register_type<std::string>("string");
    string_convert_func<std::string>([](const std::string &s) { return s; });

    // Int
    register_type<int>("int");
    string_convert_func<int>([](const std::string &s) { return std::stoi(s); });

    // Float
    register_type<float>("float");
    string_convert_func<float>([](const std::string &s) { return std::stof(s); });

    // Double
    register_type<double>("double");
    string_convert_func<double>([](const std::string &s) { return std::stod(s); });

    // Bool
    register_type<bool>("bool");
    string_convert_func<bool>([](const std::string &s) {
      std::string v = s;
      std::transform(v.begin(), v.end(), v.begin(), ::tolower);
      return (v == "true" || v == "1" || v == "on");
    });

    // Long long
    register_type<long long>("long long");
    string_convert_func<long long>([](const std::string &s) { return std::stoll(s); });

    // Unsigned long
    register_type<unsigned long>("unsigned long");
    string_convert_func<unsigned long>([](const std::string &s) { return std::stoul(s); });

    // Unsigned long long
    register_type<unsigned long long>("unsigned long long");
    string_convert_func<unsigned long long>([](const std::string &s) { return std::stoull(s); });

    // Vector of double
    register_type<std::vector<double>>("vector double");
    string_convert_func<std::vector<double>>([](const std::string &s) {
      std::vector<double> result;
      std::string temp = s;
      // Replace common delimiters with space
      for (char &c: temp) {
        if (c == ',' || c == '[' || c == ']')
          c = ' ';
      }
      std::stringstream ss(temp);
      double val;
      while (ss >> val) {
        result.push_back(val);
      }
      return result;
    });
  }

} // namespace fins
