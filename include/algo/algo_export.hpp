/*******************************************************************************
 * algo_export.hpp — 算法插件导出宏（FINS_ALGO_LIST + FINS_ALGO_EXPORT）
 *
 * 内部逻辑：
 *   用户算法插件 .so 必须导出 5 个 C 工厂符号（get_plugin_count / get_algo_name /
 *   get_algo_version / create_algo / destroy_plugin）——Plugin 构造 dlopen + dlsym 解析
 *   这 5 个符号并枚举填 loaded_keys = {name}:{version}，expand_hp ④ 按 JSON [name:version]
 *   定位后调 create_algo(key) 生成 AlgoBase 实例。本头用 **X-macro** 自动生成这 5 个符号，
 *   免去手动样板（名字表 + 计数 + 分发 if 链）：
 *     FINS_ALGO_LIST(F)   — 用户定义的算法列表（元素 = F(函数名)，算法名 = 函数名
 *                           字符串化 #f），F 为内部注入的元素处理器
 *     FINS_ALGO_EXPORT(version) — 展开为 extern "C" 的 5 个 C 符号；create_algo 按
 *       [函数名:version] 分发 `new AlgoFunc<decltype(&f)>(f)`（版本经局部 ver 字符串
 *       运行时拼接，处理器宏不直接携带 version 参数——宏参数穿不透 X-macro 列表）。
 *   AlgoFunc 封装见 algo_func.hpp（configs-first 参数布局契约：配置段 + 输入段 + 输出段）。
 *
 * 用法（插件源码文件，每文件须恰好调用一次 FINS_ALGO_EXPORT）：
 *   #include "algo/algo_export.hpp"
 *   void my_algo(int gain, int in, int &out) { out = in * gain; }   // configs-first 签名
 *   void other_algo(int seed, int &out) { out = seed; }
 *   #define FINS_ALGO_LIST(F) \
 *     F(my_algo)             \
 *     F(other_algo)
 *   FINS_ALGO_EXPORT("1.0.0")
 *
 * 资源消耗：
 *   每插件 1 份静态名字表（每算法 1 个 const char*）+ create_algo 每次调用
 *   O(算法数) 次短字符串拼接（分发键比较）。
 *
 * 对外接口：
 *   FINS_ALGO_LIST(F)  — 用户定义宏：元素 = F(函数名)；F 由本头注入（名字表/计数/分发）
 *   FINS_ALGO_EXPORT(version) — 生成 5 个 C 工厂符号（C ABI，dlopen/dlsym 可解析，
 *                               与 Plugin 构造的 5 个 dlsym 符号一一对应）
 ******************************************************************************/
#pragma once

#include <string>

#include "algo_base.hpp"
#include "algo_func.hpp"
#include "../mesg/mesg.hpp"

// 元素处理器（FINS_ALGO_LIST 的 F）：
//   名字表项 / 计数项 / create_algo 分发项（ver = create_algo 局部版本串，见下）
#define FINS_ALGO_NAME_(f)   #f,
#define FINS_ALGO_COUNT_(f)  + 1
#define FINS_ALGO_CREATE_(f) \
  if (k == std::string(#f) + ":" + ver) return new fins::rt::AlgoFunc<decltype(&f)>(f);

#define FINS_ALGO_EXPORT(version) \
  extern "C" { \
    static const char *const kFinsAlgoNames[] = { FINS_ALGO_LIST(FINS_ALGO_NAME_) }; \
    static constexpr int kFinsAlgoCount = 0 FINS_ALGO_LIST(FINS_ALGO_COUNT_); \
    int get_plugin_count() { return kFinsAlgoCount; } \
    const char *get_algo_name(int i) { \
      return (i >= 0 && i < kFinsAlgoCount) ? kFinsAlgoNames[i] : ""; \
    } \
    const char *get_algo_version(int) { return version; } \
    fins::rt::AlgoBase *create_algo(const char *key) { \
      const std::string k(key); \
      const std::string ver(version); \
      FINS_ALGO_LIST(FINS_ALGO_CREATE_) \
      return nullptr; \
    } \
    void destroy_plugin(fins::rt::AlgoBase *p) { delete p; } \
  }
