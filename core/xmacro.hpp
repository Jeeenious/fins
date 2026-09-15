/*******************************************************************************
 * xmacro.hpp — 算法插件导出宏（X-macro：FINS_ALGO_LIST + FINS_ALGO_EXPORT）
 *
 * 内部逻辑：
 *   用户算法插件 .so 必须导出 5 个 C 工厂符号（get_plugin_count / get_algo_name /
 *   get_algo_version / create_algo / destroy_plugin）——Plugin 构造 dlopen + dlsym 解析
 *   这 5 个符号并枚举填 loaded_keys = {name}:{version}，agent(运行时)按 [name:version]
 *   定位后调 create_algo(key) 生成 AlgoBase 实例。本头用 X-macro 自动生成这 5 个符号。
 *   AlgoFunc 封装见 algo_func.hpp（configs-first 参数布局契约：配置段 + 输入段 + 输出段）。
 *
 * 元数据(算法/参数的名字、方向 cfg|in|out、说明)——刻意不进 .so，保证插件 .so 精简。
 * 由插件作者另维护一份同名的 sidecar JSON（如 plugin.so → plugin.meta.json），供
 * viewer / webui 可视化编辑使用（后续由 UI 上传分发），格式：
 *   {
 *     "my_algo": {
 *       "desc": "转发：输入乘增益",
 *       "params": [
 *         { "name": "gain", "role": "cfg", "type": "int",  "desc": "增益(配置)" },
 *         { "name": "in",   "role": "in",  "type": "int",  "desc": "输入值(数据)" },
 *         { "name": "out",  "role": "out", "type": "int&", "desc": "输出结果" }
 *       ]
 *     }, ...
 *   }
 *   role ∈ cfg|in|out；params 顺序 = 函数签名参数顺序（配置段 → 输入段 → 输出段）。
 *
 * 用法（插件源码文件，每文件须恰好调用一次 FINS_ALGO_EXPORT）：
 *   #include "xmacro.hpp"
 *   void my_algo(int gain, int in, int &out) { out = in * gain; }
 *   #define FINS_ALGO_LIST(F) F(my_algo)
 *   FINS_ALGO_EXPORT("1.0.0")
 *
 * 对外接口：
 *   FINS_ALGO_LIST(F)  — 用户定义宏：元素 = F(函数名)
 *   FINS_ALGO_EXPORT(version) — 5 个 C 工厂符号（dlopen/dlsym 可解析）
 ******************************************************************************/
#pragma once

#include <string>

#include "algo/algo_base.hpp"
#include "algo/algo_func.hpp"
#include "mesg/mesg.hpp"

// 元素处理器（FINS_ALGO_LIST 的 F）
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
