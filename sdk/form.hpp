/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/
#pragma once

#include <tbb/concurrent_hash_map.h>
#include <tbb/concurrent_queue.h>

namespace fins::util {

  template<typename T>
  using sptr = std::shared_ptr<T>;

  //=======================================

  template<typename T>
  using TBBMap = tbb::concurrent_hash_map<std::string, T>;

  // 读一份拷贝到 out，返回 bool（找到=true）
#define TBBMAP_GET(MAP, key, out)                                       \
  ([&] {                                                                \
    using _M = std::decay_t<decltype(MAP)>;                             \
    const auto &_k = (key);                                             \
    _M::const_accessor a;                                               \
    bool ok = (MAP).find(a, _k);                                        \
    if (ok)                                                             \
      (out) = a->second;                                                \
    FINS_LOG_DEBUG("[TBBMap] GET '{}' -> {}", _k, ok ? "hit" : "miss"); \
    return ok;                                                          \
  }())

  // 写入/覆盖（不存在则插入）
#define TBBMAP_SET(MAP, key, val)                                                  \
  do {                                                                             \
    using _M = std::decay_t<decltype(MAP)>;                                        \
    const auto &_k = (key);                                                        \
    _M::accessor a;                                                                \
    bool is_new = (MAP).insert(a, _k);                                             \
    a->second = (val);                                                             \
    FINS_LOG_DEBUG("[TBBMap] SET '{}' ({})", _k, is_new ? "insert" : "overwrite"); \
  } while (0)

  // 是否存在
#define TBBMAP_HAS(MAP, key)                           \
  ([&] {                                               \
    using _M = std::decay_t<decltype(MAP)>;            \
    const auto &_k = (key);                            \
    _M::const_accessor a;                              \
    bool ok = (MAP).find(a, _k);                       \
    FINS_LOG_DEBUG("[TBBMap] HAS '{}' -> {}", _k, ok); \
    return ok;                                         \
  }())

  // 删除
#define TBBMAP_ERASE(MAP, key)                                                  \
  ([&] {                                                                        \
    const auto &_k = (key);                                                     \
    bool ok = (MAP).erase(_k);                                                  \
    FINS_LOG_DEBUG("[TBBMap] ERASE '{}' -> {}", _k, ok ? "removed" : "absent"); \
    return ok;                                                                  \
  }())

#define TBBMAP_AT(MAP, key)                                                   \
  ([&]() -> typename std::decay_t<decltype(MAP)>::mapped_type {               \
    using Map = std::decay_t<decltype(MAP)>;                                  \
    Map::const_accessor a;                                                    \
    if ((MAP).find(a, (key))) {                                               \
      FINS_LOG_DEBUG("[TBBMap] AT '{}' -> hit", (key));                       \
      return a->second;                                                       \
    } else {                                                                  \
      FINS_LOG_DEBUG("[TBBMap] AT '{}' -> miss, throwing", (key));            \
      throw std::out_of_range("TBBMap key not found: " + std::string((key))); \
    }                                                                         \
  }())

  //=======================================

  template<typename T>
  using TBBQueue = tbb::concurrent_queue<T>;

  // 入队（拷贝或移动都行）
#define TBBQ_PUSH(Q, val)              \
  do {                                 \
    (Q).push(val);                     \
    FINS_LOG_DEBUG("[TBBQueue] PUSH"); \
  } while (0)

  // 出队到 out，返回 bool（空=false）
#define TBBQ_POP(Q, out)                                         \
  ([&] {                                                         \
    bool ok = (Q).try_pop(out);                                  \
    FINS_LOG_DEBUG("[TBBQueue] POP -> {}", ok ? "ok" : "empty"); \
    return ok;                                                   \
  }())

  // 近似判空
#define TBBQ_EMPTY(Q) ((Q).empty())

} // namespace fins::rt