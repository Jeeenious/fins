/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/
#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>
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
    (void)is_new;  /* release 下 FINS_LOG_DEBUG 为空 → 防 unused 告警 */           \
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

  // ==========================================================================
  // DoubleBuff — 双缓冲（单写多读，原子切换）
  // ==========================================================================
  //
  // 内部逻辑：
  //   两块同型缓冲 + 1 个原子索引。写方 write() 拿非激活份填充，commit() 翻转索引
  //   原子切换；读方 read() 始终读激活份。核心保证：读方要么看到旧版要么看到新版，
  //   绝不看到写一半的内容——写方只改非激活份，激活份在 commit 前不被触碰。
  //   典型用途：配置热更新（写方线程填新配置 → commit；读方读当前生效配置）。
  //
  // 线程模型（单写多读）：
  //   写方：write() → 修改 → commit()（必须在修改完成后再 commit）
  //   读方：read() → 立即使用，不长期持有引用（commit 后旧份可能被下一轮 write 覆盖）
  //   active_ 为 std::atomic<int>，read/write/commit 均 O(1)，无锁、无动态分配。
  //
  // 资源消耗：
  //   2 份 T 实例 + 2 个 std::atomic（active 索引 + commit 版本号）
  //
  // 对外接口：
  //   T&        write();   — 非激活份引用（写方填充，完成后 commit）
  //   void      commit();  — 原子切换：把刚写的那份置为激活，并递增版本号
  //   const T&  read();    — 当前激活份 const 引用（读方，勿长期持有）
  //   T&        active();  — 当前激活份非 const 引用（读方需原地修改时用——运行图推进/测试）
  //   uint64_t  version(); — 已 commit 次数（更新位）：读方据此检测配置是否热切换过
  //                          —— main 记录上次建图时的 version，变化即"更新位更新"
  //                          （见 g_state：expand_hp 由 main 检测版本变化后调用）
  // ==========================================================================
  template<typename T>
  class DoubleBuff {
  public:
    T &write() { return bufs_[1 - active_.load()]; }
    void commit() { active_.store(1 - active_.load()); ++version_; }
    const T &read() const { return bufs_[active_.load()]; }
    /// 当前激活份非 const 引用（读方需原地修改内容时用——运行图推进/测试）。
    T &active() { return bufs_[active_.load()]; }
    /// 已 commit 次数（更新位）：read 方据此检测配置是否热切换过——
    /// main 记录上次 expand_hp 时的 version，变化即"pipeline 更新位更新"，须重新建图。
    uint64_t version() const { return version_.load(); }
  private:
    T bufs_[2]{};
    std::atomic<int> active_{0};
    std::atomic<uint64_t> version_{0};
  };

  // ==========================================================================
  // DirectedAcyclicGraph — 有向无环图（dataflow DAG 结构骨架，类模板）
  // ==========================================================================
  //
  // 内部逻辑：
  //   通用有向无环图容器：顶点载荷 VertexT（如 algo 节点的局部 json 配置参数）、
  //   边载荷 EdgeT（如 mesg.hpp 的 Message，数据在两顶点间流动）。本文件只提供
  //   图结构骨架（顶点表 + 邻接边表 + 增删清空）；边的接线构建与运行期 job 生成
  //   逻辑由 g_state 的 expand_hp() 后续填充（todo：图逻辑先空出来）。
  //   EdgeT 具体化为 mesg 类型时，由包含 mesg.hpp 的调用方（g_state 等）实例化——
  //   utils 不反向依赖 mesg（mesg.hpp include 本文件，反向会循环依赖）。
  //
  // 资源消耗：
  //   每图：一张顶点表（node_id → VertexT）+ 一张邻接表（node_id → 出边
  //   vector<Edge>），均为 TBB 并发容器；clear() 全清
  //
  // 对外接口：
  //   add_node(id, payload)                    — 加顶点（payload 为 algo 的局部 json 配置参数）
  //   add_edge(from, to, stream, data)         — 加边 from → to，归属数据流 stream
  //                                              （data 为 EdgeT/Message 槽，运行期 job 读写）
  //   edges_from(id, stream)                   — 该节点输出到某 stream 的出边数据引用列表
  //                                              （job 执行后把 outputs 写进这些 Message 槽）
  //   edges_to(id, stream)                     — 该节点从某 stream 的入边数据引用列表
  //                                              （job 执行前从这些 Message 槽取输入帧）
  //   in_nodes(id)                             — 该节点入边来源顶点 id 列表（就绪判定：
  //                                              前序边来源全 Finished → 就绪）
  //   out_nodes(id)                            — 该节点出边目标顶点 id 列表（完成事件的邻居
  //                                              就绪检查：生产者完成 → 查下游消费者）
  //   clear()                                  — 清空整图（重建前调用）
  //   nodes_ / adj_ / rev_                     — 顶点表 / 出边邻接表 / 入边引用表（from,stream）
  // ==========================================================================
  template<typename VertexT, typename EdgeT>
  class DirectedAcyclicGraph {
  public:
    using NodeId = std::string;

    /// 边：from → to，归属数据流 stream（config inputs/outputs 里的 stream 名），
    /// data 为 EdgeT（mesg 数据类型 Message 槽——生产者写、消费者读，共享帧）。
    struct Edge {
      NodeId from;
      NodeId to;
      std::string stream;
      EdgeT data;
    };
    /// 入边引用记录：只记 (from, stream)，data 实际在 adj_[from] 的同名边里
    /// （edges_to 据此反查真数据，避免 rev_ 复制一份 EdgeT 导致引用不一致）。
    struct StreamRef {
      NodeId from;
      std::string stream;
    };

    /// 加顶点：载荷为 algo 节点的局部 json 配置参数
    void add_node(const NodeId &id, VertexT payload) {
      typename TBBMap<VertexT>::accessor a;
      nodes_.insert(a, id);
      a->second = std::move(payload);
    }

    /// 加边 from → to（归属数据流 stream）：data 为 EdgeT（初始 Message 槽，空帧）。
    /// 同时记入 rev_（to → (from,stream)），支持按入边反查。
    void add_edge(const NodeId &from, const NodeId &to, const std::string &stream, EdgeT data) {
      {
        typename TBBMap<std::vector<Edge>>::accessor a;
        adj_.insert(a, from);
        a->second.push_back(Edge{from, to, stream, std::move(data)});
      }
      {
        typename TBBMap<std::vector<StreamRef>>::accessor b;
        rev_.insert(b, to);
        b->second.push_back(StreamRef{from, stream});
      }
    }

    /// 该节点输出到某 stream 的出边数据引用（job 执行后把 outputs 写进这些 Message 槽）。
    /// 执行时现查、不缓存引用（expand_hp 全量重建后新 job 重新查，见 g_state 并发假设）。
    std::vector<std::reference_wrapper<EdgeT>> edges_from(const NodeId &id, const std::string &stream) {
      std::vector<std::reference_wrapper<EdgeT>> out;
      typename TBBMap<std::vector<Edge>>::accessor a;
      if (adj_.find(a, id))
        for (auto &e : a->second)
          if (e.stream == stream) out.emplace_back(e.data);
      return out;
    }

    /// 入边来源顶点 id 列表（就绪判定：前序边来源全 Finished → 就绪）。
    /// 基于 rev_（StreamRef{from,stream}），const_accessor 读，不碰真数据。
    std::vector<NodeId> in_nodes(const NodeId &id) const {
      std::vector<NodeId> out;
      typename TBBMap<std::vector<StreamRef>>::const_accessor a;
      if (rev_.find(a, id))
        for (const auto &ref : a->second) out.push_back(ref.from);
      return out;
    }

    /// 出边目标顶点 id 列表（完成事件的邻居就绪检查：生产者完成 → 查下游消费者）。
    /// 基于 adj_（出边邻接表），const_accessor 读；多端口连同一邻居去重。
    std::vector<NodeId> out_nodes(const NodeId &id) const {
      std::vector<NodeId> out;
      typename TBBMap<std::vector<Edge>>::const_accessor a;
      if (adj_.find(a, id))
        for (const auto &e : a->second)
          if (std::find(out.begin(), out.end(), e.to) == out.end()) out.push_back(e.to);
      return out;
    }

    /// 该节点从某 stream 的入边数据引用（job 执行前从这些 Message 槽取输入帧）。
    /// 经 rev_ 找 (from,stream)，再反查 adj_[from] 的真数据（引用与生产者写的是同一份）。
    std::vector<std::reference_wrapper<EdgeT>> edges_to(const NodeId &id, const std::string &stream) {
      std::vector<std::reference_wrapper<EdgeT>> out;
      typename TBBMap<std::vector<StreamRef>>::accessor ra;
      if (rev_.find(ra, id))
        for (const auto &ref : ra->second)
          if (ref.stream == stream) {
            typename TBBMap<std::vector<Edge>>::accessor a;
            if (adj_.find(a, ref.from))
              for (auto &e : a->second)
                if (e.to == id && e.stream == stream) {
                  out.emplace_back(e.data);  // 生产者写的就是这份 data（共享帧）
                  break;
                }
          }
      return out;
    }

    /// 清空整图（重建前调用）
    void clear() {
      nodes_.clear();
      adj_.clear();
      rev_.clear();
    }

    /// 顶点数（node_id 计数，测试/遍历用）
    std::size_t size() const { return nodes_.size(); }

    /// 按 node_id 取顶点载荷 const 引用（不存在抛 out_of_range）。
    /// 注意：返回的引用在访问器释放后随 map 生命周期失效（expand_hp 重建 clear 会悬垂），
    /// 调用方须立即拷贝（如 Job 执行体赋值），勿长期持有。
    const VertexT &vertex(const NodeId &id) const {
      typename TBBMap<VertexT>::const_accessor a;
      if (nodes_.find(a, id)) return a->second;
      throw std::out_of_range("DAG node not found: " + id);
    }

    /// 按 id 原地修改顶点载荷（update() 重置 finished/running、重算权值用）。
    /// 回调持 accessor（写锁）期间可安全读写；返回后引用失效，勿外逃。
    template<typename Fn>
    void mutate_vertex(const NodeId &id, Fn &&fn) {
      typename TBBMap<VertexT>::accessor a;
      if (nodes_.find(a, id)) fn(a->second);
    }

    /// 遍历全部顶点 (id, VertexT&) 并回调（update() 批量重算 abs_deadline / 调度回调
    /// 就绪检查用）。TBB range 遍历跨桶并发安全，与 accessor 读写可并存。
    template<typename Fn>
    void for_each_vertex(Fn &&fn) {
      for (auto &kv : nodes_.range()) fn(kv.first, kv.second);
    }

  private:
    TBBMap<VertexT> nodes_;                 // node_id → 顶点载荷
    TBBMap<std::vector<Edge>> adj_;         // node_id → 出边列表（邻接表，含 data）
    TBBMap<std::vector<StreamRef>> rev_;    // node_id → 入边引用列表（(from,stream)）
  };

} // namespace fins::util