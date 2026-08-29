/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/
#pragma once

#include <algorithm>
#include <atomic>
#include <functional>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <tbb/concurrent_hash_map.h>

namespace fins::util {

  /** @brief TBB 并发哈希表别名（[string]→T；g_state/DAG/装配点直接持 accessor 用）。 */
  template<typename T>
  using TBBMap = tbb::concurrent_hash_map<std::string, T>;

  /** @brief 写入/覆盖（不存在则插入）。宏 = 手写 accessor 的等价封装（预处理文本替换，零开销）；
   *  accessor 在宏作用域结束即释放锁（RAII）。装配点维护 so_ctx 等整值表用。
   * @param MAP 目标 TBBMap
   * @param key 键
   * @param val 值
   * @retval 无
   */
#define TBBMAP_SET(MAP, key, val)                                   \
  do {                                                              \
    using _M = std::decay_t<decltype(MAP)>;                         \
    const auto &_k = (key);                                         \
    _M::accessor a;                                                 \
    (MAP).insert(a, _k);                                            \
    a->second = (val);                                              \
  } while (0)

  /** @brief 删除键（不存在返回 false）。宏 = 手写 erase 的等价封装（零开销）。
   * @param MAP 目标 TBBMap
   * @param key 键
   * @retval bool 删除是否生效
   */
#define TBBMAP_ERASE(MAP, key)                                      \
  ([&] {                                                            \
    const auto &_k = (key);                                         \
    bool ok = (MAP).erase(_k);                                      \
    return ok;                                                      \
  }())

  /** @brief 定位键并就地修改值（无则默认构造插入后回调）。宏 = 手写 accessor 的等价封装
   *  （零开销）；持写锁期间回调内可安全改值，返回后锁释放（RAII）。运行时滑动窗口/
   *  环形队列追加（record_mesg/record_exec）用——SET 是整值覆盖，表达不了这种定位修改。
   * @param MAP 目标 TBBMap
   * @param key 键
   * @param fn  修改回调（值类型& → void，如 lambda 捕获待写入数据）
   * @retval 无
   */
#define TBBMAP_UPDATE(MAP, key, fn)                              \
  do {                                                           \
    using _M = std::decay_t<decltype(MAP)>;                      \
    const auto &_k = (key);                                      \
    _M::accessor a;                                              \
    (MAP).insert(a, _k);                                         \
    fn(a->second);                                               \
  } while (0)

  /** @brief 双缓冲（单写多读，原子切换）：write() 拿非激活份填充 → commit() 原子翻转索引；
   *  read() 始终读激活份——读方要么旧版要么新版，绝不半写。典型 = 配置热更新
   *  （pipeline_g.cache：RPC 写缓冲份 → pending → 主线程调度循环图静止 commit+read+parse）。
   *  资源消耗：2 份 T + 1 个 atomic 索引。 */
  template<typename T>
  class DoubleBuff {
  public:
    /** @brief 取非激活份引用（写方填充，完成后 commit()）。
     * @retval T& 非激活份
     */
    T &write() { return bufs_[1 - active_.load()]; }
    /** @brief 原子切换：把刚写的那份置为激活。
     * @retval 无
     */
    void commit() { active_.store(1 - active_.load()); }
    /** @brief 取当前激活份 const 引用（读方立即使用，勿长期持有——commit 后旧份可能被覆盖）。
     * @retval const T& 激活份
     */
    const T &read() const { return bufs_[active_.load()]; }
  private:
    T bufs_[2]{};
    std::atomic<int> active_{0};
  };

  /** @brief 通用有向无环图容器（dataflow DAG 结构骨架，类模板）：顶点载荷 VertexT（如 Workload）+
   *  边载荷 EdgeT（如 Message 槽，生产者写/消费者读共享帧）；顶点表 + 出边邻接表 + 入边引用表，
   *  均为 TBB 并发容器；clear() 全清重建。边连接标识 = 端口名（同名直连）/ 'seq:' 边名（同节点
   *  job 串行）；utils 不反向依赖 mesg（mesg.hpp include 本文件，反向会循环依赖）。 */
  template<typename VertexT, typename EdgeT>
  class DirectedAcyclicGraph {
  public:
    using NodeId = std::string;

    /** @brief 边：from → to，归属连接标识 tag（端口名 / 'seq:' 边名），data 为 EdgeT
     *  （mesg 数据类型 Message 槽——生产者写、消费者读，共享帧）。
     */
    struct Edge {
      NodeId from;
      NodeId to;
      std::string tag;
      EdgeT data;
    };
    /** @brief 入边引用记录：只记 (from, tag)，data 实际在 adj_[from] 的同名边里
     *  （edges_to 据此反查真数据，避免 rev_ 复制一份 EdgeT 导致引用不一致）。
     */
    struct StreamRef {
      NodeId from;
      std::string tag;
    };

    /** @brief 加顶点。
     * @param id      顶点 id
     * @param payload 顶点载荷（VertexT，如 Workload）
     * @retval 无
     */
    void add_node(const NodeId &id, VertexT payload) {
      typename TBBMap<VertexT>::accessor a;
      nodes_.insert(a, id);
      a->second = std::move(payload);
    }

    /** @brief 加边 from → to（归属连接标识 tag：端口名 / 'seq:' 边名）：data 为 EdgeT
     *  （初始 Message 槽，空帧）。同时记入 rev_（to → (from, tag)），支持按入边反查。
     * @param from 源顶点 id
     * @param to   目标顶点 id
     * @param tag  连接标识（端口名 / 'seq:' 边名）
     * @param data 边载荷（EdgeT，共享帧槽）
     * @retval 无
     */
    void add_edge(const NodeId &from, const NodeId &to, const std::string &tag, EdgeT data) {
      // 建图自洽校验：边引用的 from/to 顶点必须已存在（有边必有顶点）。任一缺失抛
      // std::out_of_range——把"有边无顶点"从运行期 grab_ready_workload 裸崩，提前到建图期
      // 加边那一刻、精确到这条边（消息含 from→to）。expand_hp 持锁建图，装配点 try-catch
      // 记日志拒绝本次配置。仅校验存在性，不校验是否已建边/重复边。
      {
        typename TBBMap<VertexT>::const_accessor a;
        if (!nodes_.find(a, from))
          throw std::out_of_range("DAG add_edge source missing: " + from + " -> " + to);
      }
      {
        typename TBBMap<VertexT>::const_accessor b;
        if (!nodes_.find(b, to))
          throw std::out_of_range("DAG add_edge target missing: " + from + " -> " + to);
      }
      {
        typename TBBMap<std::vector<Edge>>::accessor a;
        adj_.insert(a, from);
        a->second.push_back(Edge{from, to, tag, std::move(data)});
      }
      {
        typename TBBMap<std::vector<StreamRef>>::accessor b;
        rev_.insert(b, to);
        b->second.push_back(StreamRef{from, tag});
      }
    }

    /** @brief 该节点输出到某连接标识的出边数据引用（job 执行后把 outputs 写进这些 Message 槽）。
     *  执行时现查、不缓存引用（expand_hp 全量重建后新 job 重新查，见 g_state 并发假设）。
     * @param id  源顶点 id
     * @param tag 连接标识（端口名）
     * @retval std::vector<std::reference_wrapper<EdgeT>> 出边数据引用列表
     */
    std::vector<std::reference_wrapper<EdgeT>> edges_from(const NodeId &id, const std::string &tag) {
      std::vector<std::reference_wrapper<EdgeT>> out;
      typename TBBMap<std::vector<Edge>>::accessor a;
      if (adj_.find(a, id))
        for (auto &e : a->second)
          if (e.tag == tag) out.emplace_back(e.data);
      return out;
    }

    /** @brief 入边来源顶点 id 列表（就绪判定：前序边来源全 Finished → 就绪）。
     *  基于 rev_（StreamRef{from, tag}），const_accessor 读，不碰真数据。
     * @param id 目标顶点 id
     * @retval std::vector<NodeId> 入边来源顶点 id 列表
     */
    std::vector<NodeId> in_nodes(const NodeId &id) const {
      std::vector<NodeId> out;
      typename TBBMap<std::vector<StreamRef>>::const_accessor a;
      if (rev_.find(a, id))
        for (const auto &ref : a->second) out.push_back(ref.from);
      return out;
    }

    /** @brief 该节点从某连接标识的入边数据引用（job 执行前从这些 Message 槽取输入帧）。
     *  经 rev_ 找 (from, tag)，再反查 adj_[from] 的真数据（引用与生产者写的是同一份）。
     * @param id  目标顶点 id
     * @param tag 连接标识（端口名）
     * @retval std::vector<std::reference_wrapper<EdgeT>> 入边数据引用列表
     */
    std::vector<std::reference_wrapper<EdgeT>> edges_to(const NodeId &id, const std::string &tag) {
      std::vector<std::reference_wrapper<EdgeT>> out;
      typename TBBMap<std::vector<StreamRef>>::accessor ra;
      if (rev_.find(ra, id))
        for (const auto &ref : ra->second)
          if (ref.tag == tag) {
            typename TBBMap<std::vector<Edge>>::accessor a;
            if (adj_.find(a, ref.from))
              for (auto &e : a->second)
                if (e.to == id && e.tag == tag) {
                  out.emplace_back(e.data);  // 生产者写的就是这份 data（共享帧）
                  break;
                }
          }
      return out;
    }

    /** @brief 清空整图（重建前调用）。
     * @retval 无
     */
    void clear() {
      nodes_.clear();
      adj_.clear();
      rev_.clear();
    }

    /** @brief 顶点数（node_id 计数，测试/遍历用）。
     * @retval std::size_t 顶点数
     */
    std::size_t size() const { return nodes_.size(); }

    /** @brief 按 node_id 取顶点载荷 const 引用（不存在抛 out_of_range）。
     *  注意：返回的引用在访问器释放后随 map 生命周期失效（expand_hp 重建 clear 会悬垂），
     *  调用方须立即拷贝/使用，勿长期持有。
     * @param id 顶点 id
     * @retval const VertexT& 顶点载荷
     */
    const VertexT &vertex(const NodeId &id) const {
      typename TBBMap<VertexT>::const_accessor a;
      if (nodes_.find(a, id)) return a->second;
      throw std::out_of_range("DAG node not found: " + id);
    }

    /** @brief 按 id 原地修改顶点载荷（回调持 accessor 写锁期间可安全读写；返回后引用失效，勿外逃）。
     * @param id 顶点 id
     * @param fn 修改回调（VertexT& → void）
     * @retval 无
     */
    template<typename Fn>
    void mutate_vertex(const NodeId &id, Fn &&fn) {
      typename TBBMap<VertexT>::accessor a;
      if (nodes_.find(a, id)) fn(a->second);
    }

    /** @brief 遍历全部顶点 (id, VertexT&) 并回调（调度回绕/滚动校正遍历用）。
     *  TBB range 遍历跨桶并发安全，与 accessor 读写可并存。
     * @param fn 遍历回调 (const std::string&, VertexT& → void)
     * @retval 无
     */
    template<typename Fn>
    void for_each_vertex(Fn &&fn) {
      for (auto &kv : nodes_.range()) fn(kv.first, kv.second);
    }

  private:
    TBBMap<VertexT> nodes_;                 // node_id → 顶点载荷
    TBBMap<std::vector<Edge>> adj_;         // node_id → 出边列表（邻接表，含 data）
    TBBMap<std::vector<StreamRef>> rev_;    // node_id → 入边引用列表（(from,tag)）
  };

} // namespace fins::util
