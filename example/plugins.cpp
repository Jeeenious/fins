/*******************************************************************************
 * plugins.cpp — 用户算法插件（usr_* 函数族，编译成 plugins/plugin.so 供 loader 装载）
 *
 * 设计约定（configs-first，供负载/拓扑实验）：
 *   · 每个节点 cfg 只有一个参数 = 每拍忙等时长（µs，独占核心自旋），真实占用 CPU；
 *     usr_nop 例外——零自旋，只消费一帧（廉价终点）。
 *   · 值路由已不需要：这些算法只提供「端口形状 + 烧算力」，out/in 只为形状与数据流存在。
 *   · 反馈节点入参为 std::vector<int>（loop 聚合的最近 N 帧，运行时 bind_job 直出 typed
 *     容器，见 g_state.hpp）；cfg 节点用 "loop":{端口:N} 声明各反馈窗口。
 *
 * 形状族（每个 = 一个可导出的具体函数，供 pipeline cfg 的 [name:1.0.0] 定位）：
 *   基础    src(0→1) / sink(1→0) / relay(1→1)
 *   扇出    1→k        usr_fork(=2)、usr_fork3..9、usr_fork10(=10)  —— 多速率 fork 类按 fan 选用
 *   扇入    k→1        usr_join(=2)、usr_join3..9、usr_join10(=10)  —— 多速率 join 类按 fan 选用
 *   反馈    1 feed + k loop（k=1..10）：usr_acc(=1)  ..  usr_acc10   —— feedback 类随机窗口 N
 *   工具    usr_burn(源,自旋) / usr_nop(零自旋汇)
 *   合计 33 个算法。
 *
 *   负载/拓扑入口见 tool/uload.ipynb（gen_class 五类：多速率 multihop / fork / join、
 *   feedback、mixed；每图内部混合周期 timed 与事件 event 任务）。周期/事件(激活次数 n_j) 与
 *   目标利用率 u 的分配都在生成侧完成——本族算法只作为“按 cfg 烧掉对应 µs”的端口形状载体。
 *
 * 对外接口（FINS_ALGO_EXPORT 由 X-macro 生成 5 个 C 工厂符号）：
 *   get_plugin_count / get_algo_name / get_algo_version(恒 "1.0.0") / create_algo /
 *   destroy_plugin —— Plugin 构造 dlsym 解析后填 loaded_keys = {usr_*:1.0.0}。
 ******************************************************************************/
#include <chrono>
#include <vector>

#include "xmacro.hpp"

namespace {

// 忙等：独占核心纯自旋固定 us（循环读 steady_clock，不会被优化掉；比 sleep_for 少唤醒抖动）
static void spin_cost_us(long long us) {
  const auto target = std::chrono::steady_clock::now() + std::chrono::microseconds(us);
  while (std::chrono::steady_clock::now() < target) {}
}

// 吞掉未用参数（统一消除 -Wextra 的 unused-parameter 告警）
template <typename... Ts>
void ignore_all(Ts &&...) {}

// 统一函数体：烧掉 cfg 指定的忙等 µs（out/in 仅为形状）
#define USR_SPIN_BODY(...)         \
  do {                             \
    ignore_all(__VA_ARGS__);       \
    spin_cost_us(cfg);             \
  } while (0)

// ── 基础：源 / 汇 / 链 ──
void usr_src(int cfg, int &out) { USR_SPIN_BODY(out); }                 // 0 输入 / 1 输出
void usr_sink(int cfg, int in) { USR_SPIN_BODY(in); }                   // 1 输入 / 0 输出
void usr_relay(int cfg, int in, int &out) { USR_SPIN_BODY(in, out); }   // 1 输入 / 1 输出

// ── 扇出 1→k（k=2..10）──
void usr_fork(int cfg, int in, int &out_a, int &out_b) { USR_SPIN_BODY(in, out_a, out_b); }
void usr_fork3(int cfg, int in, int &o1, int &o2, int &o3) { USR_SPIN_BODY(in, o1, o2, o3); }
void usr_fork4(int cfg, int in, int &o1, int &o2, int &o3, int &o4) {
  USR_SPIN_BODY(in, o1, o2, o3, o4);
}
void usr_fork5(int cfg, int in, int &o1, int &o2, int &o3, int &o4, int &o5) {
  USR_SPIN_BODY(in, o1, o2, o3, o4, o5);
}
void usr_fork6(int cfg, int in, int &o1, int &o2, int &o3, int &o4, int &o5, int &o6) {
  USR_SPIN_BODY(in, o1, o2, o3, o4, o5, o6);
}
void usr_fork7(int cfg, int in, int &o1, int &o2, int &o3, int &o4, int &o5, int &o6,
               int &o7) {
  USR_SPIN_BODY(in, o1, o2, o3, o4, o5, o6, o7);
}
void usr_fork8(int cfg, int in, int &o1, int &o2, int &o3, int &o4, int &o5, int &o6,
               int &o7, int &o8) {
  USR_SPIN_BODY(in, o1, o2, o3, o4, o5, o6, o7, o8);
}
void usr_fork9(int cfg, int in, int &o1, int &o2, int &o3, int &o4, int &o5, int &o6,
               int &o7, int &o8, int &o9) {
  USR_SPIN_BODY(in, o1, o2, o3, o4, o5, o6, o7, o8, o9);
}
void usr_fork10(int cfg, int in, int &o1, int &o2, int &o3, int &o4, int &o5, int &o6,
                int &o7, int &o8, int &o9, int &o10) {
  USR_SPIN_BODY(in, o1, o2, o3, o4, o5, o6, o7, o8, o9, o10);
}

// ── 扇入 k→1（k=2..10）──
void usr_join(int cfg, int in_a, int in_b, int &out) { USR_SPIN_BODY(in_a, in_b, out); }
void usr_join3(int cfg, int i1, int i2, int i3, int &out) {
  USR_SPIN_BODY(i1, i2, i3, out);
}
void usr_join4(int cfg, int i1, int i2, int i3, int i4, int &out) {
  USR_SPIN_BODY(i1, i2, i3, i4, out);
}
void usr_join5(int cfg, int i1, int i2, int i3, int i4, int i5, int &out) {
  USR_SPIN_BODY(i1, i2, i3, i4, i5, out);
}
void usr_join6(int cfg, int i1, int i2, int i3, int i4, int i5, int i6, int &out) {
  USR_SPIN_BODY(i1, i2, i3, i4, i5, i6, out);
}
void usr_join7(int cfg, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int &out) {
  USR_SPIN_BODY(i1, i2, i3, i4, i5, i6, i7, out);
}
void usr_join8(int cfg, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8,
               int &out) {
  USR_SPIN_BODY(i1, i2, i3, i4, i5, i6, i7, i8, out);
}
void usr_join9(int cfg, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8,
               int i9, int &out) {
  USR_SPIN_BODY(i1, i2, i3, i4, i5, i6, i7, i8, i9, out);
}
void usr_join10(int cfg, int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8,
                int i9, int i10, int &out) {
  USR_SPIN_BODY(i1, i2, i3, i4, i5, i6, i7, i8, i9, i10, out);
}

// ── 反馈：1 外部输入 feed + k 路 loop 反馈（k=1..10），单输出 ──
//   每路 loop 入参 = 本节点输出最近 N 帧的 std::vector<int>，cfg 以 "loop":{h1:N1,...} 声明。
void usr_acc(int cfg, int in, const std::vector<int> &hist, int &out) {
  USR_SPIN_BODY(in, hist, out);
}
void usr_acc2(int cfg, int in, const std::vector<int> &h1, const std::vector<int> &h2,
              int &out) {
  USR_SPIN_BODY(in, h1, h2, out);
}
void usr_acc3(int cfg, int in, const std::vector<int> &h1, const std::vector<int> &h2,
              const std::vector<int> &h3, int &out) {
  USR_SPIN_BODY(in, h1, h2, h3, out);
}
void usr_acc4(int cfg, int in, const std::vector<int> &h1, const std::vector<int> &h2,
              const std::vector<int> &h3, const std::vector<int> &h4, int &out) {
  USR_SPIN_BODY(in, h1, h2, h3, h4, out);
}
void usr_acc5(int cfg, int in, const std::vector<int> &h1, const std::vector<int> &h2,
              const std::vector<int> &h3, const std::vector<int> &h4,
              const std::vector<int> &h5, int &out) {
  USR_SPIN_BODY(in, h1, h2, h3, h4, h5, out);
}
void usr_acc6(int cfg, int in, const std::vector<int> &h1, const std::vector<int> &h2,
              const std::vector<int> &h3, const std::vector<int> &h4,
              const std::vector<int> &h5, const std::vector<int> &h6, int &out) {
  USR_SPIN_BODY(in, h1, h2, h3, h4, h5, h6, out);
}
void usr_acc7(int cfg, int in, const std::vector<int> &h1, const std::vector<int> &h2,
              const std::vector<int> &h3, const std::vector<int> &h4,
              const std::vector<int> &h5, const std::vector<int> &h6,
              const std::vector<int> &h7, int &out) {
  USR_SPIN_BODY(in, h1, h2, h3, h4, h5, h6, h7, out);
}
void usr_acc8(int cfg, int in, const std::vector<int> &h1, const std::vector<int> &h2,
              const std::vector<int> &h3, const std::vector<int> &h4,
              const std::vector<int> &h5, const std::vector<int> &h6,
              const std::vector<int> &h7, const std::vector<int> &h8, int &out) {
  USR_SPIN_BODY(in, h1, h2, h3, h4, h5, h6, h7, h8, out);
}
void usr_acc9(int cfg, int in, const std::vector<int> &h1, const std::vector<int> &h2,
              const std::vector<int> &h3, const std::vector<int> &h4,
              const std::vector<int> &h5, const std::vector<int> &h6,
              const std::vector<int> &h7, const std::vector<int> &h8,
              const std::vector<int> &h9, int &out) {
  USR_SPIN_BODY(in, h1, h2, h3, h4, h5, h6, h7, h8, h9, out);
}
void usr_acc10(int cfg, int in, const std::vector<int> &h1, const std::vector<int> &h2,
               const std::vector<int> &h3, const std::vector<int> &h4,
               const std::vector<int> &h5, const std::vector<int> &h6,
               const std::vector<int> &h7, const std::vector<int> &h8,
               const std::vector<int> &h9, const std::vector<int> &h10, int &out) {
  USR_SPIN_BODY(in, h1, h2, h3, h4, h5, h6, h7, h8, h9, h10, out);
}

// ── 工具 ──
// burn：源(0→1)，cfg=忙等 µs（多速率源的明确形态；与 usr_src 等价，需事件 sink 时用它更语义化）
void usr_burn(int us, int &out) {
  ignore_all(out);
  if (us > 0) spin_cost_us(us);
}
// nop：零自旋汇(1→0)，只收帧不烧算力（当某个 event/源链路只需收帧、不想 sink 再烧算力时）
void usr_nop(int in) { ignore_all(in); }

#undef USR_SPIN_BODY

}  // namespace

// ── 算法列表（X-macro：算法名 = 函数名字符串化；版本 = FINS_ALGO_EXPORT 参数 "1.0.0"）──
#define FINS_ALGO_LIST(F) \
  F(usr_src)              \
  F(usr_sink)             \
  F(usr_relay)            \
  F(usr_fork)             \
  F(usr_fork3)            \
  F(usr_fork4)            \
  F(usr_fork5)            \
  F(usr_fork6)            \
  F(usr_fork7)            \
  F(usr_fork8)            \
  F(usr_fork9)            \
  F(usr_fork10)           \
  F(usr_join)             \
  F(usr_join3)            \
  F(usr_join4)            \
  F(usr_join5)            \
  F(usr_join6)            \
  F(usr_join7)            \
  F(usr_join8)            \
  F(usr_join9)            \
  F(usr_join10)           \
  F(usr_acc)              \
  F(usr_acc2)             \
  F(usr_acc3)             \
  F(usr_acc4)             \
  F(usr_acc5)             \
  F(usr_acc6)             \
  F(usr_acc7)             \
  F(usr_acc8)             \
  F(usr_acc9)             \
  F(usr_acc10)            \
  F(usr_burn)             \
  F(usr_nop)

// 生成 5 个 C 工厂符号（get_plugin_count / get_algo_name / get_algo_version /
// create_algo / destroy_plugin），Plugin 构造 dlsym 解析 + 填 loaded_keys = {usr_*:1.0.0}
FINS_ALGO_EXPORT("1.0.0")
