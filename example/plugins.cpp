/*******************************************************************************
 * test_plugin.cpp — 用户算法插件（usr_* 函数 × 8，编译成 .so 供 loader/so_ctx 装载）
 *
 * 内部逻辑：
 *   本文件 = 用户代码里的算法实现，CMake 编译成 N 份 test_plugin_${ID}.so（内容相同）
 *   进插件输出目录（PLUGIN_OUTPUT_DIR/plugins），装配点扫描目录 make_shared<Plugin>
 *   装载进 library_g.so_ctx（Plugin 构造 = dlopen + dlsym 5 个 C 符号 + 填 loaded_keys）。
 *   算法本体 = AlgoFunc 封装的用户裸函数 usr_*（configs-first 参数布局契约：配置段 +
 *   输入段 + 输出段，参数为具体载荷类型 int；usr_acc 的 loop 反馈入参声明 std::vector<int>
 *   （恒长 N、头部 0 补位，运行时 bind_job 直出 typed 容器，见 g_state.hpp）。处理逻辑统一
 *   "乘增益 gain"——配置注入 / 输入解包 / 输出路由任一环节错位最终值即错，可被通信
 *   测试断言捕获）。5 个 C 工厂符号由 FINS_ALGO_EXPORT 宏自动生成（X-macro 展开：
 *   名字表 = 函数名字符串化、计数、create_algo 按 [usr_*:1.0.0] 分发 AlgoFunc），
 *   免手动样板。装载后每 Plugin 的 loaded_keys = {usr_src:1.0.0, usr_sink:1.0.0,
 *   usr_relay:1.0.0, usr_join:1.0.0, usr_fork:1.0.0, usr_acc:1.0.0,
 *   usr_fork10:1.0.0, usr_join10:1.0.0, usr_fork100:1.0.0, usr_join100:1.0.0}；expand_hp ④ 按 JSON 的
 *   [name:version] 定位 so_ctx → create_algo(key) 生成 AlgoBase 实例（shared_ptr 删除器
 *   持 Plugin 保活，实例存活期间库不卸载）。全部 .so 同名同算法，遍历命中任一即可。
 *
 * 资源消耗：
 *   每 .so 一份静态名字表（10 个 const char*）+ 运行时每节点 1 个 AlgoFunc 实例（1 函数
 *   指针 + configs_ 类型化配置帧表，配置注入时解码一次、execute 零解析）。
 *
 * 对外接口（C ABI，由 FINS_ALGO_EXPORT 生成，Plugin 构造 dlsym 解析）：
 *   int get_plugin_count()            — 返回 10
 *   const char *get_algo_name(int i)  — usr_src/usr_sink/usr_relay/usr_join/usr_fork/
 *                                       usr_acc/usr_fork10/usr_join10
 *   const char *get_algo_version(int) — 恒 "1.0.0"
 *   AlgoBase *create_algo(const char *key) — 按 [usr_*:1.0.0] 分发 new AlgoFunc，未知返回 nullptr
 *   void destroy_plugin(AlgoBase *p)  — delete p
 ******************************************************************************/
#include <chrono>
#include <cstdio>
#include <string>
#include <vector>

#include "algo/algo_export.hpp"

namespace {

// ── 用户算法函数（AlgoFunc 封装，configs-first 签名：配置段 + 输入段 + 输出段）──
//   参数总数须精确 = 配置数 + 输入端口数 + 输出端口数，按"配置段 + 输入段 + 输出段"排列
//   （违背则输出段下标越界）。节点按端口形状选对应签名。
// 忙等待模拟计算时长（独占核心：纯自旋固定 us，消除 sleep_for 唤醒调度波动；循环读
// now() 有副作用，不会被编译器优化掉）
static void spin_cost_us(long long us) {
  const auto target = std::chrono::steady_clock::now() + std::chrono::microseconds(us);
  while (std::chrono::steady_clock::now() < target) {}
}

void usr_src(int cfg, int &out) { out = cfg; spin_cost_us(1000); }                // 源（0 输入 / 1 输出）
void usr_sink(int cfg, int in) { (void)cfg; spin_cost_us(1000);}  // 终端（1 输入 / 0 输出，打印收到的数据作端到端观察点）
void usr_relay(int cfg, int in, int &out) { out = in * cfg; spin_cost_us(1000);} // 处理器（1 输入 / 1 输出）
void usr_join(int cfg, int in_a, int in_b, int &out) { (void)cfg; out = in_a * in_b; spin_cost_us(1000);}
void usr_fork(int cfg, int in, int &out_a, int &out_b) { out_a = in * cfg; out_b = in * cfg; spin_cost_us(1000);}
void usr_fork10(int cfg, int in,
                int &o1, int &o2, int &o3, int &o4, int &o5,
                int &o6, int &o7, int &o8, int &o9, int &o10) {
  o1 = o2 = o3 = o4 = o5 = o6 = o7 = o8 = o9 = o10 = in * cfg;
  spin_cost_us(1000);
}
void usr_join10(int cfg,
                int i1, int i2, int i3, int i4, int i5,
                int i6, int i7, int i8, int i9, int i10,
                int &out) {
  (void)cfg;
  out = i1 * i2 * i3 * i4 * i5 * i6 * i7 * i8 * i9 * i10;
  spin_cost_us(1000);
}
void usr_fork100(int cfg, int in,
                 int &o1, int &o2, int &o3, int &o4, int &o5, int &o6, int &o7, int &o8, int &o9, int &o10,
                 int &o11, int &o12, int &o13, int &o14, int &o15, int &o16, int &o17, int &o18, int &o19, int &o20,
                 int &o21, int &o22, int &o23, int &o24, int &o25, int &o26, int &o27, int &o28, int &o29, int &o30,
                 int &o31, int &o32, int &o33, int &o34, int &o35, int &o36, int &o37, int &o38, int &o39, int &o40,
                 int &o41, int &o42, int &o43, int &o44, int &o45, int &o46, int &o47, int &o48, int &o49, int &o50,
                 int &o51, int &o52, int &o53, int &o54, int &o55, int &o56, int &o57, int &o58, int &o59, int &o60,
                 int &o61, int &o62, int &o63, int &o64, int &o65, int &o66, int &o67, int &o68, int &o69, int &o70,
                 int &o71, int &o72, int &o73, int &o74, int &o75, int &o76, int &o77, int &o78, int &o79, int &o80,
                 int &o81, int &o82, int &o83, int &o84, int &o85, int &o86, int &o87, int &o88, int &o89, int &o90,
                 int &o91, int &o92, int &o93, int &o94, int &o95, int &o96, int &o97, int &o98, int &o99, int &o100) {
  spin_cost_us(1000);
  const int v = in * cfg;   // 广播值；100 路输出连等赋同一值
  o1 = o2 = o3 = o4 = o5 = o6 = o7 = o8 = o9 = o10 =
  o11 = o12 = o13 = o14 = o15 = o16 = o17 = o18 = o19 = o20 =
  o21 = o22 = o23 = o24 = o25 = o26 = o27 = o28 = o29 = o30 =
  o31 = o32 = o33 = o34 = o35 = o36 = o37 = o38 = o39 = o40 =
  o41 = o42 = o43 = o44 = o45 = o46 = o47 = o48 = o49 = o50 =
  o51 = o52 = o53 = o54 = o55 = o56 = o57 = o58 = o59 = o60 =
  o61 = o62 = o63 = o64 = o65 = o66 = o67 = o68 = o69 = o70 =
  o71 = o72 = o73 = o74 = o75 = o76 = o77 = o78 = o79 = o80 =
  o81 = o82 = o83 = o84 = o85 = o86 = o87 = o88 = o89 = o90 =
  o91 = o92 = o93 = o94 = o95 = o96 = o97 = o98 = o99 = o100 = v;
}
// join100：100 输入 / 1 输出。用"求和"而非现有 usr_join10 的"乘积"——
// 100 路乘积 3^100 溢出 int（signed 溢出 = UB）；求和 100×3=300 安全且保留"汇聚全部输入"语义
void usr_join100(int cfg,
                 int i1, int i2, int i3, int i4, int i5, int i6, int i7, int i8, int i9, int i10,
                 int i11, int i12, int i13, int i14, int i15, int i16, int i17, int i18, int i19, int i20,
                 int i21, int i22, int i23, int i24, int i25, int i26, int i27, int i28, int i29, int i30,
                 int i31, int i32, int i33, int i34, int i35, int i36, int i37, int i38, int i39, int i40,
                 int i41, int i42, int i43, int i44, int i45, int i46, int i47, int i48, int i49, int i50,
                 int i51, int i52, int i53, int i54, int i55, int i56, int i57, int i58, int i59, int i60,
                 int i61, int i62, int i63, int i64, int i65, int i66, int i67, int i68, int i69, int i70,
                 int i71, int i72, int i73, int i74, int i75, int i76, int i77, int i78, int i79, int i80,
                 int i81, int i82, int i83, int i84, int i85, int i86, int i87, int i88, int i89, int i90,
                 int i91, int i92, int i93, int i94, int i95, int i96, int i97, int i98, int i99, int i100,
                 int &out) {
  (void)cfg;
  out = i1 + i2 + i3 + i4 + i5 + i6 + i7 + i8 + i9 + i10 +
        i11 + i12 + i13 + i14 + i15 + i16 + i17 + i18 + i19 + i20 +
        i21 + i22 + i23 + i24 + i25 + i26 + i27 + i28 + i29 + i30 +
        i31 + i32 + i33 + i34 + i35 + i36 + i37 + i38 + i39 + i40 +
        i41 + i42 + i43 + i44 + i45 + i46 + i47 + i48 + i49 + i50 +
        i51 + i52 + i53 + i54 + i55 + i56 + i57 + i58 + i59 + i60 +
        i61 + i62 + i63 + i64 + i65 + i66 + i67 + i68 + i69 + i70 +
        i71 + i72 + i73 + i74 + i75 + i76 + i77 + i78 + i79 + i80 +
        i81 + i82 + i83 + i84 + i85 + i86 + i87 + i88 + i89 + i90 +
        i91 + i92 + i93 + i94 + i95 + i96 + i97 + i98 + i99 + i100;
  spin_cost_us(1000);
}
// acc：1 输入（feed）/ 1 loop 反馈输入（acc_out，config 迭代步 N=3）/ 1 输出（acc_out，写回历史槽）。
//   hist = 运行时聚合出的最近 N 帧 std::vector<int>（旧→新，头部补 0）；out = cfg·in + 最近一帧
//   （自反馈斜坡：每拍 +cfg·in，有界可观察）。printf 观察头部补位/顺序/闭环。
void usr_acc(int cfg, int in, const std::vector<int> &hist, int &out) {
  out = cfg * in + (hist.empty() ? 0 : hist.back());
  std::printf("[usr_acc] cfg=%d in=%d hist[%zu]=(", cfg, in, hist.size());
  for (size_t i = 0; i < hist.size(); ++i) std::printf("%s%d", i ? " " : "", hist[i]);
  std::printf(") -> out=%d\n", out);
  spin_cost_us(1000);
}
} // namespace

// ── 算法列表（X-macro：算法名 = 函数名字符串化，版本 = FINS_ALGO_EXPORT 参数 "1.0.0"）──
#define FINS_ALGO_LIST(F) \
  F(usr_src)              \
  F(usr_sink)             \
  F(usr_relay)            \
  F(usr_join)             \
  F(usr_fork)             \
  F(usr_acc)              \
  F(usr_fork10)           \
  F(usr_join10)           \
  F(usr_fork100)          \
  F(usr_join100)          \

// 生成 5 个 C 工厂符号（get_plugin_count / get_algo_name / get_algo_version /
// create_algo / destroy_plugin），Plugin 构造 dlsym 解析 + 填 loaded_keys = {usr_*:1.0.0}
FINS_ALGO_EXPORT("1.0.0")
