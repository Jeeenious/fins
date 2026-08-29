/*******************************************************************************
 * test_plugin.cpp — 用户算法插件（usr_* 函数 × 8，编译成 .so 供 loader/so_ctx 装载）
 *
 * 内部逻辑：
 *   本文件 = 用户代码里的算法实现，CMake 编译成 N 份 test_plugin_${ID}.so（内容相同）
 *   进插件输出目录（PLUGIN_OUTPUT_DIR/plugins），装配点扫描目录 make_shared<Plugin>
 *   装载进 library_g.so_ctx（Plugin 构造 = dlopen + dlsym 5 个 C 符号 + 填 loaded_keys）。
 *   算法本体 = AlgoFunc 封装的用户裸函数 usr_*（configs-first 参数布局契约：配置段 +
 *   输入段 + 输出段，参数为具体载荷类型 int / std::vector<Message>，处理逻辑统一
 *   "乘增益 gain"——配置注入 / 输入解包 / 输出路由任一环节错位最终值即错，可被通信
 *   测试断言捕获）。5 个 C 工厂符号由 FINS_ALGO_EXPORT 宏自动生成（X-macro 展开：
 *   名字表 = 函数名字符串化、计数、create_algo 按 [usr_*:1.0.0] 分发 AlgoFunc），
 *   免手动样板。装载后每 Plugin 的 loaded_keys = {usr_src:1.0.0, usr_sink:1.0.0,
 *   usr_relay:1.0.0, usr_join:1.0.0, usr_fork:1.0.0, usr_acc:1.0.0,
 *   usr_fork10:1.0.0, usr_join10:1.0.0}；expand_hp ④ 按 JSON 的
 *   [name:version] 定位 so_ctx → create_algo(key) 生成 AlgoBase 实例（shared_ptr 删除器
 *   持 Plugin 保活，实例存活期间库不卸载）。全部 .so 同名同算法，遍历命中任一即可。
 *
 * 资源消耗：
 *   每 .so 一份静态名字表（8 个 const char*）+ 运行时每节点 1 个 AlgoFunc 实例（1 函数
 *   指针 + configs_ 类型化配置帧表，配置注入时解码一次、execute 零解析）。
 *
 * 对外接口（C ABI，由 FINS_ALGO_EXPORT 生成，Plugin 构造 dlsym 解析）：
 *   int get_plugin_count()            — 返回 8
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
void usr_acc(int cfg, int in, std::vector<fins::rt::Message> hist, int &out) { (void)hist; out = in * cfg; spin_cost_us(1000);}
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

// 生成 5 个 C 工厂符号（get_plugin_count / get_algo_name / get_algo_version /
// create_algo / destroy_plugin），Plugin 构造 dlsym 解析 + 填 loaded_keys = {usr_*:1.0.0}
FINS_ALGO_EXPORT("1.0.0")
