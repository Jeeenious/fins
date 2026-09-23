/*******************************************************************************
 * plugins.cpp — 用户算法插件（usr_* 函数族，编译成 plugins/plugin.so 供 loader 装载）
 *
 * 设计约定（configs-first，供负载/拓扑实验）：
 *   配置段三个参数（JSON configs 的 c{i}_0 / c{i}_1 / c{i}_2，位置式）：
 *     name = 节点 id（tracepoint 用）
 *     load  = **本拍给定的执行用时**（µs，独占核心自旋），真实占用 CPU
 *     mesg = **载荷字节数**：输出 resize 到该大小（逐节点可配，取代原编译期常量）
 *     执行用时是"给定值"：先做载荷产出（resize/赋值）并测出它已消耗的线程 CPU 时间，
 *     再自旋补足差额 → 每拍总执行用时恒等于 load，与载荷大小无关（spin_cost_us 见下）。
 *     ⚠ mesg 越大，载荷产出本身越贵；若它吃掉全部预算，spin_cost_us 会如实上报实际消耗
 *     （不再补自旋），该拍实际用时将**超过** load —— 生成侧须保证 load 大于载荷成本。
 *   · 值路由已不需要：这些算法只提供「端口形状 + 烧算力」，o1/i1 只为形状与数据流存在。
 *   · 签名类别约定：配置 = 按值小标量（name 用 const 引用）；**输入一律 const T&**、
 *     输出一律 T&。输入按值会在每拍深拷整份载荷（mesg 大时每输入每拍都拷一份）。
 *   · 反馈节点入参为 fins::rt::History<std::string>（字段历史槽的最近 N 帧只读视图，零 payload
 *     拷贝）；pipeline load 用 "hist":[{端口:N}] 声明各窗口。
 *
 * 形状族（每个 = 一个可导出的具体函数，供 pipeline load 的 [name:1.0.0] 定位）：
 *   基础    src(0→1) / sink(1→0) / relay(1→1)
 *   扇出    1→k        usr_fork(=2)、usr_fork3..9、usr_fork10(=10)  —— 多速率 fork 类按 fan 选用
 *   扇入    k→1        usr_join(=2)、usr_join3..9、usr_join10(=10)  —— 多速率 join 类按 fan 选用
 *   反馈    1 feed + k loop（k=1..10）：usr_acc(=1)  ..  usr_acc10   —— feedback 类随机窗口 N
 *   合计 31 个算法。
 *
 *   负载/拓扑入口见 tool/uload.ipynb（gen_class 五类：多速率 multihop / fork / join、
 *   feedback、mixed；每图内部混合周期 timed 与事件 event 任务）。周期/事件(激活次数 n_j) 与
 *   目标利用率 u 的分配都在生成侧完成——本族算法只作为“按 load 烧掉对应 µs”的端口形状载体。
 *
 * 对外接口（FINS_ALGO_EXPORT 由 X-macro 生成 5 个 C 工厂符号）：
 *   get_plugin_count / get_algo_name / get_algo_version(恒 "1.0.0") / create_algo /
 *   destroy_plugin —— Plugin 构造 dlsym 解析后填 loaded_keys = {usr_*:1.0.0}。
 ******************************************************************************/
#include "trace/algo_tracepoints.hpp"
#include "core/xmacro.hpp"

#include <chrono>
#include <ctime>
#include <vector>

namespace {

  // 忙等：独占核心纯自旋固定 us（循环读 steady_clock，不会被优化掉；比 sleep_for 少唤醒抖动）
  inline long long thread_cpu_time_us() {
    struct timespec ts{};

    clock_gettime(CLOCK_THREAD_CPUTIME_ID, &ts);

    return static_cast<long long>(ts.tv_sec) * 1'000'000LL + static_cast<long long>(ts.tv_nsec) / 1'000LL;
  }

  // 忙等补时：把本 job 的线程 CPU 时间**补足到 budget_us**。
  //
  //   budget_us = 该节点给定的执行用时 load，来自 wcet 模型）——执行用时是**给定的**，
  //               不随载荷大小漂移；
  //   spent_us  = 调用前本 job 已消耗的线程 CPU 时间（载荷产出 resize/赋值等，由调用方测）。
  //
  // 自旋只补差额 budget_us − spent_us；working 上报 = spent_us + 自旋量 ≈ budget_us，
  // 故时间线上"节点执行用时"恒等于给定值，与载荷成本无关（载荷只是把这段预算提前花掉）。
  void spin_cost_us(const std::string& name, long long budget_us, long long spent_us) {

    const long long need = budget_us - spent_us; // 还需补足的自旋量

    if (need <= 0) {
      // 载荷已吃掉全部预算（甚至超出）：不补自旋，仍上报实际消耗，便于发现预算偏小
      if (spent_us > 0)
        tracepoint(
              algo,
              working,
              name.c_str(),
              sched_getcpu(),
              spent_us);
      return;
    }

    // 补时起点（相对量：只关心这段自旋自身消耗的 CPU 时间）
    const long long start_cpu_us = thread_cpu_time_us();

    // 当前 execution segment 的起点
    long long segment_start_cpu_us = start_cpu_us;

    // 当前所在 CPU
    int segment_cpu = sched_getcpu();

    // 首段要把载荷的 spent_us 一并计入上报值（各段合计 = budget_us）
    bool first_segment = true;

    while (true) {
      const long long current_cpu_us = thread_cpu_time_us();
      const long long total_cpu_us = current_cpu_us - start_cpu_us;

      // 1. 检查任务是否已经获得足够的 CPU execution time
      if (total_cpu_us >= need) {
        const long long segment_cpu_us = need - (segment_start_cpu_us - start_cpu_us);
        const long long reported_us = segment_cpu_us + (first_segment ? spent_us : 0);
        if (reported_us > 0) {
          tracepoint(
                algo,
                working,
                name.c_str(),
                segment_cpu,
                reported_us);
        }
        break;
      }

      // 2. 检查 CPU 是否发生 migration
      const int current_cpu = sched_getcpu();
      if (current_cpu != segment_cpu) {
        const long long segment_cpu_us = current_cpu_us - segment_start_cpu_us;
        const long long reported_us = segment_cpu_us + (first_segment ? spent_us : 0);
        if (reported_us > 0) {
          tracepoint(
                algo,
                working,
                name.c_str(),
                segment_cpu,
                reported_us);
        }
        first_segment = false;
        segment_cpu = current_cpu;
        segment_start_cpu_us = current_cpu_us;
      }
    }
  }

  // ── 基础：源 / 汇 / 链 ──
  void usr_src(const std::string& name, int load, int mesg,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  } // 0 输入 / 1 输出
  void usr_sink(const std::string& name, int load, [[maybe_unused]] int mesg,
    const std::string &i1) {
    tracepoint(algo, execute, name.c_str());

    spin_cost_us(name, load, 0);

    tracepoint(algo, complete, name.c_str());
  } // 1 输入 / 0 输出
  void usr_relay(const std::string& name, int load, int mesg,
    const std::string &i1,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  } // 1 输入 / 1 输出

  // ── 扇出 1→k（k=2..10）──
  void usr_fork(const std::string& name, int load, int mesg,
    const std::string &i1,
    std::string &o1,
    std::string &o2) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);
    o2.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_fork3(const std::string& name, int load, int mesg,
    const std::string &i1,
    std::string &o1,
    std::string &o2,
    std::string &o3) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);
    o2.resize(mesg);
    o3.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_fork4(const std::string& name, int load, int mesg,
    const std::string &i1,
    std::string &o1,
    std::string &o2,
    std::string &o3,
    std::string &o4) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);
    o2.resize(mesg);
    o3.resize(mesg);
    o4.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_fork5(const std::string& name, int load, int mesg,
    const std::string &i1,
    std::string &o1,
    std::string &o2,
    std::string &o3,
    std::string &o4,
    std::string &o5) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);
    o2.resize(mesg);
    o3.resize(mesg);
    o4.resize(mesg);
    o5.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_fork6(const std::string& name, int load, int mesg,
    const std::string &i1,
    std::string &o1,
    std::string &o2,
    std::string &o3,
    std::string &o4,
    std::string &o5,
    std::string &o6) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);
    o2.resize(mesg);
    o3.resize(mesg);
    o4.resize(mesg);
    o5.resize(mesg);
    o6.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_fork7(const std::string& name, int load, int mesg,
    const std::string &i1,
    std::string &o1,
    std::string &o2,
    std::string &o3,
    std::string &o4,
    std::string &o5,
    std::string &o6,
    std::string &o7) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);
    o2.resize(mesg);
    o3.resize(mesg);
    o4.resize(mesg);
    o5.resize(mesg);
    o6.resize(mesg);
    o7.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_fork8(const std::string& name, int load, int mesg,
    const std::string &i1,
    std::string &o1,
    std::string &o2,
    std::string &o3,
    std::string &o4,
    std::string &o5,
    std::string &o6,
    std::string &o7,
    std::string &o8) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);
    o2.resize(mesg);
    o3.resize(mesg);
    o4.resize(mesg);
    o5.resize(mesg);
    o6.resize(mesg);
    o7.resize(mesg);
    o8.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_fork9(const std::string& name, int load, int mesg,
    const std::string &i1,
    std::string &o1,
    std::string &o2,
    std::string &o3,
    std::string &o4,
    std::string &o5,
    std::string &o6,
    std::string &o7,
    std::string &o8,
    std::string &o9) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);
    o2.resize(mesg);
    o3.resize(mesg);
    o4.resize(mesg);
    o5.resize(mesg);
    o6.resize(mesg);
    o7.resize(mesg);
    o8.resize(mesg);
    o9.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_fork10(const std::string& name, int load, int mesg,
    const std::string &i1,
    std::string &o1,
    std::string &o2,
    std::string &o3,
    std::string &o4,
    std::string &o5,
    std::string &o6,
    std::string &o7,
    std::string &o8,
    std::string &o9,
    std::string &o10) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);
    o2.resize(mesg);
    o3.resize(mesg);
    o4.resize(mesg);
    o5.resize(mesg);
    o6.resize(mesg);
    o7.resize(mesg);
    o8.resize(mesg);
    o9.resize(mesg);
    o10.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }

  // ── 扇入 k→1（k=2..10）──
  void usr_join(const std::string& name, int load, int mesg,
    const std::string &i1,
    const std::string &i2,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_join3(const std::string& name, int load, int mesg,
    const std::string &i1,
    const std::string &i2,
    const std::string &i3,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_join4(const std::string& name, int load, int mesg,
    const std::string &i1,
    const std::string &i2,
    const std::string &i3,
    const std::string &i4,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_join5(const std::string& name, int load, int mesg,
    const std::string &i1,
    const std::string &i2,
    const std::string &i3,
    const std::string &i4,
    const std::string &i5,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_join6(const std::string& name, int load, int mesg,
    const std::string &i1,
    const std::string &i2,
    const std::string &i3,
    const std::string &i4,
    const std::string &i5,
    const std::string &i6,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_join7(const std::string& name, int load, int mesg,
    const std::string &i1,
    const std::string &i2,
    const std::string &i3,
    const std::string &i4,
    const std::string &i5,
    const std::string &i6,
    const std::string &i7,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_join8(const std::string& name, int load, int mesg,
    const std::string &i1,
    const std::string &i2,
    const std::string &i3,
    const std::string &i4,
    const std::string &i5,
    const std::string &i6,
    const std::string &i7,
    const std::string &i8,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_join9(const std::string& name, int load, int mesg,
    const std::string &i1,
    const std::string &i2,
    const std::string &i3,
    const std::string &i4,
    const std::string &i5,
    const std::string &i6,
    const std::string &i7,
    const std::string &i8,
    const std::string &i9,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_join10(const std::string& name, int load, int mesg,
    const std::string &i1,
    const std::string &i2,
    const std::string &i3,
    const std::string &i4,
    const std::string &i5,
    const std::string &i6,
    const std::string &i7,
    const std::string &i8,
    const std::string &i9,
    const std::string &i10,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }

  // ── 反馈：1 外部输入 feed + k 路 loop 反馈（k=1..10），单输出 ──
  //   每路 loop 入参 = 本节点输出最近 N 帧的只读视图 load 以 "loop":{h1:N1,...} 声明。
  void usr_acc(const std::string& name, int load, int mesg,
    const std::string &i1,
    const fins::rt::History<std::string>
    &hist, std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_acc2(const std::string& name, int load, int mesg,
    const std::string &i1,
    const fins::rt::History<std::string> &h1,
    const fins::rt::History<std::string> &h2,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_acc3(const std::string& name, int load, int mesg,
    const std::string &i1,
    const fins::rt::History<std::string> &h1,
    const fins::rt::History<std::string> &h2,
    const fins::rt::History<std::string> &h3,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_acc4(const std::string& name, int load, int mesg,
    const std::string &i1,
    const fins::rt::History<std::string> &h1,
    const fins::rt::History<std::string> &h2,
    const fins::rt::History<std::string> &h3,
    const fins::rt::History<std::string> &h4,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_acc5(const std::string& name, int load, int mesg,
    const std::string &i1,
    const fins::rt::History<std::string> &h1,
    const fins::rt::History<std::string> &h2,
    const fins::rt::History<std::string> &h3,
    const fins::rt::History<std::string> &h4,
    const fins::rt::History<std::string> &h5,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_acc6(const std::string& name, int load, int mesg,
    const std::string &i1,
    const fins::rt::History<std::string> &h1,
    const fins::rt::History<std::string> &h2,
    const fins::rt::History<std::string> &h3,
    const fins::rt::History<std::string> &h4,
    const fins::rt::History<std::string> &h5,
    const fins::rt::History<std::string> &h6,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_acc7(const std::string& name, int load, int mesg,
    const std::string &i1,
    const fins::rt::History<std::string> &h1,
    const fins::rt::History<std::string> &h2,
    const fins::rt::History<std::string> &h3,
    const fins::rt::History<std::string> &h4,
    const fins::rt::History<std::string> &h5,
    const fins::rt::History<std::string> &h6,
    const fins::rt::History<std::string> &h7,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_acc8(const std::string& name, int load, int mesg,
    const std::string &i1,
    const fins::rt::History<std::string> &h1,
    const fins::rt::History<std::string> &h2,
    const fins::rt::History<std::string> &h3,
    const fins::rt::History<std::string> &h4,
    const fins::rt::History<std::string> &h5,
    const fins::rt::History<std::string> &h6,
    const fins::rt::History<std::string> &h7,
    const fins::rt::History<std::string> &h8,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_acc9(const std::string& name, int load, int mesg,
    const std::string &i1,
    const fins::rt::History<std::string> &h1,
    const fins::rt::History<std::string> &h2,
    const fins::rt::History<std::string> &h3,
    const fins::rt::History<std::string> &h4,
    const fins::rt::History<std::string> &h5,
    const fins::rt::History<std::string> &h6,
    const fins::rt::History<std::string> &h7,
    const fins::rt::History<std::string> &h8,
    const fins::rt::History<std::string> &h9,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }
  void usr_acc10(const std::string& name, int load, int mesg,
    const std::string &i1,
    const fins::rt::History<std::string> &h1,
    const fins::rt::History<std::string> &h2,
    const fins::rt::History<std::string> &h3,
    const fins::rt::History<std::string> &h4,
    const fins::rt::History<std::string> &h5,
    const fins::rt::History<std::string> &h6,
    const fins::rt::History<std::string> &h7,
    const fins::rt::History<std::string> &h8,
    const fins::rt::History<std::string> &h9,
    const fins::rt::History<std::string> &h10,
    std::string &o1) {
    tracepoint(algo, execute, name.c_str());

    const long long t0_cpu_us = thread_cpu_time_us();

    o1.resize(mesg);

    spin_cost_us(name, load, thread_cpu_time_us() - t0_cpu_us);

    tracepoint(algo, complete, name.c_str());
  }

#undef USR_SPIN_BODY

} // namespace

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
  F(usr_acc10)

// 生成 5 个 C 工厂符号（get_plugin_count / get_algo_name / get_algo_version /
// create_algo / destroy_plugin），Plugin 构造 dlsym 解析 + 填 loaded_keys = {usr_*:1.0.0}
FINS_ALGO_EXPORT("1.0.0")
