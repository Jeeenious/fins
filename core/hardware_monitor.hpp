// ============================================================================
// HardwareMonitor — 定时触发 on_sample + 观测能力（observe_cpu/observe_mem 填调用方容器）
// ============================================================================
//
// 内部逻辑：
//   start() 起 1 个后台触发线程，每 interval_ms（init 配置，默认 1000ms）触发一次
//   on_sample 回调（计时周期完成）。**观测逻辑内置在组件**：observe_cpu() / observe_mem()
//   **把结果填入调用方传入的容器（每核 CPU 使用率 vector / 内存使用率 atomic），组件自身
//   不引用任何全局对象**——把结果写到 g_state 全局观测对象（core_usages_g / mem_usage_g）
//   由装配点在 on_sample 回调内把全局容器传给 observe_* 完成（组件不含业务、不依赖全局状态）。
//   observe_cpu()：读 /proc/stat 的 cpuN 行，与上一轮差值算每核 CPU 使用率 → 填 vector
//     （索引=核号；首轮只记基线不填——使用率须两次采样差值才有意义）。
//   observe_mem()：读 /proc/meminfo（MemTotal/MemAvailable）→ 返回使用率（失败返回 0）。
//   触发与观测分离：run() 只触发 on_sample（不自动采样），装配点回调内显式调 observe_*，
//   并决定传哪个全局容器（写全局 / 调度分析 / 打印）。
//   停止：stop() 置 stop_ 后 join 触发线程；sleep_for 期间置位最多延迟一个触发周期。
//   /proc 仅 Linux/WSL 存在；非 Linux 下读取失败则 CPU 不填 / mem 保持原值，不报错。
//
// 资源消耗：
//   - 线程：1 个后台触发线程（普通线程，不绑核）；空闲时 sleep_for 不占 CPU
//   - 观测（observe_cpu/observe_mem）：每轮读 /proc/stat（O(核数) 解析）+ /proc/meminfo，
//     写调用方容器（每核 1 个 float / 1 个 float）；无锁
//
// 对外接口（装配示例）：
//   HardwareMonitor::instance().init(1000.0f)    — 定触发周期 ms（默认 1000）
//   HardwareMonitor::instance().on_sample(cb)     — 每周期完成触发（start 前注册）
//   HardwareMonitor::instance().observe_cpu(core_usages_g)  — 填每核 CPU（索引=核号）
//   HardwareMonitor::instance().observe_mem()               — 观测内存，返回使用率
//   HardwareMonitor::instance().start()           — 起触发线程（幂等）
//   HardwareMonitor::instance().stop()            — 停触发线程（幂等，析构复用）
//   典型装配：
//     on_sample([] {
//       HardwareMonitor::instance().observe_cpu(core_usages_g);   // 填全局每核 CPU
//       mem_usage_g = HardwareMonitor::instance().observe_mem();  // 拿返回的内存使用率
//     });
// ============================================================================

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <fstream>
#include <functional>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "utils/logger.hpp"

namespace fins::rt {

class HardwareMonitor {
public:
  static HardwareMonitor &instance() {
    static HardwareMonitor inst;
    return inst;
  }
  HardwareMonitor(const HardwareMonitor &) = delete;
  HardwareMonitor &operator=(const HardwareMonitor &) = delete;

  /// 定触发周期（ms）。start 前调用。
  void init(float interval_ms = 1000.0f) { interval_ms_ = interval_ms; }

  /// 注册回调（start 前调用）：每周期完成（计时到点）触发；装配点回调内显式调 observe() 完成观测。
  void on_sample(std::function<void()> cb) { cb_ = std::move(cb); }

  /// 起 1 个后台触发线程（幂等：已在监听则忽略）。
  void start() {
    if (ticking_.joinable()) {
      FINS_LOG_WARN("[HardwareMonitor] already started, ignore.");
      return;
    }
    stop_ = false;
    ticking_ = std::thread([this] { run(); });
  }

  /// 停触发线程（幂等，析构复用；停止延迟最多一个触发周期）。
  void stop() {
    if (!ticking_.joinable()) return;
    stop_ = true;
    ticking_.join();
  }

  /// 观测每核 CPU 使用率（0~100，索引=核号），填入调用方给的 vector（首轮只记基线不填）。
  /// **组件不引用任何全局对象**——容器由装配点传入（如 core_usages_g），组件只填充调用方容器。
  void observe_cpu(std::vector<float> &core_usages) {
    core_usages.clear();
    std::vector<CpuStat> cur;
    if (read_cpu_stats(cur)) {
      if (have_prev_ && cur.size() == prev_cpu_.size()) {
        core_usages.resize(cur.size());
        for (size_t i = 0; i < cur.size(); ++i)
          core_usages[i] = cpu_usage_pct(prev_cpu_[i], cur[i]);
      }
      prev_cpu_ = std::move(cur);
      have_prev_ = true;
    }
  }

  /// 观测内存使用率（0~100），返回结果（失败返回 0）。**组件不引用任何全局对象**——
  /// 由装配点在 on_sample 回调内拿到返回值再写全局 / 消费。
  float observe_mem() {
    float v = 0.0f;
    read_mem_pct(v);
    return v;
  }

private:
  HardwareMonitor() = default;
  ~HardwareMonitor() { stop(); }

  struct CpuStat {
    unsigned long long user = 0, nice = 0, system = 0, idle = 0, iowait = 0,
                       irq = 0, softirq = 0, steal = 0;
  };

  /// 读 /proc/stat 的 cpuN 行（cpu 行在最前，读到非 cpu 前缀停）；cpuN 按 0,1,... 顺序 → 索引即核号。
  static bool read_cpu_stats(std::vector<CpuStat> &out) {
    std::ifstream ifs("/proc/stat");
    if (!ifs) return false;
    std::string line;
    while (std::getline(ifs, line)) {
      if (line.rfind("cpu", 0) != 0) break;
      if (line.size() > 3 && line[3] == ' ') continue;  // 跳过 "cpu " 总行
      std::istringstream iss(line);
      std::string tag;
      iss >> tag;
      CpuStat s;
      iss >> s.user >> s.nice >> s.system >> s.idle >> s.iowait
          >> s.irq >> s.softirq >> s.steal;
      out.push_back(s);
    }
    return !out.empty();
  }

  /// 读 /proc/meminfo → 内存使用率 %（1 - MemAvailable/MemTotal）。
  static bool read_mem_pct(float &out_pct) {
    std::ifstream ifs("/proc/meminfo");
    if (!ifs) return false;
    unsigned long long total = 0, avail = 0;
    std::string key, unit;
    unsigned long long val = 0;
    while (ifs >> key >> val >> unit) {
      if (key == "MemTotal:") total = val;
      else if (key == "MemAvailable:") avail = val;
      if (total && avail) break;
    }
    if (!total) return false;
    out_pct = (1.0f - (float)avail / (float)total) * 100.0f;
    return true;
  }

  /// 两轮 /proc/stat 差值的核使用率：(Δtotal − Δidle) / Δtotal。
  static float cpu_usage_pct(const CpuStat &a, const CpuStat &b) {
    const auto ta = a.user + a.nice + a.system + a.idle + a.iowait + a.irq + a.softirq + a.steal;
    const auto tb = b.user + b.nice + b.system + b.idle + b.iowait + b.irq + b.softirq + b.steal;
    const auto ia = a.idle + a.iowait;
    const auto ib = b.idle + b.iowait;
    const unsigned long long dt = tb > ta ? tb - ta : 0;
    const unsigned long long di = ib > ia ? ib - ia : 0;
    if (dt == 0) return 0.0f;
    return (float)(dt - di) / (float)dt * 100.0f;
  }

  void run() {
    while (!stop_.load()) {
      if (cb_) cb_();
      if (stop_.load()) break;
      std::this_thread::sleep_for(std::chrono::milliseconds((long long)interval_ms_));
    }
  }

  std::atomic<bool> stop_{false};
  std::thread ticking_;
  float interval_ms_{1000.0f};
  std::function<void()> cb_;
  std::vector<CpuStat> prev_cpu_;
  bool have_prev_ = false;
};

}  // namespace fins::rt
