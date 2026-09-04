// ============================================================================
// agent_main — 独立功能测试主程序（接收外部 JSON 配置启动数据流调度）
// ============================================================================
//
// 资源消耗：
//   - 线程：main 主线程（调度循环）+ 1 个计时线程（时间点释放）+ 2 个绑核 worker
//           （thread_pool start 参数）+ RPC 1 个监听线程 + HTTP 请求并发池（start 参数）
//           + PluginLoader 1 个监听线程 + 文件事件回调派发池（start 参数）
//   - 图/实例：随 JSON 配置 expand_hp 建图；每 job 实例 1 个 AlgoBase + 1 闭包
//   - .so：插件目录下每个 .so 装载为 1 个 Plugin（dlopen handle + C 工厂）
//
// 对外接口（装配示例）：
//   agent_main [rpc_port=18080] [plugin_dir=./lib] [num_workers=2]
//   PluginLoader::on_library_add/modify/delete(handler) — 增量增/改/删回调（装配点 SET/ERASE）
//   PluginLoader::init(plugin_dir) / start(num_threads) — 初始化目录 / 启动监听（存量装载 main 开头做）
//   RPCListener::on_pipeline_update("/update", handler) — 建图路由（存 JSON + pending 置位）
//   RPCListener::init(host,port,chost,cport) / start(num_threads) — 监听
//   ThreadPool::on_execute(cb) / start(n) / stop() — worker 带锁单步事务（cb 收线程序号 wid，
//                                                      计时聚合按线程独立存 TBBMap[wid]）
//   graph_g.mtx / cv / stopped / pending — 调度公开成员（装配点直读直写）
//   graph_g.check_algo_ready(pipeline_g, library_g) — 插件就绪检查（expand 前；false = 算法未全部注册）
//   graph_g.expand_hp(pipeline_g, library_g) — 建图唯一入口（主线程循环持 mtx 调）
//   graph_g.grab_delay_workload() — 计时线程拉最近待释放时间点（拿到 tp 后与 worker 对称：
//                                     锁外执行 tp->job() 即 sleep_until 到点 → 回锁置 Finished + notify）
// ============================================================================

#define FINS_EXPORT_TRACING_PATH "./tool/temp/tracing.csv"   // 导出目录固定 tool/temp（相对启动 cwd=仓库根）
#define FINS_EXPORT_DGRAPH_PATH "./tool/temp/dag.json"

#define FINS_STATIC_PRIORITY 0                                  // 1 = 静态优先级
#define FINS_DYNAMIC_PRIORITY 0                                 // 1 = 动态优先级
#define FINS_PRIORITY_POLICY fins::sched::Policy::EDF           // 换策略改这一行（RM/DM/SJF/LJF/DENSITY/DEPTH/HEIGHT/LLF...）

#define FINS_CAL_MAKESPAN 0                                     // 1 = rollover 算 makespan 上界并告警过载
#define FINS_MAKESPAN_METHOD fins::sched::MakespanMethod::MPB   // 估计方法（GRAHAM/MPB）

#define FINS_CAL_WCET 0                                         // 1 = rollover 用执行历史自整定 wcet
#define FINS_WCET_METHOD fins::sched::WcetMethod::PQUANTILE     // 估计方法（HWM/PQUANTILE）

#define FINS_ALGO_LIB_PATH "./lib"   // 插件目录（可通过命令行参数覆盖）
#define FINS_CLIENT_IP "0.0.0.0"     // orchestrator 上报端口（可通过命令行参数覆盖）
#define FINS_CLIENT_PORT 18080
#define FINS_SERVER_IP "0.0.0.0"    // orchestrator 监听端口（可通过命令行参数覆盖）
#define FINS_SERVER_PORT 18080

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <vector>
#include "utils/form.hpp"
#include "utils/tracing.hpp"
#include "utils/logger.hpp"
#include "g_state.hpp"
#include "RPC_listener.hpp"
#include "hardware_monitor.hpp"
#include "plugin_loader.hpp"
#include "thread_pool.hpp"
#include "schedule/makespan_updater.hpp"
#include "schedule/wcet_updater.hpp"
#include "schedule/priority_updater.hpp"

using namespace fins::rt;
namespace fs = std::filesystem;

int main(int argc, char **argv) {
  const int rpc_port = argc > 1 ? std::atoi(argv[1]) : FINS_CLIENT_PORT;
  const std::string plugin_dir = argc > 2 ? argv[2] : FINS_ALGO_LIB_PATH;
  int num_workers = argc > 3 ? std::atoi(argv[3]) : 2;   // 线程池 worker 数；非法/≤0 回落默认 2
  if (num_workers < 1) num_workers = 2;

  // 导出目录先行自建：tracing.csv / dag.json 均写 ./tool/temp（相对启动 cwd），缺目录 ofstream 静默失败
#ifdef FINS_EXPORT_TRACING_PATH
  fs::create_directories(fs::path(FINS_EXPORT_TRACING_PATH).parent_path());
#endif

  // ── 装配 wcet_updater：FINS_WCET_METHOD 方法（PQUANTILE = 99% 分位 + 20% 裕度）。
  wcet_updater = fins::sched::make_wcet_updater(FINS_WCET_METHOD);
  // ── 装配 makespan_updater：FINS_MAKESPAN_METHOD 方法（MPB）。
  makespan_updater = fins::sched::make_makespan_updater(FINS_MAKESPAN_METHOD,
      [] { return graph_g.graph_version; },
      [num_workers] { return num_workers; });

  // ── 装配 priority_updater：FINS_PRIORITY_POLICY 策略（默认 EDF 动态优先级）。
  priority_updater = fins::sched::make_priority(FINS_PRIORITY_POLICY,
    [] { return graph_g.graph_version; },
    [num_workers] { return num_workers; });

  // ── 临时：插件加载：全局插件初始化（存量扫描装载）──
  try {
    for (const auto &entry : fs::recursive_directory_iterator(
             plugin_dir, fs::directory_options::skip_permission_denied)) {
      const std::string p = entry.path().string();
      auto ctx = std::make_shared<Plugin>(p);   // 构造装载：dlopen + 解析符号 + 填 keys
      TBBMAP_SET(library_g.so_ctx, p, ctx);
      FINS_LOG_INFO("[agent] lib add: {}", p);
    }
  } catch (...) {}

  // ── 装配 PluginLoader：loader 只做库机制（增量增/改/删事件回调注入），存量装载上面已做 ──
  PluginLoader::instance().on_library_add([](const std::string &so, const std::shared_ptr<Plugin>& ctx) {
    // 新增 .so：ctx 已构造装载，直接 SET
    TBBMAP_SET(library_g.so_ctx, so, ctx);
    FINS_LOG_INFO("[agent] lib add: {}", so);
    graph_g.cv.notify_all();   // 库变更 → 唤醒主循环即时重查插件就绪（defer 自动恢复）
  });
  PluginLoader::instance().on_library_modify([](const std::string &so, const std::shared_ptr<Plugin>& ctx) {
    std::shared_ptr<Plugin> old;   // 局部拷贝：宏作用域外 shared_ptr 引用计数保活
    TBBMAP_READ(library_g.so_ctx, so, [&](const auto &v) { old = v; });
    if (old) old->take_keys();
    TBBMAP_ERASE(library_g.so_ctx, so);
    TBBMAP_SET(library_g.so_ctx, so, ctx);
    FINS_LOG_INFO("[agent] lib modify: {}", so);
    graph_g.cv.notify_all();   // 库变更 → 唤醒主循环即时重查插件就绪（defer 自动恢复）
  });
  PluginLoader::instance().on_library_delete([](const std::string &so) {
    std::shared_ptr<Plugin> old;   // 局部拷贝：宏作用域外 shared_ptr 引用计数保活
    TBBMAP_READ(library_g.so_ctx, so, [&](const auto &v) { old = v; });
    if (old) old->take_keys();
    TBBMAP_ERASE(library_g.so_ctx, so);
    FINS_LOG_INFO("[agent] lib delete: {}", so);
    graph_g.cv.notify_all();   // 库变更 → 唤醒主循环即时重查插件就绪
  });
  PluginLoader::instance().init(plugin_dir);
  PluginLoader::instance().start(4);

  // ── 装配 RPCListener：收包只存 JSON + 置 pending（解析/建图由主线程循环做）──
  RPCListener::instance().on_pipeline_update("/update",
    [](const nlohmann::json &j) {
      std::lock_guard lk(pipeline_g.wr_lock());
      pipeline_g.cache.write() = j;
      graph_g.pending = true;
      graph_g.cv.notify_all();
      FINS_LOG_INFO("[agent] pipeline modified");
    });
  RPCListener::instance().on_library_add("/plugin/add", plugin_dir, [](const std::string &) {});
  RPCListener::instance().on_library_modify("/plugin/modify", plugin_dir, [](const std::string &) {});
  RPCListener::instance().on_library_delete("/plugin/delete", plugin_dir, [](const std::string &) {});
  RPCListener::instance().init(FINS_CLIENT_IP, rpc_port, FINS_SERVER_IP, FINS_SERVER_PORT);
  RPCListener::instance().start(4);

  // ── 装配 HardwareMonitor：组件定时触发 on_sample，回调内显式调 observe() 写全局
  HardwareMonitor::instance().init(1000.0f);
  HardwareMonitor::instance().on_sample([] {
    // 装配点业务：observe() 输出参数 → 写全局观测对象（组件本身不操作全局）
    HardwareMonitor::instance().observe_cpu(core_usages_g);
    mem_usage_g = HardwareMonitor::instance().observe_mem();
  });
  HardwareMonitor::instance().start();

  // ── 装配 ThreadPool worker：带锁单步事务（拉取 → 锁外执行 → 回锁直做完成事件）──
  ThreadPool::instance().on_execute([]() -> bool {

    std::unique_lock lk(graph_g.mtx);
    for (;;) {
      if (graph_g.stopped.load())
        return false;

      if (auto w = graph_g.grab_ready_workload()) {

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::WAKE);   // 唤醒（含首轮 = 线程启动）
#endif

        lk.unlock();

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::RELEASE, w->id);   // 执行（job 开始）
#endif

        w->job();

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::FINISHED, w->id);   // 结束（job 完成）
#endif

        lk.lock();

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::TEMP_1, w->id);   // 结束（job 完成）
#endif

        graph_g.trigger_workload_ready(w->id);   // 回锁直做完成事件：置 done + 传播 pred_left + 入 ready

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::TEMP_2, w->id);   // 结束（job 完成）
#endif

        graph_g.cv.notify_all();   // 有新增就绪才唤醒(叶子/无后继完成的完成不再空唤醒全池 → 减惊群与锁抖动)

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::SLEEP, w->id);   // 结束（job 完成）
#endif

        return true;
      }

      graph_g.cv.wait_for(lk, std::chrono::milliseconds(1));   // 等完成/回绕/expand_hp/停止（notify 快路径 + 1ms 超时兜底 lost wakeup）
    }
  });
  ThreadPool::instance().start(num_workers);

  // ── 停止信号：SIGINT/SIGTERM → 只置原子停止位（async-signal-safe；不调 cv.notify_all——
  //    condition_variable 非 async-signal-safe，信号上下文调 stdlib 是 UB）。
  //    主循环/worker/计时线程全部 cv.wait_for(1ms) 兜底超时，1ms 内自行醒来看到 stopped 退出。──
  std::signal(SIGINT,  [](int) { graph_g.stopped = true; });
  std::signal(SIGTERM, [](int) { graph_g.stopped = true; });
  FINS_LOG_INFO("[agent] listening on :{} plugin_dir={}", rpc_port, plugin_dir);

  // ── 计时线程：与 worker 完全对称——grab tp 延迟时间点 → 锁外执行其 sleep job → 回锁置 Finished
  //    + notify（与 worker 完成事件共同唤醒主线程调度循环）。tp 顶点在 pin_sync 建图时已写入
  //    job = sleep_until（绝对释放时刻，job 内实时读 hyper_start_ms → rollover 平移自动对齐）──
  std::thread timer_th([&] {
    std::unique_lock tl(graph_g.mtx);
    while (!graph_g.stopped.load()) {
      if (const auto tp = graph_g.grab_delay_workload()) {

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::WAKE, "timer");   // 结束（job 完成）
#endif

        tl.unlock();

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::RELEASE, "timer");   // 结束（job 完成）
#endif

        tp->job();                              // 锁外执行：sleep_until 睡到释放时刻（延迟实现）

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::FINISHED, "timer");   // 结束（job 完成）
#endif

        tl.lock();
        graph_g.trigger_workload_ready(tp->id);   // 回锁直做完成事件：置 done + 传播 pred_left（释放后继 job 顶点）
        graph_g.cv.notify_all();   // 有新增就绪才唤醒(空时间点不空唤醒)

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::SLEEP, "timer");   // 结束（job 完成）
#endif

        continue;
      }
      graph_g.cv.wait_for(tl, std::chrono::milliseconds(1));   // 无待释放时间点 → 等事件（notify 快路径 + 1ms 超时兜底 lost wakeup）
    }
  });

  {
    std::unique_lock lk(graph_g.mtx);
    while (!graph_g.stopped.load()) {
      if (graph_g.is_hp_done() && graph_g.pending.load()) {

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::WAKE, "main::expand");   // 唤醒（含首轮 = 线程启动）
        fins::util::trace_record(fins::util::TraceKind::RELEASE, "main::expand");   // 释放（取到 job）
#endif

        nlohmann::json cfg;
        {
          std::lock_guard wlk(pipeline_g.wr_lock());   // 与 RPC handler 同锁：cache 写/commit/read 串行
          pipeline_g.cache.commit();
          cfg = pipeline_g.cache.read();               // 拷贝激活份，锁内不留引用
        }

        try {
          pipeline_g.parse_pipeline(cfg);
          pipeline_g.check_topology();
        } catch (const std::exception &e) {
          graph_g.pending = false;   // 非法配置：丢弃（不重试）
          FINS_LOG_ERROR("[agent] pipeline parse/topology failed: {}", e.what());
          graph_g.cv.notify_all();
          continue;
        }

        // 算法就绪检查（独立于 expand_hp）：pipeline 引用键须全部在插件库已注册键中；
        // 缺失 → 保留 pending 等热加载（on_library_* notify 唤醒重查），不丢弃配置
        {
          const auto pipe_keys = pipeline_g.algo_keys();
          const auto lib_keys  = library_g.algo_keys();   // 全部已注册算法键（去重集合）
          bool ready = true;
          for (const auto &key : pipe_keys)
            if (!lib_keys.contains(key)) { ready = false; break; }
          if (!ready) {
            FINS_LOG_INFO("[agent] algo not ready, defer (pending kept, wait plugin load)");
            graph_g.cv.wait_for(lk, std::chrono::milliseconds(500));   // 等热加载 notify 唤醒重试
            continue;   // 保留 pending，不丢弃配置
          }
        }


        try {
          graph_g.expand_hp(pipeline_g, library_g);
          FINS_LOG_INFO("[agent] pipeline applied: {} vertices", graph_g.dag.size());
        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[agent] pipeline apply failed: {}", e.what());
        }

        graph_g.pending = false;   // 已应用（成功/失败均清除，勿残留导致 commit 翻到未写份交替重建）
        graph_g.cv.notify_all();

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::FINISHED, "main::expand");   // 释放（取到 job）
        fins::util::trace_record(fins::util::TraceKind::SLEEP, "main::expand");   // 释放（取到 job）
#endif

        continue;
      }
      if (!graph_g.is_hp_empty() && graph_g.is_hp_done()) {   // 有超周期才回绕（一次性图保持静止，防清 done_ 后 is_hp_done 变 false → 新配置永不 apply）

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::WAKE, "main::rollover");   // 唤醒（含首轮 = 线程启动
        fins::util::trace_record(fins::util::TraceKind::RELEASE, "main::rollover");   // 释放（取到 job）
#endif

        graph_g.rollover_hp();

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::FINISHED, "main::rollover");   // 紧贴 rollover_hp 后：量纯 rollover 用时（不含 notify_all）
#endif

        graph_g.cv.notify_all();

#ifdef FINS_EXPORT_TRACING_PATH
        fins::util::trace_record(fins::util::TraceKind::SLEEP, "main::rollover");
#endif

        continue;
      }
      graph_g.cv.wait_for(lk, std::chrono::milliseconds(1));   // 纯事件等待（notify 快路径 + 1ms 超时兜底 lost wakeup）
    }
  }

  // ── 回收：先置停止位唤醒全部线程 → 计时线程 → worker → 组件 stop ──
  graph_g.stopped = true;
  graph_g.cv.notify_all();
  FINS_LOG_INFO("[agent] teardown: join timer thread");
  if (timer_th.joinable()) timer_th.join();
  FINS_LOG_INFO("[agent] teardown: stop thread pool");
  ThreadPool::instance().stop();
  FINS_LOG_INFO("[agent] teardown: stop remote call listener");
  RPCListener::instance().stop();
  FINS_LOG_INFO("[agent] teardown: stop plugin dir watchdog");
  PluginLoader::instance().stop();
  FINS_LOG_INFO("[agent] teardown: stop hardware monitor");
  HardwareMonitor::instance().stop();

#ifdef FINS_EXPORT_TRACING_PATH
  fins::util::trace_export(FINS_EXPORT_TRACING_PATH);
#endif

  FINS_LOG_INFO("[agent] bye");
  return 0;
}
