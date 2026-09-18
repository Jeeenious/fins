#include "trace/core_tracepoints.hpp"

#define FINS_EXPORT_DGRAPH_PATH "./tool/temp/dag.json"

#define FINS_STATIC_PRIORITY 0                                  // 1 = 静态优先级
#define FINS_DYNAMIC_PRIORITY 0                                 // 1 = 动态优先级
#define FINS_PRIORITY_POLICY fins::sched::Policy::EDF           // 换策略改这一行（RM/DM/SJF/LJF/DENSITY/DEPTH/HEIGHT/LLF...）

#define FINS_CAL_MAKESPAN 0                                     // 1 = rollover 算 makespan 上界并告警过载
#define FINS_MAKESPAN_METHOD fins::sched::MakespanMethod::MPB   // 估计方法（GRAHAM/MPB）

#define FINS_CAL_WCET 0                                         // 1 = rollover 用执行历史自整定 wcet
#define FINS_WCET_METHOD fins::sched::WcetMethod::PQUANTILE     // 估计方法（HWM/PQUANTILE）

#define FINS_ROLLOVER_LATE_REANCHOR 1   // 排空晚于边界（过载）时的翻页策略：
                                        //   0 = 严格等下一拍：跳到下一未来网格边界（跳漏拍、空等、相位不漂移）
                                        //   1 = 提前到这一拍：立刻以此刻为新起点重启（相位重置、无空闲空洞、之后按新节拍 now+j·H 跑）

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
#include "core/utils/form.hpp"
#include "core/utils/logger.hpp"
#include "core/g_state.hpp"
#include "core/RPC_listener.hpp"
#include "core/hardware_monitor.hpp"
#include "core/plugin_loader.hpp"
#include "core/thread_pool.hpp"
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

      tracepoint(fins, wake);

      if (auto w = graph_g.grab_ready_workload()) {

        lk.unlock();

        tracepoint(fins, release);

        // ★ 异常兜底：job 抛异常也必须走下面的完成事件。否则该顶点永不完成 → is_hp_done()
        //   永为假 → 主循环永不 rollover_hp() → 整个超周期静默停摆（实测只留几行 ERROR，
        //   之后零翻页）。记日志后继续，让问题暴露在日志里而不是卡死调度。
        try {
          w->job();
        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[agent] job {} threw: {}", w->id, e.what());
        } catch (...) {
          FINS_LOG_ERROR("[agent] job {} threw unknown exception", w->id);
        }

        tracepoint(fins, finished);

        lk.lock();

        // ★ 唤醒条件：① 本完成新增了就绪后继 → 唤醒空闲 worker 帮忙吃 batch（叶子/无后继的完成
        //   不空唤醒全池，减惊群）；② 本完成让超周期完工 → 唤醒主循环翻页/应用新配置。②不可省：
        //   叶子节点无后继，完工事件否则无人通知，主循环只能等自身超时兜底才发现 is_hp_done()，
        //   翻页被推迟 → 释放间隔不再等于标称周期（实测 +0.56ms 且全为正）。
        if (graph_g.trigger_workload_ready(w->id) || graph_g.is_hp_done())
          graph_g.cv.notify_all();

        continue;   // ★ 积压快路径：不回池重进(免 ThreadPool 再调 cb + 重新抢锁)，持锁回循环顶
                    //   直接再 grab_ready_workload()——有积压立刻接着抓；只有 grab 取空(无积压)
                    //   才落到下方 wait_for 睡觉。执行期已 unlock，其他 worker 仍可趁隙取任务，
                    //   不损多核并行。返回 false 仅当 stopped。
      }

      tracepoint(fins, sleep);

      // grab 取空（无积压）→ 睡（10ms 兜底）。★ 这里 w 是 if 初始化的 nullptr，**不可**解引用 w->id
      graph_g.cv.wait_for(lk, std::chrono::milliseconds(10));   // 无积压才睡：等完成/回绕/expand_hp/停止（notify 快路径 + 10ms 超时兜底 lost wakeup）

    }
  });
  ThreadPool::instance().start(num_workers);

  // ── 停止信号：SIGINT/SIGTERM → 只置原子停止位（async-signal-safe；不调 cv.notify_all——
  //    condition_variable 非 async-signal-safe，信号上下文调 stdlib 是 UB）。
  //    各线程的等待都带超时兜底（worker/计时线程 10ms、主循环 100ms），超时后自行看到 stopped 退出；
  //    正常收尾走下方 teardown 的 stopped=true + notify_all 立即唤醒。──
  std::signal(SIGINT,  [](int) { graph_g.stopped = true; });
  std::signal(SIGTERM, [](int) { graph_g.stopped = true; });
  FINS_LOG_INFO("[agent] listening on :{} plugin_dir={}", rpc_port, plugin_dir);

  // ── 计时线程：与 worker 完全对称——grab tp 延迟时间点 → 锁外执行其 sleep job → 回锁置 Finished
  //    + notify（与 worker 完成事件共同唤醒主线程调度循环）。tp 顶点在 pin_sync 建图时已写入
  //    job = sleep_until（绝对释放时刻，job 内实时读 hyper_start_ms → rollover 平移自动对齐）──
  std::thread timer_th([&] {

    std::unique_lock tl(graph_g.mtx);
    for (;;) {

      if (graph_g.stopped.load())
        break;

      if (const auto tp = graph_g.grab_delay_workload()) {

        tl.unlock();

        tp->job();                              // 锁外执行：sleep_until 睡到释放时刻（延迟实现）

        tl.lock(); // 回锁直做完成事件：置 done + 传播 pred_left（释放后继 job 顶点）；返回是否新增就绪

        // 同 worker 完成路径：新增就绪唤醒 worker；超周期完工唤醒主循环（tp 可能是最后一个顶点）
        if (graph_g.trigger_workload_ready(tp->id) || graph_g.is_hp_done())
          graph_g.cv.notify_all();

        continue;
      }

      graph_g.cv.wait_for(tl, std::chrono::milliseconds(10));   // 无待释放时间点 → 等事件（notify 快路径 + 10ms 超时兜底 lost wakeup）

    }
  });

  {
    std::unique_lock lk(graph_g.mtx);
    for (;;) {

      if (graph_g.stopped.load())
        break;

      if (graph_g.is_hp_done() && graph_g.pending.load()) {

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
            graph_g.cv.wait_for(lk, std::chrono::milliseconds(100));   // 等热加载 notify 唤醒重试（100ms 仅兜底）
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

        continue;
      }
      if (!graph_g.is_hp_empty() && graph_g.is_hp_done()) {   // 有超周期才回绕（一次性图保持静止，防清 done_ 后 is_hp_done 变 false → 新配置永不 apply）

        graph_g.rollover_hp();

        graph_g.cv.notify_all();

        continue;
      }

      graph_g.cv.wait_for(lk, std::chrono::milliseconds(100));   // 纯事件等待（所有状态跃迁都有 notify；100ms 仅兜底 lost wakeup）

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

  FINS_LOG_INFO("[agent] bye");
  return 0;
}
