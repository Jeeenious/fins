// ============================================================================
// agent_main — 独立功能测试主程序（接收外部 JSON 配置启动数据流调度）
// ============================================================================
//
// 内部逻辑：
//   main 作为装配点，把业务回调注册进组件（组件不感知业务），随后阻塞在
//   main 调度循环直到收到停止信号：
//   1. 插件加载（main 开头 create_directories 后）：装配点遍历 plugin_dir 存量装载 .so
//      → make_shared<Plugin>(path) → SET library_g.so_ctx（Guard/PluginLoader 不承担存量扫描）；
//      增量热加载由 PluginLoader::on_library_add/modify/delete 回调维护（SET/ERASE 由装配点做）。
//   2. RPCListener::on_pipeline_update("/update") — 收包：存原始 JSON 到
//      pipeline_g.cache.write()（不解析，HTTP 恒 200）+ 直写 graph_g.pending = true
//      + cv.notify_all()；建图由 main 调度循环图静止时 commit + parse_pipeline
//      + check_topology + expand_hp 就地重建。
//   3. RPCListener::on_library_add/modify/delete 路由 — .so 上传接口（本测试用
//      插件目录扫描即可，路由保留作远程注入备选）。
//   4. ThreadPool::on_execute — worker 带锁单步事务：持 graph_g.mtx 拉取最优先
//      就绪（grab_ready_workload 置 Running，返回图内顶点 Workload*）→ 锁外
//      w->job() 执行 → 回锁直做完成事件（w->state = Finished + cv.notify_all()）。
//   main 调度循环（事件驱动）：被 worker 完成 / 计时线程释放 / pending 任一 notify
//   唤醒后集中做状态与优先级更新（update_abs_deadline + priority_updater，不回固定
//   1ms 轮询；时间推进不在此做）；图静止 is_hp_done() → 有配置（pending）commit +
//   parse + check_topology + check_algo_ready（算法插件未全部注册 → 图不动保持旧图、
//   库热加载 on_library_* notify 唤醒重试）+ expand_hp / 有超周期 rollover_hp 回绕，
//   两者后各调 wcet_updater 一次（超周期开始刷新 wcet）→ 否则 cv.wait() 纯事件等待。
//   计时线程：与 worker 完全对称——grab_delay_workload 拿最近待释放时间点（tp 顶点，
//   建图时已写入 job = sleep_until 绝对释放时刻）→ 置 Running → 锁外执行 tp->job()
//   （即睡到点）→ 回锁直做完成事件（Finished + notify），与 worker 完成事件共同唤醒
//   主线程调度循环。延迟只在此线程执行，不占 worker。
//   SIGINT/SIGTERM → graph_g.stopped = true + cv.notify_all() 退出循环 → 回收
//   （置停止位唤醒全部 → timer_th.join() → pool.stop() → RPC.stop() → loader.stop()）。
//
// 资源消耗：
//   - 线程：main 主线程（调度循环）+ 1 个计时线程（时间点释放）+ 2 个绑核 worker
//           （thread_pool start 参数）+ RPC 1 个监听线程 + HTTP 请求并发池（start 参数）
//           + PluginLoader 1 个监听线程 + 文件事件回调派发池（start 参数）
//   - 图/实例：随 JSON 配置 expand_hp 建图；每 job 实例 1 个 AlgoBase + 1 闭包
//   - .so：插件目录下每个 .so 装载为 1 个 Plugin（dlopen handle + C 工厂）
//
// 对外接口（装配示例）：
//   agent_main [rpc_port=18080] [plugin_dir=/tmp/fins_agent_plugins]
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

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <vector>
#include <form.hpp>
#include "../RPC_listener.hpp"
#include "../g_state.hpp"
#include "../hardware_monitor.hpp"
#include "../plugin_loader.hpp"
#include "../thread_pool.hpp"
#include "../utils/logger.hpp"

using namespace fins::rt;
namespace fs = std::filesystem;

static fins::util::TBBMap<std::deque<long long>> exec_hist;
static fins::util::TBBMap<std::deque<long long>> idle_hist;
static std::deque<long long> expand_latency{};
static std::deque<long long> rollover_latency{};

int main(int argc, char **argv) {
  const int rpc_port = argc > 1 ? std::atoi(argv[1]) : 18080;
  const std::string plugin_dir = argc > 2 ? argv[2] : "./plugins";

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
  PluginLoader::instance().on_library_add([](const std::string &so, std::shared_ptr<Plugin> ctx) {
    // 新增 .so：ctx 已构造装载，直接 SET
    TBBMAP_SET(library_g.so_ctx, so, ctx);
    FINS_LOG_INFO("[agent] lib add: {}", so);
    graph_g.cv.notify_all();   // 库变更 → 唤醒主循环即时重查插件就绪（defer 自动恢复）
  });
  PluginLoader::instance().on_library_modify([](const std::string &so, std::shared_ptr<Plugin> ctx) {
    std::shared_ptr<Plugin> old;
    {
      typename fins::util::TBBMap<std::shared_ptr<Plugin>>::const_accessor ca;
      if (library_g.so_ctx.find(ca, so)) old = ca->second;
    }
    if (old) old->take_keys();
    TBBMAP_ERASE(library_g.so_ctx, so);
    TBBMAP_SET(library_g.so_ctx, so, ctx);
    FINS_LOG_INFO("[agent] lib modify: {}", so);
    graph_g.cv.notify_all();   // 库变更 → 唤醒主循环即时重查插件就绪（defer 自动恢复）
  });
  PluginLoader::instance().on_library_delete([](const std::string &so) {
    std::shared_ptr<Plugin> old;
    {
      typename fins::util::TBBMap<std::shared_ptr<Plugin>>::const_accessor ca;
      if (library_g.so_ctx.find(ca, so)) old = ca->second;
    }
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
  RPCListener::instance().init("0.0.0.0", rpc_port, "0.0.0.0", rpc_port);
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
  ThreadPool::instance().on_execute([](int wid) -> bool {

    std::unique_lock lk(graph_g.mtx);
    for (;;) {
      if (graph_g.stopped.load()) return false;

#if FINS_TIMING
      const auto _t0 = std::chrono::steady_clock::now();   // 空闲段起点：完成上一任务 → 拉到下一任务（含 cv.wait）
#endif

      if (auto w = graph_g.grab_ready_workload()) {
        w->state = Workload::State::Running;   // 拉取置 Running
        lk.unlock();

#if FINS_TIMING
        const auto _t1 = std::chrono::steady_clock::now();
#endif

        w->job();

#if FINS_TIMING
      const long long exec_us = std::chrono::duration<double, std::micro>(
          std::chrono::steady_clock::now() - _t1).count();   // 本线程本次实测（job 闭包总时长）
#endif

        lk.lock();
        w->state = Workload::State::Finished;   // 回锁直做完成事件
        graph_g.cv.notify_all();

#if FINS_TIMING
      const long long idle_us = std::chrono::duration<double, std::micro>(
          std::chrono::steady_clock::now() - _t0).count() - exec_us;   // 本线程本次空闲（完成上一任务 → 拉到下一任务）
      TBBMAP_UPDATE(exec_hist, std::to_string(wid), [&](auto &q) { q.push_back(exec_us); });
      TBBMAP_UPDATE(idle_hist, std::to_string(wid), [&](auto &q) { q.push_back(idle_us); });
#endif

        return true;
      }
      graph_g.cv.wait_for(lk, std::chrono::milliseconds(1));   // 等完成/回绕/expand_hp/停止（notify 快路径 + 1ms 超时兜底 lost wakeup）
    }
  });
  ThreadPool::instance().start(2);

  // ── 停止信号：SIGINT/SIGTERM → 置停止位唤醒 main 循环与 worker ──
  std::signal(SIGINT,  [](int) { graph_g.stopped = true; graph_g.cv.notify_all(); });
  std::signal(SIGTERM, [](int) { graph_g.stopped = true; graph_g.cv.notify_all(); });
  FINS_LOG_INFO("[agent] listening on :{} plugin_dir={}", rpc_port, plugin_dir);

  // ── 计时线程：与 worker 完全对称——grab tp 延迟时间点 → 锁外执行其 sleep job → 回锁置 Finished
  //    + notify（与 worker 完成事件共同唤醒主线程调度循环）。tp 顶点在 pin_sync 建图时已写入
  //    job = sleep_until（绝对释放时刻，job 内实时读 hyper_start_ms → rollover 平移自动对齐）──
  std::thread timer_th([&] {
    std::unique_lock tl(graph_g.mtx);
    while (!graph_g.stopped.load()) {
      if (auto tp = graph_g.grab_delay_workload()) {
        tp->state = Workload::State::Running;   // 拉取置 Running（同 worker）
        tl.unlock();
        tp->job();                              // 锁外执行：sleep_until 睡到释放时刻（延迟实现）
        tl.lock();
        tp->state = Workload::State::Finished;  // 回锁直做完成事件
        graph_g.cv.notify_all();
        continue;
      }
      graph_g.cv.wait_for(tl, std::chrono::milliseconds(1));   // 无待释放时间点 → 等事件（notify 快路径 + 1ms 超时兜底 lost wakeup）
    }
  });

  {
    std::unique_lock lk(graph_g.mtx);
    while (!graph_g.stopped.load()) {
      // 状态与优先级更新：每次事件（worker 完成 / 时间点释放 / pending）唤醒后集中做一次，
      // 不回固定 1ms 轮询。priority_updater 读 update_abs_deadline 的最新 ddl。
      graph_g.update_abs_deadline();
      if (wcet_updater) wcet_updater(graph_g);   // 每次事件唤醒 → wcet 更新一次（滚动排期后刷新）
      if (priority_updater) priority_updater(graph_g);  // 每次事件唤醒 → 优先级更新一次（滚动排期后刷新）
      if (graph_g.is_hp_done() && graph_g.pending.load()) {

#if FINS_TIMING
        const auto _e0 = std::chrono::steady_clock::now();   // expand_hp 建图计时起点
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
            if (!lib_keys.count(key)) { ready = false; break; }
          if (!ready) {
            FINS_LOG_INFO("[agent] algo not ready, defer (pending kept, wait plugin load)");
            graph_g.cv.wait_for(lk, std::chrono::milliseconds(500));   // 等热加载 notify 唤醒重试
            continue;   // 保留 pending，不丢弃配置
          }
        }


        try {
          graph_g.expand_hp(pipeline_g, library_g);
          FINS_LOG_INFO("[agent] pipeline applied: {} vertices", graph_g.dag.size());
          if (wcet_updater) wcet_updater(graph_g);   // 超周期开始（新图）：wcet 更新一次
        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[agent] pipeline apply failed: {}", e.what());
        }

#if FINS_TIMING
        const long long expand_us = std::chrono::duration<double, std::micro>(
            std::chrono::steady_clock::now() - _e0).count();
        expand_latency.push_back(expand_us);
        FINS_LOG_INFO("[agent] expand_hp cost {} us", expand_us);
#endif

        graph_g.pending = false;   // 已应用（成功/失败均清除，勿残留导致 commit 翻到未写份交替重建）
        graph_g.cv.notify_all();

        continue;
      }
      if (graph_g.hyper_period_ms > 0 && graph_g.is_hp_done() && !graph_g.pending.load()) {

#if FINS_TIMING
        const auto _r0 = std::chrono::steady_clock::now();   // rollover_hp 回绕计时起点
#endif

        graph_g.rollover_hp();

#if FINS_TIMING
        const long long rollover_us = std::chrono::duration<double, std::micro>(
            std::chrono::steady_clock::now() - _r0).count();
        rollover_latency.push_back(rollover_us);
        FINS_LOG_INFO("[agent] rollover_hp cost {} us", rollover_us);
#endif

        graph_g.cv.notify_all();
        continue;
      }
      graph_g.cv.wait_for(lk, std::chrono::milliseconds(1));   // 纯事件等待（notify 快路径 + 1ms 超时兜底 lost wakeup）
    }
  }

  // ── 回收：先置停止位唤醒全部线程 → 计时线程 → worker → 组件 stop ──
  graph_g.stopped = true;
  graph_g.cv.notify_all();
  if (timer_th.joinable()) timer_th.join();   // 回收计时线程（可能在 tp->job() 睡到释放点，join 等其退出）
  ThreadPool::instance().stop();
  RPCListener::instance().stop();
  PluginLoader::instance().stop();
  HardwareMonitor::instance().stop();

#if FINS_TIMING
  // ── CSV 导出：每 worker 的原始 exec/idle 样本序列（exec 与 idle 一一配对；供离线分析/绘图）──
  {
    std::ofstream ofs("exec_stats.csv");
    ofs << "wid,idx,exec_us,idle_us\n";
    for (auto it = exec_hist.begin(); it != exec_hist.end(); ++it) {
      const std::string &wid = it->first;
      const auto &eq = it->second;
      typename fins::util::TBBMap<std::deque<long long>>::const_accessor ca;
      const std::deque<long long> *iq = nullptr;
      if (idle_hist.find(ca, wid)) iq = &ca->second;
      const size_t n = iq ? std::min(eq.size(), iq->size()) : 0;
      for (size_t i = 0; i < n; ++i)
        ofs << wid << ',' << i << ',' << eq[i] << ',' << (*iq)[i] << '\n';
    }
    ofs.close();
  }
  // ── CSV 导出：主线程 expand/rollover 耗时样本（kind: expand=建图 / rollover=超周期回绕）──
  {
    std::ofstream ofs("main_loop_stats.csv");
    ofs << "kind,idx,us,extra\n";
    for (size_t i = 0; i < expand_latency.size(); ++i)
      ofs << "expand," << i << ',' << expand_latency[i] << ",\n";
    for (size_t i = 0; i < rollover_latency.size(); ++i)
      ofs << "rollover," << i << ',' << rollover_latency[i] << ",\n";
    ofs << "\n";
    ofs.close();
  }
#endif

  FINS_LOG_INFO("[agent] bye");
  return 0;
}
