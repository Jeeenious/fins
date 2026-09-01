/*******************************************************************************
 * Copyright (c) 2026.
 * IWIN-FINS Lab, Shanghai Jiao Tong University.
 *******************************************************************************/

#pragma once

// ============================================================================
// RemoteCallListener — RPC 路由注册器（单例）
// ============================================================================
//
// 内部逻辑：
//   纯 HTTP 传输层，不含业务逻辑。业务方通过 on_pipeline_update(path, handler) 注册路由
//   （如 /update → handler 内：pipeline_g.cache.write() = j 存原始配置 JSON 到缓冲份 +
//   **graph_g.pending = true + graph_g.cv.notify_all() 置"新配置待应用"并唤醒**；解析/建图不在
//   收包 handler 内——main 主线程调度循环图静止时 commit JSON →
//   pipeline_g.parse_pipeline + check_topology + expand_hp 就地重建，
//   见 g_state；置 pending + notify 唤醒 main 调度循环（图静止且阻塞 wait 时
//   也要重建。RPC 不解析故 HTTP 恒 200，格式错误由调度循环消费时兜底日志），本类只做"翻译"：
//     收 HTTP 请求 → 解析 body JSON → 调用 handler(json) → 统一回 {"status":"success"}；
//     业务抛异常 catch 转 400 {"status":"error","message":...}。不感知 pipeline /
//     plugin / 调度。应答与业务解耦：handler 只执行（void），结果细节由业务方自行
//     查全局状态（如 graph_g.dag），不依赖本层返回值。
//   on_library_add / on_library_modify / on_library_delete 提供 .so 的增/改/删回调：
//     每个都带 main 装配的通知回调 cb(so_path)——被调用时先调 cb 把目标 so 路径
//     通知 main（so 加载状态 = library_g.so_ctx 是否有该 so，见 g_state），再
//     multipart 收 file part "so" 写到 plugin_dir（add/modify 原子写 tmp→rename，
//     delete 按文件名删；落地目录作为 on_library_* 的路由参数传入，不在回调里重复传）——
//     落地后由 PluginLoader 的 inotify 消费（on_library_add 回调注入 Plugin，装配点置
//     library_g.so_ctx），本类不感知 loader，只负责"通知 + 收文件写目录"。plugin_dir 是
//     RPC 路由参数（非业务回调），与 PluginLoader 的 init(plugin_dir) 定目录同理——
//     目录与业务回调分离。
//
// 线程模型：
//   ┌─────────────────────────────────────────────────────────────┐
//   │  main / HTTP 线程           │  listening_ 后台线程             │
//   │  ────────────────          │  ────────────────              │
//   │  start()                   │  server_->listen(...) 阻塞      │
//   │    ├─ 建 client_，向远程     │    ├─ read(...) 轮询收 HTTP 请求 │
//   │    │  注册本进程 ip:port     │    └─ 触发 handler 线程:         │
//   │    └─ 启动 listening_ 线程   │       已注册路由的业务回调        │
//   │  注册路由并装配业务回调       │   （如 validate + write/commit）   │
//   │    └─ 注册 POST 路由         │                                │
//   └─────────────────────────────────────────────────────────────┘
//   handler 由 httplib ThreadPool(num_threads) 执行 → 同一时刻最多 num_threads 个请求
//   并发（线程池大小 = start 唯一参数，见对外接口）；具体路由回调（建图等）由业务方
//   装配，本类不感知。
//
// 资源消耗：
//   - 1 个 httplib::Server + 1 个 httplib::Client + 1 个监听线程
//   - 请求处理复用 httplib 线程池（new_task_queue = start(num_threads) 显式构建），无额外线程
//   - 每个注册路由占 1 个 std::function 闭包
//   - .so 上传：multipart body 整体进内存；原子落地时瞬时写一份 .tmp 再 rename
//
// 对外接口：
//   RPCListener::instance()                       — 单例
//   path 通用约定：path = HTTP 路由路径。客户端 POST 到 server 的
//       http://<ip>:<port>/<path> 即命中该路由（如 POST /plugin/add → on_library_add）；
//       业务方装配时写死字符串即可（on_library_add("/plugin/add", cb)）。
//       path 只决定"哪个 URL 触发哪个回调"，与业务逻辑无关；日志里也仅用于标识哪个路由出错。
//   on_pipeline_update(path, handler)             — 注册 POST 路由
//         handler: const nlohmann::json& → void（只执行；成功统一回 {"status":"success"}，
//                  抛异常自动转 400，不返回业务结果）
//   on_library_add(path, plugin_dir, cb)          — 注册 .so 新增路由（multipart 收 "so"
//         part，原子写 plugin_dir）。被调用时先调 cb(so_path) 登记库状态 Unloaded
//         （先改状态再收文件），落地后 PluginLoader on_library_add 注入 Plugin → 装配点置 Loaded
//   on_library_modify(path, plugin_dir, cb)       — 注册 .so 更新路由（同 add，覆盖写；
//         触发 on_library_modify/on_library_add）
//   on_library_delete(path, plugin_dir, cb)       — 注册 .so 删除路由（按 "so" part 的
//         filename 删 plugin_dir 下同名文件；被调用时先调 cb(so_path) 登记状态，触发 on_library_delete）
//         cb: void(const std::string &so_path) — 目标 so 完整路径（plugin_dir/filename）
//   init(server_host, server_port, client_host, client_port)
//                                                — 初始化：保存监听/上报端点（start 前调用）
//   start(num_threads)                        — 启动监听（num_threads = HTTP 请求并发线程池
//                                                 大小，new_task_queue）；建 client_，向远程
//                                                 orchestrator 上报本进程 ip:port
//   装配示例（业务方 main 中）：
//     RPCListener::instance().init("127.0.0.1", 8000, "127.0.0.1", 8000);  // 先定端点
//     RPCListener::instance().start(4);  // 启动监听，4 = HTTP 请求并发线程池大小
//     RPCListener::instance().on_pipeline_update("/update",
//         [](const nlohmann::json &j) {
//           std::lock_guard lk(pipeline_g.wr_lock());         // ① 多 RPC 并发写 cache 串行（main/worker 不碰此锁）
//           pipeline_g.cache.write() = j;                     // ② 存原始配置 JSON 到缓冲份（不解析，HTTP 恒 200）
//           graph_g.pending = true;                           // ③ 置"新配置待应用"（公开成员直写）
//           graph_g.cv.notify_all();                         //    唤醒 main 主线程调度循环（图静止时也重建）
//         });
//     // 解析/建图时机：运行图本轮全部顶点执行完（图静止）→ main 主线程调度循环
//     // 检测 pending → commit JSON →
//     // pipeline_g.parse_pipeline(read()) + check_topology + expand_hp 就地重建
//     // 重建（格式错误在此兜底日志，见 g_state）。
//     // 库状态登记：算法定位表 = library_g.so_ctx（见 g_state）。
//     // 通知回调在收文件前被调，loader 消费事件后 on_library_add/modify 回调注入
//     // (so, Plugin)、on_library_delete 注入 (so)——library_g.so_ctx 的 SET/ERASE 由
//     // 装配点回调完成，此处 notify 仅作通知（计数/日志）。
//     auto notify = [](const std::string &so) { /* main 可计数/日志 */ };
//     RPCListener::instance().on_library_add("/lib/add", plugin_dir, notify);
//     RPCListener::instance().on_library_modify("/lib/mod", plugin_dir, notify);
//     RPCListener::instance().on_library_delete("/lib/del", plugin_dir, notify);
// ============================================================================

#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <thread>

#include "third_party/httplib.h"
#include "third_party/json.hpp"
#include "utils/logger.hpp"

namespace fins::rt {

  class RPCListener {
  private:
    std::unique_ptr<httplib::Server> server_{};
    std::unique_ptr<httplib::Client> client_{};
    std::thread listening_{};
    // 监听/上报端点（init() 设置，start() 使用）——与 PluginLoader::init(plugin_dir)
    // 统一"初始化定端点/目录、start 只定并发线程数"的接口风格
    std::string server_host_, client_host_;
    int server_port_ = 0, client_port_ = 0;

  public:
    static RPCListener &instance() {
      static RPCListener inst;
      return inst;
    }

  private:
    RPCListener() {
      server_ = std::make_unique<httplib::Server>();  // 必须先建 server_，on_route 才可注册路由
      // 请求并发线程池由 start(num_threads) 显式设置（new_task_queue），此处只建 server_
    };
    ~RPCListener() { stop(); };
    RPCListener(const RPCListener &) = delete;
    RPCListener &operator=(const RPCListener &) = delete;

  public:
    /// 注册 POST 路由：path 的处理函数由业务方装配（本类只做 HTTP 翻译）。
    /// handler 收到解析后的 JSON 请求，只执行不返回；本层统一回 {"status":"success"}，
    /// 业务抛异常统一转 400。应答与业务解耦——需要结果细节时业务方自行查全局状态
    /// （如 pipeline.config），不依赖本层返回值。
    /// handler 直接按值捕获进 httplib 的 Post 闭包（含 std::function 自身），
    /// 不存私有成员——生命周期随 server_ 的路由表，无需额外存储。
    void on_pipeline_update(const std::string &path,
                            std::function<void (const nlohmann::json &)> handler) const {
      server_->Post(path,
        [=, handler = std::move(handler)] (
          const httplib::Request &req, httplib::Response &res) {
        try {
          auto call = nlohmann::json::parse(req.body);

          handler(call);  // 业务回调只执行，结果由本层统一应答

          res.set_content(nlohmann::json{{"status", "success"}}.dump(), "application/json");
        }
        catch (const std::exception &e) {
          FINS_LOG_ERROR("{}: {}", path, e.what());

          res.status = 400;

          res.set_content(nlohmann::json{{"status", "error"}, {"message", e.what()}}.dump(), "application/json");
        }
      });
    }

    /// 注册 .so 新增路由：multipart 收 file part "so"，被调用时先调 cb(dst) 通知
    /// main（so 加载状态 = library_g.so_ctx 是否有该 so），再把 .so 原子写到 plugin_dir
    /// （tmp→rename），落地触发 PluginLoader 的 on_add → make_shared<Plugin>(path)
    /// 构造装载置 library_g.so_ctx。
    void on_library_add(const std::string &path, const std::string &plugin_dir,
                        std::function<void(const std::string &so_path)> cb) const {
      server_->Post(path, [this, path, cb = std::move(cb), plugin_dir](const httplib::Request &req, httplib::Response &res) {
        auto err = [&res, &path](const std::string &msg) {
          FINS_LOG_ERROR("{}: {}", path, msg);
          res.status = 400;
          res.set_content(nlohmann::json{{"status", "error"}, {"message", msg}}.dump(),
                          "application/json");
        };
        try {
          if (!req.is_multipart_form_data() || !req.form.has_file("so")) {
            err("expect multipart with a 'so' file part");
            return;
          }
          const auto so = req.form.get_file("so");
          const auto fname = std::filesystem::path(so.filename).filename().string();  // 防路径穿越
          const auto dst = std::filesystem::path(plugin_dir) / fname;

          if (cb) cb(dst.string());  // 先通知 cb（登记状态），再收文件

          const auto tmp = std::filesystem::path(dst.string() + ".tmp");
          {
            std::ofstream ofs(tmp, std::ios::binary);
            if (!ofs) throw std::runtime_error("cannot open " + tmp.string());
            ofs.write(so.content.data(), static_cast<std::streamsize>(so.content.size()));
          }
          std::filesystem::rename(tmp, dst);
          res.set_content(nlohmann::json{{"status", "success"}, {"so", fname}}.dump(),
                          "application/json");
        } catch (const std::exception &e) {
          err(e.what());
        }
      });
    }

    /// 注册 .so 更新路由：同 add（覆盖写 work_dir_ 下同名 .so），触发 on_modify/on_add。
    void on_library_modify(const std::string &path, const std::string &plugin_dir,
                           std::function<void(const std::string &so_path)> cb) const {
      server_->Post(path, [this, path, cb = std::move(cb), plugin_dir](const httplib::Request &req, httplib::Response &res) {
        auto err = [&res, &path](const std::string &msg) {
          FINS_LOG_ERROR("{}: {}", path, msg);
          res.status = 400;
          res.set_content(nlohmann::json{{"status", "error"}, {"message", msg}}.dump(),
                          "application/json");
        };
        try {
          if (!req.is_multipart_form_data() || !req.form.has_file("so")) {
            err("expect multipart with a 'so' file part");
            return;
          }
          const auto so = req.form.get_file("so");
          const auto fname = std::filesystem::path(so.filename).filename().string();  // 防路径穿越
          const auto dst = std::filesystem::path(plugin_dir) / fname;

          if (cb) cb(dst.string());  // 先通知 cb（登记状态），再收文件

          const auto tmp = std::filesystem::path(dst.string() + ".tmp");
          {
            std::ofstream ofs(tmp, std::ios::binary);
            if (!ofs) throw std::runtime_error("cannot open " + tmp.string());
            ofs.write(so.content.data(), static_cast<std::streamsize>(so.content.size()));
          }
          std::filesystem::rename(tmp, dst);
          res.set_content(nlohmann::json{{"status", "success"}, {"so", fname}}.dump(),
                          "application/json");
        } catch (const std::exception &e) {
          err(e.what());
        }
      });
    }

    /// 注册 .so 删除路由：被调用时先调 cb(dst) 通知状态，再按 "so" part 的 filename
    /// 删除 work_dir_ 下同名 .so，触发 PluginLoader 的 on_delete → 装配点 take_keys() 取走
    /// keys 并移除 library_g.so_ctx 条目（引用计数归零时析构 dlclose 卸载库）。
    void on_library_delete(const std::string &path, const std::string &plugin_dir,
                           std::function<void(const std::string &so_path)> cb) const {
      server_->Post(path, [this, path, cb = std::move(cb), plugin_dir](const httplib::Request &req, httplib::Response &res) {
        auto err = [&res, &path](const std::string &msg) {
          FINS_LOG_ERROR("{}: {}", path, msg);
          res.status = 400;
          res.set_content(nlohmann::json{{"status", "error"}, {"message", msg}}.dump(),
                          "application/json");
        };
        try {
          if (!req.is_multipart_form_data() || !req.form.has_file("so")) {
            err("expect multipart with a 'so' file part");
            return;
          }
          const auto so = req.form.get_file("so");
          const auto fname = std::filesystem::path(so.filename).filename().string();  // 防路径穿越
          const auto dst = std::filesystem::path(plugin_dir) / fname;

          if (cb) cb(dst.string());  // 先改状态（登记 Unloaded），再删文件

          const bool removed = std::filesystem::remove(dst);
          res.set_content(nlohmann::json{{"status", "success"}, {"removed", removed}, {"so", fname}}.dump(),
                          "application/json");
        } catch (const std::exception &e) {
          err(e.what());
        }
      });
    }

    /// @brief 初始化：保存监听/上报端点。与 PluginLoader::init 统一"初始化定端点/
    /// 目录、start 只定并发线程数"的接口风格——端点是配置，start 的唯一参数是线程数。
    /// 必须在 start(num_threads) 前调用。
    void init(const std::string &server_host, const int server_port,
              const std::string &client_host, const int client_port) {
      server_host_ = server_host;
      server_port_ = server_port;
      client_host_ = client_host;
      client_port_ = client_port;
    }

    /// 启动监听：listen() 是阻塞调用，须由独立后台线程执行。
    /// 同时建立 client_，向远程 orchestrator 上报本进程 ip:port。
    /// num_threads = HTTP 请求处理并发线程池大小（httplib new_task_queue，start 唯一参数）。
    void start(const int num_threads) {
      // 幂等：已在监听则忽略——重复 start() 重赋值 joinable 的 listening_ 线程会 terminate
      if (listening_.joinable()) {
        FINS_LOG_WARN("[RPCListener] already started, ignore.");
        return;
      }
      server_->new_task_queue = [num_threads] { return new httplib::ThreadPool(num_threads); };
      client_ = std::make_unique<httplib::Client>(client_host_.c_str(), client_port_);

      listening_ = std::thread([this]() {
        server_->listen(server_host_.c_str(), server_port_);
      });
    }

    /// @brief 停止监听：停 httplib server（含其 HTTP 请求并发线程池）+ 回收 listening_ 线程。
    /// 幂等（server_->stop() 可重复；join 后 listening_ 非 joinable）；析构复用。
    void stop() {
      server_->stop();
      if (listening_.joinable()) listening_.join();
    }
  };

} // namespace fins::rt
