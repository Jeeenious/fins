/*******************************************************************************
 * Copyright (c) 2026.
 * IWIN-FINS Lab, Shanghai Jiao Tong University.
 *******************************************************************************/

#pragma once

// ============================================================================
// PluginLoader — 插件目录扫描器（单例）
// ============================================================================
//
// 内部逻辑：
//   扫描工作目录下的 .so 导入插件。loader 是纯库机制：guard 文件事件（增/改/删）
//   → make_shared<Plugin>(path) 构造装载（构造器 dlopen handle + 解析 C 工厂符号 + 填
//   loaded_keys）→ 通过 on_library_add / on_library_modify / on_library_delete 回调把事件载荷
//   注入给装配点。library_g.so_ctx（[so_path] → Plugin，g_state.hpp）的维护（SET/ERASE）
//   由装配点回调完成，loader 不直接访问全局表：
//     - on_library_add    载荷 (so_path, shared_ptr<Plugin>)——ctx 已构造装载，装配点 SET
//     - on_library_modify 载荷 (so_path, shared_ptr<Plugin>)——ctx 是重载的新 Plugin，
//                     装配点先卸旧（读旧 ctx → take_keys → ERASE）再 SET 新
//     - on_library_delete 载荷 (so_path)——文件已从磁盘消失，装配点读旧 ctx →
//                     take_keys()（取 keys 通知）→ ERASE（引用计数归零时析构 dlclose）
//   回调注册与 start 分离：guard 文件事件 → Plugin 构造装载库机制接线在装配接口
//   on_library_add/on_library_modify/on_library_delete 内部完成——main 装配的 handler 直接移动
//   捕获进 guard 闭包（同 RPCListener::on_pipeline_update，不存私有成员），闭包内
//   make_shared<Plugin>(path) 后调 handler。线程池由 start(num_threads) 显式构建
//   （guard_.build_thread_pool(num_threads)）；start() 不含任何回调注册/接线，只做
//   build_thread_pool + watch + scan_existing（启动扫描加载目录下已有插件，走
//   on_library_add 路径）+ 起线程。init(plugin_dir) 先定插件目录（配置），
//   start 只接收线程数作为唯一参数（与 RPCListener::init/start 接口风格一致）。
//
// 线程模型：
//   ┌─────────────────────────────────────────────────────────────┐
//   │  main / HTTP 线程           │  watching_ 后台线程              │
//   │  ────────────────          │  ────────────────              │
//   │  on_library_add/mod/del ←装配   │  guard_.start()                │
//   │  start(dir)                │    ├─ read(inotify_fd) 轮询     │
//   │  ▲                         │    └─ 触发接线:                    │
//   │  └── 回调载荷注入 ──────────┘       on_add  → make_shared<Plugin>(path) → handler(so, ctx)
//   │      (so_path, Plugin)             on_mod  → make_shared<Plugin>(path) → handler(so, ctx)
//   │      装配点回调维护                on_del  → handler(so)
//   │      library_g.so_ctx 表     │  每变一次 → handler 捕获进 guard 闭包│
//   └─────────────────────────────────────────────────────────────┘
//   (library_g.so_ctx 为 TBB concurrent_hash_map，多线程安全；由装配点回调维护)
//
// 资源消耗：
//   - 线程：1 个 watching_ 后台线程（文件系统轮询，inotify 阻塞等待）+
//           start(num_threads) 显式构建的 num_threads 个文件事件回调派发线程
//   - 内存：每个 .so 一个 Plugin（dlopen handle + C 符号 + key 列表），由回调载荷
//           交给装配点存 library_g.so_ctx；实例化（expand_hp()）遍历本表定位
//   - 加载：每个 .so 一次 dlopen/dlsym（Plugin 构造器）；实例化不再 dlopen，只查本表
//
// 卸载时机说明：
//   文件系统删除 .so 只是 unlink 目录项，内核保留 inode + 数据页，
//   因为 dlopen 持有内存映射（引用计数）。因此 on_library_delete 回调触发时：
//     1. .so 文件已从磁盘消失
//     2. 但库代码仍在进程地址空间中，所有已创建的 algo 实例正常工作
//     3. 装配点回调从 library_g.so_ctx 读旧 ctx → ctx->take_keys()（取 keys）→ ERASE
//     4. 无实例引用时 Plugin 析构 → dlclose 卸载库
//   只要有任何 algo 实例持有 shared_ptr<Plugin>（expand_hp() 的删除器里），库就
//   不会被卸载。这是 Linux dlopen/dlclose 的保证，与文件是否仍在磁盘无关。
//
// 对外接口：
//   instance()                        — 单例
//   on_library_add(handler)               — 注册「新增」回调（装配点在 main 调用）：
//                                        载荷 (so_path, shared_ptr<Plugin> ctx)——ctx 已
//                                        构造装载（dlopen + 解析符号 + 填 keys），
//                                        装配点在回调内 TBBMAP_SET(library_g.so_ctx, so, ctx)
//   on_library_modify(handler)            — 注册「修改」回调：载荷同 add（ctx 是重载新 Plugin）；
//                                        装配点在回调内先卸旧（读旧 ctx → take_keys → ERASE）
//                                        再 SET 新 ctx
//   on_library_delete(handler)            — 注册「删除」回调：载荷仅 (so_path)（文件已删）；
//                                        装配点在回调内读旧 ctx → take_keys → ERASE
//   init(plugin_dir)                  — 初始化：设置插件目录（start 前调用）
//   start(num_threads)                — 启动监听线程（build_thread_pool(num_threads)
//                                        构建文件事件回调派发线程池 + watch(plugin_dir_) +
//                                        scan_existing() 启动扫描加载已有插件 +
//                                        起 watching_；接线在 on_library_add/on_library_modify/
//                                        on_library_delete 内完成，start 前调用）
//   事件载荷：add/modify = (so_path, shared_ptr<Plugin>)；delete = (so_path)。
//   loader 只完成库机制（make_shared<Plugin>(path) 构造装载），library_g.so_ctx 维护由装配点回调负责。
//   关联：g_state.hpp 的 Plugin 构造装载 / take_keys（库机制成员）+ expand_hp()（遍历
//           library_g.so_ctx 定位实例化 + 武装 AlgoBase）
// ============================================================================

#include <dlfcn.h>
#include <functional>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "utils/fs.hpp"
#include "g_state.hpp"  // Plugin（回调载荷类型）+ library_g.so_ctx（装配点回调维护）

namespace fins::rt {
  class PluginLoader {
    std::unique_ptr<util::Guard> guard_{}; // 后台检查文件系统是否更新
    std::thread watching_; // 后台检查占用的阻塞线程
    std::string plugin_dir_; // 插件目录（init() 设置，start() 使用）

  public:
    static PluginLoader &instance() {
      static PluginLoader inst;
      return inst;
    }
    PluginLoader(const PluginLoader &) = delete;
    PluginLoader &operator=(const PluginLoader &) = delete;

  private:
    PluginLoader() { guard_ = std::make_unique<util::Guard>(); }
    ~PluginLoader() { stop(); }

  public:
    /// 注册「新增」回调（装配点在 main 中调用，start 前）：扫描到新 .so 导入后触发。
    /// 载荷 (so_path, ctx)：ctx 已由 Plugin 构造器完成库机制（dlopen + 解析 C 符号 +
    /// 填 loaded_keys）。装配点在回调内 TBBMAP_SET(library_g.so_ctx, so, ctx) 维护全局表——
    /// loader 不直接访问 library_g.so_ctx。
    /// handler 直接移动捕获进 guard 闭包（同 RPCListener::on_pipeline_update），不存私有成员：
    /// guard 文件事件（IN_CREATE/IN_MOVED_TO）→ make_shared<Plugin>(path) 构造装载（纯库机制）→
    /// 调 handler(so_path, ctx)（装配点写全局表）。
    void on_library_add(std::function<void(const std::string &so_path, std::shared_ptr<Plugin> ctx)> handler) const {
      guard_->on_add([this, add_cb = std::move(handler)](const std::string &path) {
        if (!is_so_path(path)) return;
        try {
          const auto ctx = std::make_shared<Plugin>(path);   // 构造装载：dlopen + 解析符号 + 填 keys
          add_cb(path, ctx);                  // 装配点：TBBMAP_SET(library_g.so_ctx, so, ctx)
          FINS_LOG_INFO("[PluginLoader] hot-load: {}", path);
        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[PluginLoader] hot-load failed: {} — {}", path, e.what());
        }
      });
    }
    /// 注册「修改」回调：.so 被修改（IN_CLOSE_WRITE）后触发，载荷 (so_path, ctx)——
    /// ctx 是重载的新 Plugin（新 handle，已构造装载）。装配点在回调内先处理
    /// 旧条目（const_accessor 读旧 ctx → take_keys() 取走 keys → ERASE），再 TBBMAP_SET 新 ctx。
    /// loader 不直接访问 library_g.so_ctx。
    /// handler 直接移动捕获进 guard 闭包，不存私有成员：guard 文件事件 → 构造 Plugin
    /// （make_shared<Plugin>(path)，重载新 handle）→ 调 handler(so_path, ctx)（装配点卸旧 + 重填）。
    void on_library_modify(std::function<void(const std::string &so_path, std::shared_ptr<Plugin> ctx)> handler) const {
      guard_->on_modify([this, modify_cb = std::move(handler)](const std::string &path) {
        if (!is_so_path(path)) return;
        try {
          const auto ctx = std::make_shared<Plugin>(path);   // 构造装载：重载新 handle + 填 keys
          modify_cb(path, ctx);               // 装配点：卸旧条目 + SET 新
          FINS_LOG_INFO("[PluginLoader] hot-reload: {}", path);
        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[PluginLoader] hot-reload failed: {} — {}", path, e.what());
        }
      });
    }
    /// 注册「删除」回调：.so 被删后触发，载荷仅 (so_path)（文件已从磁盘消失）。
    /// 装配点在回调内读旧 ctx → take_keys()（取走 keys）→ ERASE（析构 dlclose 兜底）——
    /// loader 不直接访问 library_g.so_ctx。
    /// handler 直接移动捕获进 guard 闭包，不存私有成员：guard 文件事件（IN_DELETE/
    /// IN_MOVED_FROM）→ 调 handler(so_path)（装配点维护全局表）。
    void on_library_delete(std::function<void(const std::string &so_path)> handler) const {
      guard_->on_delete([this, del_cb = std::move(handler)](const std::string &path) {
        if (!is_so_path(path)) return;
        try {
          del_cb(path);                       // 装配点：读旧 ctx → take_keys → ERASE
          FINS_LOG_INFO("[PluginLoader] hot-unload: {}", path);
        } catch (const std::exception &e) {
          FINS_LOG_ERROR("[PluginLoader] hot-unload failed: {} — {}", path, e.what());
        }
      });
    }

    /// @brief 初始化：设置插件目录。与 RPCListener::init 统一"初始化定端点/目录、
    /// start 只定并发线程数"的接口风格——目录是配置，start 的唯一参数是线程数。
    /// 必须在 start(num_threads) 前调用。
    void init(const std::string &plugin_dir) { plugin_dir_ = plugin_dir; }

    /// @brief 启动工作目录监听线程。回调注册与 start 分离：guard 文件事件 →
    /// Plugin 构造装载库机制（make_shared<Plugin>(path)）+ main 装配的
    /// on_library_add/on_library_modify/on_library_delete 业务回调都在 start 前经上述接口完成；
    /// 文件事件回调派发线程池由本方法显式构建（guard_->build_thread_pool(num_threads)，
    /// start 唯一参数）。
    /// 本方法只做 build_thread_pool + watch(plugin_dir_) + scan_existing（启动时
    /// 扫描目录下已有插件，逐文件走 on_library_add 路径装载）+ 起 watching_ 线程，
    /// 不含任何回调注册/接线。
    void start(int num_threads) {
      // 幂等：已在监听则忽略——重复 start() 重赋值 joinable 的 watching_ 线程会 terminate
      if (watching_.joinable()) {
        FINS_LOG_WARN("[PluginLoader] already started, ignore.");
        return;
      }
      guard_->build_thread_pool(num_threads); // 文件事件回调派发线程池（start 唯一参数）
      guard_->watch(plugin_dir_);
      guard_->scan_existing();               // 启动扫描加载已有插件（on_library_add 路径）
      watching_ = std::thread([this] { guard_->start(); });
    }

    /// @brief 停止监听：请求停止文件事件循环 + 回收 watching_ 线程。
    /// 幂等（Guard::stop() 关 inotify_fd_ 置 -1 不重复 close；join 后 watching_ 非 joinable）；析构复用。
    void stop() {
      guard_->stop();
      if (watching_.joinable()) watching_.join();
    }

  private:
    /// guard 事件对目录下所有文件触发（增/改/删），loader 只处理 .so 插件文件
    /// （.json 配置后续扩展）。后缀过滤：length >= 3 保证 substr 不越界。
    static bool is_so_path(const std::string &path) {
      return path.size() >= 3 && path.compare(path.size() - 3, 3, ".so") == 0;
    }
  };
} // namespace fins::rt
