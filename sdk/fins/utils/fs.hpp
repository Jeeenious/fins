/*******************************************************************************
 * Copyright (c) 2025-2026.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// todo linux 下的测试
// todo wsl 测试
// todo windows 测试
// utils/fs.hpp

#pragma once

#include <string>
#include <cstdlib>
#include <filesystem>
#include <thread>
#include <atomic>
#include <functional>
#include <unordered_map>
#include <memory>
#include <vector>
#include <mutex>
#include <queue>
#include <condition_variable>
#include <stdexcept>
#include <iostream>

// 平台差异头文件引入
#if defined(_WIN32)
  #ifndef WIN32_LEAN_AND_MEAN
    #define WIN32_LEAN_AND_MEAN
  #endif
  #include <windows.h>
#elif defined(__linux__)
  #include <sys/inotify.h>
  #include <sys/select.h>
  #include <unistd.h>
  #include <errno.h>
#else
  #error "Unsupported OS platform"
#endif

namespace fins::util {

  namespace fs = std::filesystem;

  /// @brief 跨平台用户主目录路径展开 (~/...)
  inline std::string expand_user(const std::string &path) {
    if (path.empty() || path[0] != '~') return path;

    const char *home = nullptr;
#if defined(_WIN32)
    home = std::getenv("USERPROFILE");
    if (!home) {
      const char *drive = std::getenv("HOMEDRIVE");
      const char *hpath = std::getenv("HOMEPATH");
      if (drive && hpath) {
        static std::string win_home = std::string(drive) + std::string(hpath);
        home = win_home.c_str();
      }
    }
#else
    home = std::getenv("HOME");
#endif

    if (!home) return path;
    if (path.size() == 1) return std::string(home);
    if (path[1] == '/' || path[1] == '\\')
      return std::string(home) + path.substr(1);
    return path;
  }

  /// @brief 跨平台文件系统变更监听类 (WSL / Linux / Windows)
  ///
  /// 内部逻辑：
  ///   监听目录下文件增/改/删（Linux inotify / Windows ReadDirectoryChangesW），
  ///   事件匹配到回调后经内置线程池异步派发（不阻塞监听线程）。回调注册
  ///   （on_add/on_modify/on_delete）与启动（start）分离：装配点先注册回调、
  ///   再显式 build_thread_pool() 构建线程池、最后 start() 阻塞运行事件循环。
  ///
  /// 资源消耗：
  ///   - 线程：监听线程 1 个（调用 start() 的线程）+ 线程池 num_threads 个工作线程
  ///   - 文件句柄：每监听目录（含递归子目录）一个 inotify wd / Windows HANDLE
  ///
  /// 对外接口：
  ///   build_thread_pool(n)   — 显式构建内置线程池（start 前；未调用则 start 惰性 4 线程）
  ///   watch(dir)             — 添加监听目录（可多次，start 前调用）
  ///   on_add/on_modify/on_delete(cb) — 注册增/改/删回调（cb: full_path → void）
  ///   scan_existing()        — 扫描已监听目录现存文件，逐文件派发 on_add 事件
  ///                            （启动时加载已有插件，PluginLoader::start 调用）
  ///   start()                — 阻塞运行事件循环（后台线程中调用）
  ///   stop()                 — 请求停止事件循环
  class Guard {
    // 内置轻量级线程池
    class ThreadPool {
    public:
      explicit ThreadPool(size_t num_threads) {
        workers_.reserve(num_threads);
        for (size_t i = 0; i < num_threads; ++i)
          workers_.emplace_back(&ThreadPool::worker_loop, this);
      }

      ~ThreadPool() {
        {
          std::lock_guard<std::mutex> lock(mtx_);
          stop_ = true;
        }
        cv_.notify_all();
        for (auto &t : workers_)
          if (t.joinable()) t.join();
      }

      ThreadPool(const ThreadPool &) = delete;
      ThreadPool &operator=(const ThreadPool &) = delete;

      void enqueue(std::function<void()> task) {
        {
          std::lock_guard<std::mutex> lock(mtx_);
          if (stop_) return;
          tasks_.push(std::move(task));
        }
        cv_.notify_one();
      }

    private:
      void worker_loop() {
        while (true) {
          std::function<void()> task;
          {
            std::unique_lock<std::mutex> lock(mtx_);
            cv_.wait(lock, [this] { return stop_ || !tasks_.empty(); });
            if (stop_ && tasks_.empty()) return;
            task = std::move(tasks_.front());
            tasks_.pop();
          }
          task();
        }
      }

      std::vector<std::thread> workers_;
      std::queue<std::function<void()>> tasks_;
      std::mutex mtx_;
      std::condition_variable cv_;
      bool stop_ = false;
    };

  public:
    using Callback = std::function<void(const std::string &full_path)>;

    Guard() = default;
    ~Guard() { stop(); }

    /// 显式构建内置线程池（装配点在组件构造时调用，如 PluginLoader 构造里
    /// guard_.build_thread_pool(4)）。线程池承载文件事件回调的异步派发；start() 前
    /// 构建，重复调用会重建旧池；未调用则 start() 按默认 4 线程惰性构建。
    void build_thread_pool(size_t num_threads = 4) {
      pool_ = std::make_unique<ThreadPool>(num_threads);
    }

    void watch(const std::string &dir) {
      std::lock_guard<std::mutex> lock(mtx_);
      pending_dirs_.push_back(expand_user(dir));
    }

    void on_add(Callback cb)    { std::lock_guard<std::mutex> lock(mtx_); on_add_ = std::move(cb); }
    void on_modify(Callback cb) { std::lock_guard<std::mutex> lock(mtx_); on_modify_ = std::move(cb); }
    void on_delete(Callback cb) { std::lock_guard<std::mutex> lock(mtx_); on_delete_ = std::move(cb); }

    /// @brief 阻塞运行事件循环
    void start() {
      stop_requested_ = false;
      if (!pool_) build_thread_pool(); // 未显式构建则惰性默认 4 线程

#if defined(__linux__)
      run_linux_loop();
#elif defined(_WIN32)
      run_windows_loop();
#endif
      pool_.reset();
    }

    /// @brief 请求停止事件循环
    void stop() {
      stop_requested_ = true;
#if defined(__linux__)
      int fd = inotify_fd_;
      if (fd >= 0) {
        inotify_fd_ = -1;
        ::close(fd);
      }
#elif defined(_WIN32)
      // Windows 侧循环会检查 stop_requested_ 自动退出
#endif
    }

  private:
    std::atomic<bool> stop_requested_{false};
    std::mutex mtx_;
    std::unique_ptr<ThreadPool> pool_;

    Callback on_add_    = nullptr;
    Callback on_modify_ = nullptr;
    Callback on_delete_ = nullptr;
    std::vector<std::string> pending_dirs_;

    /// 统一事件派发：回调非空则投递线程池异步执行（Linux/Windows 共用）。
    /// which 为事件匹配到的回调（on_add_/on_modify_/on_delete_ 的拷贝），path 为完整路径。
    void dispatch(Callback which, std::string path) {
      if (!which) return;
      pool_->enqueue([cb = std::move(which), path = std::move(path)] { cb(path); });
    }

#if defined(__linux__)
    int inotify_fd_ = -1;
    std::unordered_map<int, std::string> wd_to_dir_;

    void add_watch_recursive(int fd, const std::string &dir_path, uint32_t mask) {
      int wd = inotify_add_watch(fd, dir_path.c_str(), mask);
      if (wd >= 0) {
        wd_to_dir_[wd] = dir_path;
      }
      try {
        if (fs::exists(dir_path) && fs::is_directory(dir_path)) {
          for (const auto &entry : fs::recursive_directory_iterator(dir_path, fs::directory_options::skip_permission_denied)) {
            if (entry.is_directory()) {
              int sub_wd = inotify_add_watch(fd, entry.path().string().c_str(), mask);
              if (sub_wd >= 0) wd_to_dir_[sub_wd] = entry.path().string();
            }
          }
        }
      } catch (...) {}
    }

    void run_linux_loop() {
      inotify_fd_ = inotify_init1(IN_CLOEXEC);
      if (inotify_fd_ < 0)
        throw std::runtime_error("inotify_init1 failed");

      uint32_t mask = 0;
      {
        std::lock_guard<std::mutex> lock(mtx_);
        if (on_add_)    mask |= IN_CREATE | IN_MOVED_TO;
        if (on_modify_) mask |= IN_CLOSE_WRITE;
        if (on_delete_) mask |= IN_DELETE | IN_MOVED_FROM;

        if (mask) {
          for (const auto &dir : pending_dirs_) {
            add_watch_recursive(inotify_fd_, dir, mask);
          }
        }
        pending_dirs_.clear();
      }

      alignas(inotify_event) char buf[4096];

      while (!stop_requested_) {
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(inotify_fd_, &rfds);
        struct timeval tv = {0, 200000}; // 200ms 超时轮询

        int ret = select(inotify_fd_ + 1, &rfds, nullptr, nullptr, &tv);
        if (ret < 0) {
          if (errno == EINTR) continue;
          break; // EBADF 或被 close 取消
        }
        if (ret == 0) continue; // 超时

        ssize_t n = read(inotify_fd_, buf, sizeof(buf));
        if (n <= 0) {
          if (n < 0 && errno == EINTR) continue;
          break;
        }

        const char *p = buf;
        const char *const end = buf + n;
        while (p < end) {
          auto *ev = reinterpret_cast<const inotify_event *>(p);
          p += sizeof(inotify_event) + ev->len;

          if (ev->mask & IN_IGNORED) {
            std::lock_guard<std::mutex> lock(mtx_);
            wd_to_dir_.erase(ev->wd);
            continue;
          }
          if (ev->len == 0) continue;

          std::string path;
          Callback which;
          {
            std::lock_guard<std::mutex> lock(mtx_);
            auto it = wd_to_dir_.find(ev->wd);
            if (it == wd_to_dir_.end()) continue;

            path = it->second + "/" + ev->name;

            // 动态补充新创建的子目录监听
            if ((ev->mask & IN_ISDIR) && (ev->mask & (IN_CREATE | IN_MOVED_TO))) {
              int sub_wd = inotify_add_watch(inotify_fd_, path.c_str(), mask);
              if (sub_wd >= 0) wd_to_dir_[sub_wd] = path;
            }

            if      (ev->mask & (IN_CREATE | IN_MOVED_TO))   which = on_add_;
            else if (ev->mask & (IN_DELETE | IN_MOVED_FROM)) which = on_delete_;
            else if (ev->mask & IN_CLOSE_WRITE)              which = on_modify_;
          }

          dispatch(std::move(which), std::move(path));
        }
      }

      if (inotify_fd_ >= 0) {
        ::close(inotify_fd_);
        inotify_fd_ = -1;
      }
    }
#endif // __linux__

#if defined(_WIN32)
    void run_windows_loop() {
      std::vector<HANDLE> handles;
      std::vector<std::string> dir_paths;

      {
        std::lock_guard<std::mutex> lock(mtx_);
        for (const auto &dir : pending_dirs_) {
          HANDLE hDir = CreateFileA(
              dir.c_str(),
              FILE_LIST_DIRECTORY,
              FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
              NULL,
              OPEN_EXISTING,
              FILE_FLAG_BACKUP_SEMANTICS,
              NULL);
          if (hDir != INVALID_HANDLE_VALUE) {
            handles.push_back(hDir);
            dir_paths.push_back(dir);
          }
        }
        pending_dirs_.clear();
      }

      if (handles.empty()) return;

      alignas(DWORD) char buffer[4096];
      DWORD bytesReturned = 0;

      while (!stop_requested_) {
        for (size_t i = 0; i < handles.size(); ++i) {
          if (ReadDirectoryChangesW(
                  handles[i],
                  buffer,
                  sizeof(buffer),
                  TRUE, // 递归监听子目录
                  FILE_NOTIFY_CHANGE_FILE_NAME | FILE_NOTIFY_CHANGE_DIR_NAME | FILE_NOTIFY_CHANGE_LAST_WRITE,
                  &bytesReturned,
                  NULL,
                  NULL)) {

            if (bytesReturned == 0) continue;

            FILE_NOTIFY_INFORMATION *pNotify = reinterpret_cast<FILE_NOTIFY_INFORMATION *>(buffer);
            while (pNotify) {
              std::wstring wname(pNotify->FileName, pNotify->FileNameLength / sizeof(WCHAR));
              std::string filename(wname.begin(), wname.end());
              std::string full_path = dir_paths[i] + "\\" + filename;

              Callback which = nullptr;
              {
                std::lock_guard<std::mutex> lock(mtx_);
                if (pNotify->Action == FILE_ACTION_ADDED || pNotify->Action == FILE_ACTION_RENAMED_NEW_NAME) {
                  which = on_add_;
                } else if (pNotify->Action == FILE_ACTION_REMOVED || pNotify->Action == FILE_ACTION_RENAMED_OLD_NAME) {
                  which = on_delete_;
                } else if (pNotify->Action == FILE_ACTION_MODIFIED) {
                  which = on_modify_;
                }
              }

              dispatch(std::move(which), std::move(full_path));

              if (pNotify->NextEntryOffset == 0) break;
              pNotify = reinterpret_cast<FILE_NOTIFY_INFORMATION *>(
                  reinterpret_cast<char *>(pNotify) + pNotify->NextEntryOffset);
            }
          }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200)); // 200ms 超时轮询
      }

      for (HANDLE h : handles) {
        CloseHandle(h);
      }
    }
#endif // _WIN32
  };

} // namespace fins::util