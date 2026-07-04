/*******************************************************************************
 * Copyright (c) 2025.
 * IWIN-FINS Lab, Shanghai Jiao Tong University, Shanghai, China.
 * All rights reserved.
 ******************************************************************************/

// service/service_manager.hpp

#pragma once

#include <any>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <typeindex>
#include <vector>

#include <fins/macros.hpp>

namespace fins {

  // 极速瘦代理，仅占用 16 字节（1个实例指针 + 1个通用函数指针）
  template<typename Ret, typename... Args>
  class FastDelegate {
  private:
    using StubFunc = Ret(*)(void*, Args&&...);
    void* instance_ = nullptr;
    StubFunc stub_  = nullptr;

    // 非 const 成员函数桩
    template<typename T, Ret(T::*MemFunc)(Args...)>
    static Ret member_stub(void* instance, Args&&... args) {
      return (static_cast<T*>(instance)->*MemFunc)(std::forward<Args>(args)...);
    }

    // const 成员函数桩
    template<typename T, Ret(T::*MemFunc)(Args...) const>
    static Ret const_member_stub(void* instance, Args&&... args) {
      return (static_cast<const T*>(instance)->*MemFunc)(std::forward<Args>(args)...);
    }

    // 自由函数 / 静态函数桩
    template<Ret(*FreeFunc)(Args...)>
    static Ret free_stub(void*, Args&&... args) {
      return FreeFunc(std::forward<Args>(args)...);
    }

    // std::function 回退桩 —— 通过堆上的 std::function 转发
    static Ret function_stub(void* instance, Args&&... args) {
      return (*static_cast<std::function<Ret(Args...)>*>(instance))(std::forward<Args>(args)...);
    }

  public:
    FastDelegate() = default;

    // 绑定非 const 成员函数
    template<typename T, Ret(T::*MemFunc)(Args...)>
    void bind(T* instance) {
      instance_ = instance;
      stub_ = &member_stub<T, MemFunc>;
    }

    // 绑定 const 成员函数
    template<typename T, Ret(T::*MemFunc)(Args...) const>
    void bind(const T* instance) {
      instance_ = const_cast<T*>(instance);
      stub_ = &const_member_stub<T, MemFunc>;
    }

    // 绑定自由函数
    template<Ret(*FreeFunc)(Args...)>
    void bind() {
      instance_ = nullptr;
      stub_ = &free_stub<FreeFunc>;
    }

    // 从 std::function 构造（仅用于兼容/迁移场景，会分配堆内存）
    static FastDelegate from_function(std::function<Ret(Args...)> func) {
      FastDelegate d;
      auto* heap_func = new std::function<Ret(Args...)>(std::move(func));
      d.instance_ = heap_func;
      d.stub_ = &function_stub;
      return d;
    }

    // 从运行时成员函数指针构造（堆分配 instance+mfp 对，适合 dynamic-register 场景）
    template<typename T>
    static FastDelegate from_member(T* instance, Ret(T::*mfp)(Args...)) {
      struct MemberPair {
        T* instance;
        Ret(T::*mfp)(Args...);
      };
      auto* pair = new MemberPair{instance, mfp};
      FastDelegate d;
      d.instance_ = pair;
      d.stub_ = +[](void* p, Args&&... args) -> Ret {
        auto* mp = static_cast<MemberPair*>(p);
        return (mp->instance->*mp->mfp)(std::forward<Args>(args)...);
      };
      return d;
    }

    // const 版本
    template<typename T>
    static FastDelegate from_member(const T* instance, Ret(T::*mfp)(Args...) const) {
      struct MemberPair {
        const T* instance;
        Ret(T::*mfp)(Args...) const;
      };
      auto* pair = new MemberPair{instance, mfp};
      FastDelegate d;
      d.instance_ = pair;
      d.stub_ = +[](void* p, Args&&... args) -> Ret {
        auto* mp = static_cast<MemberPair*>(p);
        return (mp->instance->*mp->mfp)(std::forward<Args>(args)...);
      };
      return d;
    }

    inline Ret operator()(Args&&... args) const {
      return stub_(instance_, std::forward<Args>(args)...);
    }

    explicit operator bool() const { return stub_ != nullptr; }
  };

  class ServiceHandler {
  public:
    virtual ~ServiceHandler() = default;
    virtual std::any invoke(const std::any* args, size_t count) = 0;
  };

  class FINS_API ServiceManager {
  public:
    using ServiceCallback = std::function<std::any(const std::vector<std::any> &)>;

    struct ServiceEntry {
      ServiceCallback callback;
      std::type_index input_type_id = std::type_index(typeid(void));
      std::type_index output_type_id = std::type_index(typeid(void));
      std::unique_ptr<ServiceHandler> handler;
    };

    static ServiceManager &get_instance();

    ServiceManager(const ServiceManager &) = delete;
    ServiceManager &operator=(const ServiceManager &) = delete;

    void register_service(const std::string &topic, ServiceCallback cb, std::type_index inputs_id,
                          std::type_index outputs_id);

    void register_service_handler(const std::string &topic, std::unique_ptr<ServiceHandler> handler,
                                  std::type_index inputs_id, std::type_index outputs_id);

    std::future<std::any> call_service(const std::string &topic, std::vector<std::any> args,
                                       std::type_index req_inputs_id, std::type_index req_outputs_id);

    const ServiceEntry* get_service_entry(const std::string &topic) {
      std::lock_guard<std::mutex> lock(map_mutex_);
      auto it = services_.find(topic);
      if (it != services_.end()) {
        return &it->second;
      }
      return nullptr;
    }

    ServiceHandler* get_service_handler(const std::string &topic) {
      std::lock_guard<std::mutex> lock(map_mutex_);
      auto it = services_.find(topic);
      if (it != services_.end()) {
        return it->second.handler.get();
      }
      return nullptr;
    }

    // void record_service_call(const std::string &topic, int64_t duration_ns);  // 性能统计已禁用

    template<typename Ret, typename... Args>
    void register_typed_service(const std::string &topic, FastDelegate<Ret, Args...> delegate) {
      std::lock_guard<std::mutex> lock(map_mutex_);
      typed_services_[topic] = std::any(std::move(delegate));
    }

    template<typename Ret, typename... Args>
    FastDelegate<Ret, Args...> get_typed_service(const std::string &topic) {
      std::lock_guard<std::mutex> lock(map_mutex_);
      auto it = typed_services_.find(topic);
      if (it == typed_services_.end()) {
        return FastDelegate<Ret, Args...>{};
      }
      try {
        return std::any_cast<FastDelegate<Ret, Args...>>(it->second);
      } catch (const std::bad_any_cast&) {
        throw std::runtime_error("[ServiceManager] Type mismatch for topic: " + topic);
      }
    }

  private:
    ServiceManager();
    ~ServiceManager();

    struct Task {
      std::string topic;
      std::vector<std::any> args;
      std::shared_ptr<std::promise<std::any>> promise;
      std::type_index input_check = std::type_index(typeid(void));
      std::type_index output_check = std::type_index(typeid(void));
    };

    void worker_loop();

  private:
    // struct ServiceCallStats {  // 性能统计已禁用
    //   uint64_t call_count = 0;
    //   int64_t total_duration_ns = 0;
    // };

    std::map<std::string, ServiceEntry> services_;
    std::map<std::string, std::any> typed_services_; // 强类型直连通道
    std::mutex map_mutex_;

    // mutable std::map<std::string, ServiceCallStats> service_stats_;  // 性能统计已禁用
    // mutable std::mutex stats_mutex_;
    // mutable std::chrono::steady_clock::time_point last_stats_report_{};

    std::deque<Task> tasks_;
    std::mutex queue_mutex_;
    std::condition_variable cv_;

    std::thread worker_thread_;
    std::atomic<bool> stop_;
  };

#define FINS_SERVICE_MANAGER ServiceManager::get_instance()

// =========================================================================
  // 🌟 优化修复版：极速、无锁、支持默认构造的原地调用进程内客户端句柄
  // =========================================================================
  template<typename Ret, typename... Args>
  class ServiceClient {
  private:
    std::string topic_;
    mutable FastDelegate<Ret, Args...> bound_func_{};
    mutable std::atomic<bool> resolved_{false};
    mutable std::mutex resolve_mutex_;

    void resolve() const {
      if (topic_.empty()) {
        throw std::runtime_error("[ServiceClient] Attempted to call an uninitialized/empty service client!");
      }
      if (!resolved_.load(std::memory_order_acquire)) {
        std::lock_guard<std::mutex> lock(resolve_mutex_);
        if (!resolved_.load(std::memory_order_relaxed)) {
          bound_func_ = FINS_SERVICE_MANAGER.get_typed_service<Ret, Args...>(topic_);
          resolved_.store(true, std::memory_order_release);
        }
      }
    }

  public:
    // 🌟 核心修复：提供默认无参构造函数，允许在结构体/类中作为成员变量默认初始化
    ServiceClient() : topic_("") {}

    explicit ServiceClient(std::string topic) : topic_(std::move(topic)) {}

    // 手动实现拷贝与移动构造（绕过不可拷贝的 std::mutex）
    ServiceClient(const ServiceClient& other) {
      topic_ = other.topic_;
      resolved_.store(other.resolved_.load(std::memory_order_relaxed), std::memory_order_relaxed);
      bound_func_ = other.bound_func_;
    }

    ServiceClient(ServiceClient&& other) noexcept {
      topic_ = std::move(other.topic_);
      resolved_.store(other.resolved_.load(std::memory_order_relaxed), std::memory_order_relaxed);
      bound_func_ = std::move(other.bound_func_);
    }

    // 手动实现赋值操作符
    ServiceClient& operator=(const ServiceClient& other) {
      if (this != &other) {
        topic_ = other.topic_;
        resolved_.store(other.resolved_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        bound_func_ = other.bound_func_;
      }
      return *this;
    }

    ServiceClient& operator=(ServiceClient&& other) noexcept {
      if (this != &other) {
        topic_ = std::move(other.topic_);
        resolved_.store(other.resolved_.load(std::memory_order_relaxed), std::memory_order_relaxed);
        bound_func_ = std::move(other.bound_func_);
      }
      return *this;
    }

    Ret operator()(Args... args) const {
      if (resolved_.load(std::memory_order_acquire)) [[likely]] {
        // auto t1 = std::chrono::steady_clock::now();  // 性能统计已禁用
        Ret r = bound_func_(std::move(args)...);
        // auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
        //     std::chrono::steady_clock::now() - t1).count();
        // FINS_SERVICE_MANAGER.record_service_call(topic_, ns);
        return r;
      }
      resolve();
      if (!bound_func_) [[unlikely]] {
        throw std::runtime_error("[ServiceClient] Service not found on topic: " + topic_);
      }
      // auto t1 = std::chrono::steady_clock::now();  // 性能统计已禁用
      Ret r = bound_func_(std::move(args)...);
      // auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
      //     std::chrono::steady_clock::now() - t1).count();
      // FINS_SERVICE_MANAGER.record_service_call(topic_, ns);
      return r;
    }
  };

  template<typename Signature>
  struct ServiceClientSignatureType;

  template<typename Ret, typename... Args>
  struct ServiceClientSignatureType<Ret(Args...)> {
    using type = ServiceClient<Ret, Args...>;
  };

  template<typename Signature>
  using Client = typename ServiceClientSignatureType<Signature>::type;

} // namespace fins