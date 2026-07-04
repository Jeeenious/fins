#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <deque>
#include <condition_variable>
#include <thread>
#include <fstream>
#include <atomic>
#include <chrono>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fins/utils/time.hpp>
#include <fins/utils/logger.hpp>
#include <fins/third_party/json.hpp>

namespace fins {

struct MsgPerfRecord {
    std::string node_id;
    int port;
    std::string port_desc;
    int64_t acq_time_ns;
    int64_t recv_time_ns;
    int64_t comp_time_ns;
    int64_t cpu_duration_ns;
    int64_t thread_id;
};

/**
 * @brief 性能监控器（单例） / Performance monitor (singleton)
 * @details 收集和管理所有节点的性能记录。通过 `start()` 启动，
 *          `stop()` 停止，数据写入 `~/.fins/performance/` 目录的 JSONL 文件。
 *
 *          Collects and manages performance records from all nodes. Start via
 *          `start()`, stop via `stop()`. Data is written to JSONL files in
 *          `~/.fins/performance/`.
 */
class PerformanceMonitor {
public:
    static PerformanceMonitor& get_instance() {
        static PerformanceMonitor instance;
        return instance;
    }

    void push_record(MsgPerfRecord&& record) {
        if (!running_) return;
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push_back(std::move(record));
            if (queue_.size() > 5000) {
                queue_.pop_front();
            }
        }
        cv_.notify_one();
    }

    void start(const std::string& filename = "") {
        if (running_) return;
        
        std::string home_dir = std::string(getenv("HOME")) + "/.fins/performance";
        struct stat st = {0};
        if (stat(home_dir.c_str(), &st) == -1) {
            mkdir(home_dir.c_str(), 0755);
        }
        
        if (filename.empty()) {
            auto now = std::chrono::system_clock::now();
            auto time_t = std::chrono::system_clock::to_time_t(now);
            auto tm = *std::localtime(&time_t);
            char buffer[64];
            std::strftime(buffer, sizeof(buffer), "%Y%m%d_%H%M%S", &tm);
            filename_ = home_dir + "/runtime_" + buffer + ".jsonl";
        } else {
            filename_ = home_dir + "/" + filename;
        }
        
        running_ = true;
        worker_thread_ = std::thread(&PerformanceMonitor::worker_loop, this);
    }

    void stop() {
        if (!running_) return;
        running_ = false;
        cv_.notify_all();
        if (worker_thread_.joinable()) {
            worker_thread_.join();
            FINS_LOG_INFO("[Perf] Performance data saved to: {}", filename_);
        }
    }

private:
    PerformanceMonitor() : running_(false) {}
    ~PerformanceMonitor() { stop(); }

    void worker_loop() {
        pthread_setname_np(pthread_self(), "fins_perf_monitor");
        std::ofstream ofs(filename_, std::ios::app);
        while (running_ || !queue_.empty()) {
            std::deque<MsgPerfRecord> batch;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait_for(lock, std::chrono::milliseconds(200), [this] { 
                    return !queue_.empty() || !running_; 
                });
                batch.swap(queue_);
            }

            for (const auto& r : batch) {
                nlohmann::json j;
                j["id"] = r.node_id;
                j["p"] = r.port;
                j["port_desc"] = r.port_desc;
                j["acq"] = r.acq_time_ns;
                j["recv"] = r.recv_time_ns;
                j["comp"] = r.comp_time_ns;
                j["lat_ms"] = (r.comp_time_ns - r.recv_time_ns) / 1000000.0;
                j["sys_lat_ms"] = (r.recv_time_ns - r.acq_time_ns) / 1000000.0;
                j["cpu_ms"] = r.cpu_duration_ns / 1000000.0;
                j["sched_wait_ms"] = (r.comp_time_ns - r.recv_time_ns - r.cpu_duration_ns) / 1000000.0;
                j["tid"] = r.thread_id;
                ofs << j.dump() << "\n";
            }
            ofs.flush();
        }
    }

    std::deque<MsgPerfRecord> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::thread worker_thread_;
    std::atomic<bool> running_;
    std::string filename_;
};

/**
 * @brief 作用域计时器 / Scoped segment timer
 * @details RAII 计时器，由 Node::recorder() 创建。在构造时记录起始时间，
 *          析构时自动计算耗时并提交到 PerformanceMonitor。
 *
 *          RAII timer created by Node::recorder(). Records start time on construction,
 *          then automatically computes elapsed time and submits to PerformanceMonitor
 *          on destruction.
 *
 * @par 示例 / Example
 * @code
 * void on_image(const Msg<Image>& msg) {
 *   auto timer = recorder("inference", msg.acq_time);
 *   // 执行推理... / Run inference...
 *   // timer 在此作用域结束时自动记录耗时
 *   // timer records elapsed time at end of scope
 * }
 * @endcode
 */
class ScopedSegmentTimer {
public:
    ScopedSegmentTimer(std::string node_id, std::string segment_name, AcqTime acq_time)
        : node_id_(std::move(node_id)), segment_name_(std::move(segment_name)),
          acq_ts_(acq_time), active_(true) {
        start_ts_ = fins::now();
        start_cpu_ns_ = get_thread_cpu_time_ns();
    }

    ScopedSegmentTimer(ScopedSegmentTimer&& other) noexcept
        : node_id_(std::move(other.node_id_)), segment_name_(std::move(other.segment_name_)),
          acq_ts_(other.acq_ts_), start_ts_(other.start_ts_), start_cpu_ns_(other.start_cpu_ns_), active_(other.active_) {
        other.active_ = false;
    }

    ScopedSegmentTimer(std::string node_id, std::string segment_name, double acq_time_sec)
        : ScopedSegmentTimer(node_id, segment_name, fins::from_seconds(acq_time_sec)) {}

    ScopedSegmentTimer& operator=(ScopedSegmentTimer&& other) noexcept {
        if (this != &other) {
            node_id_ = std::move(other.node_id_);
            segment_name_ = std::move(other.segment_name_);
            acq_ts_ = other.acq_ts_;
            start_ts_ = other.start_ts_;
            start_cpu_ns_ = other.start_cpu_ns_;
            active_ = other.active_;
            other.active_ = false;
        }
        return *this;
    }

    ScopedSegmentTimer(const ScopedSegmentTimer&) = delete;
    ScopedSegmentTimer& operator=(const ScopedSegmentTimer&) = delete;

    ~ScopedSegmentTimer() {
        if (active_) {
            auto end_ts_ = fins::now();
            auto end_cpu_ns_ = get_thread_cpu_time_ns();
            PerformanceMonitor::get_instance().push_record({
                node_id_, -1, segment_name_,
                to_nanoseconds(acq_ts_),
                to_nanoseconds(start_ts_),
                to_nanoseconds(end_ts_),
                (end_cpu_ns_ - start_cpu_ns_),
                (int64_t)pthread_self()
            });
        }
    }

private:
    std::string node_id_;
    std::string segment_name_;
    AcqTime acq_ts_;
    AcqTime start_ts_;
    int64_t start_cpu_ns_;
    bool active_;
};

#define FINS_PERF_MONITOR fins::PerformanceMonitor::get_instance()
}