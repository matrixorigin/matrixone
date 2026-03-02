#pragma once

#include <any>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <future>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <vector>
#include <csignal>

#ifdef __linux__
#include <pthread.h>
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#include <raft/core/resources.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <raft/core/handle.hpp>
#include <raft/core/device_resources.hpp>
#include <raft/core/device_resources_snmg.hpp>
#pragma GCC diagnostic pop

namespace matrixone {

/**
 * @brief Wrapper for RAFT resources to manage their lifecycle.
 * Supports both single-GPU and single-node multi-GPU (SNMG) modes.
 */
class RaftHandleWrapper {
public:
    // Default constructor for single-GPU mode (uses current device)
    RaftHandleWrapper() : resources_(std::make_unique<raft::device_resources>()) {}

    // Constructor for single-GPU mode with a specific device ID
    explicit RaftHandleWrapper(int device_id) {
        RAFT_CUDA_TRY(cudaSetDevice(device_id));
        resources_ = std::make_unique<raft::device_resources>();
    }

    // Constructor for multi-GPU mode (SNMG)
    explicit RaftHandleWrapper(const std::vector<int>& devices) {
        if (devices.empty()) {
            resources_ = std::make_unique<raft::device_resources>();
        } else {
            // Ensure the main device is set before creating SNMG resources
            RAFT_CUDA_TRY(cudaSetDevice(devices[0]));
            resources_ = std::make_unique<raft::device_resources_snmg>(devices);
        }
    }

    ~RaftHandleWrapper() = default;

    raft::resources* get_raft_resources() const { return resources_.get(); }

private:
    std::unique_ptr<raft::resources> resources_;
};

/**
 * @brief A thread-safe blocking queue for task distribution.
 */
template <typename T>
class ThreadSafeQueue {
public:
    void push(T value) {
        {
            std::lock_guard<std::mutex> lock(mu_);
            queue_.push_back(std::move(value));
        }
        cv_.notify_one();
    }

    bool pop(T& value) {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [this] { return !queue_.empty() || stopped_; });
        if (queue_.empty()) return false;
        value = std::move(queue_.front());
        queue_.pop_front();
        return true;
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lock(mu_);
            stopped_ = true;
        }
        cv_.notify_all();
    }

    bool is_stopped() const {
        std::lock_guard<std::mutex> lock(mu_);
        return stopped_;
    }

private:
    std::deque<T> queue_;
    mutable std::mutex mu_;
    std::condition_variable cv_;
    bool stopped_ = false;
};

struct CuvsTaskResult {
    uint64_t ID;
    std::any Result;
    std::exception_ptr Error;
};

/**
 * @brief Manages storage and retrieval of task results.
 */
class CuvsTaskResultStore {
public:
    CuvsTaskResultStore() : next_id_(1), stopped_(false) {}

    uint64_t GetNextJobID() { return next_id_.fetch_add(1); }

    void Store(const CuvsTaskResult& result) {
        std::unique_lock<std::mutex> lock(mu_);
        if (auto it = pending_.find(result.ID); it != pending_.end()) {
            auto promise = std::move(it->second);
            pending_.erase(it);
            lock.unlock();
            promise->set_value(result);
        } else {
            results_[result.ID] = result;
        }
    }

    std::future<CuvsTaskResult> Wait(uint64_t jobID) {
        std::unique_lock<std::mutex> lock(mu_);
        if (stopped_) {
            std::promise<CuvsTaskResult> p;
            p.set_exception(std::make_exception_ptr(std::runtime_error("CuvsTaskResultStore stopped before result was available")));
            return p.get_future();
        }

        if (auto it = results_.find(jobID); it != results_.end()) {
            std::promise<CuvsTaskResult> p;
            p.set_value(std::move(it->second));
            results_.erase(it);
            return p.get_future();
        }

        auto promise = std::make_shared<std::promise<CuvsTaskResult>>();
        pending_[jobID] = promise;
        return promise->get_future();
    }

    void Stop() {
        std::lock_guard<std::mutex> lock(mu_);
        stopped_ = true;
        for (auto& pair : pending_) {
            pair.second->set_exception(std::make_exception_ptr(std::runtime_error("CuvsTaskResultStore stopped before result was available")));
        }
        pending_.clear();
        results_.clear();
    }

private:
    std::atomic<uint64_t> next_id_;
    std::mutex mu_;
    std::map<uint64_t, std::shared_ptr<std::promise<CuvsTaskResult>>> pending_;
    std::map<uint64_t, CuvsTaskResult> results_;
    bool stopped_;
};

/**
 * @brief dedicated worker pool for executing cuVS (RAFT) tasks in GPU-enabled threads.
 */
class CuvsWorker {
public:
    using RaftHandle = RaftHandleWrapper;
    using UserTaskFn = std::function<std::any(RaftHandle&)>;

    struct CuvsTask {
        uint64_t ID;
        UserTaskFn Fn;
    };

    explicit CuvsWorker(size_t n_threads, int device_id = -1) 
        : n_threads_(n_threads), device_id_(device_id) {
        if (n_threads == 0) throw std::invalid_argument("Thread count must be > 0");
    }

    CuvsWorker(size_t n_threads, const std::vector<int>& devices)
        : n_threads_(n_threads), devices_(devices) {
        if (n_threads == 0) throw std::invalid_argument("Thread count must be > 0");
    }

    ~CuvsWorker() { Stop(); }

    CuvsWorker(const CuvsWorker&) = delete;
    CuvsWorker& operator=(const CuvsWorker&) = delete;

    void Start(UserTaskFn init_fn = nullptr, UserTaskFn stop_fn = nullptr) {
        if (started_.exchange(true)) return;
        main_thread_ = std::thread(&CuvsWorker::run_main_loop, this, std::move(init_fn), std::move(stop_fn));
        signal_thread_ = std::thread(&CuvsWorker::signal_handler_loop, this);
    }

    void Stop() {
        if (!started_.load() || stopped_.exchange(true)) return;

        tasks_.stop();
        {
            std::lock_guard<std::mutex> lock(event_mu_);
            should_stop_ = true;
        }
        event_cv_.notify_all();

        if (main_thread_.joinable()) main_thread_.join();
        if (signal_thread_.joinable()) signal_thread_.join();
        for (auto& t : sub_workers_) if (t.joinable()) t.join();
        
        sub_workers_.clear();
        result_store_.Stop();
    }

    uint64_t Submit(UserTaskFn fn) {
        if (stopped_.load()) throw std::runtime_error("Cannot submit task: worker stopped");
        uint64_t id = result_store_.GetNextJobID();
        tasks_.push({id, std::move(fn)});
        return id;
    }

    std::future<CuvsTaskResult> Wait(uint64_t id) { return result_store_.Wait(id); }

    std::exception_ptr GetFirstError() {
        std::lock_guard<std::mutex> lock(event_mu_);
        return fatal_error_;
    }

private:
    void run_main_loop(UserTaskFn init_fn, UserTaskFn stop_fn) {
        pin_thread(0);
        auto resource = setup_resource();
        if (!resource) return;

        if (init_fn) {
            try { init_fn(*resource); }
            catch (...) { report_fatal_error(std::current_exception()); return; }
        }

        // Defer stop_fn cleanup
        auto defer_cleanup = [&]() { if (stop_fn) try { stop_fn(*resource); } catch (...) {} };
        std::shared_ptr<void> cleanup_guard(nullptr, [&](...) { defer_cleanup(); });

        if (n_threads_ == 1) {
            CuvsTask task;
            while (tasks_.pop(task)) execute_task(task, *resource);
        } else {
            for (size_t i = 0; i < n_threads_; ++i) {
                sub_workers_.emplace_back(&CuvsWorker::worker_sub_loop, this);
            }
            std::unique_lock<std::mutex> lock(event_mu_);
            event_cv_.wait(lock, [this] { return should_stop_ || fatal_error_; });
        }
        std::cout << "DEBUG: CuvsWorker main loop finished." << std::endl;
    }

    void worker_sub_loop() {
        pin_thread(-1);
        auto resource = setup_resource();
        if (!resource) return;

        CuvsTask task;
        while (tasks_.pop(task)) execute_task(task, *resource);
    }

    void execute_task(const CuvsTask& task, RaftHandle& resource) {
        CuvsTaskResult res{task.ID};
        try { res.Result = task.Fn(resource); }
        catch (...) { 
            res.Error = std::current_exception(); 
            std::cerr << "ERROR: Task " << task.ID << " failed." << std::endl;
        }
        result_store_.Store(res);
    }

    std::unique_ptr<RaftHandle> setup_resource() {
        try {
            if (!devices_.empty()) {
                return std::make_unique<RaftHandle>(devices_);
            } else if (device_id_ >= 0) {
                return std::make_unique<RaftHandle>(device_id_);
            } else {
                return std::make_unique<RaftHandle>();
            }
        } catch (...) {
            report_fatal_error(std::current_exception());
            std::cerr << "ERROR: Failed to setup RAFT resource." << std::endl;
            return nullptr;
        }
    }

    void report_fatal_error(std::exception_ptr err) {
        std::lock_guard<std::mutex> lock(event_mu_);
        if (!fatal_error_) fatal_error_ = err;
        should_stop_ = true;
        event_cv_.notify_all();
    }

    void pin_thread(int cpu_id) {
#ifdef __linux__
        static std::atomic<int> next_cpu_id{1};
        int id = (cpu_id >= 0) ? cpu_id : (next_cpu_id.fetch_add(1) % std::thread::hardware_concurrency());
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(id, &cpuset);
        if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) != 0) {
            std::cerr << "WARNING: Failed to set affinity for thread to core " << id << std::endl;
        }
#endif
    }

    void signal_handler_loop() {
        static std::atomic<bool> signal_received{false};
        auto handler = [](int) { signal_received.store(true); };
        std::signal(SIGTERM, handler);
        std::signal(SIGINT, handler);

        while (!stopped_.load() && !signal_received.load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(200));
        }
        if (signal_received.load()) {
            std::cout << "DEBUG: CuvsWorker received shutdown signal." << std::endl;
            std::lock_guard<std::mutex> lock(event_mu_);
            should_stop_ = true;
            event_cv_.notify_all();
        }
    }

    size_t n_threads_;
    int device_id_ = -1;
    std::vector<int> devices_;
    std::atomic<bool> started_{false};
    std::atomic<bool> stopped_{false};
    ThreadSafeQueue<CuvsTask> tasks_;
    CuvsTaskResultStore result_store_;
    std::thread main_thread_;
    std::thread signal_thread_;
    std::vector<std::thread> sub_workers_;

    std::mutex event_mu_;
    std::condition_variable event_cv_;
    bool should_stop_ = false;
    std::exception_ptr fatal_error_;
};

} // namespace matrixone
