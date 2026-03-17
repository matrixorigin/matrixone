/* 
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

#ifdef __linux__
#include <pthread.h>
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#include <raft/core/resources.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <raft/core/resource/comms.hpp>
#include <raft/core/handle.hpp>
#include <raft/core/device_resources.hpp>
#include <raft/core/device_resources_snmg.hpp>
#pragma GCC diagnostic pop

namespace matrixone {

/**
 * @brief Wrapper for RAFT resources to manage their lifecycle.
 * Supports both single-GPU and single-node multi-GPU (SNMG) modes.
 */
class raft_handle_wrapper_t {
public:
    // Default constructor for single-GPU mode (uses current device)
    raft_handle_wrapper_t() : resources_(std::make_unique<raft::device_resources>()) {}

    // Constructor for single-GPU mode with a specific device ID
    explicit raft_handle_wrapper_t(int device_id) {
        RAFT_CUDA_TRY(cudaSetDevice(device_id));
        resources_ = std::make_unique<raft::device_resources>();
    }

    // Constructor for multi-GPU mode (SNMG)
    // force_mg: If true, use device_resources_snmg even if devices.size() == 1 (useful for testing)
    explicit raft_handle_wrapper_t(const std::vector<int>& devices, bool force_mg = false) {
        if (devices.empty()) {
            resources_ = std::make_unique<raft::device_resources>();
        } else if (devices.size() == 1 && !force_mg) {
            RAFT_CUDA_TRY(cudaSetDevice(devices[0]));
            resources_ = std::make_unique<raft::device_resources>();
        } else {
            // Ensure the main device is set before creating SNMG resources
            RAFT_CUDA_TRY(cudaSetDevice(devices[0]));
            resources_ = std::make_unique<raft::device_resources_snmg>(devices);
        }
    }

    ~raft_handle_wrapper_t() = default;

    raft::resources* get_raft_resources() const { return resources_.get(); }

private:
    std::unique_ptr<raft::resources> resources_;
};

/**
 * @brief Helper to check if a RAFT handle is configured for Multi-GPU (SNMG).
 */
static inline bool is_snmg_handle(raft::resources* res) {
    return dynamic_cast<const raft::device_resources_snmg*>(res) != nullptr;
}

/**
 * @brief A thread-safe blocking queue for task distribution.
 */
template <typename T>
class thread_safe_queue_t {
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

struct cuvs_task_result_t {
    uint64_t id;
    std::any result;
    std::exception_ptr error;
};

/**
 * @brief Manages storage and retrieval of task results.
 */
class cuvs_task_result_store_t {
public:
    cuvs_task_result_store_t() : next_id_(1), stopped_(false) {}

    uint64_t get_next_job_id() { return next_id_.fetch_add(1); }

    void store(const cuvs_task_result_t& result) {
        std::unique_lock<std::mutex> lock(mu_);
        if (auto it = pending_.find(result.id); it != pending_.end()) {
            auto promise = std::move(it->second);
            pending_.erase(it);
            lock.unlock();
            promise->set_value(result);
        } else {
            results_[result.id] = result;
        }
    }

    std::future<cuvs_task_result_t> wait(uint64_t job_id) {
        std::unique_lock<std::mutex> lock(mu_);
        if (stopped_) {
            std::promise<cuvs_task_result_t> p;
            p.set_exception(std::make_exception_ptr(std::runtime_error("cuvs_task_result_store_t stopped before result was available")));
            return p.get_future();
        }

        if (auto it = results_.find(job_id); it != results_.end()) {
            std::promise<cuvs_task_result_t> p;
            p.set_value(std::move(it->second));
            results_.erase(it);
            return p.get_future();
        }

        auto promise = std::make_shared<std::promise<cuvs_task_result_t>>();
        pending_[job_id] = promise;
        return promise->get_future();
    }

    void stop() {
        std::lock_guard<std::mutex> lock(mu_);
        stopped_ = true;
        for (auto& pair : pending_) {
            pair.second->set_exception(std::make_exception_ptr(std::runtime_error("cuvs_task_result_store_t stopped before result was available")));
        }
        pending_.clear();
        results_.clear();
    }

private:
    std::atomic<uint64_t> next_id_;
    std::mutex mu_;
    std::map<uint64_t, std::shared_ptr<std::promise<cuvs_task_result_t>>> pending_;
    std::map<uint64_t, cuvs_task_result_t> results_;
    bool stopped_;
};

/**
 * @brief dedicated worker pool for executing cuVS (RAFT) tasks in GPU-enabled threads.
 */
class cuvs_worker_t {
public:
    using raft_handle = raft_handle_wrapper_t;
    using user_task_fn = std::function<std::any(raft_handle&)>;

    struct cuvs_task_t {
        uint64_t id;
        user_task_fn fn;
    };

    explicit cuvs_worker_t(size_t n_threads, int device_id = -1) 
        : n_threads_(n_threads), device_id_(device_id) {
        if (n_threads == 0) throw std::invalid_argument("Thread count must be > 0");
    }

    cuvs_worker_t(size_t n_threads, const std::vector<int>& devices, bool force_mg = false)
        : n_threads_(n_threads), devices_(devices), force_mg_(force_mg) {
        if (n_threads == 0) throw std::invalid_argument("Thread count must be > 0");
    }

    ~cuvs_worker_t() { stop(); }

    cuvs_worker_t(const cuvs_worker_t&) = delete;
    cuvs_worker_t& operator=(const cuvs_worker_t&) = delete;

    void start(user_task_fn init_fn = nullptr, user_task_fn stop_fn = nullptr) {
        if (started_.exchange(true)) return;
        main_thread_ = std::thread(&cuvs_worker_t::run_main_loop, this, std::move(init_fn), std::move(stop_fn));
    }

    void stop() {
        if (!started_.load() || stopped_.exchange(true)) return;

        tasks_.stop();
        {
            std::lock_guard<std::mutex> lock(event_mu_);
            should_stop_ = true;
        }
        event_cv_.notify_all();

        if (main_thread_.joinable()) main_thread_.join();
        for (auto& t : sub_workers_) if (t.joinable()) t.join();
        
        sub_workers_.clear();
        result_store_.stop();
    }

    uint64_t submit(user_task_fn fn) {
        if (stopped_.load()) throw std::runtime_error("Cannot submit task: worker stopped");
        uint64_t id = result_store_.get_next_job_id();
        tasks_.push({id, std::move(fn)});
        return id;
    }

    std::future<cuvs_task_result_t> wait(uint64_t id) { return result_store_.wait(id); }

    std::exception_ptr get_first_error() {
        std::lock_guard<std::mutex> lock(event_mu_);
        return fatal_error_;
    }

private:
    void run_main_loop(user_task_fn init_fn, user_task_fn stop_fn) {
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
            cuvs_task_t task;
            while (tasks_.pop(task)) execute_task(task, *resource);
        } else {
            for (size_t i = 0; i < n_threads_; ++i) {
                sub_workers_.emplace_back(&cuvs_worker_t::worker_sub_loop, this);
            }
            std::unique_lock<std::mutex> lock(event_mu_);
            event_cv_.wait(lock, [this] { return should_stop_ || fatal_error_; });
        }
    }

    void worker_sub_loop() {
        pin_thread(-1);
        auto resource = setup_resource();
        if (!resource) return;

        cuvs_task_t task;
        while (tasks_.pop(task)) execute_task(task, *resource);
    }

    void execute_task(const cuvs_task_t& task, raft_handle& resource) {
        cuvs_task_result_t res;
        res.id = task.id;
        try { res.result = task.fn(resource); }
        catch (...) { 
            res.error = std::current_exception(); 
            std::cerr << "ERROR: Task " << task.id << " failed." << std::endl;
        }
        result_store_.store(res);
    }

    std::unique_ptr<raft_handle> setup_resource() {
        try {
            if (!devices_.empty()) {
                return std::make_unique<raft_handle>(devices_, force_mg_);
            } else if (device_id_ >= 0) {
                return std::make_unique<raft_handle>(device_id_);
            } else {
                return std::make_unique<raft_handle>();
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

    size_t n_threads_;
    int device_id_ = -1;
    std::vector<int> devices_;
    bool force_mg_ = false;
    std::atomic<bool> started_{false};
    std::atomic<bool> stopped_{false};
    thread_safe_queue_t<cuvs_task_t> tasks_;
    cuvs_task_result_store_t result_store_;
    std::thread main_thread_;
    std::vector<std::thread> sub_workers_;

    std::mutex event_mu_;
    std::condition_variable event_cv_;
    bool should_stop_ = false;
    std::exception_ptr fatal_error_;
};

} // namespace matrixone
