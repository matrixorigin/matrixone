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

#include <raft/core/device_resources_snmg.hpp>
#include <raft/core/resource/comms.hpp>
#include <raft/core/resources.hpp>
#include "helper.h"
#include "cuvs_types.h"

#include <any>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>
#include <map>
#include <unordered_map>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <ctime>
#include <iostream>

namespace matrixone {

// =============================================================================
// cuvs_worker_t — Developer Guide
// =============================================================================
//
// PURPOSE
// -------
// cuvs_worker_t manages a pool of CPU threads, each pinned to a GPU device,
// that execute GPU work (build, extend, search) asynchronously.  Each index
// type (CAGRA, IVF-Flat, IVF-PQ) owns one cuvs_worker_t for its lifetime.
//
// THREAD MODEL
// ------------
// On start(), two categories of threads are spawned:
//
//   Main thread (1):
//     - Pinned to devices_[0].
//     - Drains main_tasks_ queue exclusively.
//     - Used for serialized operations that must run on the primary GPU:
//       build, extend, serialize/deserialize, merge setup.
//     - submit_main() enqueues to this thread.
//
//   Device worker pool (nthread_ threads):
//     - Distributed across devices_ round-robin: thread i → devices_[i % n].
//     - Each thread drains device_queues_[i % n] (one queue per physical GPU).
//     - Used for parallel/concurrent operations: search round-robin, SHARDED
//       build/extend (one job per GPU simultaneously).
//     - submit() enqueues to the next device queue (round-robin via next_device_idx_).
//     - submit_all_devices_no_wait() pushes one job to EACH device queue.
//     - submit_all_devices() = submit_all_devices_no_wait() + wait for all.
//
//   Each thread gets its own raft_handle_wrapper_t with its own raft::resources
//   (and therefore its own CUDA stream) so GPU ops on different threads do not
//   serialize on the same stream.
//
// TASK SUBMISSION SUMMARY
// -----------------------
//   submit_main(fn)               → main thread, primary GPU, serialized
//   submit(fn)                    → round-robin device thread, load-balanced
//   submit_all_devices_no_wait(fn)→ one task per GPU, concurrent, returns job IDs
//   submit_all_devices(fn)        → same + blocks until all complete
//   broadcast(fn)                 → alias for submit_all_devices (with wait)
//
// RESULT TRACKING
// ---------------
// Every submit* returns a uint64_t job ID.  call wait(id).get() to block until
// the task completes and retrieve cuvs_task_result_t { result: std::any, error }.
// cuvs_task_result_store_t is a sharded (64 shards) lock-striped map of
// promise/future pairs.  If wait() is called before the task completes, a
// placeholder promise is registered; the worker fulfills it on completion.
// If wait() is called after the task completes, the stored result is returned
// immediately via a pre-fulfilled future.
//
// LIFECYCLE
// ---------
//   1. Construct: cuvs_worker_t(nthread, devices, mode)
//      - Creates one thread_safe_queue_t per GPU (capacity 1000 tasks each).
//      - Does NOT start any threads yet.
//   2. start(init_fn, stop_fn):
//      - Spawns main_thread_ + nthread_ device_threads_.
//      - init_fn (optional) is called once per thread with its raft_handle.
//        Used by index types to register the cuVS index pointer on the handle
//        (handle.set_index_ptr) so per-thread searches avoid map lookups.
//      - stop_fn (optional) is called once per thread when its queue is drained
//        and the worker is stopping.  Used to clean up device-side state.
//   3. submit / submit_main / submit_all_devices — enqueue work.
//   4. wait(id).get() — block until work completes, retrieve result or rethrow.
//   5. stop():
//      - Sets running_ = false, drains all queues by calling stop() on each.
//      - Joins all threads (main + device pool).
//      - Fulfills any pending placeholder futures with a "Worker stopped" error.
//
// raft_handle_wrapper_t
// ---------------------
// Wraps raft::resources (CUDA stream + allocators) with device metadata:
//   - device_id_: physical CUDA device ID
//   - rank_:      logical rank (index into devices_ vector)
//   - mode_:      distribution mode (SINGLE_GPU / REPLICATED / SHARDED)
//   - index_ptr_: std::any — used by REPLICATED mode to cache a per-thread
//                 pointer to the local GPU index (set in init_fn, read in search)
//                 so hot-path searches avoid taking the index map mutex.
//   - sync():     calls raft::resource::sync_stream(); with force_all_ranks=true
//                 also syncs all other ranks (used by build completion).
//
// BATCHING (submit_batched)
// -------------------------
// Optional path for aggregating multiple concurrent float-query search requests
// into a single cuVS call to improve GPU utilization.
//   - Enabled by set_use_batching(true) on the index.
//   - submit_batched(key, req, exec_fn): groups requests under a key (per-index
//     string), flushes when >= 16 requests accumulate or after 100 µs delay.
//   - exec_fn receives the batched requests and a vector of per-request setters
//     (callbacks to resolve individual futures).
//   - The batch is flushed either eagerly (>= 16 reqs) or via a scheduled task
//     that sleeps 100 µs to allow more requests to arrive.
//   - For SHARDED mode, the batch flush task is sent to the main thread.
//
// DISTRIBUTION MODE USAGE BY INDEX TYPES
// ----------------------------------------
//   SINGLE_GPU:
//     build    → submit_main
//     extend   → submit_main
//     search   → submit (round-robin; with multiple threads, multiple searches
//                can overlap on the same GPU via separate streams)
//
//   REPLICATED:
//     build    → submit_all_devices (each GPU builds its own full copy)
//     extend   → submit_all_devices (each GPU extends its copy concurrently)
//     search   → submit (round-robin across GPU replicas)
//
//   SHARDED:
//     build    → submit_all_devices (each GPU builds its shard)
//     extend   → NOT supported (throws at index level)
//     search   → submit_all_devices_no_wait (all shards search concurrently)
//                → results collected and merged by merge_sharded_results()
//
// THREAD SAFETY
// -------------
// - thread_safe_queue_t: all operations protected by internal mutex + condvars.
//   Bounded capacity (1000) — push blocks if full, pop blocks if empty.
// - cuvs_task_result_store_t: 64-shard lock-striped; each shard has its own
//   mutex so high-concurrency wait/store calls rarely contend.
// - next_device_idx_: atomic uint32_t, incremented without lock for round-robin.
// - batch_mutex_: guards the batches_ map (per-key batch_t allocation only).
//   per-batch batch_t::mu guards the request list and scheduled flag.
//
// =============================================================================

inline std::string get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto now_c = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::tm now_tm;
    localtime_r(&now_c, &now_tm);
    char buf[64];
    std::strftime(buf, sizeof(buf), "%H:%M:%S", &now_tm);
    std::stringstream ss;
    ss << buf << "." << std::setfill('0') << std::setw(3) << ms.count();
    return ss.str();
}

inline const char* mode_name(distribution_mode_t mode) {
    switch (mode) {
        case DistributionMode_SINGLE_GPU: return "SINGLE_GPU";
        case DistributionMode_SHARDED: return "SHARDED";
        case DistributionMode_REPLICATED: return "REPLICATED";
        default: return "UNKNOWN";
    }
}

/**
 * @brief Thread-safe queue for worker tasks.
 */
template <typename T>
class thread_safe_queue_t {
public:
    void push(T item) {
        std::unique_lock<std::mutex> lock(mu_);
        cond_can_push_.wait(lock, [this]() { return stopped_ || (capacity_ == 0 || queue_.size() < capacity_); });
        if (stopped_) return;
        queue_.push(std::move(item));
        cond_can_pop_.notify_one();
    }

    bool pop(T& item) {
        std::unique_lock<std::mutex> lock(mu_);
        cond_can_pop_.wait(lock, [this]() { return stopped_ || !queue_.empty(); });
        if (queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        cond_can_push_.notify_one();
        return true;
    }

    bool pop_wait(T& item, std::chrono::microseconds timeout) {
        std::unique_lock<std::mutex> lock(mu_);
        if (!cond_can_pop_.wait_for(lock, timeout, [this]() { return stopped_ || !queue_.empty(); })) {
            return false;
        }
        if (queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        cond_can_push_.notify_one();
        return true;
    }

    bool try_pop(T& item) {
        std::unique_lock<std::mutex> lock(mu_);
        if (queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        cond_can_push_.notify_one();
        return true;
    }

    void stop() {
        std::lock_guard<std::mutex> lock(mu_);
        stopped_ = true;
        cond_can_pop_.notify_all();
        cond_can_push_.notify_all();
    }

    bool is_stopped() const {
        std::lock_guard<std::mutex> lock(mu_);
        return stopped_;
    }

    void set_capacity(size_t capacity) {
        std::lock_guard<std::mutex> lock(mu_);
        capacity_ = capacity;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return queue_.size();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mu_);
        return queue_.empty();
    }

private:
    std::queue<T> queue_;
    mutable std::mutex mu_;
    std::condition_variable cond_can_pop_;
    std::condition_variable cond_can_push_;
    size_t capacity_ = 0;
    bool stopped_ = false;
};

struct cuvs_task_result_t {
    std::any result;
    std::exception_ptr error;
};

/**
 * @brief Store for tracking and waiting on async task results.
 */
class cuvs_task_result_store_t {
public:
    uint64_t get_next_job_id() { return next_id_++; }

    void store(uint64_t id, cuvs_task_result_t result) {
        auto& shard = shards_[id % num_shards];
        std::lock_guard<std::mutex> lock(shard.mu);
        shard.results[id] = result;
        auto it = shard.placeholders.find(id);
        if (it != shard.placeholders.end()) {
            it->second.set_value(result);
            shard.placeholders.erase(it);
        }
    }

    std::shared_future<cuvs_task_result_t> wait(uint64_t id) {
        auto& shard = shards_[id % num_shards];
        std::lock_guard<std::mutex> lock(shard.mu);
        auto it = shard.results.find(id);
        if (it != shard.results.end()) {
            std::promise<cuvs_task_result_t> p;
            auto res = std::move(it->second);
            shard.results.erase(it);
            p.set_value(res);
            return p.get_future().share();
        }
        return shard.placeholders[id].get_future().share();
    }

    void stop() {
        for (uint32_t i = 0; i < num_shards; ++i) {
            auto& shard = shards_[i];
            std::lock_guard<std::mutex> lock(shard.mu);
            for (auto& pair : shard.placeholders) {
                try {
                    pair.second.set_exception(std::make_exception_ptr(std::runtime_error("Worker stopped")));
                } catch (...) {}
            }
            shard.placeholders.clear();
        }
    }

private:
    static constexpr uint32_t num_shards = 64;
    struct shard_t {
        std::unordered_map<uint64_t, cuvs_task_result_t> results;
        std::unordered_map<uint64_t, std::promise<cuvs_task_result_t>> placeholders;
        std::mutex mu;
    };

    std::atomic<uint64_t> next_id_{0};
    shard_t shards_[num_shards];
};

/**
 * @brief Wrapper around raft::resources to provide a consistent interface for workers.
 */
class raft_handle_wrapper_t {
public:
    raft_handle_wrapper_t(int device_id, int rank = 0, std::shared_ptr<raft::device_resources_snmg> mg_res = nullptr,
                         distribution_mode_t mode = DistributionMode_SINGLE_GPU) 
        : device_id_(device_id), rank_(rank), mg_res_(mg_res), mode_(mode) {
        if (mg_res) {
            res_ = std::make_shared<raft::resources>(raft::resource::get_device_resources_for_rank(*mg_res, rank));
        } else if (device_id >= 0) {
            res_ = std::make_shared<raft::resources>();
        } else {
            // CPU Context
            res_ = nullptr;
        }
    }

    std::shared_ptr<raft::resources> get_raft_resources() const { return res_; }
    int get_device_id() const { return device_id_; }
    int get_rank() const { return rank_; }
    distribution_mode_t get_mode() const { return mode_; }

    /**
     * @brief Performs synchronization.
     * @param force_all_ranks If true, performs a collective sync across all ranks.
     */
    void sync(bool force_all_ranks = false) {
        if (!res_) return;
        raft::resource::sync_stream(*res_);

        if (force_all_ranks && mg_res_ && rank_ == 0) {
            int num_ranks = 0;
            if (raft::resource::comms_initialized(*res_)) {
                num_ranks = raft::resource::get_comms(*res_).get_size();
            } else {
                num_ranks = raft::resource::get_num_ranks(*res_);
            }

            for (int i = 1; i < num_ranks; ++i) {
                auto rank_res = raft::resource::get_device_resources_for_rank(*mg_res_, i);
                raft::resource::sync_stream(rank_res);
            }
        }
    }

    // Deprecated: use sync()
    void sync_all_devices() { sync(true); }

    void set_index_ptr(std::any ptr) { index_ptr_ = ptr; }
    std::any get_index_ptr() const { return index_ptr_; }

private:
    int device_id_;
    int rank_;
    std::shared_ptr<raft::device_resources_snmg> mg_res_;
    std::shared_ptr<raft::resources> res_;
    distribution_mode_t mode_;
    std::any index_ptr_;
};

class cuvs_worker_t {
public:
    using raft_handle = raft_handle_wrapper_t;
    using task_fn_t = std::function<std::any(raft_handle&)>;

    struct cuvs_task_t {
        uint64_t id;
        task_fn_t fn;
    };

    cuvs_worker_t(uint32_t nthread, const std::vector<int>& devices, distribution_mode_t mode = DistributionMode_SINGLE_GPU)
        : nthread_(std::max(nthread, (uint32_t)devices.size())), devices_(devices), mode_(mode), running_(false), use_batching_(false), per_thread_device_(false), next_device_idx_(0) {
        
        // One queue per physical GPU device
        for (size_t i = 0; i < devices_.size(); ++i) {
            auto q = std::make_unique<thread_safe_queue_t<cuvs_task_t>>();
            q->set_capacity(1000);
            device_queues_.push_back(std::move(q));
        }
        main_tasks_.set_capacity(1000);
    }

    ~cuvs_worker_t() { try { stop(); } catch (...) {} }

    void start(std::function<std::any(raft_handle&)> init_fn = nullptr,
               std::function<std::any(raft_handle&)> stop_fn = nullptr) {
        if (running_) return;
        running_ = true;

        // Start Main Thread (only for main_tasks_)
        main_thread_ = std::thread([this, init_fn, stop_fn] {
            int device_id = devices_.empty() ? -1 : devices_[0];
            if (device_id >= 0) cudaSetDevice(device_id);
            raft_handle handle(device_id, 0, nullptr, mode_);
            if (init_fn) init_fn(handle);
            this->run_main_loop(handle, stop_fn);
        });

        // Start Pool of Device Worker Threads
        for (uint32_t i = 0; i < nthread_; ++i) {
            if (devices_.empty()) break;
            
            // Shared Pool with Device Affinity
            int device_idx = i % devices_.size(); 
            int device_id = devices_[device_idx];
            int rank = device_idx; 

            device_threads_.emplace_back([this, device_id, device_idx, rank, init_fn, stop_fn] {
                cudaSetDevice(device_id);
                
                // Each thread in the pool gets its own raft::resources (so separate CUDA streams)
                raft_handle handle(device_id, rank, nullptr, mode_);
                
                if (init_fn) init_fn(handle);
                this->run_device_loop(handle, stop_fn, device_idx);
            });
        }
    }

    void stop() {
        if (!running_) return;
        running_ = false;

        for (auto& q : device_queues_) q->stop();
        main_tasks_.stop();

        if (main_thread_.joinable()) main_thread_.join();
        for (auto& w : device_threads_) {
            if (w.joinable()) w.join();
        }
        device_threads_.clear();
        results_store_.stop();
    }

    uint64_t submit(task_fn_t fn) {
        if (!running_) throw std::runtime_error("Worker is not running");
        uint64_t id = results_store_.get_next_job_id();
        uint32_t d_idx = next_device_idx_++ % devices_.size();
        device_queues_[d_idx]->push({id, std::move(fn)});
        return id;
    }

    uint64_t submit_main(task_fn_t fn) {
        if (!running_) throw std::runtime_error("Worker is not running");
        uint64_t id = results_store_.get_next_job_id();
        main_tasks_.push({id, std::move(fn)});
        return id;
    }

    uint64_t submit_to_rank(size_t rank, task_fn_t fn) {
        if (!running_) throw std::runtime_error("Worker is not running");
        if (rank >= device_queues_.size())
            throw std::runtime_error("submit_to_rank: rank out of range");
        uint64_t id = results_store_.get_next_job_id();
        device_queues_[rank]->push({id, std::move(fn)});
        return id;
    }

    std::vector<uint64_t> submit_all_devices_no_wait(task_fn_t fn) {
        if (!running_) throw std::runtime_error("Worker is not running");
        std::vector<uint64_t> ids;
        for (size_t i = 0; i < devices_.size(); ++i) {
            uint64_t id = results_store_.get_next_job_id();
            device_queues_[i]->push({id, fn});
            ids.push_back(id);
        }
        return ids;
    }

    void submit_all_devices(task_fn_t fn) {
        auto ids = submit_all_devices_no_wait(fn);
        for (auto id : ids) wait(id).get();
    }

    void broadcast(task_fn_t fn) {
        if (!running_) throw std::runtime_error("Worker is not running");
        std::vector<uint64_t> ids;
        // In shared pool mode, "broadcast" means push to ALL device queues
        // Multiple threads per queue will pick them up
        for (size_t i = 0; i < devices_.size(); ++i) {
            uint64_t id = results_store_.get_next_job_id();
            device_queues_[i]->push({id, fn});
            ids.push_back(id);
        }
        for (auto id : ids) wait(id).get();
    }

    std::shared_future<cuvs_task_result_t> wait(uint64_t task_id) {
        return results_store_.wait(task_id);
    }

    distribution_mode_t get_mode() const { return mode_; }

    uint32_t nthread() const { return nthread_; }

    void set_use_batching(bool enable) { use_batching_ = enable; }
    bool use_batching() const { return use_batching_; }
    void set_per_thread_device(bool enable) { per_thread_device_ = enable; }

    template <typename ResT, typename ReqT>
    std::future<ResT> submit_batched(const std::string& key, ReqT req, 
                                     std::function<void(raft_handle&, const std::vector<std::any>&, const std::vector<std::function<void(std::any)>>&)> exec_fn) {
        bool should_flush_now = false;
        bool should_schedule = false;
        std::future<ResT> future;
        
        std::shared_ptr<batch_t> batch;
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            if (!running_) throw std::runtime_error("Worker not running");

            auto& b = batches_[key];
            if (!b) {
                b = std::make_shared<batch_t>();
                b->exec_fn = exec_fn;
                b->scheduled = false;
            }
            batch = b;
        }

        {
            std::lock_guard<std::mutex> lock(batch->mu);
            if (!batch->scheduled) {
                batch->scheduled = true;
                should_schedule = true;
            }

            auto promise = std::make_shared<std::promise<ResT>>();
            future = promise->get_future();
            batch->reqs.push_back(req);
            batch->setters.push_back([promise](std::any res) {
                if (res.type() == typeid(std::exception_ptr)) {
                    promise->set_exception(std::any_cast<std::exception_ptr>(res));
                } else {
                    promise->set_value(std::any_cast<ResT>(res));
                }
            });

            if (batch->reqs.size() >= 16) {
                should_flush_now = true;
            }
        }

        if (should_flush_now) {
            this->flush_batch(key);
        } else if (should_schedule) {
            this->submit([this, key](raft_handle&) -> std::any {
                std::this_thread::sleep_for(std::chrono::microseconds(100));
                this->flush_batch(key);
                return std::any();
            });
        }

        return future;
    }

private:
    struct batch_t {
        std::vector<std::any> reqs;
        std::vector<std::function<void(std::any)>> setters;
        std::function<void(raft_handle&, const std::vector<std::any>&, const std::vector<std::function<void(std::any)>>&)> exec_fn;
        std::atomic<bool> scheduled;
        std::mutex mu;
    };

    void run_device_loop(raft_handle& handle, std::function<std::any(raft_handle&)> stop_fn, uint32_t d_idx) {
        cuvs_task_t task;
        while (device_queues_[d_idx]->pop(task)) {
            execute_task(task, handle);
        }
        if (stop_fn) stop_fn(handle);
    }

    void run_main_loop(raft_handle& handle, std::function<std::any(raft_handle&)> stop_fn) {
        cuvs_task_t task;
        while (main_tasks_.pop(task)) {
            execute_task(task, handle);
        }
        if (stop_fn) stop_fn(handle);
    }

    void execute_task(const cuvs_task_t& task, raft_handle& handle) {
        cuvs_task_result_t result;
        try {
            result.result = task.fn(handle);
        } catch (const std::exception& e) {
            std::cout << "[ERROR " << get_timestamp() << "] Worker execute_task error id=" << task.id << " rank=" << handle.get_rank() << ": " << e.what() << std::endl;
            result.error = std::current_exception();
        } catch (...) {
            std::cout << "[ERROR " << get_timestamp() << "] Worker execute_task unknown error id=" << task.id << " rank=" << handle.get_rank() << std::endl;
            result.error = std::current_exception();
        }
        results_store_.store(task.id, result);
    }

    void flush_batch(const std::string& key) {
        std::shared_ptr<batch_t> batch;
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            auto it = batches_.find(key);
            if (it == batches_.end()) return;
            batch = it->second;
        }

        std::vector<std::any> reqs;
        std::vector<std::function<void(std::any)>> setters;
        {
            std::lock_guard<std::mutex> lock(batch->mu);
            if (batch->reqs.empty()) {
                batch->scheduled = false;
                return;
            }
            reqs = std::move(batch->reqs);
            setters = std::move(batch->setters);
            batch->scheduled = false;
        }
        
        auto exec_fn = batch->exec_fn;
        auto task_fn = [reqs = std::move(reqs), setters = std::move(setters), exec_fn, key](raft_handle& handle) -> std::any {
            try {
                exec_fn(handle, reqs, setters);
            } catch (const std::exception& e) {
                std::cout << "[ERROR " << get_timestamp() << "] Worker batch exec_fn error key=" << key << ": " << e.what() << std::endl;
                auto err = std::current_exception();
                for (auto& setter : setters) setter(err);
            } catch (...) {
                std::cout << "[ERROR " << get_timestamp() << "] Worker batch exec_fn unknown error key=" << key << std::endl;
                auto err = std::current_exception();
                for (auto& setter : setters) setter(err);
            }
            return std::any();
        };

        try {
            if (mode_ == DistributionMode_SHARDED) {
                this->submit_main(task_fn);
            } else {
                this->submit(task_fn);
            }
        } catch (...) {
            auto err = std::current_exception();
            for (auto& setter : setters) setter(err);
        }
    }

    uint32_t nthread_;
    std::vector<int> devices_;
    distribution_mode_t mode_;
    std::thread main_thread_;
    std::vector<std::thread> device_threads_;
    std::atomic<bool> running_;
    bool use_batching_;
    bool per_thread_device_;

    std::vector<std::unique_ptr<thread_safe_queue_t<cuvs_task_t>>> device_queues_;
    thread_safe_queue_t<cuvs_task_t> main_tasks_;
    std::atomic<uint32_t> next_device_idx_;

    cuvs_task_result_store_t results_store_;

    std::mutex batch_mutex_;
    std::map<std::string, std::shared_ptr<batch_t>> batches_;
};

} // namespace matrixone
