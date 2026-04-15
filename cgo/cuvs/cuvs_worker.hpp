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
//      - Sets stopping_ = true (blocks new external submit* calls immediately).
//      - Flushes all pending batches while threads are still running.
//      - Waits (via condition variable) for all in-flight tasks to complete.
//      - Sets running_ = false, stops queues, joins all threads.
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
//   - Enabled by set_batch_window(window_us > 0) on the index; 0 = disabled.
//   - submit_batched(key, req, exec_fn): groups requests under a key (per-index
//     string), flushes when >= 16 requests accumulate or after window_us delay.
//   - exec_fn receives the batched requests and a vector of per-request setters
//     (callbacks to resolve individual futures).
//   - The batch is flushed either eagerly (>= 16 reqs) or via a scheduled task
//     that sleeps window_us µs to allow more requests to arrive.
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
//     extend   → submit_to_rank (only extend to last shard)
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
        if (stopped_) throw std::runtime_error("Queue is stopped");
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
        std::shared_ptr<std::promise<cuvs_task_result_t>> promise;
        {
            std::lock_guard<std::mutex> lock(shard.mu);
            if (stopped_) return; 

            auto it = shard.placeholders.find(id);
            if (it != shard.placeholders.end()) {
                if (it->second.promise) {
                    promise = std::move(it->second.promise);
                }
                shard.placeholders.erase(it);  // prevent leak: placeholder served its purpose
            } else {
                shard.results[id] = result;
            }
        }
        if (promise) {
            try { promise->set_value(result); } catch (...) {}
        }
    }

    std::shared_future<cuvs_task_result_t> wait(uint64_t id) {
        auto& shard = shards_[id % num_shards];
        std::lock_guard<std::mutex> lock(shard.mu);

        if (stopped_) {
            auto p = std::make_shared<std::promise<cuvs_task_result_t>>();
            auto f = p->get_future().share();
            p->set_exception(std::make_exception_ptr(std::runtime_error("Worker stopped")));
            return f;
        }

        auto pit = shard.placeholders.find(id);
        if (pit != shard.placeholders.end()) {
            return pit->second.future;
        }

        auto rit = shard.results.find(id);
        if (rit != shard.results.end()) {
            auto p = std::make_shared<std::promise<cuvs_task_result_t>>();
            auto f = p->get_future().share();
            p->set_value(std::move(rit->second));
            shard.results.erase(rit);
            // Do not cache into placeholders — result is consumed once; callers
            // must hold the returned shared_future for multiple .get() calls.
            return f;
        }

        auto p = std::make_shared<std::promise<cuvs_task_result_t>>();
        auto f = p->get_future().share();
        shard.placeholders[id] = {p, f};
        return f;
    }

    // Releases all bookkeeping for `id`. Must be called when the result is no
    // longer needed (e.g. fire-and-forget tasks) or when the caller abandons
    // the future. Any subsequent wait(id) will create a new unfulfilled future.
    void discard(uint64_t id) {
        auto& shard = shards_[id % num_shards];
        std::lock_guard<std::mutex> lock(shard.mu);
        shard.results.erase(id);
        shard.placeholders.erase(id);
    }

    void stop() {
        for (uint32_t i = 0; i < num_shards; ++i) {
            auto& shard = shards_[i];
            decltype(shard.placeholders) phs;
            {
                std::lock_guard<std::mutex> lock(shard.mu);
                stopped_ = true;
                phs = std::move(shard.placeholders);
                shard.results.clear();
            }
            for (auto& [id, ph] : phs) {
                try {
                    if (ph.promise) {
                        ph.promise->set_exception(
                            std::make_exception_ptr(std::runtime_error("Worker stopped")));
                    }
                } catch (...) {}
            }
        }
    }

private:
    static constexpr uint32_t num_shards = 64;
    std::atomic<bool> stopped_{false};

    struct placeholder_t {
        std::shared_ptr<std::promise<cuvs_task_result_t>> promise;
        std::shared_future<cuvs_task_result_t> future;
    };

    struct shard_t {
        std::unordered_map<uint64_t, cuvs_task_result_t> results;
        std::unordered_map<uint64_t, placeholder_t> placeholders;
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
    raft_handle_wrapper_t(int device_id, int rank = 0, 
                         distribution_mode_t mode = DistributionMode_SINGLE_GPU) 
        : device_id_(device_id), rank_(rank), mode_(mode) {
        if (device_id >= 0) {
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
    void sync() {
        if (!res_) return;
        raft::resource::sync_stream(*res_);
    }

    void set_index_ptr(std::any ptr) { index_ptr_ = ptr; }
    std::any get_index_ptr() const { return index_ptr_; }

private:
    int device_id_;
    int rank_;
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
        bool fire_and_forget = false;  // skip results_store_ — used for internal flush tasks
    };

    cuvs_worker_t(uint32_t nthread, const std::vector<int>& devices, distribution_mode_t mode = DistributionMode_SINGLE_GPU)
        : nthread_(std::max(nthread, (uint32_t)devices.size())), devices_(devices), mode_(mode), running_(false), stopping_(false), batch_window_us_(0), per_thread_device_(false), next_device_idx_(0), in_flight_tasks_(0) {
        
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
        stopping_.store(false);
        running_ = true;

        // Start Main Thread (only for main_tasks_)
        main_thread_ = std::thread([this, init_fn, stop_fn] {
            int device_id = devices_.empty() ? -1 : devices_[0];
            if (device_id >= 0) cudaSetDevice(device_id);
            raft_handle handle(device_id, 0, mode_);
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
                raft_handle handle(device_id, rank, mode_);
                
		// only main thread will run init_fn and stop_fn
                this->run_device_loop(handle, nullptr, device_idx);
            });
        }
    }

    void stop() {
        bool was_stopping = stopping_.exchange(true);
        if (was_stopping) return;  // already stopping or stopped

        // 1. Flush all pending batches while threads are still running.
        //    stopping_ = true blocks new external submit* calls; running_ is still
        //    true here so flush_batch's internal submissions go through normally.
        {
            std::vector<std::string> keys;
            {
                std::lock_guard<std::mutex> lock(batch_mutex_);
                for (auto const& [key, b] : batches_) keys.push_back(key);
            }
            for (auto const& key : keys) {
                this->flush_batch(key);
            }

            // Cancel anything that still hasn't been flushed (race safety)
            std::lock_guard<std::mutex> lock(batch_mutex_);
            auto err = std::make_exception_ptr(std::runtime_error("Worker stopped (batch cancelled)"));
            for (auto& [key, batch] : batches_) {
                std::lock_guard<std::mutex> b_lock(batch->mu);
                if (!batch->flushed) {
                    batch->flushed = true;
                    for (auto& setter : batch->setters) {
                        try { setter(err); } catch (...) {}
                    }
                    batch->setters.clear();
                }
            }
            batches_.clear();
        }

        // 2. Wait for all in-flight tasks (including flush tasks) to complete
        this->sync();

        // 3. Now block all further submissions and drain the thread queues
        running_.store(false);
        for (auto& q : device_queues_) q->stop();
        main_tasks_.stop();

        if (main_thread_.joinable()) main_thread_.join();
        for (auto& w : device_threads_) {
            if (w.joinable()) w.join();
        }
        device_threads_.clear();

        // 4. Drain physical queues (defensive; should be empty after sync)
        cuvs_task_t task;
        while (main_tasks_.try_pop(task)) {
            in_flight_tasks_--;
            if (!task.fire_and_forget) {
                cuvs_task_result_t result;
                result.error = std::make_exception_ptr(std::runtime_error("Worker stopped"));
                results_store_.store(task.id, result);
            }
        }
        for (auto& q : device_queues_) {
            while (q->try_pop(task)) {
                in_flight_tasks_--;
                if (!task.fire_and_forget) {
                    cuvs_task_result_t result;
                    result.error = std::make_exception_ptr(std::runtime_error("Worker stopped"));
                    results_store_.store(task.id, result);
                }
            }
        }

        results_store_.stop();
    }

    void sync() {
        std::unique_lock<std::mutex> lock(sync_mu_);
        sync_cv_.wait(lock, [this]() { return in_flight_tasks_.load() == 0; });
    }

    uint64_t submit(task_fn_t fn) {
        if (!running_ || stopping_) throw std::runtime_error("Worker is not running");
        if (devices_.empty()) throw std::runtime_error("No devices configured");
        in_flight_tasks_++;
        uint64_t id = results_store_.get_next_job_id();
        try {
            uint32_t d_idx = next_device_idx_++ % devices_.size();
            device_queues_[d_idx]->push({id, std::move(fn)});
        } catch (...) {
            in_flight_tasks_--;
            results_store_.discard(id);
            throw;
        }
        return id;
    }

    uint64_t submit_main(task_fn_t fn) {
        if (!running_ || stopping_) throw std::runtime_error("Worker is not running");
        in_flight_tasks_++;
        uint64_t id = results_store_.get_next_job_id();
        try {
            main_tasks_.push({id, std::move(fn)});
        } catch (...) {
            in_flight_tasks_--;
            results_store_.discard(id);
            throw;
        }
        return id;
    }

    uint64_t submit_to_rank(size_t rank, task_fn_t fn) {
        if (!running_ || stopping_) throw std::runtime_error("Worker is not running");
        if (rank >= device_queues_.size())
            throw std::runtime_error("submit_to_rank: rank out of range");
        in_flight_tasks_++;
        uint64_t id = results_store_.get_next_job_id();
        try {
            device_queues_[rank]->push({id, std::move(fn)});
        } catch (...) {
            in_flight_tasks_--;
            results_store_.discard(id);
            throw;
        }
        return id;
    }

    std::vector<uint64_t> submit_all_devices_no_wait(task_fn_t fn) {
        if (!running_ || stopping_) throw std::runtime_error("Worker is not running");
        std::vector<uint64_t> ids;
        for (size_t i = 0; i < devices_.size(); ++i) {
            in_flight_tasks_++;
            uint64_t id = results_store_.get_next_job_id();
            try {
                device_queues_[i]->push({id, fn});
            } catch (...) {
                in_flight_tasks_--;
                results_store_.discard(id);
                throw;
            }
            ids.push_back(id);
        }
        return ids;
    }

    void submit_all_devices(task_fn_t fn) {
        auto ids = submit_all_devices_no_wait(fn);
        for (auto id : ids) wait(id).get();
    }


    std::shared_future<cuvs_task_result_t> wait(uint64_t task_id) {
        return results_store_.wait(task_id);
    }

    distribution_mode_t get_mode() const { return mode_; }

    uint32_t nthread() const { return nthread_; }

    void set_batch_window(int64_t window_us) {
        // Sync and flush before changing mode
        this->sync();
        std::vector<std::string> keys;
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            for (auto const& [key, b] : batches_) keys.push_back(key);
        }
        for (auto const& key : keys) {
            this->flush_batch(key);
        }
        this->sync();
        batch_window_us_ = window_us;
    }
    int64_t batch_window() const { return batch_window_us_; }
    void set_per_thread_device(bool enable) { per_thread_device_ = enable; }

    template <typename ResT, typename ReqT>
    std::shared_future<ResT> submit_batched(const std::string& key, ReqT req, 
                                     std::function<void(raft_handle&, const std::vector<std::any>&, const std::vector<std::function<void(std::any)>>&)> exec_fn) {
        bool should_flush_now = false;
        bool should_schedule = false;
        std::shared_future<ResT> future;
        
        while (true) {
            std::shared_ptr<batch_t> batch;
            {
                std::lock_guard<std::mutex> lock(batch_mutex_);
                if (!running_ || stopping_) throw std::runtime_error("Worker not running");

                auto& b = batches_[key];
                if (!b) {
                    b = std::make_shared<batch_t>();
                    b->scheduled = false;
                    b->flushed = false;
                }
                b->exec_fn = exec_fn;
                batch = b;
            }

            {
                std::lock_guard<std::mutex> lock(batch->mu);
                if (batch->flushed) continue; // Race: retry with new batch

                if (!batch->scheduled) {
                    batch->scheduled = true;
                    should_schedule = true;
                }

                auto promise = std::make_shared<std::promise<ResT>>();
                future = promise->get_future().share();
                batch->reqs.push_back(req);

                auto fulfilled = std::make_shared<std::atomic<bool>>(false);
                batch->setters.push_back([promise, fulfilled](std::any res) {
                    if (fulfilled->exchange(true)) return;
                    try {
                        if (res.type() == typeid(std::exception_ptr)) {
                            promise->set_exception(std::any_cast<std::exception_ptr>(res));
                        } else {
                            promise->set_value(std::any_cast<ResT>(res));
                        }
                    } catch (const std::future_error& e) {
                    } catch (...) {
                        try { promise->set_exception(std::current_exception()); } catch (...) {}
                    }
                });
                if (batch->reqs.size() >= 16) {
                    should_flush_now = true;
                }
            }

            if (should_flush_now) {
                this->flush_batch(key);
            } else if (should_schedule) {
                try {
                    this->submit_fire_and_forget([this, key](raft_handle&) -> std::any {
                        std::this_thread::sleep_for(std::chrono::microseconds(batch_window_us_));
                        this->flush_batch(key);
                        return std::any();
                    });
                } catch (...) {
                    this->flush_batch(key);
                }
            }
            break;
        }

        return future;
    }

private:
    struct batch_t {
        std::vector<std::any> reqs;
        std::vector<std::function<void(std::any)>> setters;
        std::function<void(raft_handle&, const std::vector<std::any>&, const std::vector<std::function<void(std::any)>>&)> exec_fn;
        bool scheduled;
        bool flushed;
        std::mutex mu;

        ~batch_t() {
            if (!setters.empty()) {
                auto err = std::make_exception_ptr(std::runtime_error("Batch destroyed (broken promise)"));
                for (auto& s : setters) {
                    try { s(err); } catch (...) {}
                }
            }
        }
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
        struct flight_guard {
            std::atomic<int64_t>& counter;
            std::condition_variable& cv;
            std::mutex& mu;
            ~flight_guard() {
                if (--counter == 0) {
                    // Acquire sync_mu_ before notifying to prevent the lost-wakeup
                    // race: without it, a notify between sync()'s pred-false check
                    // and its first cv.wait() call would fire to nobody and hang.
                    std::lock_guard<std::mutex> lk(mu);
                    cv.notify_all();
                }
            }
        } guard{in_flight_tasks_, sync_cv_, sync_mu_};

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
        if (!task.fire_and_forget) {
            results_store_.store(task.id, result);
        }
    }

    void flush_batch(const std::string& key) {
        std::shared_ptr<batch_t> batch;
        std::function<void(raft_handle&, const std::vector<std::any>&, const std::vector<std::function<void(std::any)>>&)> exec_fn;
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            auto it = batches_.find(key);
            if (it == batches_.end()) return;
            batch = it->second;
            exec_fn = batch->exec_fn;
        }

        std::vector<std::any> reqs;
        std::vector<std::function<void(std::any)>> setters;
        {
            std::lock_guard<std::mutex> lock(batch->mu);
            if (batch->flushed || batch->reqs.empty()) {
                batch->scheduled = false;
                return;
            }
            batch->flushed = true;
            reqs = std::move(batch->reqs);
            setters = std::move(batch->setters);
            batch->scheduled = false;
        }

        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            if (batches_.count(key) > 0 && batches_[key] == batch) {
                batches_.erase(key);
            }
        }

        struct setters_guard_t {
            std::vector<std::function<void(std::any)>> setters;
            bool fulfilled = false;
            ~setters_guard_t() {
                if (!fulfilled) {
                    auto err = std::make_exception_ptr(std::runtime_error("Batch task cancelled"));
                    for (auto& setter : setters) {
                        try { setter(err); } catch (...) {}
                    }
                }
            }
        };
        auto guard = std::make_shared<setters_guard_t>();
        guard->setters = std::move(setters);

        auto task_fn = [reqs = std::move(reqs), guard, exec_fn, key](raft_handle& handle) -> std::any {
            try {
                exec_fn(handle, reqs, guard->setters);
            } catch (const std::exception& e) {
                std::ostringstream oss;
                oss << "[ERROR " << get_timestamp() << "] Worker batch exec_fn error key=" << key << ": " << e.what();
                fprintf(stderr, "%s\n", oss.str().c_str());
                auto err = std::current_exception();
                for (auto& setter : guard->setters) {
                    try { setter(err); } catch (...) {}
                }
            } catch (...) {
                std::ostringstream oss;
                oss << "[ERROR " << get_timestamp() << "] Worker batch exec_fn unknown error key=" << key;
                fprintf(stderr, "%s\n", oss.str().c_str());
                auto err = std::current_exception();
                for (auto& setter : guard->setters) {
                    try { setter(err); } catch (...) {}
                }
            }
            guard->fulfilled = true;
            return std::any();
        };

        try {
            if (mode_ == DistributionMode_SHARDED) {
                this->submit_fire_and_forget_main(task_fn);
            } else {
                this->submit_fire_and_forget(task_fn);
            }
        } catch (...) {
        }
    }

    // Internal helpers for fire-and-forget tasks (flush tasks, scheduled batches).
    // These bypass the results_store_ entirely — no id is allocated, no result stored.
    void submit_fire_and_forget(task_fn_t fn) {
        if (!running_) throw std::runtime_error("Worker is not running");
        if (devices_.empty()) throw std::runtime_error("No devices configured");
        in_flight_tasks_++;
        try {
            uint32_t d_idx = next_device_idx_++ % devices_.size();
            device_queues_[d_idx]->push({0, std::move(fn), /*fire_and_forget=*/true});
        } catch (...) {
            in_flight_tasks_--;
            throw;
        }
    }

    void submit_fire_and_forget_main(task_fn_t fn) {
        if (!running_) throw std::runtime_error("Worker is not running");
        in_flight_tasks_++;
        try {
            main_tasks_.push({0, std::move(fn), /*fire_and_forget=*/true});
        } catch (...) {
            in_flight_tasks_--;
            throw;
        }
    }

    uint32_t nthread_;
    std::vector<int> devices_;
    distribution_mode_t mode_;
    std::thread main_thread_;
    std::vector<std::thread> device_threads_;
    std::atomic<bool> running_;
    std::atomic<bool> stopping_;  // set true at start of stop(); blocks new external submits
    int64_t batch_window_us_;  // batching window in microseconds; 0 = disabled
    bool per_thread_device_;

    std::vector<std::unique_ptr<thread_safe_queue_t<cuvs_task_t>>> device_queues_;
    thread_safe_queue_t<cuvs_task_t> main_tasks_;
    std::atomic<uint32_t> next_device_idx_;
    std::atomic<int64_t> in_flight_tasks_;
    std::mutex            sync_mu_;
    std::condition_variable sync_cv_;

    cuvs_task_result_store_t results_store_;

    std::mutex batch_mutex_;
    std::map<std::string, std::shared_ptr<batch_t>> batches_;
};

} // namespace matrixone
