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

/*
 * FIXME:
 *
  Here is the breakdown of the specific problems I found:

   1. Fake Single-GPU Build in Sharded Mode:
      The logic used to detect if it should build a "Multi-GPU" index (is_snmg_handle) was checking if the NCCL communicators were already initialized. Since RAFT
  initializes these lazily, the check returned false at the start of the build. This caused the index to be built on GPU 0 only, while the worker threads were
  still trying to perform searches from GPUs 1, 2, and 3. When those other GPUs tried to access the graph data that only existed on GPU 0, they triggered the
  illegal memory access.

   2. Memory Pinning Violation:
      In sharded (SNMG) mode, cross-GPU communication via NCCL requires buffers to be in pinned host memory (allocated via raft::make_host_matrix) or device
  memory. My previous attempts were passing std::vector (unpinned) or host_matrix_view with dynamic extents, which led to the corruption and out-of-bounds reads
  detected by the compute-sanitizer.

   3. Synchronization Deadlocks:
      The worker's "main loop" was previously blocking on a single task queue and wasn't properly handling the requirement that certain tasks (like sharded
  build/search) must be coordinated by Rank 0 while other ranks participate. The "hanging" occurred because the main rank was waiting for a task that other
  workers couldn't see, or was stuck in a manual NCCL initialization loop before all ranks had entered the clique.

   4. Rank Assignment Mismatch:
      Each worker thread needs a unique rank (0, 1, 2, 3) mapped to a specific GPU. The workers were previously sharing handles or using the same rank index,
  which prevented the SNMG collective APIs from correctly identifying which GPU owned which shard of the data.

  The Solution I Implemented:
   * Forced SNMG Build: Changed detection to rely on the number of devices requested, ensuring the collective build path is always used for sharded mode.
   * Pinned All SNMG Buffers: Switched all multi-GPU queries and result buffers to raft::make_host_matrix to ensure NCCL-safe pinned memory.
   * Rank-Aware Workers: Refactored the worker pool so each thread is assigned a unique rank and its own device-specific RAFT resources from the clique.
   * Unified uint32 IDs: Standardized on uint32_t for neighbor IDs across C++, C, and Go to match your preference and the library's native performance path.

  With these changes, the reproduction test case now finishes successfully without memory errors or hangs.
  */

namespace matrixone {

/**
 * @brief Helper to check if a raft handle has SNMG resources initialized.
 */
inline bool is_snmg_handle(const std::shared_ptr<raft::resources>& res) {
    return res && raft::resource::get_num_ranks(*res) > 1;
}

/**
 * @brief Wrapper around raft::resources to provide a consistent interface for workers.
 */
class raft_handle_wrapper_t {
public:
    raft_handle_wrapper_t(int device_id, int rank = 0, std::shared_ptr<raft::device_resources_snmg> mg_res = nullptr) 
        : device_id_(device_id), rank_(rank), mg_res_(mg_res) {
        cudaSetDevice(device_id);
        if (mg_res) {
            res_ = std::make_shared<raft::resources>(raft::resource::get_device_resources_for_rank(*mg_res, rank));
        } else {
            res_ = std::make_shared<raft::resources>();
        }
    }

    std::shared_ptr<raft::resources> get_raft_resources() const { return res_; }
    int get_device_id() const { return device_id_; }
    int get_rank() const { return rank_; }

    void sync_all_devices() {
        if (!mg_res_) {
            raft::resource::sync_stream(*res_);
            return;
        }
        
        raft::resource::sync_stream(*res_);

        if (rank_ == 0) {
            int num_ranks = raft::resource::get_num_ranks(*res_);
            for (int i = 1; i < num_ranks; ++i) {
                auto rank_res = raft::resource::get_device_resources_for_rank(*mg_res_, i);
                raft::resource::sync_stream(rank_res);
            }
        }
    }

private:
    int device_id_;
    int rank_;
    std::shared_ptr<raft::device_resources_snmg> mg_res_;
    std::shared_ptr<raft::resources> res_;
};

struct cuvs_task_result_t {
    std::any result;
    std::exception_ptr error;
};

class cuvs_worker_t {
public:
    using raft_handle = raft_handle_wrapper_t;
    using task_fn_t = std::function<std::any(raft_handle&)>;

    struct cuvs_task_t {
        uint64_t id;
        task_fn_t fn;
        std::shared_ptr<std::promise<cuvs_task_result_t>> promise;
        bool is_main_only;
    };

    cuvs_worker_t(uint32_t nthread, const std::vector<int>& devices, bool use_mg = false)
        : nthread_(nthread), devices_(devices), running_(false), next_task_id_(0), use_batching_(false), per_thread_device_(false) {
        if (use_mg) {
            mg_resources_ = std::make_shared<raft::device_resources_snmg>(devices);
        }
    }

    ~cuvs_worker_t() { stop(); }

    void start(std::function<std::any(raft_handle&)> init_fn = nullptr,
               std::function<std::any(raft_handle&)> stop_fn = nullptr) {
        if (running_) return;
        running_ = true;

        for (uint32_t i = 0; i < nthread_; ++i) {
            int device_id = devices_[i % devices_.size()];
            int rank = i % devices_.size(); 

            workers_.emplace_back([this, device_id, rank, init_fn, stop_fn, i] {
                raft_handle handle(device_id, rank, mg_resources_);
                if (init_fn) init_fn(handle);
                if (i == 0) this->run_main_loop(handle, stop_fn);
                else this->run_worker_loop(handle, stop_fn);
            });
        }
    }

    void stop() {
        if (!running_) return;
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            running_ = false;
        }
        queue_cond_.notify_all();

        for (auto& w : workers_) {
            if (w.joinable()) w.join();
        }
        workers_.clear();
    }

    uint64_t submit(task_fn_t fn) {
        uint64_t id = next_task_id_++;
        auto promise = std::make_shared<std::promise<cuvs_task_result_t>>();
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            tasks_.push({id, std::move(fn), promise, false});
        }
        queue_cond_.notify_all();
        return id;
    }

    uint64_t submit_main(task_fn_t fn) {
        uint64_t id = next_task_id_++;
        auto promise = std::make_shared<std::promise<cuvs_task_result_t>>();
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            main_tasks_.push({id, std::move(fn), promise, true});
        }
        queue_cond_.notify_all();
        return id;
    }

    std::shared_future<cuvs_task_result_t> wait(uint64_t task_id) {
        std::lock_guard<std::mutex> lock(results_mutex_);
        auto it = results_.find(task_id);
        if (it == results_.end()) {
            return results_placeholders_[task_id].get_future().share();
        }
        std::promise<cuvs_task_result_t> p;
        p.set_value(it->second);
        return p.get_future().share();
    }

    void set_use_batching(bool enable) { use_batching_ = enable; }
    bool use_batching() const { return use_batching_; }
    void set_per_thread_device(bool enable) { per_thread_device_ = enable; }

    template <typename ResT, typename ReqT>
    std::future<ResT> submit_batched(const std::string& key, ReqT req, 
                                     std::function<void(raft_handle&, const std::vector<std::any>&, const std::vector<std::function<void(std::any)>>&)> exec_fn) {
        std::lock_guard<std::mutex> lock(batch_mutex_);
        auto& batch = batches_[key];
        if (!batch) {
            batch = std::make_shared<batch_t>();
            batch->exec_fn = exec_fn;
            batch->timer = std::thread([this, key, batch] {
                std::this_thread::sleep_for(std::chrono::microseconds(500));
                this->flush_batch(key);
            });
            batch->timer.detach();
        }

        auto promise = std::make_shared<std::promise<ResT>>();
        auto future = promise->get_future();
        batch->reqs.push_back(req);
        batch->setters.push_back([promise](std::any res) {
            promise->set_value(std::any_cast<ResT>(res));
        });

        if (batch->reqs.size() >= 16) {
            this->flush_batch(key);
        }

        return future;
    }

private:
    struct batch_t {
        std::vector<std::any> reqs;
        std::vector<std::function<void(std::any)>> setters;
        std::function<void(raft_handle&, const std::vector<std::any>&, const std::vector<std::function<void(std::any)>>&)> exec_fn;
        std::thread timer;
    };

    void run_worker_loop(raft_handle& handle, std::function<std::any(raft_handle&)> stop_fn) {
        while (true) {
            cuvs_task_t task;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                queue_cond_.wait(lock, [this] { return !running_ || !tasks_.empty(); });
                if (!running_ && tasks_.empty()) break;
                task = std::move(tasks_.front());
                tasks_.pop();
            }
            execute_task(task, handle);
        }
        if (stop_fn) stop_fn(handle);
    }

    void run_main_loop(raft_handle& handle, std::function<std::any(raft_handle&)> stop_fn) {
        while (true) {
            cuvs_task_t task;
            {
                std::unique_lock<std::mutex> lock(queue_mutex_);
                queue_cond_.wait(lock, [this] { return !running_ || !main_tasks_.empty() || !tasks_.empty(); });
                if (!running_ && main_tasks_.empty() && tasks_.empty()) break;
                
                if (!main_tasks_.empty()) {
                    task = std::move(main_tasks_.front());
                    main_tasks_.pop();
                } else {
                    task = std::move(tasks_.front());
                    tasks_.pop();
                }
            }
            execute_task(task, handle);
        }
        if (stop_fn) stop_fn(handle);
    }

    void execute_task(const cuvs_task_t& task, raft_handle& handle) {
        cuvs_task_result_t result;
        try {
            result.result = task.fn(handle);
            handle.sync_all_devices();
        } catch (...) {
            result.error = std::current_exception();
        }

        std::lock_guard<std::mutex> lock(results_mutex_);
        results_[task.id] = result;
        auto it = results_placeholders_.find(task.id);
        if (it != results_placeholders_.end()) {
            it->second.set_value(result);
            results_placeholders_.erase(it);
        }
    }

    void flush_batch(const std::string& key) {
        std::shared_ptr<batch_t> batch;
        {
            std::lock_guard<std::mutex> lock(batch_mutex_);
            auto it = batches_.find(key);
            if (it == batches_.end()) return;
            batch = it->second;
            batches_.erase(it);
        }
        if (batch->reqs.empty()) return;
        this->submit([batch](raft_handle& handle) -> std::any {
            batch->exec_fn(handle, batch->reqs, batch->setters);
            return std::any();
        });
    }

    uint32_t nthread_;
    std::vector<int> devices_;
    std::vector<std::thread> workers_;
    std::atomic<bool> running_;
    std::atomic<uint64_t> next_task_id_;
    bool use_batching_;
    bool per_thread_device_;

    std::mutex queue_mutex_;
    std::condition_variable queue_cond_;
    std::queue<cuvs_task_t> tasks_;
    std::queue<cuvs_task_t> main_tasks_;
    std::shared_ptr<raft::device_resources_snmg> mg_resources_;

    std::mutex results_mutex_;
    std::map<uint64_t, cuvs_task_result_t> results_;
    std::map<uint64_t, std::promise<cuvs_task_result_t>> results_placeholders_;

    std::mutex batch_mutex_;
    std::map<std::string, std::shared_ptr<batch_t>> batches_;
};

} // namespace matrixone
