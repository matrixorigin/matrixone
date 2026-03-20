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
 * The cuvs_worker_t manages a pool of threads, each assigned a unique rank and its own device-specific
 * RAFT resources. This architecture addresses several critical issues found in sharded (SNMG) mode:
 *
 * 1. Build Coordination: Collective builds are correctly performed by ensuring all ranks participate.
 * 2. Memory Safety: All multi-GPU query and result buffers must use pinned host memory (raft::make_host_matrix)
 *    to satisfy NCCL requirements.
 * 3. Multi-threaded Search: Search operations can be executed on any worker thread (using submit())
 *    rather than being restricted to the main thread.
 * 4. Rank Integrity: Each worker thread maintains its own raft_handle_wrapper_t with a unique rank
 *    and device assignment.
 */

namespace matrixone {

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
        std::lock_guard<std::mutex> lock(mu_);
        results_[id] = result;
        auto it = placeholders_.find(id);
        if (it != placeholders_.end()) {
            it->second.set_value(result);
            placeholders_.erase(it);
        }
    }

    std::shared_future<cuvs_task_result_t> wait(uint64_t id) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = results_.find(id);
        if (it != results_.end()) {
            std::promise<cuvs_task_result_t> p;
            p.set_value(it->second);
            return p.get_future().share();
        }
        return placeholders_[id].get_future().share();
    }

    void stop() {
        std::lock_guard<std::mutex> lock(mu_);
        for (auto& pair : placeholders_) {
            pair.second.set_exception(std::make_exception_ptr(std::runtime_error("Worker stopped")));
        }
        placeholders_.clear();
    }

private:
    std::atomic<uint64_t> next_id_{0};
    std::map<uint64_t, cuvs_task_result_t> results_;
    std::map<uint64_t, std::promise<cuvs_task_result_t>> placeholders_;
    std::mutex mu_;
};

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

class cuvs_worker_t {
public:
    using raft_handle = raft_handle_wrapper_t;
    using task_fn_t = std::function<std::any(raft_handle&)>;

    struct cuvs_task_t {
        uint64_t id;
        task_fn_t fn;
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
        results_store_.stop();
    }

    uint64_t submit(task_fn_t fn) {
        uint64_t id = results_store_.get_next_job_id();
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (!running_) throw std::runtime_error("Worker is not running");
            tasks_.push({id, std::move(fn), false});
        }
        queue_cond_.notify_all();
        return id;
    }

    uint64_t submit_main(task_fn_t fn) {
        uint64_t id = results_store_.get_next_job_id();
        {
            std::lock_guard<std::mutex> lock(queue_mutex_);
            if (!running_) throw std::runtime_error("Worker is not running");
            main_tasks_.push({id, std::move(fn), true});
        }
        queue_cond_.notify_all();
        return id;
    }

    std::shared_future<cuvs_task_result_t> wait(uint64_t task_id) {
        return results_store_.wait(task_id);
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
            if (res.type() == typeid(std::exception_ptr)) {
                promise->set_exception(std::any_cast<std::exception_ptr>(res));
            } else {
                promise->set_value(std::any_cast<ResT>(res));
            }
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
        results_store_.store(task.id, result);
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
            try {
                batch->exec_fn(handle, batch->reqs, batch->setters);
            } catch (...) {
                auto err = std::current_exception();
                for (auto& setter : batch->setters) setter(err);
            }
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

    cuvs_task_result_store_t results_store_;

    std::mutex batch_mutex_;
    std::map<std::string, std::shared_ptr<batch_t>> batches_;
};

} // namespace matrixone
