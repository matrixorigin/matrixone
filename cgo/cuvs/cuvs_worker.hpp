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
            try {
                pair.second.set_exception(std::make_exception_ptr(std::runtime_error("Worker stopped")));
            } catch (...) {}
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
 * @brief Wrapper around raft::resources to provide a consistent interface for workers.
 */
class raft_handle_wrapper_t {
public:
    raft_handle_wrapper_t(int device_id, int rank = 0, std::shared_ptr<raft::device_resources_snmg> mg_res = nullptr,
                         distribution_mode_t mode = DistributionMode_SINGLE_GPU) 
        : device_id_(device_id), rank_(rank), mg_res_(mg_res), mode_(mode) {
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
    distribution_mode_t get_mode() const { return mode_; }

    /**
     * @brief Performs synchronization.
     * @param force_all_ranks If true, performs a collective sync across all ranks.
     */
    void sync(bool force_all_ranks = false) {
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

private:
    int device_id_;
    int rank_;
    std::shared_ptr<raft::device_resources_snmg> mg_res_;
    std::shared_ptr<raft::resources> res_;
    distribution_mode_t mode_;
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
        : nthread_(nthread), devices_(devices), mode_(mode), running_(false), use_batching_(false), per_thread_device_(false) {
        
        if (mode == DistributionMode_SHARDED || mode == DistributionMode_REPLICATED) {
            mg_resources_ = std::make_shared<raft::device_resources_snmg>(devices);
            init_mg_comms(*mg_resources_, devices);
        }
        tasks_.set_capacity(1000);
        main_tasks_.set_capacity(1000);
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
                raft_handle handle(device_id, rank, mg_resources_, mode_);
                if (init_fn) init_fn(handle);
                if (i == 0) this->run_main_loop(handle, stop_fn);
                else this->run_worker_loop(handle, stop_fn);
            });
        }
    }

    void stop() {
        if (!running_) return;
        running_ = false;
        
        tasks_.stop();
        main_tasks_.stop();
        
        {
            std::lock_guard<std::mutex> lock(shared_cv_mu_);
            shared_cv_.notify_all();
        }

        for (size_t i = 0; i < workers_.size(); ++i) {
            if (workers_[i].joinable()) workers_[i].join();
        }
        workers_.clear();
        results_store_.stop();
    }

    uint64_t submit(task_fn_t fn) {
        if (!running_) throw std::runtime_error("Worker is not running");
        uint64_t id = results_store_.get_next_job_id();
        tasks_.push({id, std::move(fn)});
        {
            std::lock_guard<std::mutex> lock(shared_cv_mu_);
            shared_cv_.notify_all();
        }
        return id;
    }

    uint64_t submit_main(task_fn_t fn) {
        if (!running_) throw std::runtime_error("Worker is not running");
        uint64_t id = results_store_.get_next_job_id();
        main_tasks_.push({id, std::move(fn)});
        {
            std::lock_guard<std::mutex> lock(shared_cv_mu_);
            shared_cv_.notify_all();
        }
        return id;
    }

    std::shared_future<cuvs_task_result_t> wait(uint64_t task_id) {
        return results_store_.wait(task_id);
    }

    std::shared_ptr<raft::device_resources_snmg> get_mg_resources() const {
        return mg_resources_;
    }

    distribution_mode_t get_mode() const { return mode_; }

    void set_use_batching(bool enable) { use_batching_ = enable; }
    bool use_batching() const { return use_batching_; }
    void set_per_thread_device(bool enable) { per_thread_device_ = enable; }

    template <typename ResT, typename ReqT>
    std::future<ResT> submit_batched(const std::string& key, ReqT req, 
                                     std::function<void(raft_handle&, const std::vector<std::any>&, const std::vector<std::function<void(std::any)>>&)> exec_fn) {
        bool should_flush = false;
        std::future<ResT> future;
        
        {
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
                should_flush = true;
            }
        }

        if (should_flush) {
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
        cuvs_task_t task;
        while (tasks_.pop(task)) {
            execute_task(task, handle);
        }
        if (stop_fn) stop_fn(handle);
    }

    void run_main_loop(raft_handle& handle, std::function<std::any(raft_handle&)> stop_fn) {
        while (true) {
            cuvs_task_t task;
            bool found = false;
            
            if (main_tasks_.try_pop(task)) {
                found = true;
            } else if (tasks_.try_pop(task)) {
                found = true;
            }

            if (found) {
                execute_task(task, handle);
            } else {
                if (!running_ && main_tasks_.empty() && tasks_.empty()) break;
                
                std::unique_lock<std::mutex> lock(shared_cv_mu_);
                shared_cv_.wait_for(lock, std::chrono::milliseconds(10), [this]() {
                    return !main_tasks_.empty() || !tasks_.empty() || !running_;
                });
            }
        }
        if (stop_fn) stop_fn(handle);
    }

    void execute_task(const cuvs_task_t& task, raft_handle& handle) {
        cuvs_task_result_t result;
        try {
            result.result = task.fn(handle);
            // No global sync here; indices call handle.sync() as needed.
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
        try {
            this->submit([batch](raft_handle& handle) -> std::any {
                try {
                    batch->exec_fn(handle, batch->reqs, batch->setters);
                } catch (...) {
                    auto err = std::current_exception();
                    for (auto& setter : batch->setters) setter(err);
                }
                return std::any();
            });
        } catch (...) {
            auto err = std::current_exception();
            for (auto& setter : batch->setters) setter(err);
        }
    }

    uint32_t nthread_;
    std::vector<int> devices_;
    distribution_mode_t mode_;
    std::vector<std::thread> workers_;
    std::atomic<bool> running_;
    bool use_batching_;
    bool per_thread_device_;

    thread_safe_queue_t<cuvs_task_t> tasks_;
    thread_safe_queue_t<cuvs_task_t> main_tasks_;
    std::shared_ptr<raft::device_resources_snmg> mg_resources_;

    std::mutex shared_cv_mu_;
    std::condition_variable shared_cv_;

    cuvs_task_result_store_t results_store_;

    std::mutex batch_mutex_;
    std::map<std::string, std::shared_ptr<batch_t>> batches_;
};

} // namespace matrixone
