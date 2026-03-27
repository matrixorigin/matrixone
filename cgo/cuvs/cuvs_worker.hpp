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
        

        if (mode == DistributionMode_SHARDED) {
            mg_resources_ = std::make_shared<raft::device_resources_snmg>(devices);
            init_mg_comms(*mg_resources_, devices);
        }
        
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
            raft_handle handle(device_id, 0, mg_resources_, mode_);
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
                bool give_mg = (mg_resources_ != nullptr) && (mode_ != DistributionMode_SINGLE_GPU);
                
                // Each thread in the pool gets its own raft::resources (so separate CUDA streams)
                raft_handle handle(device_id, rank, give_mg ? mg_resources_ : nullptr, mode_);
                
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

    std::shared_ptr<raft::device_resources_snmg> get_mg_resources() const {
        return mg_resources_;
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
    std::shared_ptr<raft::device_resources_snmg> mg_resources_;
    std::atomic<uint32_t> next_device_idx_;

    cuvs_task_result_store_t results_store_;

    std::mutex batch_mutex_;
    std::map<std::string, std::shared_ptr<batch_t>> batches_;
};

} // namespace matrixone
