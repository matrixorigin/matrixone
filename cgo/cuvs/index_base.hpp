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

#include "cuvs_types.h"
#include "cuvs_worker.hpp"
#include "quantize.hpp"
#include <cuvs/distance/distance.hpp>
#include <vector>
#include <string>
#include <memory>
#include <shared_mutex>

namespace matrixone {

using ::distance_type_t;
using ::quantization_t;
using ::distribution_mode_t;

/**
 * @brief Base class for GPU-based indices.
 */
template <typename T, typename BuildParams>
class gpu_index_base_t {
public:
    uint32_t dimension = 0;
    uint32_t count = 0;
    distance_type_t metric;
    BuildParams build_params;
    std::vector<int> devices_;
    std::vector<T> flattened_host_dataset;
    distribution_mode_t dist_mode;

    std::unique_ptr<cuvs_worker_t> worker;
    mutable std::shared_mutex mutex_;
    bool is_loaded_ = false;
    int build_device_id_ = 0;
    std::shared_ptr<void> dataset_device_ptr_; // Keep device memory alive

    gpu_index_base_t() = default;
    virtual ~gpu_index_base_t() {
        destroy();
    }

    virtual void start() {}
    virtual void build() {}
    // virtual void save(const std::string& filename) const {}
    // virtual void load(const std::string& filename) {}

    // Common management methods
    virtual void destroy() {
        if (worker) worker->stop();
    }

    void set_per_thread_device(bool enable) {
        if (worker) worker->set_per_thread_device(enable);
    }

    void set_use_batching(bool enable) {
        if (worker) worker->set_use_batching(enable);
    }

    uint32_t cap() const { return count; }
    uint32_t len() const { return count; }

    void add_chunk(const T* chunk_data, uint64_t chunk_count) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (is_loaded_) throw std::runtime_error("Cannot add chunk to built index");
        
        size_t old_size = flattened_host_dataset.size();
        flattened_host_dataset.resize(old_size + chunk_count * dimension);
        std::copy(chunk_data, chunk_data + chunk_count * dimension, flattened_host_dataset.begin() + old_size);
        current_offset_ += static_cast<uint32_t>(chunk_count);
    }

    void add_chunk_float(const float* chunk_data, uint64_t chunk_count) {
        uint64_t job_id = worker->submit_main(
            [this, chunk_data, chunk_count](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                
                // If quantization is needed (T is 1-byte)
                if constexpr (sizeof(T) == 1) {
                    auto queries_host_view = raft::make_host_matrix_view<const float, int64_t>(chunk_data, chunk_count, dimension);
                    auto queries_device = raft::make_device_matrix<float, int64_t>(*res, chunk_count, dimension);
                    raft::copy(*res, queries_device.view(), queries_host_view);

                    auto chunk_device_target = raft::make_device_matrix<T, int64_t>(*res, chunk_count, dimension);
                    
                    if (!quantizer_.is_trained()) {
                        int64_t n_train = std::min((int64_t)chunk_count, (int64_t)1000);
                        auto train_view = raft::make_device_matrix_view<const float, int64_t>(queries_device.data_handle(), n_train, dimension);
                        quantizer_.train(*res, train_view);
                    }
                    quantizer_.template transform<T>(*res, queries_device.view(), chunk_device_target.data_handle(), true);
                    
                    std::vector<T> chunk_host_target(chunk_count * dimension);
                    raft::copy(*res, raft::make_host_matrix_view<T, int64_t>(chunk_host_target.data(), chunk_count, dimension), chunk_device_target.view());
                    raft::resource::sync_stream(*res);

                    std::unique_lock<std::shared_mutex> lock(mutex_);
                    size_t old_size = flattened_host_dataset.size();
                    flattened_host_dataset.resize(old_size + chunk_count * dimension);
                    std::copy(chunk_host_target.begin(), chunk_host_target.end(), flattened_host_dataset.begin() + old_size);
                    current_offset_ += static_cast<uint32_t>(chunk_count);
                } else {
                    std::unique_lock<std::shared_mutex> lock(mutex_);
                    size_t old_size = flattened_host_dataset.size();
                    flattened_host_dataset.resize(old_size + chunk_count * dimension);
                    std::copy(chunk_data, chunk_data + chunk_count * dimension, flattened_host_dataset.begin() + old_size);
                    current_offset_ += static_cast<uint32_t>(chunk_count);
                }
                return std::any();
            }
        );
        worker->wait(job_id).get();
    }

    void train_quantizer(const float* train_data, uint64_t n_samples) {
        uint64_t job_id = worker->submit_main(
            [this, train_data, n_samples](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                auto train_host_view = raft::make_host_matrix_view<const float, int64_t>(train_data, n_samples, dimension);
                auto train_device = raft::make_device_matrix<float, int64_t>(*res, n_samples, dimension);
                raft::copy(*res, train_device.view(), train_host_view);
                quantizer_.train(*res, train_device.view());
                raft::resource::sync_stream(*res);
                return std::any();
            }
        );
        worker->wait(job_id).get();
    }

    void set_quantizer(float min, float max) {
        quantizer_.set_quantizer(min, max);
    }

    void get_quantizer(float* min, float* max) {
        *min = quantizer_.min();
        *max = quantizer_.max();
    }

    virtual std::string info() const {
        std::string json = "{";
        json += "\"dimension\": " + std::to_string(dimension) + ", ";
        json += "\"count\": " + std::to_string(count) + ", ";
        json += "\"metric\": \"" + std::to_string((int)metric) + "\", ";
        json += "\"dist_mode\": \"" + std::to_string((int)dist_mode) + "\", ";
        json += "\"devices\": [";
        for (size_t i = 0; i < devices_.size(); ++i) {
            json += std::to_string(devices_[i]) + (i == devices_.size() - 1 ? "" : ", ");
        }
        json += "]";
        return json; // Caller will close the object or add more fields
    }

protected:
    scalar_quantizer_t<float> quantizer_;
    uint64_t current_offset_ = 0;
};

} // namespace matrixone
