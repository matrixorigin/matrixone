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

#include "cuvs_worker.hpp"
#include "cuvs_types.h"
#include "quantize.hpp"
#include <vector>
#include <string>
#include <memory>
#include <shared_mutex>
#include <algorithm>
#include <stdexcept>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#include <raft/core/resources.hpp>
#include <raft/core/device_mdarray.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/copy.cuh>
#pragma GCC diagnostic pop

// cuVS includes
#include <cuvs/distance/distance.hpp>

namespace matrixone {

/**
 * @brief gpu_index_base_t provides common functionality for all GPU-based indexes.
 * It manages host dataset, worker pool, quantization, and basic properties.
 */
template <typename T, typename BuildParams>
class gpu_index_base_t {
public:
    std::vector<T> flattened_host_dataset;
    std::vector<int> devices_;
    std::string filename_;
    
    cuvs::distance::DistanceType metric;
    uint32_t dimension;
    uint32_t count;
    BuildParams build_params;
    distribution_mode_t dist_mode;

    std::unique_ptr<cuvs_worker_t> worker;
    mutable std::shared_mutex mutex_;
    bool is_loaded_ = false;
    std::shared_ptr<void> dataset_device_ptr_; // Keep device memory alive

    gpu_index_base_t() = default;
    virtual ~gpu_index_base_t() {
        destroy();
    }

    // Common management methods
    virtual void destroy() {
        if (worker) worker->stop();
    }

    void set_use_batching(bool enable) {
        if (worker) worker->set_use_batching(enable);
    }

    void set_per_thread_device(bool enable) {
        if (worker) worker->set_per_thread_device(enable);
    }

    void set_quantizer(float min, float max) {
        quantizer_ = scalar_quantizer_t<float>(min, max);
    }

    void get_quantizer(float* min, float* max) const {
        if (!quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
        *min = quantizer_.min();
        *max = quantizer_.max();
    }

    void train_quantizer(const float* train_data, uint64_t n_samples) {
        if (!train_data || n_samples == 0) return;
        uint64_t job_id = worker->submit(
            [&, train_data, n_samples](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                auto train_device = raft::make_device_matrix<float, int64_t>(*res, n_samples, dimension);
                raft::copy(*res, train_device.view(), raft::make_host_matrix_view<const float, int64_t>(train_data, n_samples, dimension));
                quantizer_.train(*res, train_device.view());
                return std::any();
            }
        );
        auto result_wait = worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
    }

    void add_chunk(const T* chunk_data, uint64_t chunk_count) {
        if (current_offset_ + chunk_count > count) throw std::runtime_error("offset out of bounds");
        std::copy(chunk_data, chunk_data + (chunk_count * dimension), flattened_host_dataset.begin() + (current_offset_ * dimension));
        current_offset_ += chunk_count;
    }

    void add_chunk_float(const float* chunk_data, uint64_t chunk_count) {
        if (current_offset_ + chunk_count > count) throw std::runtime_error("offset out of bounds");
        
        uint64_t row_offset = current_offset_;
        uint64_t job_id = worker->submit(
            [&, chunk_data, chunk_count, row_offset](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                
                // If quantization is needed (T is 1-byte)
                if constexpr (sizeof(T) == 1) {
                    if (!quantizer_.is_trained()) {
                        int64_t n_train = std::min(static_cast<int64_t>(chunk_count), static_cast<int64_t>(500));
                        auto train_device = raft::make_device_matrix<float, int64_t>(*res, n_train, dimension);
                        raft::copy(*res, train_device.view(), raft::make_host_matrix_view<const float, int64_t>(chunk_data, n_train, dimension));
                        quantizer_.train(*res, train_device.view());
                    }

                    auto chunk_device_float = raft::make_device_matrix<float, int64_t>(*res, chunk_count, dimension);
                    raft::copy(*res, chunk_device_float.view(), raft::make_host_matrix_view<const float, int64_t>(chunk_data, chunk_count, dimension));
                    quantizer_.template transform<T>(*res, chunk_device_float.view(), flattened_host_dataset.data() + (row_offset * dimension), false);
                    raft::resource::sync_stream(*res);
                } else if constexpr (std::is_same_v<T, float>) {
                    std::copy(chunk_data, chunk_data + (chunk_count * dimension), flattened_host_dataset.begin() + (row_offset * dimension));
                } else {
                    auto chunk_device_float = raft::make_device_matrix<float, int64_t>(*res, chunk_count, dimension);
                    raft::copy(*res, chunk_device_float.view(), raft::make_host_matrix_view<const float, int64_t>(chunk_data, chunk_count, dimension));
                    auto out_view = raft::make_host_matrix_view<T, int64_t>(flattened_host_dataset.data() + (row_offset * dimension), chunk_count, dimension);
                    raft::copy(*res, out_view, chunk_device_float.view());
                    raft::resource::sync_stream(*res);
                }
                return std::any();
            }
        );
        
        auto result_wait = worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        current_offset_ += chunk_count;
    }

    uint32_t cap() const {
        return count;
    }

    uint32_t len() const {
        return static_cast<uint32_t>(current_offset_);
    }

protected:
    scalar_quantizer_t<float> quantizer_;
    uint64_t current_offset_ = 0;
};

} // namespace matrixone
