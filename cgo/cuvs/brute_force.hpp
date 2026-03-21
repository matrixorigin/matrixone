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

/*
 * Brute Force Index Implementation
 * Supported data types (T): float, half, int8_t, uint8_t
 * Neighbor ID type: int64_t
 */

#pragma once

#include "index_base.hpp"
#include "cuvs_worker.hpp"
#include "cuvs_types.h"
#include "quantize.hpp"
#include "helper.h"

#include <cuda_fp16.h>
#include <raft/util/cudart_utils.hpp>

#include <algorithm>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#include <raft/core/copy.cuh>
#include <raft/core/device_mdarray.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/device_resources_snmg.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/resources.hpp>

#include <cuvs/distance/distance.hpp>
#include <cuvs/neighbors/brute_force.hpp>
#pragma GCC diagnostic pop

namespace matrixone {

/**
 * @brief Search result containing neighbor IDs and distances.
 * Common for all brute force instantiations.
 */
struct brute_force_search_result_t {
    std::vector<int64_t> neighbors; // Indices of nearest neighbors
    std::vector<float> distances;  // Distances to nearest neighbors
};

/**
 * @brief gpu_brute_force_t implements a Brute Force index that can run on a single GPU.
 */
template <typename T>
class gpu_brute_force_t : public gpu_index_base_t<T, brute_force_build_params_t> {
public:
    // We force DistT=float for all our indices to avoid template bloat and satisfy cuVS
    using brute_force_index = cuvs::neighbors::brute_force::index<T, float>;
    using search_result_t = brute_force_search_result_t;

    // Internal index storage
    std::unique_ptr<brute_force_index> index_;

    ~gpu_brute_force_t() override {
        this->destroy();
    }

    // Unified Constructor for building from dataset
    gpu_brute_force_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension,
                       distance_type_t m, uint32_t nthread, int device_id) {

        this->dimension = dimension;
        this->count = static_cast<uint32_t>(count_vectors);
        this->metric = m;
        this->devices_ = {device_id};
        this->current_offset_ = static_cast<uint32_t>(count_vectors);

        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_, false);

        this->flattened_host_dataset.resize(this->count * this->dimension);
        if (dataset_data) {
            std::copy(dataset_data, dataset_data + (this->count * this->dimension), this->flattened_host_dataset.begin());
        }
    }

    // Constructor for chunked input (pre-allocates)
    gpu_brute_force_t(uint64_t total_count, uint32_t dimension, distance_type_t m,
                       uint32_t nthread, int device_id) {

        this->dimension = dimension;
        this->count = static_cast<uint32_t>(total_count);
        this->metric = m;
        this->devices_ = {device_id};
        this->current_offset_ = 0;

        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_, false);

        this->flattened_host_dataset.resize(this->count * this->dimension);
    }

    void start() override {
        auto init_fn = [](raft_handle_wrapper_t&) -> std::any { return std::any(); };
        auto stop_fn = [&](raft_handle_wrapper_t&) -> std::any {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            index_.reset();
            this->quantizer_.reset();
            this->dataset_device_ptr_.reset();
            return std::any();
        };
        this->worker->start(init_fn, stop_fn);
    }

    void build() override {
        if (this->count == 0) {
            this->is_loaded_ = true;
            return;
        }
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                this->build_internal(handle);
                return std::any();
            }
        );
        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        this->is_loaded_ = true;
        this->flattened_host_dataset.clear();
        this->flattened_host_dataset.shrink_to_fit();
    }

    void build_internal(raft_handle_wrapper_t& handle) {
        std::unique_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        // Create and own the device memory
        using dataset_t = raft::device_matrix<T, int64_t>;
        auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
        
        RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device.data_handle(), this->flattened_host_dataset.data(),
                                    this->flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                    raft::resource::get_cuda_stream(*res)));

        index_.reset(new brute_force_index(cuvs::neighbors::brute_force::build(
            *res, raft::make_const_mdspan(dataset_device.view()), 
            static_cast<cuvs::distance::DistanceType>(this->metric))));
        
        // Store the mdarray in shared_ptr<void> to keep it alive
        this->dataset_device_ptr_ = std::make_shared<dataset_t>(std::move(dataset_device));
        raft::resource::sync_stream(*res);
    }

    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const brute_force_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (!this->is_loaded_ || !index_) return search_result_t{};

        auto task = [this, num_queries, limit, sp, queries_data](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_internal(handle, queries_data, num_queries, limit, sp);
        };
        uint64_t job_id = this->worker->submit(task);
        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        return std::any_cast<search_result_t>(result_wait.result);
    }

    search_result_t search_internal(raft_handle_wrapper_t& handle, const T* queries_data, uint64_t num_queries, uint32_t limit, const brute_force_search_params_t& sp) {
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        auto queries_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)num_queries, (int64_t)this->dimension);
        raft::copy(*res, queries_device.view(), raft::make_host_matrix_view<const T, int64_t>(queries_data, num_queries, this->dimension));

        auto neighbors_device = raft::make_device_matrix<int64_t, int64_t>(*res, (int64_t)num_queries, (int64_t)limit);
        auto distances_device = raft::make_device_matrix<float, int64_t>(*res, (int64_t)num_queries, (int64_t)limit);

        cuvs::neighbors::brute_force::search(*res, *index_, 
                                            raft::make_const_mdspan(queries_device.view()), 
                                            neighbors_device.view(), distances_device.view());

        raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device.view());
        raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device.view());

        raft::resource::sync_stream(*res);
        return search_res;
    }

    search_result_t search_float(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const brute_force_search_params_t& sp) {
        if constexpr (std::is_same_v<T, float>) return search(queries_data, num_queries, query_dimension, limit, sp);
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (!this->is_loaded_ || !index_) return search_result_t{};

        auto task = [this, num_queries, query_dimension, limit, sp, queries_data](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_float_internal(handle, queries_data, num_queries, query_dimension, limit, sp);
        };
        uint64_t job_id = this->worker->submit(task);
        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        return std::any_cast<search_result_t>(result_wait.result);
    }

    search_result_t search_float_internal(raft_handle_wrapper_t& handle, const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const brute_force_search_params_t& sp) {
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        auto q_dev_f = raft::make_device_matrix<float, int64_t>(*res, num_queries, this->dimension);
        raft::copy(*res, q_dev_f.view(), raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, this->dimension));
        
        auto q_dev_t = raft::make_device_matrix<T, int64_t>(*res, num_queries, this->dimension);
        if constexpr (sizeof(T) == 1) {
            if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
            this->quantizer_.template transform<T>(*res, q_dev_f.view(), q_dev_t.data_handle(), true);
        } else {
            raft::copy(*res, q_dev_t.view(), q_dev_f.view());
        }
        raft::resource::sync_stream(*res);

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        auto neighbors_device = raft::make_device_matrix<int64_t, int64_t>(*res, (int64_t)num_queries, (int64_t)limit);
        auto distances_device = raft::make_device_matrix<float, int64_t>(*res, (int64_t)num_queries, (int64_t)limit);

        cuvs::neighbors::brute_force::search(*res, *index_, 
                                            raft::make_const_mdspan(q_dev_t.view()), 
                                            neighbors_device.view(), distances_device.view());

        raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device.view());
        raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device.view());

        raft::resource::sync_stream(*res);
        return search_res;
    }

    void destroy() override {
        if (this->worker) this->worker->stop();
        std::unique_lock<std::shared_mutex> lock(this->mutex_);
        index_.reset();
        this->quantizer_.reset();
        this->dataset_device_ptr_.reset();
    }
};

} // namespace matrixone
