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

#include "index_base.hpp"
#include "cuvs_worker.hpp"
#include "cuvs_types.h"

#include <cuda_fp16.h>
#include <raft/util/cudart_utils.hpp>

#include <algorithm>
#include <memory>
#include <vector>
#include <future>
#include <shared_mutex>
#include <stdexcept>
#include <string>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
// RAFT includes
#include <raft/core/device_mdarray.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/host_mdspan.hpp>
#include <raft/core/resources.hpp>

// cuVS includes
#include <cuvs/distance/distance.hpp>
#include <cuvs/neighbors/brute_force.hpp>
#include "quantize.hpp"
#pragma GCC diagnostic pop


namespace matrixone {

/**
 * @brief Brute-force nearest neighbor search on GPU.
 */
template <typename T>
class gpu_brute_force_t : public gpu_index_base_t<T, brute_force_build_params_t> {
public:
    // Explicitly use float for distance type to avoid mismatch with search results
    std::unique_ptr<cuvs::neighbors::brute_force::index<T, float>> index;

    ~gpu_brute_force_t() override {
        this->destroy();
    }

    gpu_brute_force_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension,
                      cuvs::distance::DistanceType m, uint32_t nthread, int device_id, quantization_t qtype = Quantization_F32) {
        this->dimension = dimension;
        this->count = static_cast<uint32_t>(count_vectors);
        this->metric = m;
        this->devices_ = {device_id};
        this->current_offset_ = static_cast<uint32_t>(count_vectors);

        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_);

        this->flattened_host_dataset.resize(this->count * this->dimension);
        if (dataset_data) {
            std::copy(dataset_data, dataset_data + (this->count * this->dimension), this->flattened_host_dataset.begin());
        }
    }

    gpu_brute_force_t(uint64_t total_count, uint32_t dimension, cuvs::distance::DistanceType m,
                      uint32_t nthread, int device_id, quantization_t qtype = Quantization_F32) {
        this->dimension = dimension;
        this->count = static_cast<uint32_t>(total_count);
        this->metric = m;
        this->devices_ = {device_id};
        this->current_offset_ = 0;

        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_);

        this->flattened_host_dataset.resize(this->count * this->dimension);
    }

    void start() override {
        auto init_fn = [](raft_handle_wrapper_t&) -> std::any { return std::any(); };
        auto stop_fn = [&](raft_handle_wrapper_t&) -> std::any {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            index.reset();
            this->quantizer_.reset();
            this->dataset_device_ptr_.reset();
            return std::any();
        };
        this->worker->start(init_fn, stop_fn);
    }

    void build() override {
        if (this->flattened_host_dataset.empty() && this->current_offset_ > 0) {
            this->count = static_cast<uint32_t>(this->current_offset_);
            this->flattened_host_dataset.resize(this->count * this->dimension);
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
        auto res = handle.get_raft_resources();
        if (this->flattened_host_dataset.empty()) {
            index = nullptr;
            return;
        }

        auto dataset_device = new auto(raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
            *res, static_cast<int64_t>(this->count), static_cast<int64_t>(this->dimension)));
        
        this->dataset_device_ptr_ = std::shared_ptr<void>(dataset_device, [](void* ptr) {
            delete static_cast<raft::device_matrix<T, int64_t, raft::layout_c_contiguous>*>(ptr);
        });

        RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device->data_handle(), this->flattened_host_dataset.data(),
                                    this->flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                    raft::resource::get_cuda_stream(*res)));

        cuvs::neighbors::brute_force::index_params index_params;
        index_params.metric = this->metric;

        auto idx = cuvs::neighbors::brute_force::build(*res, index_params, raft::make_const_mdspan(dataset_device->view()));
        index = std::make_unique<cuvs::neighbors::brute_force::index<T, float>>(std::move(idx));

        raft::resource::sync_stream(*res);
    }

    struct search_result_t {
        std::vector<int64_t> neighbors; // Indices of nearest neighbors
        std::vector<float> distances;  // Distances to nearest neighbors
    };

    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit) {
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (query_dimension != this->dimension) throw std::runtime_error("dimension mismatch");
        if (!this->is_loaded_ || !index) return search_result_t{};

        uint64_t job_id = this->worker->submit(
            [&, num_queries, limit, queries_data](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto res = handle.get_raft_resources();

                auto queries_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));
                raft::copy(*res, queries_device.view(), raft::make_host_matrix_view<const T, int64_t, raft::layout_c_contiguous>(queries_data, num_queries, this->dimension));

                auto neighbors_device = raft::make_device_matrix<int64_t, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
                auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

                cuvs::neighbors::brute_force::search_params search_params;
                cuvs::neighbors::brute_force::search(*res, search_params, *index,
                                                     raft::make_const_mdspan(queries_device.view()), neighbors_device.view(), distances_device.view());

                search_result_t s_res;
                s_res.neighbors.resize(num_queries * limit);
                s_res.distances.resize(num_queries * limit);

                raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t, raft::layout_c_contiguous>(s_res.neighbors.data(), num_queries, limit), neighbors_device.view());
                raft::copy(*res, raft::make_host_matrix_view<float, int64_t, raft::layout_c_contiguous>(s_res.distances.data(), num_queries, limit), distances_device.view());

                raft::resource::sync_stream(*res);
                return s_res;
            }
        );

        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        return std::any_cast<search_result_t>(result_wait.result);
    }

    search_result_t search_float(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit) {
        if constexpr (std::is_same_v<T, float>) {
            return search(queries_data, num_queries, query_dimension, limit);
        }

        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (query_dimension != this->dimension) throw std::runtime_error("dimension mismatch");
        if (!this->is_loaded_ || !index) return search_result_t{};

        uint64_t job_id = this->worker->submit(
            [&, num_queries, limit, queries_data](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto res = handle.get_raft_resources();

                auto q_dev_f = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(*res, num_queries, this->dimension);
                raft::copy(*res, q_dev_f.view(), raft::make_host_matrix_view<const float, int64_t, raft::layout_c_contiguous>(queries_data, num_queries, this->dimension));
                
                auto q_dev_t = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(*res, num_queries, this->dimension);
                if constexpr (sizeof(T) == 1) {
                    if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
                    this->quantizer_.template transform<T>(*res, q_dev_f.view(), q_dev_t.data_handle(), true);
                } else {
                    raft::copy(*res, q_dev_t.view(), q_dev_f.view());
                }

                auto neighbors_device = raft::make_device_matrix<int64_t, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
                auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

                cuvs::neighbors::brute_force::search_params search_params;
                cuvs::neighbors::brute_force::search(*res, search_params, *index,
                                                     raft::make_const_mdspan(q_dev_t.view()), neighbors_device.view(), distances_device.view());

                search_result_t s_res;
                s_res.neighbors.resize(num_queries * limit);
                s_res.distances.resize(num_queries * limit);

                raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t, raft::layout_c_contiguous>(s_res.neighbors.data(), num_queries, limit), neighbors_device.view());
                raft::copy(*res, raft::make_host_matrix_view<float, int64_t, raft::layout_c_contiguous>(s_res.distances.data(), num_queries, limit), distances_device.view());

                raft::resource::sync_stream(*res);
                return s_res;
            }
        );

        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        return std::any_cast<search_result_t>(result_wait.result);
    }

    std::string info() const override {
        std::string json = gpu_index_base_t<T, brute_force_build_params_t>::info();
        json += ", \"type\": \"BruteForce\", \"brute_force\": {";
        if (index) {
            json += "\"size\": " + std::to_string(index->size());
        } else {
            json += "\"size\": 0, \"built\": false";
        }
        json += "}}";
        return json;
    }
};

} // namespace matrixone
