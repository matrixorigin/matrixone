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
#include "cuvs_worker.hpp" // For cuvs_worker_t and raft_handle_wrapper_t
#include <raft/util/cudart_utils.hpp> // For RAFT_CUDA_TRY
#include <cuda_fp16.h> // For half

// Standard library includes
#include <algorithm>   // For std::copy
#include <iostream>    // For simulation debug logs
#include <memory>
#include <numeric>     // For std::iota
#include <stdexcept>   // For std::runtime_error
#include <string>      
#include <type_traits> 
#include <vector>
#include <future>      // For std::promise and std::future
#include <limits>      // For std::numeric_limits
#include <shared_mutex> // For std::shared_mutex

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
// RAFT includes
#include <raft/core/device_mdarray.hpp> // For raft::device_matrix
#include <raft/core/device_mdspan.hpp>   // Required for device_matrix_view
#include <raft/core/host_mdarray.hpp> // For raft::host_matrix
#include <raft/core/resources.hpp>       // Core resource handle
#include <raft/linalg/map.cuh>           
#include <raft/core/copy.cuh>            // For raft::copy with type conversion


// cuVS includes
#include <cuvs/distance/distance.hpp>    // cuVS distance API
#include <cuvs/neighbors/brute_force.hpp>
#include "quantize.hpp"
#pragma GCC diagnostic pop


namespace matrixone {

/**
 * @brief Brute-force nearest neighbor search on GPU.
 * @tparam T Data type of the vector elements (e.g., float, half).
 */
template <typename T>
class gpu_brute_force_t : public gpu_index_base_t<T, brute_force_build_params_t> {
public:
    std::unique_ptr<cuvs::neighbors::brute_force::index<T, float>> index;

    ~gpu_brute_force_t() override {
        this->destroy();
    }

    /**
     * @brief Constructor for brute-force search.
     */
    gpu_brute_force_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, cuvs::distance::DistanceType m,
                       uint32_t nthread, int device_id = 0) {
        
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

    /**
     * @brief Constructor for an empty index (chunked addition support).
     */
    gpu_brute_force_t(uint64_t total_count, uint32_t dimension, cuvs::distance::DistanceType m, 
                       uint32_t nthread, int device_id = 0) {
        
        this->dimension = dimension;
        this->count = static_cast<uint32_t>(total_count);
        this->metric = m;
        this->devices_ = {device_id};
        this->current_offset_ = 0;

        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_);
        this->flattened_host_dataset.resize(this->count * this->dimension);
    }

    /**
     * @brief Starts the worker and initializes resources.
     */
    void start() {
        auto init_fn = [](raft_handle_wrapper_t&) -> std::any {
            return std::any();
        };

        auto stop_fn = [&](raft_handle_wrapper_t&) -> std::any {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            index.reset();
            this->dataset_device_ptr_.reset();
            return std::any();
        };

        this->worker->start(init_fn, stop_fn);
    }

    /**
     * @brief Loads the dataset to the GPU and builds the index.
     */
    void build() {
        std::unique_lock<std::shared_mutex> lock(this->mutex_);
        if (this->is_loaded_) return;

        if (this->count == 0) {
            index = nullptr;
            this->is_loaded_ = true;
            return;
        }

        if (this->current_offset_ > 0 && this->current_offset_ < this->count) {
            this->count = static_cast<uint32_t>(this->current_offset_);
            this->flattened_host_dataset.resize(this->count * this->dimension);
        }

        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                if (this->flattened_host_dataset.empty()) {
                    index = nullptr;
                    return std::any();
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

                index = std::make_unique<cuvs::neighbors::brute_force::index<T, float>>(
                    cuvs::neighbors::brute_force::build(*res, index_params, raft::make_const_mdspan(dataset_device->view())));

                raft::resource::sync_stream(*res);
                return std::any();
            }
        );

        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        this->is_loaded_ = true;
        // Clear host dataset after building to save memory
        this->flattened_host_dataset.clear();
        this->flattened_host_dataset.shrink_to_fit();
    }

    /**
     * @brief Search result containing neighbor IDs and distances.
     */
    struct search_result_t {
        std::vector<int64_t> neighbors; // Indices of nearest neighbors
        std::vector<float> distances;  // Distances to nearest neighbors
    };

    /**
     * @brief Performs brute-force search for given queries.
     */
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
                RAFT_CUDA_TRY(cudaMemcpyAsync(queries_device.data_handle(), queries_data,
                                         num_queries * this->dimension * sizeof(T), cudaMemcpyHostToDevice,
                                         raft::resource::get_cuda_stream(*res)));

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

                RAFT_CUDA_TRY(cudaMemcpyAsync(s_res.neighbors.data(), neighbors_device.data_handle(),
                                         s_res.neighbors.size() * sizeof(int64_t), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*res)));
                RAFT_CUDA_TRY(cudaMemcpyAsync(s_res.distances.data(), distances_device.data_handle(),
                                         s_res.distances.size() * sizeof(float), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*res)));

                raft::resource::sync_stream(*res);

                for (size_t i = 0; i < s_res.neighbors.size(); ++i) {
                    if (s_res.neighbors[i] == std::numeric_limits<int64_t>::max() || 
                        s_res.neighbors[i] == 4294967295LL || s_res.neighbors[i] < 0) {
                        s_res.neighbors[i] = -1;
                    }
                }
                return s_res;
            }
        );

        auto result = this->worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<search_result_t>(result.result);
    }

    /**
     * @brief Performs brute-force search for given float32 queries, with on-the-fly conversion if needed.
     */
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
                
                auto queries_device_float = raft::make_device_matrix<float, int64_t>(*res, num_queries, this->dimension);
                raft::copy(*res, queries_device_float.view(), raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, this->dimension));
                
                auto queries_device_target = raft::make_device_matrix<T, int64_t>(*res, num_queries, this->dimension);
                raft::copy(*res, queries_device_target.view(), queries_device_float.view());

                auto neighbors_device = raft::make_device_matrix<int64_t, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
                auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

                cuvs::neighbors::brute_force::search_params search_params;
                cuvs::neighbors::brute_force::search(*res, search_params, *index,
                                                     raft::make_const_mdspan(queries_device_target.view()), neighbors_device.view(), distances_device.view());

                search_result_t s_res;
                s_res.neighbors.resize(num_queries * limit);
                s_res.distances.resize(num_queries * limit);

                RAFT_CUDA_TRY(cudaMemcpyAsync(s_res.neighbors.data(), neighbors_device.data_handle(),
                                         s_res.neighbors.size() * sizeof(int64_t), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*res)));
                RAFT_CUDA_TRY(cudaMemcpyAsync(s_res.distances.data(), distances_device.data_handle(),
                                         s_res.distances.size() * sizeof(float), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*res)));

                raft::resource::sync_stream(*res);

                for (size_t i = 0; i < s_res.neighbors.size(); ++i) {
                    if (s_res.neighbors[i] == std::numeric_limits<int64_t>::max() || 
                        s_res.neighbors[i] == 4294967295LL || s_res.neighbors[i] < 0) {
                        s_res.neighbors[i] = -1;
                    }
                }
                return s_res;
            }
        );

        auto result = this->worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<search_result_t>(result.result);
    }

    void info() const override {
        gpu_index_base_t<T, brute_force_build_params_t>::info();
        std::cout << "Brute-Force Specific Info:" << std::endl;
        if (index) {
            std::cout << "  Size: " << index->size() << std::endl;
        } else {
            std::cout << "  (Index not built yet)" << std::endl;
        }
    }
};

} // namespace matrixone
