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
#include "utils.hpp"
#pragma GCC diagnostic pop


namespace matrixone {

/**
 * @brief Brute-force nearest neighbor search on GPU.
 * @tparam T Data type of the vector elements (e.g., float, half).
 */
template <typename T>
class gpu_brute_force_t {
public:
    std::vector<T> flattened_host_dataset;
    std::unique_ptr<cuvs::neighbors::brute_force::index<T, float>> index;
    cuvs::distance::DistanceType metric;
    uint32_t dimension;
    uint32_t count;
    int device_id_;
    std::unique_ptr<cuvs_worker_t> worker;
    std::shared_mutex mutex_;
    bool is_loaded_ = false;
    std::shared_ptr<void> dataset_device_ptr_;
    uint64_t current_offset_ = 0;

    ~gpu_brute_force_t() {
        destroy();
    }

    /**
     * @brief Constructor for brute-force search.
     */
    gpu_brute_force_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, cuvs::distance::DistanceType m,
                       uint32_t nthread, int device_id = 0)
        : dimension(dimension), count(static_cast<uint32_t>(count_vectors)), metric(m), device_id_(device_id), current_offset_(static_cast<uint32_t>(count_vectors)) {
        worker = std::make_unique<cuvs_worker_t>(nthread, device_id_);

        flattened_host_dataset.resize(count * dimension);
        if (dataset_data) {
            std::copy(dataset_data, dataset_data + (count * dimension), flattened_host_dataset.begin());
        }
    }

    /**
     * @brief Constructor for an empty index (chunked addition support).
     */
    gpu_brute_force_t(uint64_t total_count, uint32_t dimension, cuvs::distance::DistanceType m, 
                       uint32_t nthread, int device_id = 0)
        : dimension(dimension), count(static_cast<uint32_t>(total_count)), metric(m), device_id_(device_id), current_offset_(0) {
        worker = std::make_unique<cuvs_worker_t>(nthread, device_id_);
        flattened_host_dataset.resize(count * dimension);
    }

    /**
     * @brief Starts the worker and initializes resources.
     */
    void start() {
        auto init_fn = [](raft_handle_wrapper_t& handle) -> std::any {
            return std::any();
        };

        auto stop_fn = [&](raft_handle_wrapper_t& handle) -> std::any {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            index.reset();
            dataset_device_ptr_.reset();
            return std::any();
        };

        worker->start(init_fn, stop_fn);
    }

    /**
     * @brief Loads the dataset to the GPU and builds the index.
     */
    void load() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (is_loaded_) return;

        if (count == 0) {
            index = nullptr;
            is_loaded_ = true;
            return;
        }

        if (current_offset_ > 0 && current_offset_ < count) {
            count = static_cast<uint32_t>(current_offset_);
            flattened_host_dataset.resize(count * dimension);
        }

        uint64_t job_id = worker->submit(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                if (flattened_host_dataset.empty()) {
                    index = nullptr;
                    return std::any();
                }

                auto dataset_device = new auto(raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(count), static_cast<int64_t>(dimension)));
                
                dataset_device_ptr_ = std::shared_ptr<void>(dataset_device, [](void* ptr) {
                    delete static_cast<raft::device_matrix<T, int64_t, raft::layout_c_contiguous>*>(ptr);
                });

                RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device->data_handle(), flattened_host_dataset.data(),
                                         flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                         raft::resource::get_cuda_stream(*res)));

                cuvs::neighbors::brute_force::index_params index_params;
                index_params.metric = metric;

                index = std::make_unique<cuvs::neighbors::brute_force::index<T, float>>(
                    cuvs::neighbors::brute_force::build(*res, index_params, raft::make_const_mdspan(dataset_device->view())));

                raft::resource::sync_stream(*res);
                return std::any();
            }
        );

        auto result_wait = worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        is_loaded_ = true;
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
        if (!queries_data || num_queries == 0 || dimension == 0) return search_result_t{};
        if (query_dimension != this->dimension) throw std::runtime_error("dimension mismatch");
        if (!is_loaded_ || !index) return search_result_t{};

        uint64_t job_id = worker->submit(
            [&, num_queries, limit](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                auto res = handle.get_raft_resources();
                
                auto queries_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(dimension));
                RAFT_CUDA_TRY(cudaMemcpyAsync(queries_device.data_handle(), queries_data,
                                         num_queries * dimension * sizeof(T), cudaMemcpyHostToDevice,
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

        auto result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<search_result_t>(result.result);
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
                
                // If conversion is needed
                if constexpr (!std::is_same_v<T, float>) {
                    auto chunk_device_float = raft::make_device_matrix<float, int64_t>(*res, chunk_count, dimension);
                    raft::copy(*res, chunk_device_float.view(), raft::make_host_matrix_view<const float, int64_t>(chunk_data, chunk_count, dimension));
                    auto out_view = raft::make_host_matrix_view<T, int64_t>(flattened_host_dataset.data() + (row_offset * dimension), chunk_count, dimension);
                    raft::copy(*res, out_view, chunk_device_float.view());
                    raft::resource::sync_stream(*res);
                } else {
                    std::copy(chunk_data, chunk_data + (chunk_count * dimension), flattened_host_dataset.begin() + (row_offset * dimension));
                }
                return std::any();
            }
        );
        
        auto result_wait = worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        current_offset_ += chunk_count;
    }

    void destroy() {
        if (worker) worker->stop();
    }
};

} // namespace matrixone
