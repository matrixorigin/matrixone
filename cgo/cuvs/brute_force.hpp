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
#include <raft/linalg/map.cuh>           // RESTORED: map.cuh
#include <raft/core/copy.cuh>            // For raft::copy with type conversion


// cuVS includes
#include <cuvs/distance/distance.hpp>    // cuVS distance API
#include <cuvs/neighbors/brute_force.hpp> // Correct include
#pragma GCC diagnostic pop


namespace matrixone {

/**
 * @brief Brute-force nearest neighbor search on GPU.
 * @tparam T Data type of the vector elements (e.g., float, half).
 */
template <typename T>
class gpu_brute_force_t {
public:
    std::vector<T> flattened_host_dataset; // Host-side copy of the dataset
    std::unique_ptr<cuvs::neighbors::brute_force::index<T, float>> index; // cuVS brute-force index
    cuvs::distance::DistanceType metric; // Distance metric
    uint32_t dimension; // Dimension of vectors
    uint32_t count; // Number of vectors in the dataset
    int device_id_; // CUDA device ID
    std::unique_ptr<cuvs_worker_t> worker; // Asynchronous task worker
    std::shared_mutex mutex_; // Protects index and data access
    bool is_loaded_ = false; // Whether the index is loaded into GPU memory
    std::shared_ptr<void> dataset_device_ptr_; // Pointer to device-side dataset memory

    ~gpu_brute_force_t() {
        destroy();
    }

    /**
     * @brief Constructor for brute-force search.
     * @param dataset_data Pointer to the flattened dataset on host.
     * @param count_vectors Number of vectors.
     * @param dimension Vector dimension.
     * @param m Distance metric.
     * @param nthread Number of worker threads.
     * @param device_id GPU device ID.
     */
    gpu_brute_force_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, cuvs::distance::DistanceType m,
                       uint32_t nthread, int device_id = 0)
        : dimension(dimension), count(static_cast<uint32_t>(count_vectors)), metric(m), device_id_(device_id) {
        worker = std::make_unique<cuvs_worker_t>(nthread, device_id_);

        // Resize flattened_host_dataset and copy data from the flattened array
        flattened_host_dataset.resize(count * dimension); // Total elements
        if (dataset_data) {
            std::copy(dataset_data, dataset_data + (count * dimension), flattened_host_dataset.begin());
        }
    }

    /**
     * @brief Loads the dataset to the GPU and builds the index.
     */
    void load() {
        std::unique_lock<std::shared_mutex> lock(mutex_); // Acquire exclusive lock
        if (is_loaded_) return;

        std::promise<bool> init_complete_promise;
        std::future<bool> init_complete_future = init_complete_promise.get_future();

        auto init_fn = [&](raft_handle_wrapper_t& handle) -> std::any {
            if (flattened_host_dataset.empty()) { // Use new member
                index = nullptr; // Ensure index is null if no data
                init_complete_promise.set_value(true); // Signal completion even if empty
                return std::any();
            }

            auto dataset_device = new auto(raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                *handle.get_raft_resources(), static_cast<int64_t>(count), static_cast<int64_t>(dimension)));
            
            dataset_device_ptr_ = std::shared_ptr<void>(dataset_device, [](void* ptr) {
                delete static_cast<raft::device_matrix<T, int64_t, raft::layout_c_contiguous>*>(ptr);
            });

            RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device->data_handle(), flattened_host_dataset.data(),
                                     flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                     raft::resource::get_cuda_stream(*handle.get_raft_resources())));

            cuvs::neighbors::brute_force::index_params index_params; // Correct brute_force namespace
            index_params.metric = metric;

            index = std::make_unique<cuvs::neighbors::brute_force::index<T, float>>(
                cuvs::neighbors::brute_force::build(*handle.get_raft_resources(), index_params, raft::make_const_mdspan(dataset_device->view()))); // Use raft::make_const_mdspan

            raft::resource::sync_stream(*handle.get_raft_resources()); // Synchronize after build

            init_complete_promise.set_value(true); // Signal that initialization is complete
            return std::any();
        };
        auto stop_fn = [&](raft_handle_wrapper_t& handle) -> std::any {
            if (index) { // Check if unique_ptr holds an object
                index.reset();
            }
            dataset_device_ptr_.reset();
            return std::any();
        };
        worker->start(init_fn, stop_fn);

        init_complete_future.get(); // Wait for the init_fn to complete
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
     * @param queries_data Pointer to flattened query vectors on host.
     * @param num_queries Number of query vectors.
     * @param query_dimension Dimension of query vectors.
     * @param limit Number of nearest neighbors to find.
     * @return Search results.
     */
    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit) {
        if (!queries_data || num_queries == 0 || dimension == 0) { // Check for invalid input
            return search_result_t{};
        }
        if (query_dimension != this->dimension) {
            throw std::runtime_error("Query dimension does not match index dimension.");
        }
        if (limit == 0) {
            return search_result_t{};
        }
        if (!index) {
            return search_result_t{};
        }

        size_t queries_rows = num_queries;
        size_t queries_cols = dimension; // Use the class's dimension

        uint64_t job_id = worker->submit(
            [&, queries_rows, queries_cols, limit](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_); // Acquire shared read-only lock inside worker thread
                
                auto queries_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                    *handle.get_raft_resources(), static_cast<int64_t>(queries_rows), static_cast<int64_t>(queries_cols));
                RAFT_CUDA_TRY(cudaMemcpyAsync(queries_device.data_handle(), queries_data,
                                         queries_rows * queries_cols * sizeof(T), cudaMemcpyHostToDevice,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));

                auto neighbors_device = raft::make_device_matrix<int64_t, int64_t, raft::layout_c_contiguous>(
                    *handle.get_raft_resources(), static_cast<int64_t>(queries_rows), static_cast<int64_t>(limit));
                auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                    *handle.get_raft_resources(), static_cast<int64_t>(queries_rows), static_cast<int64_t>(limit));

                cuvs::neighbors::brute_force::search_params search_params;
                cuvs::neighbors::brute_force::search(*handle.get_raft_resources(), search_params, *index,
                                                     raft::make_const_mdspan(queries_device.view()), neighbors_device.view(), distances_device.view());

                search_result_t res;
                res.neighbors.resize(queries_rows * limit);
                res.distances.resize(queries_rows * limit);

                RAFT_CUDA_TRY(cudaMemcpyAsync(res.neighbors.data(), neighbors_device.data_handle(),
                                         res.neighbors.size() * sizeof(int64_t), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));
                RAFT_CUDA_TRY(cudaMemcpyAsync(res.distances.data(), distances_device.data_handle(),
                                         res.distances.size() * sizeof(float), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));

                raft::resource::sync_stream(*handle.get_raft_resources());

                // Post-process to handle sentinels
                for (size_t i = 0; i < res.neighbors.size(); ++i) {
                    if (res.neighbors[i] == std::numeric_limits<int64_t>::max() || 
                        res.neighbors[i] == 4294967295LL || 
                        res.neighbors[i] < 0) {
                        res.neighbors[i] = -1;
                    }
                }
                
                return res;
            }
        );

        cuvs_task_result_t result = worker->wait(job_id).get();
        if (result.error) {
            std::rethrow_exception(result.error);
        }

        return std::any_cast<search_result_t>(result.result);
    }

    void destroy() {
        if (worker) {
            worker->stop();
        }
    }
};

} // namespace matrixone
