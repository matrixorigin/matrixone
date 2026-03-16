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

#include <raft/core/resources.hpp>
#include <raft/core/device_mdarray.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/copy.cuh>
#include <cuvs/neighbors/brute_force.hpp>
#include <cuvs/distance/distance.hpp>
#include "helper.h"
#include <vector>
#include <cstdint>

namespace matrixone {

/**
 * @brief Performs an ad-hoc brute-force search on GPU without using a worker thread.
 *        This is intended for scenarios where an index is not pre-built and the
 *        search needs to be executed immediately in the current thread context.
 * 
 * @tparam T Data type of the vector elements (e.g., float, half).
 * @param res RAFT resources handle.
 * @param dataset Host pointer to the dataset vectors.
 * @param n_rows Number of vectors in the dataset.
 * @param dim Dimension of each vector.
 * @param queries Host pointer to the query vectors.
 * @param n_queries Number of query vectors.
 * @param limit Number of nearest neighbors to find (k).
 * @param metric Distance metric to use.
 * @param neighbors Host pointer to store the resulting neighbor IDs (size: n_queries * limit).
 * @param distances Host pointer to store the resulting distances (size: n_queries * limit).
 */
template <typename T>
void adhoc_brute_force_search(const raft::resources& res,
                              const T* dataset,
                              uint64_t n_rows,
                              uint32_t dim,
                              const T* queries,
                              uint64_t n_queries,
                              uint32_t limit,
                              cuvs::distance::DistanceType metric,
                              int64_t* neighbors,
                              float* distances) {
    auto stream = raft::resource::get_cuda_stream(res);

    // Helper to align sizes to 256 bytes (CUDA default alignment)
    auto align_size = [](size_t size) {
        return (size + 255) & ~255;
    };

    // 1. Calculate total buffer sizes with alignment
    size_t dataset_bytes = n_rows * dim * sizeof(T);
    size_t queries_bytes = n_queries * dim * sizeof(T);
    size_t neighbors_bytes = n_queries * limit * sizeof(int64_t);
    size_t distances_bytes = n_queries * limit * sizeof(float);

    size_t dataset_alloc = align_size(dataset_bytes);
    size_t queries_alloc = align_size(queries_bytes);
    size_t neighbors_alloc = align_size(neighbors_bytes);
    size_t total_bytes = dataset_alloc + queries_alloc + neighbors_alloc + distances_bytes;

    // Use a single allocation for all temporary buffers to reduce overhead
    void* d_ptr = nullptr;
    RAFT_CUDA_TRY(cudaMallocAsync(&d_ptr, total_bytes, stream));

    char* d_dataset = static_cast<char*>(d_ptr);
    char* d_queries = d_dataset + dataset_alloc;
    char* d_neighbors = d_queries + queries_alloc;
    char* d_distances = d_neighbors + neighbors_alloc;

    // 2. Async copies to Device
    RAFT_CUDA_TRY(cudaMemcpyAsync(d_dataset, dataset, dataset_bytes, cudaMemcpyHostToDevice, stream));
    RAFT_CUDA_TRY(cudaMemcpyAsync(d_queries, queries, queries_bytes, cudaMemcpyHostToDevice, stream));

    // 3. Prepare Views (zero allocation)
    auto dataset_view = raft::make_device_matrix_view<const T, int64_t>(reinterpret_cast<const T*>(d_dataset), n_rows, dim);
    auto queries_view = raft::make_device_matrix_view<const T, int64_t>(reinterpret_cast<const T*>(d_queries), n_queries, dim);
    auto neighbors_view = raft::make_device_matrix_view<int64_t, int64_t>(reinterpret_cast<int64_t*>(d_neighbors), n_queries, limit);
    auto distances_view = raft::make_device_matrix_view<float, int64_t>(reinterpret_cast<float*>(d_distances), n_queries, limit);

    // 4. Build temporary index (view-based, very fast)
    cuvs::neighbors::brute_force::index_params index_params;
    index_params.metric = metric;
    auto index = cuvs::neighbors::brute_force::build(res, index_params, raft::make_const_mdspan(dataset_view));

    // 5. Execute Search
    cuvs::neighbors::brute_force::search_params search_params;
    cuvs::neighbors::brute_force::search(res, search_params, index, 
                                         raft::make_const_mdspan(queries_view), 
                                         neighbors_view, 
                                         distances_view);

    // 6. Async copy results back to host
    RAFT_CUDA_TRY(cudaMemcpyAsync(neighbors, d_neighbors, neighbors_bytes, cudaMemcpyDeviceToHost, stream));
    RAFT_CUDA_TRY(cudaMemcpyAsync(distances, d_distances, distances_bytes, cudaMemcpyDeviceToHost, stream));

    // 7. Synchronize
    raft::resource::sync_stream(res);

    // 8. Async free
    RAFT_CUDA_TRY(cudaFreeAsync(d_ptr, stream));

    // Handle invalid neighbor indices (consistent with existing brute_force.hpp)
    for (size_t i = 0; i < n_queries * limit; ++i) {
        if (neighbors[i] == std::numeric_limits<int64_t>::max() || 
            neighbors[i] == 4294967295LL || neighbors[i] < 0) {
            neighbors[i] = -1;
        }
    }
}

} // namespace matrixone
