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

// =============================================================================
// adhoc.hpp — Developer Guide
// =============================================================================
//
// PURPOSE
// -------
// Provides a single stateless function adhoc_brute_force_search<T>() for
// one-shot exact nearest-neighbor search on the GPU without building or owning
// a persistent index.  Used when the dataset changes frequently or is too small
// to justify index construction overhead.
//
// DESIGN
// ------
// No index object, no worker threads, no lifecycle.  The caller supplies a
// raft::resources (CUDA stream + allocators) and host pointers; the function
// handles all device memory management internally.
//
// Single allocation strategy:
//   One cudaMallocAsync covers the full working set:
//     [dataset | queries | neighbors | distances]
//   Each region is 256-byte aligned.  A single cudaFreeAsync at the end
//   releases everything.  This minimizes CUDA allocator overhead for small
//   repeated calls.
//
// EXECUTION SEQUENCE
// ------------------
//   1. cudaMallocAsync — one contiguous block.
//   2. Async H→D copy for dataset and queries.
//   3. sync_stream — ensure copies are done before build.
//   4. brute_force::build — constructs a temporary view-based index (no copy).
//   5. brute_force::search — exact k-NN on device.
//   6. Async D→H copy for neighbors and distances.
//   7. sync_stream — wait for results.
//   8. cudaFreeAsync — release device memory.
//   9. Post-process invalid IDs: cuVS returns INT64_MAX or UINT32_MAX for
//      positions with no valid neighbor; these are normalized to -1.
//
// CALLER RESPONSIBILITY
// ---------------------
// The caller owns the raft::resources and must ensure the CUDA device is
// set appropriately before calling.  The function is fully synchronous from
// the caller's perspective (both sync_stream calls ensure completion before
// return).
//
// LIMITATIONS
// -----------
// - No soft-delete filtering (no bitset).
// - No host_id mapping — returns raw internal 0-based positions.
// - T must be float or half; int8/uint8 quantization is not applied here.
// - No batching or concurrency — one blocking call per invocation.
//
// =============================================================================

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
    raft::copy(res, raft::make_device_matrix_view<T, int64_t>(reinterpret_cast<T*>(d_dataset), (int64_t)n_rows, (int64_t)dim), raft::make_host_matrix_view<const T, int64_t>(dataset, (int64_t)n_rows, (int64_t)dim));
    raft::copy(res, raft::make_device_matrix_view<T, int64_t>(reinterpret_cast<T*>(d_queries), (int64_t)n_queries, (int64_t)dim), raft::make_host_matrix_view<const T, int64_t>(queries, (int64_t)n_queries, (int64_t)dim));
    raft::resource::sync_stream(res);

    // 3. Prepare Views (zero allocation)
    auto dataset_view = raft::make_device_matrix_view<const T, int64_t>(reinterpret_cast<const T*>(d_dataset), (int64_t)n_rows, (int64_t)dim);
    auto queries_view = raft::make_device_matrix_view<const T, int64_t>(reinterpret_cast<const T*>(d_queries), (int64_t)n_queries, (int64_t)dim);
    auto neighbors_view = raft::make_device_matrix_view<int64_t, int64_t>(reinterpret_cast<int64_t*>(d_neighbors), (int64_t)n_queries, (int64_t)limit);
    auto distances_view = raft::make_device_matrix_view<float, int64_t>(reinterpret_cast<float*>(d_distances), (int64_t)n_queries, (int64_t)limit);

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
    raft::copy(res, raft::make_host_matrix_view<int64_t, int64_t>(neighbors, (int64_t)n_queries, (int64_t)limit), neighbors_view);
    raft::copy(res, raft::make_host_matrix_view<float, int64_t>(distances, (int64_t)n_queries, (int64_t)limit), distances_view);

    // 7. Synchronize
    raft::resource::sync_stream(res);

    // 8. Async free
    RAFT_CUDA_TRY(cudaFreeAsync(d_ptr, stream));

    if (metric == cuvs::distance::DistanceType::InnerProduct) {
        for (size_t i = 0; i < n_queries * limit; ++i) {
            distances[i] *= -1.0f;
        }
    }

    // Handle invalid neighbor indices (consistent with existing brute_force.hpp)
    for (size_t i = 0; i < n_queries * limit; ++i) {
        if (neighbors[i] == std::numeric_limits<int64_t>::max() || 
            neighbors[i] == 4294967295LL || neighbors[i] < 0) {
            neighbors[i] = -1;
        }
    }
}

} // namespace matrixone
