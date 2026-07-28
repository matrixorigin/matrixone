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
#include "index_base.hpp"  // clamp_k_to_index_size / scatter_with_padding / fill_all_sentinel / transform_distance
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

    // cuVS rejects k > n_rows. Clamp to effective_k and pad the caller's
    // (n_queries × limit) buffers with (-1, FLT_MAX) — same pattern as the
    // persistent-index search paths. See index_base.hpp for helpers.
    const uint32_t effective_k = clamp_k_to_index_size(limit, n_rows);

    if (effective_k == 0) {
        // Empty dataset — no GPU work, no temp index to build.
        fill_all_sentinel<int64_t>(neighbors, distances,
                                   static_cast<size_t>(n_queries) * limit,
                                   /*neighbor_sentinel=*/-1LL);
        return;
    }

    // Helper to align sizes to 256 bytes (CUDA default alignment)
    auto align_size = [](size_t size) {
        return (size + 255) & ~255;
    };

    // 1. Calculate total buffer sizes with alignment. Sized to effective_k
    // (not limit) — cuVS writes exactly n_queries * effective_k entries.
    size_t dataset_bytes = n_rows * dim * sizeof(T);
    size_t queries_bytes = n_queries * dim * sizeof(T);
    size_t neighbors_bytes = static_cast<size_t>(n_queries) * effective_k * sizeof(int64_t);
    size_t distances_bytes = static_cast<size_t>(n_queries) * effective_k * sizeof(float);

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

    // 3. Prepare Views (zero allocation). Neighbors / distances at effective_k.
    auto dataset_view = raft::make_device_matrix_view<const T, int64_t>(reinterpret_cast<const T*>(d_dataset), (int64_t)n_rows, (int64_t)dim);
    auto queries_view = raft::make_device_matrix_view<const T, int64_t>(reinterpret_cast<const T*>(d_queries), (int64_t)n_queries, (int64_t)dim);
    auto neighbors_view = raft::make_device_matrix_view<int64_t, int64_t>(reinterpret_cast<int64_t*>(d_neighbors), (int64_t)n_queries, (int64_t)effective_k);
    auto distances_view = raft::make_device_matrix_view<float, int64_t>(reinterpret_cast<float*>(d_distances), (int64_t)n_queries, (int64_t)effective_k);

    // 4. Build temporary index (view-based, very fast)
    cuvs::neighbors::brute_force::index_params index_params;
    index_params.metric = metric;
    auto index = cuvs::neighbors::brute_force::build(res, index_params, raft::make_const_mdspan(dataset_view));

    // 5. Execute Search — cuVS reads k from neighbors_view.extent(1) = effective_k.
    cuvs::neighbors::brute_force::search_params search_params;
    cuvs::neighbors::brute_force::search(res, search_params, index,
                                         raft::make_const_mdspan(queries_view),
                                         neighbors_view,
                                         distances_view);

    // 6. Async copy results back to host. Fast path when effective_k == limit
    // writes directly into the caller buffer; otherwise stage in a tight host
    // tmp and scatter row-by-row with sentinel padding so the (n_queries × limit)
    // row layout the caller expects is preserved.
    std::vector<int64_t> tmp_n;
    std::vector<float>   tmp_d;
    if (effective_k == limit) {
        raft::copy(res, raft::make_host_matrix_view<int64_t, int64_t>(neighbors, (int64_t)n_queries, (int64_t)limit), neighbors_view);
        raft::copy(res, raft::make_host_matrix_view<float, int64_t>(distances, (int64_t)n_queries, (int64_t)limit), distances_view);
    } else {
        tmp_n.resize(static_cast<size_t>(n_queries) * effective_k);
        tmp_d.resize(static_cast<size_t>(n_queries) * effective_k);
        raft::copy(res, raft::make_host_matrix_view<int64_t, int64_t>(tmp_n.data(), (int64_t)n_queries, (int64_t)effective_k), neighbors_view);
        raft::copy(res, raft::make_host_matrix_view<float, int64_t>(tmp_d.data(), (int64_t)n_queries, (int64_t)effective_k), distances_view);
    }

    // 7. Synchronize
    raft::resource::sync_stream(res);

    // 8. Async free
    RAFT_CUDA_TRY(cudaFreeAsync(d_ptr, stream));

    if (effective_k != limit) {
        scatter_with_padding<int64_t>(neighbors, distances,
                                      tmp_n.data(), tmp_d.data(),
                                      n_queries, limit, effective_k,
                                      /*neighbor_sentinel=*/-1LL);
    }

    // InnerProduct sign flip — sentinel-preserving. distance_type_t and
    // cuvs::distance::DistanceType share the same numeric layout (see
    // cuvs_types.h and adhoc_c.cpp:39's reverse cast).
    transform_distance(static_cast<distance_type_t>(metric),
                       distances,
                       static_cast<size_t>(n_queries) * limit);

    // Defense-in-depth: cuvs may still write junk values (INT64_MAX,
    // UINT32_MAX, INT32_MAX, etc.) into the [0, effective_k) valid slots
    // — e.g. for fewer-than-k matches after dedup. Bounds-check against
    // n_rows; padded slots [effective_k, limit) already pass (neighbors=-1).
    for (size_t i = 0; i < static_cast<size_t>(n_queries) * limit; ++i) {
        if (neighbors[i] < 0 || neighbors[i] >= static_cast<int64_t>(n_rows)) {
            neighbors[i] = -1;
        }
    }
}

} // namespace matrixone
