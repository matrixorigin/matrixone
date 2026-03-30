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
#include <cuvs/distance/distance.hpp>
#include "helper.h"
#include <vector>
#include <cstdint>

namespace matrixone {

// =============================================================================
// distance.hpp — Developer Guide
// =============================================================================
//
// PURPOSE
// -------
// Provides two stateless functions for GPU pairwise distance computation between
// two sets of vectors X (n_x × dim) and Y (n_y × dim), producing an
// (n_x × n_y) float32 distance matrix on the host.
// Uses cuvs::distance::pairwise_distance internally.
//
// FUNCTIONS
// ---------
// pairwise_distance_async<T>(res, x, n_x, y, n_y, dim, metric, dist)
//   - Allocates device memory, uploads X and Y, runs pairwise_distance,
//     copies results back to host — all asynchronously on the CUDA stream
//     embedded in res.
//   - Returns the raw device pointer (void*) to the caller.
//   - The stream is NOT synchronized before returning.
//   - The caller must: (1) sync the stream before reading dist[], then
//     (2) call cudaFreeAsync(d_ptr, stream) to release device memory.
//   - Use when multiple async operations are chained and the caller manages
//     synchronization and cleanup explicitly.
//
// pairwise_distance<T>(res, x, n_x, y, n_y, dim, metric, dist)
//   - Wraps pairwise_distance_async; synchronizes the stream and frees
//     device memory before returning.
//   - Fully synchronous from the caller's perspective.
//   - Use for simple one-shot calls where async management is not needed.
//
// MEMORY LAYOUT
// -------------
// Single cudaMallocAsync covers:  [X | Y | dist_matrix]
// Each region is 256-byte aligned to satisfy CUDA alignment requirements.
// The distance matrix region (n_x * n_y * 4 bytes) is NOT padded — it is
// the last region and exact sizing is sufficient.
//
// METRICS
// -------
// metric is distance_type_t (from cuvs_types.h); cast to
// cuvs::distance::DistanceType before calling pairwise_distance.
// Supported metrics depend on cuVS (L2, L2Sqrt, InnerProduct, Cosine, etc.).
//
// CALLER RESPONSIBILITY
// ---------------------
// - The caller owns the raft::resources and must set the CUDA device.
// - dist[] must be pre-allocated on the host with size n_x * n_y * sizeof(float).
// - For pairwise_distance_async, stream sync and cudaFreeAsync are the caller's
//   responsibility.
//
// LIMITATIONS
// -----------
// - Only float32 input (T=float) is typical; other T types work if cuVS supports
//   the metric for that type.
// - No batching — the full X and Y matrices must fit in device memory.
//
// =============================================================================

/**
 * @brief Performs a pairwise distance calculation on GPU asynchronously.
 * 
 * @tparam T Data type of the vector elements (e.g., float, half).
 * @param res RAFT resources handle.
 * @param x Host pointer to the first set of vectors (X).
 * @param n_x Number of vectors in X.
 * @param y Host pointer to the second set of vectors (Y).
 * @param n_y Number of vectors in Y.
 * @param dim Dimension of each vector.
 * @param metric Distance metric to use.
 * @param dist Host pointer to store the resulting distances (size: n_x * n_y).
 * @return void* The device pointer for temporary buffers (must be freed with cudaFreeAsync).
 */
template <typename T>
void* pairwise_distance_async(const raft::resources& res,
                             const T* x,
                             uint64_t n_x,
                             const T* y,
                             uint64_t n_y,
                             uint32_t dim,
                             distance_type_t metric,
                             float* dist) {
    auto stream = raft::resource::get_cuda_stream(res);

    // Helper to align sizes to 256 bytes (CUDA default alignment)
    auto align_size = [](size_t size) {
        return (size + 255) & ~255;
    };

    // 1. Calculate total buffer sizes with alignment
    size_t x_bytes = n_x * dim * sizeof(T);
    size_t y_bytes = n_y * dim * sizeof(T);
    size_t dist_bytes = n_x * n_y * sizeof(float);

    size_t x_alloc = align_size(x_bytes);
    size_t y_alloc = align_size(y_bytes);
    size_t total_bytes = x_alloc + y_alloc + dist_bytes;

    // Use a single allocation for all temporary buffers to reduce overhead
    void* d_ptr = nullptr;
    RAFT_CUDA_TRY(cudaMallocAsync(&d_ptr, total_bytes, stream));

    char* d_x = static_cast<char*>(d_ptr);
    char* d_y = d_x + x_alloc;
    char* d_dist = d_y + y_alloc;

    // 2. Async copies to Device
    raft::copy(res, raft::make_device_matrix_view<T, int64_t>(reinterpret_cast<T*>(d_x), (int64_t)n_x, (int64_t)dim), raft::make_host_matrix_view<const T, int64_t>(x, (int64_t)n_x, (int64_t)dim));
    raft::copy(res, raft::make_device_matrix_view<T, int64_t>(reinterpret_cast<T*>(d_y), (int64_t)n_y, (int64_t)dim), raft::make_host_matrix_view<const T, int64_t>(y, (int64_t)n_y, (int64_t)dim));

    // 3. Prepare Views (zero allocation)
    auto x_view = raft::make_device_matrix_view<const T, int64_t>(reinterpret_cast<const T*>(d_x), (int64_t)n_x, (int64_t)dim);
    auto y_view = raft::make_device_matrix_view<const T, int64_t>(reinterpret_cast<const T*>(d_y), (int64_t)n_y, (int64_t)dim);
    auto dist_view = raft::make_device_matrix_view<float, int64_t>(reinterpret_cast<float*>(d_dist), (int64_t)n_x, (int64_t)n_y);

    // 4. Execute Pairwise Distance
    cuvs::distance::pairwise_distance(res, x_view, y_view, dist_view, static_cast<cuvs::distance::DistanceType>(metric));

    // 5. Async copy results back to host
    raft::copy(res, raft::make_host_matrix_view<float, int64_t>(dist, (int64_t)n_x, (int64_t)n_y), dist_view);

    return d_ptr;
}

/**
 * @brief Performs a pairwise distance calculation on GPU.
 * 
 * @tparam T Data type of the vector elements (e.g., float, half).
 * @param res RAFT resources handle.
 * @param x Host pointer to the first set of vectors (X).
 * @param n_x Number of vectors in X.
 * @param y Host pointer to the second set of vectors (Y).
 * @param n_y Number of vectors in Y.
 * @param dim Dimension of each vector.
 * @param metric Distance metric to use.
 * @param dist Host pointer to store the resulting distances (size: n_x * n_y).
 */
template <typename T>
void pairwise_distance(const raft::resources& res,
                       const T* x,
                       uint64_t n_x,
                       const T* y,
                       uint64_t n_y,
                       uint32_t dim,
                       distance_type_t metric,
                       float* dist) {
    auto stream = raft::resource::get_cuda_stream(res);
    void* d_ptr = pairwise_distance_async(res, x, n_x, y, n_y, dim, metric, dist);
    raft::resource::sync_stream(res);
    
    if (metric == DistanceType_InnerProduct) {
        for (uint64_t i = 0; i < n_x * n_y; ++i) {
            dist[i] *= -1.0f;
        }
    }

    RAFT_CUDA_TRY(cudaFreeAsync(d_ptr, stream));
}

} // namespace matrixone
