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

    // 1. Prepare Dataset on Device
    auto dataset_device = raft::make_device_matrix<T, int64_t>(res, n_rows, dim);
    RAFT_CUDA_TRY(cudaMemcpy(dataset_device.data_handle(), dataset, n_rows * dim * sizeof(T), cudaMemcpyHostToDevice));

    // 2. Prepare Queries on Device
    auto queries_device = raft::make_device_matrix<T, int64_t>(res, n_queries, dim);
    RAFT_CUDA_TRY(cudaMemcpy(queries_device.data_handle(), queries, n_queries * dim * sizeof(T), cudaMemcpyHostToDevice));

    // 3. Prepare Results on Device
    auto neighbors_device = raft::make_device_matrix<int64_t, int64_t>(res, n_queries, limit);
    auto distances_device = raft::make_device_matrix<float, int64_t>(res, n_queries, limit);

    // 4. Build temporary index (view-based, very fast)
    cuvs::neighbors::brute_force::index_params index_params;
    index_params.metric = metric;
    auto index = cuvs::neighbors::brute_force::build(res, index_params, raft::make_const_mdspan(dataset_device.view()));

    // 5. Execute Search
    cuvs::neighbors::brute_force::search_params search_params;
    cuvs::neighbors::brute_force::search(res, search_params, index, 
                                         raft::make_const_mdspan(queries_device.view()), 
                                         neighbors_device.view(), 
                                         distances_device.view());

    // 6. Copy results back to host
    RAFT_CUDA_TRY(cudaMemcpy(neighbors, neighbors_device.data_handle(), n_queries * limit * sizeof(int64_t), cudaMemcpyDeviceToHost));
    RAFT_CUDA_TRY(cudaMemcpy(distances, distances_device.data_handle(), n_queries * limit * sizeof(float), cudaMemcpyDeviceToHost));

    // 7. Synchronize to ensure host data is ready
    raft::resource::sync_stream(res);

    // Handle invalid neighbor indices (consistent with existing brute_force.hpp)
    for (size_t i = 0; i < n_queries * limit; ++i) {
        if (neighbors[i] == std::numeric_limits<int64_t>::max() || 
            neighbors[i] == 4294967295LL || neighbors[i] < 0) {
            neighbors[i] = -1;
        }
    }
}

} // namespace matrixone
