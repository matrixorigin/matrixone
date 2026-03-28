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

#include "cuvs_types.h"

#ifdef __cplusplus
#include <raft/core/resources.hpp>
#include <raft/core/host_mdspan.hpp>
#include <cuvs/distance/distance.hpp>
#include <vector>
#include <string>
#include <cuda_fp16.h>

namespace matrixone {

/**
 * @brief Helper to check if a raft handle has SNMG resources initialized.
 */
bool is_snmg_handle(const raft::resources& res);

/**
 * @brief Initialize NCCL communicators for a multi-GPU resource container.
 */
void init_mg_comms(raft::resources& mg_res, const std::vector<int>& devices);

/**
 * @brief Inject a raw NCCL communicator into raft::resources.
 * This is a wrapper to avoid multiple definitions of std_comms.hpp.
 */
void inject_nccl_comm(raft::resources* res, void* nccl_comm, int size, int rank);

/**
 * @brief Save a host matrix to a file in MODF format.
 */
void save_host_matrix(const std::string& filename, raft::host_matrix_view<const float, int64_t, raft::row_major> view);

/**
 * @brief Helper to set an error message in a C-compatible way.
 */
void set_errmsg(void* errmsg, const char* context, const char* message);

/**
 * @brief Get raft resources for the given device (thread-local, one per device per thread).
 * Also calls cudaSetDevice(device_id) to ensure the calling thread is on the right device.
 */
const raft::resources& get_raft_resources(int device_id = 0);

/**
 * @brief Returns the next GPU device ID in round-robin order across all visible devices.
 * The device count is queried once and cached.
 */
int get_next_device_id();

/**
 * @brief Convert distance type from C enum to cuVS enum.
 */
cuvs::distance::DistanceType convert_distance_type(distance_type_t metric_c);

/**
 * @brief Performs float to half conversion on device.
 */
void convert_f32_to_f16_on_device(const raft::resources& res, const float* src, half* dst, uint64_t total_elements);

/**
 * @brief Performs half to float conversion on device.
 */
void convert_f16_to_f32_on_device(const raft::resources& res, const half* src, float* dst, uint64_t total_elements);

} // namespace matrixone
#endif

// C-compatible wrappers if needed
#ifdef __cplusplus
extern "C" {
#endif
    int gpu_get_device_count();
    void gpu_get_device_list(int* devices, int count);
    void gpu_convert_f32_to_f16(const float* src, void* dst, uint64_t total_elements, int device_id, void* errmsg);
// Pinned memory management
void* gpu_alloc_pinned(uint64_t size, void* errmsg);
void gpu_free_pinned(void* ptr, void* errmsg);

#ifdef __cplusplus
}
#endif
