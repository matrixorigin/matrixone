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
#include <raft/core/host_mdspan.hpp>
#include <cuvs/distance/distance.hpp>
#include <vector>
#include <string>
#include "cuvs_types.h"

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
 * @brief Get global raft resources (for ad-hoc operations).
 */
const raft::resources& get_raft_resources();

/**
 * @brief Convert distance type from C enum to cuVS enum.
 */
cuvs::distance::DistanceType convert_distance_type(distance_type_t metric_c);

} // namespace matrixone

// C-compatible wrappers if needed
extern "C" {
    int gpu_get_device_count();
    void gpu_get_device_list(int* devices, int count);
    void gpu_convert_f32_to_f16(const float* src, void* dst, uint64_t total_elements, int device_id, void* errmsg);
}
