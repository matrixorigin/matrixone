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

#include "distance_c.h"
#include "distance.hpp"
#include <cuda_runtime.h>
#include <cuda_fp16.h>

extern "C" {

void gpu_pairwise_distance(const void* x,
                           uint64_t n_x,
                           const void* y,
                           uint64_t n_y,
                           uint32_t dim,
                           distance_type_t metric,
                           quantization_t qtype,
                           int device_id,
                           float* dist,
                           void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        if (!x || !y || !dist || n_x == 0 || n_y == 0 || dim == 0) return;

        RAFT_CUDA_TRY(cudaSetDevice(device_id));
        const raft::resources& res = matrixone::get_raft_resources();

        if (qtype == Quantization_F32) {
            matrixone::pairwise_distance<float>(res, static_cast<const float*>(x), n_x, static_cast<const float*>(y), n_y, dim, metric, dist);
        } else if (qtype == Quantization_F16) {
            matrixone::pairwise_distance<half>(res, static_cast<const half*>(x), n_x, static_cast<const half*>(y), n_y, dim, metric, dist);
        } else {
            throw std::runtime_error("Unsupported quantization type for pairwise_distance");
        }

    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_pairwise_distance", e.what());
    }
}

} // extern "C"
