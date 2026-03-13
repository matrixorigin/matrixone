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

#include "adhoc_c.h"
#include "adhoc.hpp"
#include "helper.h"
#include <raft/core/resources.hpp>
#include <cuda_runtime.h>

extern "C" {

void gpu_adhoc_brute_force_search(const void* dataset,
                                  uint64_t n_rows,
                                  uint32_t dim,
                                  const void* queries,
                                  uint64_t n_queries,
                                  uint32_t limit,
                                  distance_type_t metric,
                                  quantization_t qtype,
                                  int device_id,
                                  int64_t* neighbors,
                                  float* distances,
                                  void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cudaSetDevice(device_id);
        const auto& res = matrixone::get_raft_resources();
        auto m = static_cast<cuvs::distance::DistanceType>(metric);

        if (qtype == Quantization_F32) {
            matrixone::adhoc_brute_force_search<float>(res, 
                                                       static_cast<const float*>(dataset), 
                                                       n_rows, dim, 
                                                       static_cast<const float*>(queries), 
                                                       n_queries, limit, m, 
                                                       neighbors, distances);
        } else if (qtype == Quantization_F16) {
            matrixone::adhoc_brute_force_search<half>(res, 
                                                      static_cast<const half*>(dataset), 
                                                      n_rows, dim, 
                                                      static_cast<const half*>(queries), 
                                                      n_queries, limit, m, 
                                                      neighbors, distances);
        } else {
            throw std::runtime_error("Unsupported quantization type for adhoc search");
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_adhoc_brute_force_search", e.what());
    }
}

void gpu_adhoc_brute_force_search_float(const float* dataset,
                                        uint64_t n_rows,
                                        uint32_t dim,
                                        const float* queries,
                                        uint64_t n_queries,
                                        uint32_t limit,
                                        distance_type_t metric,
                                        int device_id,
                                        int64_t* neighbors,
                                        float* distances,
                                        void* errmsg) {
    gpu_adhoc_brute_force_search(dataset, n_rows, dim, queries, n_queries, limit, metric, Quantization_F32, device_id, neighbors, distances, errmsg);
}

} // extern "C"
