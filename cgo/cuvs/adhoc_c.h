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

#ifndef ADHOC_C_H
#define ADHOC_C_H

#include "helper.h"
#include "cuvs_types.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Performs an ad-hoc brute-force search on GPU.
 * 
 * @param dataset Host pointer to the dataset vectors.
 * @param n_rows Number of vectors in the dataset.
 * @param dim Dimension of each vector.
 * @param queries Host pointer to the query vectors.
 * @param n_queries Number of query vectors.
 * @param limit Number of nearest neighbors to find (k).
 * @param metric Distance metric to use.
 * @param qtype Quantization type (F32, F16).
 * @param device_id GPU device ID to use.
 * @param neighbors Host pointer to store the resulting neighbor IDs (size: n_queries * limit).
 * @param distances Host pointer to store the resulting distances (size: n_queries * limit).
 * @param errmsg Pointer to store error message if any.
 */
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
                                  void* errmsg);

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
                                        void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // ADHOC_C_H
