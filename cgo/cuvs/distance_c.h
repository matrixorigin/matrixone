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

#ifndef DISTANCE_C_H
#define DISTANCE_C_H

#include "helper.h"
#include "cuvs_types.h"
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Performs a pairwise distance calculation on GPU.
 * 
 * @param x Host pointer to the first set of vectors (X).
 * @param n_x Number of vectors in X.
 * @param y Host pointer to the second set of vectors (Y).
 * @param n_y Number of vectors in Y.
 * @param dim Dimension of each vector.
 * @param metric Distance metric to use.
 * @param qtype Quantization type (F32, F16).
 * @param device_id GPU device ID to use.
 * @param dist Host pointer to store the resulting distances (size: n_x * n_y).
 * @param errmsg Pointer to store error message if any.
 */
void gpu_pairwise_distance(const void* x,
                           uint64_t n_x,
                           const void* y,
                           uint64_t n_y,
                           uint32_t dim,
                           distance_type_t metric,
                           quantization_t qtype,
                           int device_id,
                           float* dist,
                           void* errmsg);

uint64_t gpu_pairwise_distance_launch(const void* x,
                                     uint64_t n_x,
                                     const void* y,
                                     uint64_t n_y,
                                     uint32_t dim,
                                     distance_type_t metric,
                                     quantization_t qtype,
                                     int device_id,
                                     float* dist,
                                     void* errmsg);

void gpu_pairwise_distance_wait(uint64_t job_id, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // DISTANCE_C_H
