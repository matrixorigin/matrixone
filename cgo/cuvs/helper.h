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

#ifndef MO_CUVS_C_HELPER_H
#define MO_CUVS_C_HELPER_H

#include "cuvs_types.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Returns the number of CUDA-capable devices available.
 * @return Number of GPU devices.
 */
int gpu_get_device_count();

/**
 * @brief Lists the IDs of available CUDA devices.
 * @param devices Output array to store device IDs.
 * @param max_count Maximum number of device IDs to store.
 * @return Number of device IDs written to the array.
 */
int gpu_get_device_list(int* devices, int max_count);

/**
 * @brief Converts float32 data to float16 (half) on GPU.
 * @param src Pointer to source float32 data on host or device.
 * @param dst Pointer to destination float16 data on device.
 * @param total_elements Total number of elements to convert.
 * @param device_id ID of the GPU device to use.
 * @param errmsg Pointer to store error message if any.
 */
void gpu_convert_f32_to_f16(const float* src, void* dst, uint64_t total_elements, int device_id, void* errmsg);

/**
 * @brief Standardized helper to set an error message.
 * @param errmsg Pointer to the error message destination.
 * @param prefix Prefix for the error message (e.g., function name).
 * @param what The actual error description.
 */
void set_errmsg(void* errmsg, const char* prefix, const char* what);

#ifdef __cplusplus
}

#include <cuvs/distance/distance.hpp>
namespace matrixone {
    cuvs::distance::DistanceType convert_distance_type(distance_type_t metric_c);
}
#endif

#endif // MO_CUVS_C_HELPER_H
