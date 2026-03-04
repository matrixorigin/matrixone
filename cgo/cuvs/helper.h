#ifndef MO_CUVS_C_HELPER_H
#define MO_CUVS_C_HELPER_H

#include "cuvs_types.h"

#ifdef __cplusplus
extern "C" {
#endif

int gpu_get_device_count();
int gpu_get_device_list(int* devices, int max_count);

// Converts float32 data to float16 (half) on GPU
void gpu_convert_f32_to_f16(const float* src, void* dst, uint64_t total_elements, int device_id, void* errmsg);

// Standardized error message helper
void set_errmsg(void* errmsg, const char* prefix, const char* what);

#ifdef __cplusplus
}

#include <cuvs/distance/distance.hpp>
namespace matrixone {
    cuvs::distance::DistanceType convert_distance_type(distance_type_t metric_c);
}
#endif

#endif // MO_CUVS_C_HELPER_H
