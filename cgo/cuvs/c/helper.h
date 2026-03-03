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

#ifdef __cplusplus
}
#endif

#endif // MO_CUVS_C_HELPER_H
