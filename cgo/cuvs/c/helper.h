#ifndef MO_CUVS_HELPER_H
#define MO_CUVS_HELPER_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    DistanceType_L2Expanded,
    DistanceType_L1,
    DistanceType_InnerProduct,
    DistanceType_CosineSimilarity,
    DistanceType_Jaccard,
    DistanceType_Hamming,
    DistanceType_Unknown
} distance_type_t;

typedef enum {
    Quantization_F32,
    Quantization_F16,
    Quantization_INT8,
    Quantization_UINT8
} quantization_t;

int gpu_get_device_count();
int gpu_get_device_list(int* devices, int max_count);

// Converts float32 data to float16 (half) on GPU
// src: host float32 array
// dst: host float16 array (pre-allocated, uint16_t in C/Go)
// total_elements: number of elements to convert
// device_id: GPU device to use for conversion
// errmsg: pointer to char* for error message
void gpu_convert_f32_to_f16(const float* src, void* dst, uint64_t total_elements, int device_id, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // MO_CUVS_HELPER_H
