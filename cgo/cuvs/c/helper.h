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
} CuvsDistanceTypeC;

typedef enum {
    Quantization_F32,
    Quantization_F16,
    Quantization_INT8,
    Quantization_UINT8
} CuvsQuantizationC;

int GpuGetDeviceCount();
int GpuGetDeviceList(int* devices, int max_count);

#ifdef __cplusplus
}
#endif

#endif // MO_CUVS_HELPER_H
