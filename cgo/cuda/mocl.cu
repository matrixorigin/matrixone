#include <stdint.h>

// Device code
extern "C" __global__ void l2distance_f32(
        double *res, int n, int vecsz, bool sq,
        const uint32_t *offlenA, const uint8_t *A, 
        const uint32_t *offlenB, const uint8_t *B) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int loop = vecsz / sizeof(float);
    if (i < n) {
        res[i] = 0;
        uint32_t offA = offlenA[i * 6 + 1];
        uint32_t offB = offlenB[i * 6 + 1];
        float *astart = (float *)(A + offA);
        float *bstart = (float *)(B + offB); 
        for (int j = 0; j < loop; j++) {
            float diff = astart[j] - bstart[j];
            res[i] += diff * diff;
        }
        if (!sq) {
            res[i] = sqrt(res[i]);
        }
    }
}

extern "C" __global__ void l2distance_f32_const(
        double *res, int n, int vecsz, bool sq,
        const uint32_t *offlenA, const uint8_t *A,
        const float *B) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int loop = vecsz / sizeof(float);
    if (i < n) {
        res[i] = 0;
        uint32_t offA = offlenA[i * 6 + 1];
        float *astart = (float *)(A + offA);
        for (int j = 0; j < loop; j++) {
            float diff = astart[j] - B[j];
            res[i] += diff * diff;
        }
        if (!sq) {
            res[i] = sqrt(res[i]);
        }
    }
}

extern "C" __global__ void l2distance_f64(
        double *res, int n, int vecsz, bool sq,
        const uint32_t *offlenA, const uint8_t *A, 
        const uint32_t *offlenB, const uint8_t *B) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int loop = vecsz / sizeof(double);
    if (i < n) {
        res[i] = 0;
        uint32_t offA = offlenA[i * 6 + 1];
        uint32_t offB = offlenB[i * 6 + 1];
        double *astart = (double *)(A + offA);
        double *bstart = (double *)(B + offB); 
        for (int j = 0; j < loop; j++) {
            float diff = astart[j] - bstart[j];
            res[i] += diff * diff;
        }
        if (!sq) {
            res[i] = sqrt(res[i]);
        }
    }
}

extern "C" __global__ void l2distance_f64_const(
        double *res, int n, int vecsz, bool sq,
        const uint32_t *offlenA, const uint8_t *A,
        const double *B) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int loop = vecsz / sizeof(double);
    if (i < n) {
        res[i] = 0;
        uint32_t offA = offlenA[i * 6 + 1];
        double *astart = (double *)(A + offA);
        for (int j = 0; j < loop; j++) {
            float diff = astart[j] - B[j];
            res[i] += diff * diff;
        }
        if (!sq) {
            res[i] = sqrt(res[i]);
        }
    }
}
