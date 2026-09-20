#include <stdint.h>
#include <math.h>

__device__ double xcall_scale_result(double scale, double value, bool square) {
    int exponent;
    double mantissa = frexp(scale, &exponent);
    if (square) {
        return ldexp(value * mantissa * mantissa, 2 * exponent);
    }
    return ldexp(value * mantissa, exponent);
}

__device__ double xcall_nonfinite_result(bool hasNaN) {
    return hasNaN ? NAN : __builtin_huge_val();
}

// Device code
extern "C" __global__ void l2distance_f32(
        double *res, int n, int vecsz, bool sq,
        const uint32_t *offlenA, const uint8_t *A, 
        const uint32_t *offlenB, const uint8_t *B) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int loop = vecsz / sizeof(float);
    if (i < n) {
        uint32_t offA = offlenA[i * 6 + 1];
        uint32_t offB = offlenB[i * 6 + 1];
        float *astart = (float *)(A + offA);
        float *bstart = (float *)(B + offB); 
        double scale = 0;
        bool hasNaN = false;
        bool hasInf = false;
        for (int j = 0; j < loop; j++) {
            double diff = (double)astart[j] - (double)bstart[j];
            double absDiff = fabs(diff);
            if (isnan(diff)) {
                hasNaN = true;
                continue;
            }
            if (isinf(absDiff)) {
                hasInf = true;
                continue;
            }
            if (absDiff > scale) {
                scale = absDiff;
            }
        }
        if (hasNaN || hasInf) {
            res[i] = xcall_nonfinite_result(hasNaN);
            return;
        }
        if (scale == 0) {
            res[i] = 0;
            return;
        }
        double sum = 0;
        for (int j = 0; j < loop; j++) {
            double scaled = ((double)astart[j] - (double)bstart[j]) / scale;
            sum += scaled * scaled;
        }
        res[i] = xcall_scale_result(scale, sq ? sum : sqrt(sum), sq);
    }
}

extern "C" __global__ void l2distance_f32_const(
        double *res, int n, int vecsz, bool sq,
        const uint32_t *offlenA, const uint8_t *A,
        const float *B) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int loop = vecsz / sizeof(float);
    if (i < n) {
        uint32_t offA = offlenA[i * 6 + 1];
        float *astart = (float *)(A + offA);
        double scale = 0;
        bool hasNaN = false;
        bool hasInf = false;
        for (int j = 0; j < loop; j++) {
            double diff = (double)astart[j] - (double)B[j];
            double absDiff = fabs(diff);
            if (isnan(diff)) {
                hasNaN = true;
                continue;
            }
            if (isinf(absDiff)) {
                hasInf = true;
                continue;
            }
            if (absDiff > scale) {
                scale = absDiff;
            }
        }
        if (hasNaN || hasInf) {
            res[i] = xcall_nonfinite_result(hasNaN);
            return;
        }
        if (scale == 0) {
            res[i] = 0;
            return;
        }
        double sum = 0;
        for (int j = 0; j < loop; j++) {
            double scaled = ((double)astart[j] - (double)B[j]) / scale;
            sum += scaled * scaled;
        }
        res[i] = xcall_scale_result(scale, sq ? sum : sqrt(sum), sq);
    }
}

extern "C" __global__ void l2distance_f64(
        double *res, int n, int vecsz, bool sq,
        const uint32_t *offlenA, const uint8_t *A, 
        const uint32_t *offlenB, const uint8_t *B) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int loop = vecsz / sizeof(double);
    if (i < n) {
        uint32_t offA = offlenA[i * 6 + 1];
        uint32_t offB = offlenB[i * 6 + 1];
        double *astart = (double *)(A + offA);
        double *bstart = (double *)(B + offB); 
        double scale = 0;
        bool hasNaN = false;
        bool hasInf = false;
        for (int j = 0; j < loop; j++) {
            double diff = astart[j] - bstart[j];
            double absDiff = fabs(diff);
            if (isnan(diff)) {
                hasNaN = true;
                continue;
            }
            if (isinf(absDiff)) {
                hasInf = true;
                continue;
            }
            if (absDiff > scale) {
                scale = absDiff;
            }
        }
        if (hasNaN || hasInf) {
            res[i] = xcall_nonfinite_result(hasNaN);
            return;
        }
        if (scale == 0) {
            res[i] = 0;
            return;
        }
        double sum = 0;
        for (int j = 0; j < loop; j++) {
            double scaled = (astart[j] - bstart[j]) / scale;
            sum += scaled * scaled;
        }
        res[i] = xcall_scale_result(scale, sq ? sum : sqrt(sum), sq);
    }
}

extern "C" __global__ void l2distance_f64_const(
        double *res, int n, int vecsz, bool sq,
        const uint32_t *offlenA, const uint8_t *A,
        const double *B) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int loop = vecsz / sizeof(double);
    if (i < n) {
        uint32_t offA = offlenA[i * 6 + 1];
        double *astart = (double *)(A + offA);
        double scale = 0;
        bool hasNaN = false;
        bool hasInf = false;
        for (int j = 0; j < loop; j++) {
            double diff = astart[j] - B[j];
            double absDiff = fabs(diff);
            if (isnan(diff)) {
                hasNaN = true;
                continue;
            }
            if (isinf(absDiff)) {
                hasInf = true;
                continue;
            }
            if (absDiff > scale) {
                scale = absDiff;
            }
        }
        if (hasNaN || hasInf) {
            res[i] = xcall_nonfinite_result(hasNaN);
            return;
        }
        if (scale == 0) {
            res[i] = 0;
            return;
        }
        double sum = 0;
        for (int j = 0; j < loop; j++) {
            double scaled = (astart[j] - B[j]) / scale;
            sum += scaled * scaled;
        }
        res[i] = xcall_scale_result(scale, sq ? sum : sqrt(sum), sq);
    }
}
