// Device code
extern "C" __global__ void l2distance_f32(
        double *res, int n, int vecsz, bool sq,
        const float *A, const float *B) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int loop = vecsz / sizeof(float);
    if (i < n) {
        res[i] = 0;
        for (int j = 0; j < loop; j++) {
            float diff = A[i * loop + j] - B[i * loop + j];
            res[i] += diff * diff;
        }
        if (!sq) {
            res[i] = sqrt(res[i]);
        }
    }
}

extern "C" __global__ void l2distance_f32_const(
        double *res, int n, int vecsz, bool sq,
        const float *A, const float *B) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int loop = vecsz / sizeof(float);
    if (i < n) {
        res[i] = 0;
        for (int j = 0; j < loop; j++) {
            float diff = A[i * loop + j] - B[j];
            res[i] += diff * diff;
        }
        if (!sq) {
            res[i] = sqrt(res[i]);
        }
    }
}

extern "C" __global__ void l2distance_f64(
        double *res, int n, int vecsz, bool sq,
        const double *A, const double *B) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int loop = vecsz / sizeof(double);
    if (i < n) {
        res[i] = 0;
        for (int j = 0; j < loop; j++) {
            double diff = A[i * loop + j] - B[i * loop + j];
            res[i] += diff * diff;
        }
        if (!sq) {
            res[i] = sqrt(res[i]);
        } 
    }
}

extern "C" __global__ void l2distance_f64_const(
        double *res, int n, int vecsz, bool sq,
        const float *A, const float *B) {
    int i = blockDim.x * blockIdx.x + threadIdx.x;
    int loop = vecsz / sizeof(double);
    res[i] = 0;
    if (i < n) {
        for (int j = 0; j < loop; j++) {
            double diff = A[i * loop + j] - B[j];
            res[i] += diff * diff;
        }
        if (!sq) {
            res[i] = sqrt(res[i]);
        }
    }
}
