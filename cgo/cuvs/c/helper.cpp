#include "helper.h"
#include <cuda_runtime.h>

int GpuGetDeviceCount() {
    int count = 0;
    cudaError_t err = cudaGetDeviceCount(&count);
    if (err != cudaSuccess) {
        return -1;
    }
    return count;
}

int GpuGetDeviceList(int* devices, int max_count) {
    int count = GpuGetDeviceCount();
    if (count <= 0) {
        return count;
    }
    
    int actual_count = (count < max_count) ? count : max_count;
    for (int i = 0; i < actual_count; ++i) {
        devices[i] = i;
    }
    return actual_count;
}
