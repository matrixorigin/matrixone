#include "helper.h"
#include <cuda_runtime.h>
#include <cuda_fp16.h>
#include <stdexcept>
#include <string>
#include <cstring>
#include <iostream>
#include <raft/util/cudart_utils.hpp>

// Simple kernel for float32 to float16 conversion
__global__ void f32_to_f16_kernel(const float* src, half* dst, uint64_t n) {
    uint64_t i = blockIdx.x * (uint64_t)blockDim.x + threadIdx.x;
    if (i < n) {
        dst[i] = __float2half(src[i]);
    }
}

extern "C" {

int GpuGetDeviceCount() {
    int count = 0;
    cudaError_t err = cudaGetDeviceCount(&count);
    if (err != cudaSuccess) {
        return -1;
    }
    return count;
}

int GpuGetDeviceList(int* devices, int max_count) {
    int count = 0;
    cudaError_t err = cudaGetDeviceCount(&count);
    if (err != cudaSuccess) {
        return -1;
    }
    int actual_count = (count > max_count) ? max_count : count;
    for (int i = 0; i < actual_count; ++i) {
        devices[i] = i;
    }
    return actual_count;
}

static void set_errmsg_helper(void* errmsg, const std::string& prefix, const std::exception& e) {
    if (errmsg) {
        std::string err_str = prefix + ": " + std::string(e.what());
        char* msg = (char*)malloc(err_str.length() + 1);
        if (msg) {
            std::strcpy(msg, err_str.c_str());
            *(static_cast<char**>(errmsg)) = msg;
        }
    } else {
        std::cerr << prefix << ": " << e.what() << std::endl;
    }
}

void GpuConvertF32ToF16(const float* src, void* dst, uint64_t total_elements, int device_id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        if (!src || !dst || total_elements == 0) return;

        RAFT_CUDA_TRY(cudaSetDevice(device_id));

        float *d_src = nullptr;
        half *d_dst = nullptr;

        // Allocate device memory
        RAFT_CUDA_TRY(cudaMalloc(&d_src, total_elements * sizeof(float)));
        RAFT_CUDA_TRY(cudaMalloc(&d_dst, total_elements * sizeof(half)));

        // Copy source to device
        RAFT_CUDA_TRY(cudaMemcpy(d_src, src, total_elements * sizeof(float), cudaMemcpyHostToDevice));

        // Launch kernel
        uint32_t threads_per_block = 256;
        uint32_t blocks = (total_elements + threads_per_block - 1) / threads_per_block;
        f32_to_f16_kernel<<<blocks, threads_per_block>>>(d_src, d_dst, total_elements);
        
        RAFT_CUDA_TRY(cudaPeekAtLastError());
        RAFT_CUDA_TRY(cudaDeviceSynchronize());

        // Copy result back to host
        RAFT_CUDA_TRY(cudaMemcpy(dst, d_dst, total_elements * sizeof(half), cudaMemcpyDeviceToHost));

        // Free device memory
        cudaFree(d_src);
        cudaFree(d_dst);

    } catch (const std::exception& e) {
        set_errmsg_helper(errmsg, "Error in GpuConvertF32ToF16", e);
    }
}

} // extern "C"
