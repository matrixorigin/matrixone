#include "helper.h"
#include "cuvs_worker.hpp"
#include <cuda_runtime.h>
#include <cuda_fp16.h>
#include <stdexcept>
#include <string>
#include <cstring>
#include <iostream>
#include <raft/util/cudart_utils.hpp>

// Vectorized kernel processing 2 elements per thread
__global__ void f32_to_f16_vectorized_kernel(const float2* src, half2* dst, uint64_t n_pairs) {
    uint64_t i = blockIdx.x * (uint64_t)blockDim.x + threadIdx.x;
    if (i < n_pairs) {
        dst[i] = __float22half2_rn(src[i]);
    }
}

// Fallback kernel for the last element if total_elements is odd
__global__ void f32_to_f16_tail_kernel(const float* src, half* dst, uint64_t index) {
    dst[index] = __float2half(src[index]);
}

extern "C" {

int gpu_get_device_count() {
    int count = 0;
    cudaError_t err = cudaGetDeviceCount(&count);
    if (err != cudaSuccess) {
        return -1;
    }
    return count;
}

int gpu_get_device_list(int* devices, int max_count) {
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

void gpu_convert_f32_to_f16(const float* src, void* dst, uint64_t total_elements, int device_id, void* errmsg) {
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

        // Launch vectorized kernel for pairs
        uint64_t n_pairs = total_elements / 2;
        if (n_pairs > 0) {
            uint32_t threads_per_block = 256;
            uint32_t blocks = (n_pairs + threads_per_block - 1) / threads_per_block;
            f32_to_f16_vectorized_kernel<<<blocks, threads_per_block>>>((const float2*)d_src, (half2*)d_dst, n_pairs);
        }

        // Handle the tail if odd
        if (total_elements % 2 != 0) {
            f32_to_f16_tail_kernel<<<1, 1>>>(d_src, d_dst, total_elements - 1);
        }
        
        RAFT_CUDA_TRY(cudaPeekAtLastError());
        RAFT_CUDA_TRY(cudaDeviceSynchronize());

        // Copy result back to host
        RAFT_CUDA_TRY(cudaMemcpy(dst, d_dst, total_elements * sizeof(half), cudaMemcpyDeviceToHost));

        // Free device memory
        cudaFree(d_src);
        cudaFree(d_dst);

    } catch (const std::exception& e) {
        set_errmsg_helper(errmsg, "Error in gpu_convert_f32_to_f16", e);
    }
}

} // extern "C"
