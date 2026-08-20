/* 
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "helper.h"
#include "device_memory.hpp"
#include <unordered_map>
#include <stdexcept>
#include <cuda_runtime.h>
#include <cuda_fp16.h>
#include <fstream>
#include <cstring>
#include <thread>
#include <chrono>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <raft/util/cudart_utils.hpp>

// F16C / AVX intrinsics for the host fp32→fp16 cast. Available on Haswell+
// (Intel) and Excavator+ / Zen+ (AMD). The Makefile passes -march=native via
// -Xcompiler so these are unconditionally enabled on the build host's ISA.
#if defined(__F16C__) && defined(__AVX__)
#include <immintrin.h>
#endif

namespace matrixone {

void set_errmsg(void* errmsg, const char* context, const char* message) noexcept {
    if (!errmsg) return;
    char** err_ptr_ptr = static_cast<char**>(errmsg);
    try {
        std::string full_msg = std::string(context ? context : "") + ": " +
                               std::string(message ? message : "");
        *err_ptr_ptr = strdup(full_msg.c_str());
    } catch (...) {
        // String construction or allocation failed under OOM. Fall back
        // to a static literal — strdup of a non-null literal can still
        // return NULL on OOM, in which case the caller sees NULL (which
        // it already had to handle).
        *err_ptr_ptr = strdup("set_errmsg: allocation failed");
    }
}

std::string get_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto now_c = std::chrono::system_clock::to_time_t(now);
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
    std::tm now_tm;
    localtime_r(&now_c, &now_tm);
    char buf[64];
    std::strftime(buf, sizeof(buf), "%H:%M:%S", &now_tm);
    std::ostringstream ss;
    ss << buf << "." << std::setfill('0') << std::setw(3) << ms.count();
    return ss.str();
}

void log_err(const std::string& msg) {
    std::cerr << "[ERROR " << get_timestamp() << "] " << msg << std::endl;
}

int get_next_device_id() {
    static std::atomic<uint64_t> counter{0};
    static const int device_count = []() {
        int n = 0;
        return (cudaGetDeviceCount(&n) == cudaSuccess && n > 0) ? n : 1;
    }();
    return static_cast<int>(counter.fetch_add(1, std::memory_order_relaxed) % static_cast<uint64_t>(device_count));
}

const raft::resources& get_raft_resources(int device_id) {
    thread_local std::unordered_map<int, std::unique_ptr<raft::resources>> res_map;
    
    // Always set the device before accessing (or lazily creating) resources for it.
    // This is necessary because Go's runtime may reuse the same OS thread for 
    // different goroutines targeting different devices, and other CGO calls 
    // might have changed the current device on this thread.
    RAFT_CUDA_TRY(cudaSetDevice(device_id));

    auto& res_ptr = res_map[device_id];
    if (!res_ptr) {
        res_ptr = std::make_unique<raft::resources>();
    }
    return *res_ptr;
}

cuvs::distance::DistanceType convert_distance_type(distance_type_t metric_c) {
    switch (metric_c) {
        case DistanceType_L2Expanded: return cuvs::distance::DistanceType::L2Expanded;
        case DistanceType_L2SqrtExpanded: return cuvs::distance::DistanceType::L2SqrtExpanded;
        case DistanceType_CosineExpanded: return cuvs::distance::DistanceType::CosineExpanded;
        case DistanceType_L1: return cuvs::distance::DistanceType::L1;
        case DistanceType_L2Unexpanded: return cuvs::distance::DistanceType::L2Unexpanded;
        case DistanceType_L2SqrtUnexpanded: return cuvs::distance::DistanceType::L2SqrtUnexpanded;
        case DistanceType_InnerProduct: return cuvs::distance::DistanceType::InnerProduct;
        case DistanceType_Linf: return cuvs::distance::DistanceType::Linf;
        case DistanceType_Canberra: return cuvs::distance::DistanceType::Canberra;
        case DistanceType_LpUnexpanded: return cuvs::distance::DistanceType::LpUnexpanded;
        case DistanceType_CorrelationExpanded: return cuvs::distance::DistanceType::CorrelationExpanded;
        case DistanceType_JaccardExpanded: return cuvs::distance::DistanceType::JaccardExpanded;
        case DistanceType_HellingerExpanded: return cuvs::distance::DistanceType::HellingerExpanded;
        case DistanceType_Haversine: return cuvs::distance::DistanceType::Haversine;
        case DistanceType_BrayCurtis: return cuvs::distance::DistanceType::BrayCurtis;
        case DistanceType_JensenShannon: return cuvs::distance::DistanceType::JensenShannon;
        case DistanceType_HammingUnexpanded: return cuvs::distance::DistanceType::HammingUnexpanded;
        case DistanceType_KLDivergence: return cuvs::distance::DistanceType::KLDivergence;
        case DistanceType_RusselRaoExpanded: return cuvs::distance::DistanceType::RusselRaoExpanded;
        case DistanceType_DiceExpanded: return cuvs::distance::DistanceType::DiceExpanded;
        case DistanceType_BitwiseHamming: return cuvs::distance::DistanceType::BitwiseHamming;
        case DistanceType_Precomputed: return cuvs::distance::DistanceType::Precomputed;
        default:
            throw std::runtime_error("Unknown or unsupported distance type");
    }
}

} // namespace matrixone

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

__global__ void f16_to_f32_vectorized_kernel(const half2* src, float2* dst, uint64_t n_pairs) {
    uint64_t i = blockIdx.x * (uint64_t)blockDim.x + threadIdx.x;
    if (i < n_pairs) {
        dst[i] = __half22float2(src[i]);
    }
}

__global__ void f16_to_f32_tail_kernel(const half* src, float* dst, uint64_t index) {
    dst[index] = __half2float(src[index]);
}

namespace matrixone {

void convert_f32_to_f16_on_device(const raft::resources& res, const float* src, half* dst, uint64_t total_elements) {
    if (!src || !dst || total_elements == 0) return;
    
    auto stream = raft::resource::get_cuda_stream(res);
    uint64_t n_pairs = total_elements / 2;
    if (n_pairs > 0) {
        uint32_t threads_per_block = 256;
        uint32_t blocks = (n_pairs + threads_per_block - 1) / threads_per_block;
        f32_to_f16_vectorized_kernel<<<blocks, threads_per_block, 0, stream>>>((const float2*)src, (half2*)dst, n_pairs);
    }
    
    if (total_elements % 2 != 0) {
        f32_to_f16_tail_kernel<<<1, 1, 0, stream>>>(src, dst, total_elements - 1);
    }
}

void convert_f16_to_f32_on_device(const raft::resources& res, const half* src, float* dst, uint64_t total_elements) {
    if (!src || !dst || total_elements == 0) return;

    auto stream = raft::resource::get_cuda_stream(res);
    uint64_t n_pairs = total_elements / 2;
    if (n_pairs > 0) {
        uint32_t threads_per_block = 256;
        uint32_t blocks = (n_pairs + threads_per_block - 1) / threads_per_block;
        f16_to_f32_vectorized_kernel<<<blocks, threads_per_block, 0, stream>>>((const half2*)src, (float2*)dst, n_pairs);
    }

    if (total_elements % 2 != 0) {
        f16_to_f32_tail_kernel<<<1, 1, 0, stream>>>(src, dst, total_elements - 1);
    }
}

void cast_float_to_half_host(const float* __restrict__ src,
                             half* __restrict__ dst, size_t n) {
    if (!src || !dst || n == 0) return;
#if defined(__F16C__) && defined(__AVX__)
    // F16C does IEEE round-to-nearest-even — bit-identical to the device-side
    // raft::copy cast (mdspan_copy_kernel<__half>), so recall is preserved.
    size_t i = 0;
    for (; i + 8 <= n; i += 8) {
        __m256 v  = _mm256_loadu_ps(src + i);
        __m128i h = _mm256_cvtps_ph(v, _MM_FROUND_TO_NEAREST_INT | _MM_FROUND_NO_EXC);
        _mm_storeu_si128(reinterpret_cast<__m128i*>(dst + i), h);
    }
    // Tail (≤ 7 elements): scalar __float2half_rn — same IEEE
    // round-to-nearest-even rounding mode as the F16C SIMD path above.
    for (; i < n; ++i) {
        dst[i] = __float2half_rn(src[i]);
    }
#else
    for (size_t i = 0; i < n; ++i) {
        dst[i] = __float2half_rn(src[i]);
    }
#endif
}

int64_t rows_fitting_gpu_mem(size_t per_row_bytes, const char* who, size_t* out_free_bytes) {
    if (per_row_bytes == 0) {
        throw std::runtime_error(std::string(who) + ": per-row size is 0");
    }
    size_t free_bytes = 0, total_bytes = 0;
    cudaError_t err = cudaMemGetInfo(&free_bytes, &total_bytes);
    if (err != cudaSuccess) {
        throw std::runtime_error(std::string(who) + ": cudaMemGetInfo failed: " +
                                 cudaGetErrorString(err));
    }
    if (out_free_bytes) *out_free_bytes = free_bytes;

    int64_t max_rows = static_cast<int64_t>((free_bytes / 10 * 6) / per_row_bytes);
    return max_rows < 1 ? 1 : max_rows;
}

int64_t cap_rows_to_gpu_mem(int64_t requested_rows, size_t per_row_bytes, const char* who) {
    if (requested_rows < 1) requested_rows = 1;
    size_t free_bytes = 0;
    int64_t max_rows = rows_fitting_gpu_mem(per_row_bytes, who, &free_bytes);
    if (requested_rows > max_rows) {
        std::cerr << "[" << who << "] capped " << requested_rows << " -> " << max_rows
                  << " rows to fit 60% of " << (free_bytes >> 20)
                  << " MB free GPU mem (per_row=" << per_row_bytes << "B)" << std::endl;
        return max_rows;
    }
    return requested_rows;
}

} // namespace matrixone

extern "C" {

int gpu_get_device_count() {
    try {
        int count = 0;
        cudaGetDeviceCount(&count);
        return count;
    } catch (...) {
        return 0;
    }
}

int gpu_get_next_device_id() {
    try {
        return matrixone::get_next_device_id();
    } catch (...) {
        return 0;
    }
}

void gpu_get_device_list(int* devices, int count) {
    try {
        for (int i = 0; i < count; ++i) {
            devices[i] = i;
        }
    } catch (...) {
        matrixone::log_err("gpu_get_device_list: unknown C++ exception (swallowed)");
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
        RAFT_CUDA_TRY(cudaFree(d_src));
        RAFT_CUDA_TRY(cudaFree(d_dst));

    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_convert_f32_to_f16", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_convert_f32_to_f16", "unknown C++ exception");
    }
}

int gpu_rows_fitting_free_mem(int device_id, uint64_t per_row_bytes,
                              int64_t* out_rows, uint64_t* out_free_bytes, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        // cudaMemGetInfo reads the CURRENT device; bind the requested one first.
        RAFT_CUDA_TRY(cudaSetDevice(device_id));
        size_t free_bytes = 0;
        int64_t rows = matrixone::rows_fitting_gpu_mem(
            static_cast<size_t>(per_row_bytes), "index capacity", &free_bytes);
        if (out_rows) *out_rows = rows;
        if (out_free_bytes) *out_free_bytes = static_cast<uint64_t>(free_bytes);
        return 0;
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_rows_fitting_free_mem", e.what());
        return -1;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_rows_fitting_free_mem", "unknown C++ exception");
        return -1;
    }
}

void* gpu_device_memory_reserve(int device_id, uint64_t bytes, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto claim = matrixone::device_memory_governor::reserve_on(
            device_id, static_cast<size_t>(bytes), "build");
        // Heap the RAII claim so its lifetime can be owned by the Go caller.
        return new matrixone::device_memory_governor::reservation(std::move(claim));
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_device_memory_reserve", e.what());
        return nullptr;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_device_memory_reserve", "unknown C++ exception");
        return nullptr;
    }
}

void gpu_device_memory_release(void* token) {
    // NULL-safe so a Go defer can fire unconditionally after a failed reserve.
    if (!token) return;
    delete static_cast<matrixone::device_memory_governor::reservation*>(token);
}

uint64_t gpu_device_memory_reserved(int device_id) {
    return static_cast<uint64_t>(matrixone::device_memory_governor::reserved_bytes(device_id));
}

void* gpu_alloc_pinned(uint64_t size, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        void* ptr = nullptr;
        // Use cudaHostAllocMapped to allow direct device access if needed later
        RAFT_CUDA_TRY(cudaHostAlloc(&ptr, size, cudaHostAllocMapped));
        return ptr;
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_alloc_pinned", e.what());
        return nullptr;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_alloc_pinned", "unknown C++ exception");
        return nullptr;
    }
}

void gpu_free_pinned(void* ptr, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        if (ptr) {
            RAFT_CUDA_TRY(cudaFreeHost(ptr));
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_free_pinned", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_free_pinned", "unknown C++ exception");
    }
}

}
