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
#include <raft/core/resource/comms.hpp>
#include <raft/core/resource/nccl_comm.hpp>
#include <raft/core/resource/multi_gpu.hpp>
#include <raft/comms/std_comms.hpp>
#include <cuda_runtime.h>
#include <cuda_fp16.h>
#include <fstream>
#include <cstring>
#include <thread>
#include <raft/util/cudart_utils.hpp>

namespace matrixone {

bool is_snmg_handle(const raft::resources& res) {
    if (raft::resource::comms_initialized(res)) {
        return raft::resource::get_comms(res).get_size() > 1;
    }
    return false;
}

void init_mg_comms(raft::resources& mg_res, const std::vector<int>& devices) {
    int world_size = static_cast<int>(devices.size());
    if (world_size <= 1) return;

    std::vector<ncclComm_t> comms(world_size);

    // ncclCommInitAll is the most robust way to initialize multiple GPUs 
    // from a single thread in a single process.
    ncclResult_t res = ncclCommInitAll(comms.data(), world_size, devices.data());
    if (res != ncclSuccess) {
        throw std::runtime_error("ncclCommInitAll failed with error code " + std::to_string(res));
    }

    for (int i = 0; i < world_size; ++i) {
        raft::resources& rank_res = const_cast<raft::resources&>(
            raft::resource::get_device_resources_for_rank(mg_res, i));
        
        raft::comms::build_comms_nccl_only(&rank_res, comms[i], world_size, i);
    }
}

void inject_nccl_comm(raft::resources* res, void* nccl_comm, int size, int rank) {
    ncclComm_t comm = static_cast<ncclComm_t>(nccl_comm);
    raft::comms::build_comms_nccl_only(res, comm, size, rank);
}

void save_host_matrix(const std::string& filename, raft::host_matrix_view<const float, int64_t, raft::row_major> view) {
    std::ofstream out(filename, std::ios::binary);
    if (!out) throw std::runtime_error("Failed to open file for writing: " + filename);
    
    int64_t rows = view.extent(0);
    int64_t cols = view.extent(1);
    out.write(reinterpret_cast<const char*>(&rows), sizeof(rows));
    out.write(reinterpret_cast<const char*>(&cols), sizeof(cols));
    out.write(reinterpret_cast<const char*>(view.data_handle()), rows * cols * sizeof(float));
}

void set_errmsg(void* errmsg, const char* context, const char* message) {
    if (!errmsg) return;
    char** err_ptr_ptr = static_cast<char**>(errmsg);
    std::string full_msg = std::string(context) + ": " + message;
    *err_ptr_ptr = strdup(full_msg.c_str());
}

const raft::resources& get_raft_resources() {
    thread_local raft::resources res;
    return res;
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

extern "C" {

int gpu_get_device_count() {
    int count = 0;
    cudaGetDeviceCount(&count);
    return count;
}

void gpu_get_device_list(int* devices, int count) {
    for (int i = 0; i < count; ++i) {
        devices[i] = i;
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
        matrixone::set_errmsg(errmsg, "Error in gpu_convert_f32_to_f16", e.what());
    }
}

}
