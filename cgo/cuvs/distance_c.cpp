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

#include "distance_c.h"
#include "distance.hpp"
#include <cuda_runtime.h>
#include <cuda_fp16.h>
#include <unordered_map>
#include <mutex>
#include <atomic>

namespace matrixone {

struct gpu_job_t {
    float* host_dist;
    int64_t n_x;
    int64_t n_y;
    const raft::resources* res;
    void* d_ptr;
};

class gpu_job_mgr_t {
public:
    static gpu_job_mgr_t& get() {
        static gpu_job_mgr_t instance;
        return instance;
    }

    uint64_t add_job(gpu_job_t job) {
        uint64_t id = next_id_++;
        std::lock_guard<std::mutex> lock(mu_);
        jobs_[id] = std::move(job);
        return id;
    }

    gpu_job_t get_job(uint64_t id) {
        std::lock_guard<std::mutex> lock(mu_);
        auto it = jobs_.find(id);
        if (it == jobs_.end()) throw std::runtime_error("Invalid job ID");
        gpu_job_t job = std::move(it->second);
        jobs_.erase(it);
        return job;
    }

private:
    std::atomic<uint64_t> next_id_{1};
    std::unordered_map<uint64_t, gpu_job_t> jobs_;
    std::mutex mu_;
};

} // namespace matrixone

extern "C" {

void gpu_pairwise_distance(const void* x,
                           uint64_t n_x,
                           const void* y,
                           uint64_t n_y,
                           uint32_t dim,
                           distance_type_t metric,
                           quantization_t qtype,
                           int device_id,
                           float* dist,
                           void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        if (!x || !y || !dist || n_x == 0 || n_y == 0 || dim == 0) return;

        static thread_local int current_device = -1;
        if (current_device != device_id) {
            RAFT_CUDA_TRY(cudaSetDevice(device_id));
            current_device = device_id;
        }
        const raft::resources& res = matrixone::get_raft_resources();

        if (qtype == Quantization_F32) {
            matrixone::pairwise_distance<float>(res, static_cast<const float*>(x), n_x, static_cast<const float*>(y), n_y, dim, metric, dist);
        } else if (qtype == Quantization_F16) {
            matrixone::pairwise_distance<half>(res, static_cast<const half*>(x), n_x, static_cast<const half*>(y), n_y, dim, metric, dist);
        } else {
            throw std::runtime_error("Unsupported quantization type for pairwise_distance");
        }

    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_pairwise_distance", e.what());
    }
}

uint64_t gpu_pairwise_distance_launch(const void* x,
                                     uint64_t n_x,
                                     const void* y,
                                     uint64_t n_y,
                                     uint32_t dim,
                                     distance_type_t metric,
                                     quantization_t qtype,
                                     int device_id,
                                     float* dist,
                                     void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        if (!x || !y || !dist || n_x == 0 || n_y == 0 || dim == 0) return 0;

        static thread_local int current_device = -1;
        if (current_device != device_id) {
            RAFT_CUDA_TRY(cudaSetDevice(device_id));
            current_device = device_id;
        }
        const raft::resources& res = matrixone::get_raft_resources();

        // 1. Setup job state
        matrixone::gpu_job_t job;
        job.host_dist = dist;
        job.n_x = (int64_t)n_x;
        job.n_y = (int64_t)n_y;
        job.res = &res;
        job.d_ptr = nullptr;

        // 2. Launch kernels asynchronously
        if (qtype == Quantization_F32) {
            job.d_ptr = matrixone::pairwise_distance_async<float>(res, static_cast<const float*>(x), n_x, static_cast<const float*>(y), n_y, dim, metric, dist);
        } else if (qtype == Quantization_F16) {
            job.d_ptr = matrixone::pairwise_distance_async<half>(res, static_cast<const half*>(x), n_x, static_cast<const half*>(y), n_y, dim, metric, dist);
        } else {
            throw std::runtime_error("Unsupported quantization type for pairwise_distance");
        }

        return matrixone::gpu_job_mgr_t::get().add_job(std::move(job));

    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_pairwise_distance_launch", e.what());
        return 0;
    }
}

void gpu_pairwise_distance_wait(uint64_t job_id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        if (job_id == 0) return;
        auto job = matrixone::gpu_job_mgr_t::get().get_job(job_id);
        
        // 1. Synchronize the stream to ensure copies are finished
        raft::resource::sync_stream(*(job.res));

        // 2. Free device buffers
        if (job.d_ptr) {
            auto stream = raft::resource::get_cuda_stream(*(job.res));
            RAFT_CUDA_TRY(cudaFreeAsync(job.d_ptr, stream));
            // We should sync again if we want to be sure it's freed before returning, 
            // but cudaFreeAsync is tied to the stream, so it's fine for subsequent jobs on same stream.
            // For safety and to match expected "wait" behavior (all done), let's sync.
            raft::resource::sync_stream(*(job.res));
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_pairwise_distance_wait", e.what());
    }
}

} // extern "C"
