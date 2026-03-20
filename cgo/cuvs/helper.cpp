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
#include <fstream>
#include <cstring>
#include <thread>

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

    ncclUniqueId id;
    ncclGetUniqueId(&id);

    std::vector<std::thread> inits;
    std::vector<ncclComm_t> comms(world_size);

    for (int i = 0; i < world_size; ++i) {
        inits.emplace_back([&, i, world_size, id]() {
            cudaSetDevice(devices[i]);
            ncclCommInitRank(&comms[i], world_size, id, i);
            
            raft::resources& rank_res = const_cast<raft::resources&>(
                raft::resource::get_device_resources_for_rank(mg_res, i));
            
            raft::comms::build_comms_nccl_only(&rank_res, comms[i], world_size, i);
        });
    }

    for (auto& t : inits) t.join();
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
    static raft::resources res;
    return res;
}

} // namespace matrixone

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

}
