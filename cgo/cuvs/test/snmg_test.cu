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

/*
#include "test_framework.hpp"
#include "helper.h"
#include <raft/core/device_resources_snmg.hpp>
#include <raft/core/resource/comms.hpp>
#include <raft/core/resources.hpp>
#include <cuda_runtime.h>
#include <nccl.h>
#include <thread>
#include <vector>
#include <atomic>

using namespace matrixone;

// Sharded mode is currently disabled due to a suspected bug in cuVS or its integration.
// GDB trace showed mdspan extents being set to 18446744073709551615ul (SIZE_MAX),
// which suggests a dynamic extent initialization failure or dimension overflow/underflow
// within the multi-GPU search path.

TEST(SnmgInitTest, BasicClique) {
    int dev_count = 0;
    cudaGetDeviceCount(&dev_count);
    if (dev_count < 2) {
        TEST_LOG("Skipping SnmgInitTest::BasicClique (less than 2 GPUs)");
        return;
    }

    int world_size = std::min(dev_count, 4);
    std::vector<int> devices;
    for (int i = 0; i < world_size; ++i) devices.push_back(i);

    ncclUniqueId id;
    ncclGetUniqueId(&id);

    std::atomic<int> success_count{0};
    std::vector<std::thread> threads;

    for (int i = 0; i < world_size; ++i) {
        threads.emplace_back([&, i, world_size, id]() {
            try {
                cudaSetDevice(devices[i]);
                raft::resources res;
                
                ncclComm_t nccl_handle;
                ncclResult_t res_nccl = ncclCommInitRank(&nccl_handle, world_size, id, i);
                if (res_nccl != ncclSuccess) return;

                // Use the wrapper to avoid multiple definitions
                inject_nccl_comm(&res, static_cast<void*>(nccl_handle), world_size, i);

                if (raft::resource::comms_initialized(res)) {
                    auto& comm = raft::resource::get_comms(res);
                    if (comm.get_size() == world_size && comm.get_rank() == i) {
                        comm.barrier();
                        success_count++;
                    }
                }
                ncclCommDestroy(nccl_handle);
            } catch (...) {}
        });
    }

    for (auto& t : threads) t.join();

    ASSERT_EQ(success_count.load(), world_size);
}

TEST(SnmgInitTest, HelperInitialization) {
    int dev_count = 0;
    cudaGetDeviceCount(&dev_count);
    if (dev_count < 2) {
        TEST_LOG("Skipping SnmgInitTest::HelperInitialization (less than 2 GPUs)");
        return;
    }

    int world_size = std::min(dev_count, 4);
    std::vector<int> devices;
    for (int i = 0; i < world_size; ++i) devices.push_back(i);

    auto mg_res = std::make_shared<raft::device_resources_snmg>(devices);
    
    // Test the helper function
    ASSERT_NO_THROW(init_mg_comms(*mg_res, devices));

    // Verify each rank
    for (int i = 0; i < world_size; ++i) {
        auto& rank_res = raft::resource::get_device_resources_for_rank(*mg_res, i);
        ASSERT_TRUE(raft::resource::comms_initialized(rank_res));
        auto& comm = raft::resource::get_comms(rank_res);
        ASSERT_EQ(comm.get_size(), world_size);
        ASSERT_EQ(comm.get_rank(), i);
    }
}
*/
