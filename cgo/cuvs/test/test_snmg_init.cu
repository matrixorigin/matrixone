/* 
 * Standalone SNMG (Single-Node Multi-GPU) Initialization Test
 */

#include <raft/core/device_resources_snmg.hpp>
#include <raft/core/resource/comms.hpp>
#include <raft/core/resource/nccl_comm.hpp>
#include <raft/core/resources.hpp>
#include <raft/comms/std_comms.hpp>
#include <cuda_runtime.h>
#include <iostream>
#include <vector>
#include <thread>
#include <nccl.h>

void worker_thread(int rank, int world_size, const std::vector<int>& devices, ncclUniqueId id) {
    try {
        int dev = devices[rank];
        cudaSetDevice(dev);
        std::cout << "[Rank " << rank << "] Using device " << dev << std::endl;

        // 1. Create a basic raft::resources for this rank
        raft::resources res;

        // 2. Initialize NCCL communicator for this rank
        ncclComm_t nccl_handle;
        ncclResult_t res_nccl = ncclCommInitRank(&nccl_handle, world_size, id, rank);
        if (res_nccl != ncclSuccess) {
            std::cerr << "[Rank " << rank << "] ncclCommInitRank failed" << std::endl;
            return;
        }

        // 3. Inject into RAFT
        raft::comms::build_comms_nccl_only(&res, nccl_handle, world_size, rank);

        // 4. Verify
        if (raft::resource::comms_initialized(res)) {
            auto& comm = raft::resource::get_comms(res);
            std::cout << "[Rank " << rank << "] Comms initialized! Size: " 
                      << comm.get_size() << ", Rank: " << comm.get_rank() << std::endl;
            
            // 5. Test Barrier
            comm.barrier();
            std::cout << "[Rank " << rank << "] Barrier passed!" << std::endl;
        } else {
            std::cout << "[Rank " << rank << "] Comms NOT initialized after injection." << std::endl;
        }

        ncclCommDestroy(nccl_handle);
    } catch (const std::exception& e) {
        std::cerr << "[Rank " << rank << "] Exception: " << e.what() << std::endl;
    }
}

int main() {
    int dev_count = 0;
    cudaGetDeviceCount(&dev_count);
    if (dev_count < 2) {
        std::cout << "Need at least 2 GPUs for this test. Found: " << dev_count << std::endl;
        return 0;
    }

    int world_size = dev_count > 4 ? 4 : dev_count;
    std::vector<int> devices;
    for (int i = 0; i < world_size; ++i) devices.push_back(i);

    std::cout << "Starting SNMG test with " << world_size << " GPUs..." << std::endl;

    ncclUniqueId id;
    ncclGetUniqueId(&id);

    std::vector<std::thread> threads;
    for (int i = 0; i < world_size; ++i) {
        threads.emplace_back(worker_thread, i, world_size, devices, id);
    }

    for (auto& t : threads) t.join();

    std::cout << "SNMG test completed." << std::endl;
    return 0;
}
