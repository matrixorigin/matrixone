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

#include "../cuvs_worker.hpp"
#include "../cagra.hpp"
#include "test_framework.hpp"
#include <cstdio>
#include <cstdlib>
#include <future>

using namespace matrixone;

// Forward declaration from cagra_test.cu
void reproduce_sharded_cagra();

thread_local bool current_test_failed = false;

// Helper to get available GPU devices
std::vector<int> get_available_devices() {
    int device_count = 0;
    cudaError_t error = cudaGetDeviceCount(&device_count);
    if (error != cudaSuccess || device_count == 0) {
        return {0}; // Fallback to device 0
    }
    std::vector<int> devices;
    for (int i = 0; i < device_count; ++i) {
        devices.push_back(i);
    }
    return devices;
}

// --- cuvs_worker_t Tests ---

TEST(CuvsWorkerTest, BasicLifecycle) {
    auto devices = get_available_devices();
    uint32_t n_threads = 1;
    cuvs_worker_t worker(n_threads, devices);
    worker.start();
    worker.stop();
}

TEST(CuvsWorkerTest, SubmitTask) {
    auto devices = get_available_devices();
    uint32_t n_threads = 1;
    cuvs_worker_t worker(n_threads, devices);
    worker.start();

    auto task = [](raft_handle_wrapper_t&) -> std::any {
        return std::string("success");
    };

    uint64_t job_id = worker.submit(task);
    auto result = worker.wait(job_id).get();

    ASSERT_EQ(std::any_cast<std::string>(result.result), std::string("success"));

    worker.stop();
}

TEST(CuvsWorkerTest, MultipleThreads) {
    auto devices = get_available_devices();
    uint32_t n_threads = 4;
    cuvs_worker_t worker(n_threads, devices);
    worker.start();

    std::vector<uint64_t> ids;
    for (int i = 0; i < 10; ++i) {
        ids.push_back(worker.submit([i](raft_handle_wrapper_t&) -> std::any {
            return i * 2;
        }));
    }

    for (int i = 0; i < 10; ++i) {
        auto res = worker.wait(ids[i]).get();
        ASSERT_EQ(std::any_cast<int>(res.result), i * 2);
    }

    worker.stop();
}

TEST(CuvsWorkerTest, TaskErrorHandling) {
    auto devices = get_available_devices();
    uint32_t n_threads = 1;
    cuvs_worker_t worker(n_threads, devices);
    worker.start();

    auto fail_task = [](raft_handle_wrapper_t&) -> std::any {
        throw std::runtime_error("task failed intentionally");
    };

    uint64_t job_id = worker.submit(fail_task);
    auto result = worker.wait(job_id).get();

    ASSERT_TRUE(result.error != nullptr);
    ASSERT_TRUE(has_exception<std::runtime_error>(result.error));

    worker.stop();
}

TEST(CuvsWorkerTest, SubmitMain) {
    auto devices = get_available_devices();
    uint32_t n_threads = 2;
    cuvs_worker_t worker(n_threads, devices);
    worker.start();

    auto task = [](raft_handle_wrapper_t& handle) -> std::any {
        return 42;
    };

    std::vector<uint64_t> ids;
    for(int i=0; i<10; ++i) {
        ids.push_back(worker.submit_main(task));
    }

    for(auto id : ids) {
        auto res = worker.wait(id).get();
        ASSERT_TRUE(res.error == nullptr);
        ASSERT_EQ(std::any_cast<int>(res.result), 42);
    }

    worker.stop();
}

TEST(CuvsWorkerTest, WorkerBatching) {
    auto devices = get_available_devices();
    uint32_t n_workers = 1;
    cuvs_worker_t worker(n_workers, devices);
    worker.set_use_batching(true);
    worker.start();

    auto exec_fn = [](raft_handle_wrapper_t& handle, const std::vector<std::any>& reqs, const std::vector<std::function<void(std::any)>>& setters) {
        for (size_t i = 0; i < reqs.size(); ++i) {
            int val = std::any_cast<int>(reqs[i]);
            setters[i](val * 2);
        }
    };

    std::vector<std::future<int>> futures;
    for (int i = 0; i < 5; ++i) {
        futures.push_back(worker.submit_batched<int>("test_key", i, exec_fn));
    }

    for (int i = 0; i < 5; ++i) {
        ASSERT_EQ(futures[i].get(), i * 2);
    }

    worker.stop();
}

int main() {
    printf("[INFO    ] Starting Reproduction Case...\n");
    reproduce_sharded_cagra();
    printf("[INFO    ] Reproduction Case Finished. Running other tests...\n");
    return RUN_ALL_TESTS();
}
