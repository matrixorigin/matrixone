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

#include "cuvs_worker.hpp"
#include "cagra.hpp"
#include "helper.h"
#include "test_framework.hpp"
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <future>

using namespace matrixone;

TEST(GpuCagraTest, BasicLoadAndSearch) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    int dev_count = gpu_get_device_count();
    ASSERT_TRUE(dev_count > 0);
    std::vector<int> devices(1);
    gpu_get_device_list(devices.data(), 1);
    
    cagra_build_params_t bp = cagra_build_params_default();
    // Use smaller degrees for small test dataset to speed up build
    bp.intermediate_graph_degree = 32;
    bp.graph_degree = 16;

    gpu_cagra_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    
    auto start_time = std::chrono::steady_clock::now();
    index.start();
    index.build();
    auto end_time = std::chrono::steady_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    TEST_LOG("CAGRA Build took " << duration << " ms");

    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    cagra_search_params_t sp = cagra_search_params_default();
    
    start_time = std::chrono::steady_clock::now();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);
    end_time = std::chrono::steady_clock::now();
    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time).count();
    TEST_LOG("CAGRA Search took " << duration << " ms");

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0u);

    index.destroy();
}

TEST(GpuCagraTest, SaveAndLoadFromFile) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    std::string filename = "test_cagra.bin";
    int dev_count = gpu_get_device_count();
    ASSERT_TRUE(dev_count > 0);
    std::vector<int> devices(1);
    gpu_get_device_list(devices.data(), 1);

    // 1. Build and Save
    {
        cagra_build_params_t bp = cagra_build_params_default();
        bp.intermediate_graph_degree = 32;
        bp.graph_degree = 16;
        gpu_cagra_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.start();
        index.build();
        index.save(filename);
        index.destroy();
    }

    // 2. Load and Search
    {
        cagra_build_params_t bp = cagra_build_params_default();
        bp.intermediate_graph_degree = 32;
        bp.graph_degree = 16;
        gpu_cagra_t<float> index(filename, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.start();
        index.build();
        
        std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
        cagra_search_params_t sp = cagra_search_params_default();
        auto result = index.search(queries.data(), 1, dimension, 5, sp);
        
        ASSERT_EQ(result.neighbors.size(), (size_t)5);
        ASSERT_EQ(result.neighbors[0], 0u);

        index.destroy();
    }

    std::remove(filename.c_str());
}

TEST(GpuCagraTest, ShardedModeSimulation) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    int dev_count = gpu_get_device_count();
    ASSERT_TRUE(dev_count > 0);
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    cagra_build_params_t bp = cagra_build_params_default();
    bp.intermediate_graph_degree = 32;
    bp.graph_degree = 16;
    gpu_cagra_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_SHARDED);
    index.start();
    index.build();
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    cagra_search_params_t sp = cagra_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0u);

    index.destroy();
}

TEST(GpuCagraTest, ReplicatedModeSimulation) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    int dev_count = gpu_get_device_count();
    ASSERT_TRUE(dev_count > 0);
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    cagra_build_params_t bp = cagra_build_params_default();
    bp.intermediate_graph_degree = 32;
    bp.graph_degree = 16;
    gpu_cagra_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_REPLICATED);
    index.start();
    index.build();
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    cagra_search_params_t sp = cagra_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0u);

    index.destroy();
}

TEST(GpuCagraTest, ConcurrentShardedSearch) {
    const uint32_t dimension = 64;
    const uint64_t count = 5000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    int dev_count = gpu_get_device_count();
    if (dev_count < 2) {
        TEST_LOG("Skipping ConcurrentShardedSearch: need at least 2 GPUs");
        return;
    }
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    cagra_build_params_t bp = cagra_build_params_default();
    bp.intermediate_graph_degree = 32;
    bp.graph_degree = 16;
    gpu_cagra_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 8, DistributionMode_SHARDED);
    index.set_use_batching(false); // Force serialized/parallel path
    index.start();
    index.build();

    const int num_threads = 8;
    const int num_queries_per_thread = 20;
    std::vector<std::future<void>> futures;
    
    for (int i = 0; i < num_threads; ++i) {
        futures.push_back(std::async(std::launch::async, [&index, dimension, num_queries_per_thread]() {
            std::vector<float> query(dimension);
            cagra_search_params_t sp = cagra_search_params_default();
            for (int q = 0; q < num_queries_per_thread; ++q) {
                for (uint32_t j = 0; j < dimension; ++j) query[j] = (float)rand() / RAND_MAX;
                auto result = index.search(query.data(), 1, dimension, 5, sp);
                ASSERT_EQ(result.neighbors.size(), (size_t)5);
            }
        }));
    }

    for (auto& f : futures) f.get();

    index.destroy();
}

void reproduce_sharded_cagra() {
    const uint32_t dimension = 1024;
    const uint64_t count = 100000;
    
    printf("[INFO    ] Generating %lu vectors of dimension %u...\n", count, dimension);
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    int dev_count = gpu_get_device_count();
    if (dev_count < 1) {
        printf("[INFO    ] Skipping reproduction: need at least 1 GPU\n");
        return;
    }
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    cagra_build_params_t bp = cagra_build_params_default();
    bp.intermediate_graph_degree = 256;
    bp.graph_degree = 128;
    
    printf("[INFO    ] Building sharded CAGRA index...\n");
    gpu_cagra_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 8, DistributionMode_SHARDED);
    index.set_use_batching(false); // Reproduce Batchingfalse path
    index.start();
    index.build();

    const int num_threads = 16;
    const int num_queries_per_thread = 50;
    printf("[INFO    ] Starting concurrent search with %d threads...\n", num_threads);
    
    std::vector<std::future<void>> futures;
    for (int i = 0; i < num_threads; ++i) {
        futures.push_back(std::async(std::launch::async, [&index, dimension, num_queries_per_thread]() {
            std::vector<float> query(dimension);
            cagra_search_params_t sp = cagra_search_params_default();
            sp.itopk_size = 128;
            sp.search_width = 3;
            
            for (int q = 0; q < num_queries_per_thread; ++q) {
                for (uint32_t j = 0; j < dimension; ++j) query[j] = (float)rand() / RAND_MAX;
                auto result = index.search(query.data(), 1, dimension, 10, sp);
                if (result.neighbors.size() != 10) {
                    printf("[ERROR   ] Search failed: got %zu neighbors\n", result.neighbors.size());
                }
            }
        }));
    }

    for (auto& f : futures) f.get();
    printf("[INFO    ] Concurrent search finished successfully.\n");

    index.destroy();
}

TEST(GpuCagraTest, ReproduceBenchmarkGpuShardedCagra) {
    reproduce_sharded_cagra();
}
