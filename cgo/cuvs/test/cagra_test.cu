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

using namespace matrixone;

TEST(GpuCagraTest, BasicLoadAndSearch) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    std::vector<int> devices = {0};
    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    cagra_search_params_t sp = cagra_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0u);

    index.destroy();
}

TEST(GpuCagraTest, BasicLoadAndSearchWithIds) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    std::vector<uint32_t> ids(count);
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j) dataset[i * dimension + j] = (float)rand() / RAND_MAX;
        ids[i] = (uint32_t)(i + 1000); // Offset IDs
    }
    
    std::vector<int> devices = {0};
    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU, ids.data());
    index.start();
    index.build();

    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    cagra_search_params_t sp = cagra_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 1000u); // Should return the provided ID

    index.destroy();
}

TEST(GpuCagraTest, ParallelAddChunkWithOffset) {
    const uint32_t dimension = 16;
    const uint64_t count_per_chunk = 500;
    const uint64_t total_count = count_per_chunk * 2;
    std::vector<float> chunk1(count_per_chunk * dimension);
    std::vector<float> chunk2(count_per_chunk * dimension);
    std::vector<uint32_t> ids1(count_per_chunk);
    std::vector<uint32_t> ids2(count_per_chunk);

    for (size_t i = 0; i < count_per_chunk; ++i) {
        for (size_t j = 0; j < dimension; ++j) {
            chunk1[i * dimension + j] = (float)rand() / RAND_MAX;
            chunk2[i * dimension + j] = (float)rand() / RAND_MAX;
        }
        ids1[i] = (uint32_t)i;
        ids2[i] = (uint32_t)(i + count_per_chunk);
    }

    std::vector<int> devices = {0};
    cagra_build_params_t bp = cagra_build_params_default();
    // Pre-allocate with total_count
    gpu_cagra_t<float> index(total_count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();

    // Add chunks in parallel threads
    std::thread t1([&]() { index.add_chunk(chunk1.data(), count_per_chunk, 0, ids1.data()); });
    std::thread t2([&]() { index.add_chunk(chunk2.data(), count_per_chunk, count_per_chunk, ids2.data()); });
    t1.join();
    t2.join();

    index.build();

    // Query for a vector from the second chunk
    std::vector<float> queries(chunk2.begin(), chunk2.begin() + dimension);
    cagra_search_params_t sp = cagra_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors[0], (uint32_t)count_per_chunk);

    index.destroy();
}

TEST(GpuCagraTest, SaveAndLoadFromFile) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    std::vector<uint32_t> ids(count);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    for (size_t i = 0; i < count; ++i) ids[i] = (uint32_t)(i + 5000);

    std::string filename = "test_cagra.bin";
    std::vector<int> devices = {0};

    // 1. Build and Save
    {
        cagra_build_params_t bp = cagra_build_params_default();
        gpu_cagra_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU, ids.data());
        index.start();
        index.build();
        index.save(filename);
        index.destroy();
    }

    // 2. Load and Search
    {
        cagra_build_params_t bp = cagra_build_params_default();
        gpu_cagra_t<float> index(filename, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.start();
        index.load(filename);
        
        std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
        cagra_search_params_t sp = cagra_search_params_default();
        auto result = index.search(queries.data(), 1, dimension, 5, sp);
        
        ASSERT_EQ(result.neighbors.size(), (size_t)5);
        ASSERT_EQ(result.neighbors[0], 5000u);

        index.destroy();
    }

    std::remove(filename.c_str());
    std::remove((filename + ".ids").c_str());
}

TEST(GpuCagraTest, ReplicatedModeSimulation) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    int dev_count = gpu_get_device_count();
    ASSERT_TRUE(dev_count > 0);
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_REPLICATED);
    index.start();
    index.build();
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    cagra_search_params_t sp = cagra_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0u);

    index.destroy();
}

TEST(GpuCagraTest, ManualShardedSearch) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    int dev_count = gpu_get_device_count();
    if (dev_count < 2) {
        TEST_LOG("Skipping ManualShardedSearch: Need at least 2 GPUs");
        return;
    }
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED);
    index.start();
    index.build();
    
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    cagra_search_params_t sp = cagra_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0u);

    index.destroy();
}

TEST(GpuCagraTest, ManualShardedSearchWithIds) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    std::vector<uint32_t> ids(count);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    for (size_t i = 0; i < count; ++i) ids[i] = (uint32_t)(i + 20000);
    
    int dev_count = gpu_get_device_count();
    if (dev_count < 2) {
        TEST_LOG("Skipping ManualShardedSearchWithIds: Need at least 2 GPUs");
        return;
    }
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED, ids.data());
    index.start();
    index.build();
    
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    cagra_search_params_t sp = cagra_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 20000u);

    index.destroy();
}

TEST(GpuCagraTest, SoftDeleteSearch) {
    const uint32_t dimension = 3;
    const uint64_t count = 3;
    std::vector<float> dataset = {
        1.0, 2.0, 3.0, // ID 0
        4.0, 5.0, 6.0, // ID 1
        7.0, 8.0, 9.0  // ID 2
    };
    
    std::vector<int> devices = {0};
    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    // 1. Initial search: point 1 should be the second closest to point 0
    std::vector<float> queries = {1.0, 2.0, 3.0};
    cagra_search_params_t sp = cagra_search_params_default();
    auto result1 = index.search(queries.data(), 1, dimension, 2, sp);
    ASSERT_EQ(result1.neighbors[0], 0u);
    ASSERT_EQ(result1.neighbors[1], 1u);

    // 2. Delete point 1 and search again: point 1 should be gone, point 2 should be the second neighbor
    index.delete_id(1);
    auto result2 = index.search(queries.data(), 1, dimension, 2, sp);
    ASSERT_EQ(result2.neighbors[0], 0u);
    ASSERT_EQ(result2.neighbors[1], 2u);

    // 3. Delete point 0 and search: point 0 should be gone, point 2 should be first
    index.delete_id(0);
    auto result3 = index.search(queries.data(), 1, dimension, 1, sp);
    ASSERT_EQ(result3.neighbors[0], 2u);

    index.destroy();
}

TEST(GpuCagraTest, SoftDeleteWithCustomIds) {
    const uint32_t dimension = 2;
    const uint64_t count = 3;
    std::vector<float> dataset = {10, 10, 20, 20, 30, 30};
    std::vector<uint32_t> ids = {100, 200, 300};
    
    std::vector<int> devices = {0};
    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU, ids.data());
    index.start();
    index.build();

    std::vector<float> query = {20, 20};
    cagra_search_params_t sp = cagra_search_params_default();
    auto res1 = index.search(query.data(), 1, dimension, 1, sp);
    ASSERT_EQ(res1.neighbors[0], 200u);

    // Delete by custom ID
    index.delete_id(200);
    auto res2 = index.search(query.data(), 1, dimension, 1, sp);
    // Should now return the next closest point (100 or 300)
    ASSERT_TRUE(res2.neighbors[0] == 100u || res2.neighbors[0] == 300u);
    ASSERT_NE(res2.neighbors[0], 200u);

    index.destroy();
}

TEST(GpuCagraTest, ExtendReplicatedWithHostIds) {
    const uint32_t dimension = 16;
    const uint64_t n_base = 100;
    const uint64_t n_ext  = 10;

    int dev_count = gpu_get_device_count();
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    std::vector<float> dataset(n_base * dimension);
    std::vector<uint32_t> base_ids(n_base);
    for (uint64_t i = 0; i < n_base; ++i) {
        for (uint32_t j = 0; j < dimension; ++j)
            dataset[i * dimension + j] = (float)rand() / RAND_MAX;
        base_ids[i] = (uint32_t)(1000 + i);
    }

    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), n_base, dimension,
                             DistanceType_L2Expanded, bp, devices, 1,
                             DistributionMode_REPLICATED, base_ids.data());
    index.start();
    index.build();

    std::vector<float> ext(n_ext * dimension, 1000.0f);
    std::vector<uint32_t> ext_ids(n_ext);
    for (uint64_t i = 0; i < n_ext; ++i)
        ext_ids[i] = (uint32_t)(2000 + i);

    index.extend(ext.data(), n_ext, ext_ids.data());

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    cagra_search_params_t sp = cagra_search_params_default();

    // Query at base vector 0: expect host ID 1000
    std::vector<float> q0(dataset.begin(), dataset.begin() + dimension);
    auto r0 = index.search(q0.data(), 1, dimension, 1, sp);
    ASSERT_EQ(r0.neighbors[0], 1000u);

    // Query at extended cluster: expect host ID in [2000, 2010)
    std::vector<float> q_ext(dimension, 1000.0f);
    auto r_ext = index.search(q_ext.data(), 1, dimension, 1, sp);
    ASSERT_GE(r_ext.neighbors[0], 2000u);
    ASSERT_TRUE(r_ext.neighbors[0] < 2010u);

    index.destroy();
}

TEST(GpuCagraTest, ExtendShardedThrows) {
    const uint32_t dimension = 16;
    const uint64_t n_base = 100;

    int dev_count = gpu_get_device_count();
    if (dev_count < 2) {
        TEST_LOG("Skipping ExtendShardedThrows: Need at least 2 GPUs");
        return;
    }
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    std::vector<float> dataset(n_base * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;

    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), n_base, dimension,
                             DistanceType_L2Expanded, bp, devices, 1,
                             DistributionMode_SHARDED);
    index.start();
    index.build();

    std::vector<float> ext(10 * dimension, 1.0f);
    ASSERT_THROW(index.extend(ext.data(), 10, nullptr), std::runtime_error);

    index.destroy();
}

TEST(GpuCagraTest, ExtendWithoutHostIds) {
    const uint32_t dimension = 16;
    const uint64_t n_base = 100;
    const uint64_t n_ext  = 10;

    // Base dataset: vector i has all components = (float)rand()/RAND_MAX (random in [0,1])
    std::vector<float> dataset(n_base * dimension);
    for (uint64_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;

    std::vector<int> devices = {0};
    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), n_base, dimension,
                             DistanceType_L2Expanded, bp, devices, 1,
                             DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    // Extended vectors: all components = 1000.0 (far from base [0,1])
    std::vector<float> ext(n_ext * dimension, 1000.0f);
    index.extend(ext.data(), n_ext, nullptr);

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    cagra_search_params_t sp = cagra_search_params_default();

    // Query at base vector 0: expect sequential ID 0
    std::vector<float> q0(dataset.begin(), dataset.begin() + dimension);
    auto r0 = index.search(q0.data(), 1, dimension, 1, sp);
    ASSERT_EQ(r0.neighbors[0], 0u);

    // Query at extended cluster: expect sequential ID in [n_base, n_base+n_ext)
    std::vector<float> q_ext(dimension, 1000.0f);
    auto r_ext = index.search(q_ext.data(), 1, dimension, 1, sp);
    ASSERT_GE(r_ext.neighbors[0], (uint32_t)n_base);
    ASSERT_TRUE(r_ext.neighbors[0] < (uint32_t)(n_base + n_ext));

    index.destroy();
}

TEST(GpuCagraTest, ExtendWithHostIds) {
    const uint32_t dimension = 16;
    const uint64_t n_base = 100;
    const uint64_t n_ext  = 10;

    std::vector<float> dataset(n_base * dimension);
    std::vector<uint32_t> base_ids(n_base);
    for (uint64_t i = 0; i < n_base; ++i) {
        for (uint32_t j = 0; j < dimension; ++j)
            dataset[i * dimension + j] = (float)rand() / RAND_MAX;
        base_ids[i] = (uint32_t)(1000 + i);  // external IDs 1000..1099
    }

    std::vector<int> devices = {0};
    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), n_base, dimension,
                             DistanceType_L2Expanded, bp, devices, 1,
                             DistributionMode_SINGLE_GPU, base_ids.data());
    index.start();
    index.build();

    std::vector<float> ext(n_ext * dimension, 1000.0f);
    std::vector<uint32_t> ext_ids(n_ext);
    for (uint64_t i = 0; i < n_ext; ++i)
        ext_ids[i] = (uint32_t)(2000 + i);  // external IDs 2000..2009

    index.extend(ext.data(), n_ext, ext_ids.data());

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    cagra_search_params_t sp = cagra_search_params_default();

    // Query at base vector 0: expect host ID 1000
    std::vector<float> q0(dataset.begin(), dataset.begin() + dimension);
    auto r0 = index.search(q0.data(), 1, dimension, 1, sp);
    ASSERT_EQ(r0.neighbors[0], 1000u);

    // Query at extended cluster: expect host ID in [2000, 2010)
    std::vector<float> q_ext(dimension, 1000.0f);
    auto r_ext = index.search(q_ext.data(), 1, dimension, 1, sp);
    ASSERT_GE(r_ext.neighbors[0], 2000u);
    ASSERT_TRUE(r_ext.neighbors[0] < 2010u);

    index.destroy();
}
