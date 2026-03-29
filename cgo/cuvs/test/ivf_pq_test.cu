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
#include "ivf_pq.hpp"
#include "helper.h"
#include "test_framework.hpp"
#include <cstdio>
#include <cstdlib>
#include <thread>

using namespace matrixone;

TEST(GpuIvfPqTest, BasicLoadSearchAndCenters) {
    const uint32_t dimension = 16;
    const uint64_t count = 4;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j) {
            dataset[i * dimension + j] = (float)i;
        }
    }
    
    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 2;
    bp.m = 8;
    gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    // Verify centers
    auto centers = index.get_centers();
    ASSERT_TRUE(centers.size() > 0);

    std::vector<float> queries(dimension);
    for (size_t j = 0; j < dimension; ++j) queries[j] = 0.9f;
    
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 2;
    auto result = index.search(queries.data(), 1, dimension, 2, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)2);
    // Should be either 0 or 1
    ASSERT_TRUE(result.neighbors[0] == 0 || result.neighbors[0] == 1);

    index.destroy();
}

TEST(GpuIvfPqTest, BasicLoadAndSearchWithIds) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    std::vector<int64_t> ids(count);
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j) dataset[i * dimension + j] = (float)rand() / RAND_MAX;
        ids[i] = (int64_t)(i + 2000);
    }
    
    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 100;
    gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU, ids.data());
    index.start();
    index.build();

    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 2000);

    index.destroy();
}

TEST(GpuIvfPqTest, ParallelAddChunkWithOffset) {
    const uint32_t dimension = 16;
    const uint64_t count_per_chunk = 500;
    const uint64_t total_count = count_per_chunk * 2;
    std::vector<float> chunk1(count_per_chunk * dimension);
    std::vector<float> chunk2(count_per_chunk * dimension);
    std::vector<int64_t> ids1(count_per_chunk);
    std::vector<int64_t> ids2(count_per_chunk);

    for (size_t i = 0; i < count_per_chunk; ++i) {
        for (size_t j = 0; j < dimension; ++j) {
            chunk1[i * dimension + j] = (float)rand() / RAND_MAX;
            chunk2[i * dimension + j] = (float)rand() / RAND_MAX;
        }
        ids1[i] = (int64_t)i;
        ids2[i] = (int64_t)(i + count_per_chunk);
    }

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 100;
    gpu_ivf_pq_t<float> index(total_count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();

    std::thread t1([&]() { index.add_chunk(chunk1.data(), count_per_chunk, 0, ids1.data()); });
    std::thread t2([&]() { index.add_chunk(chunk2.data(), count_per_chunk, count_per_chunk, ids2.data()); });
    t1.join();
    t2.join();

    index.build();

    std::vector<float> queries(chunk2.begin(), chunk2.begin() + dimension);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors[0], (int64_t)count_per_chunk);

    index.destroy();
}

TEST(GpuIvfPqTest, SaveAndLoadFromFile) {
    const uint32_t dimension = 4;
    const uint64_t count = 4;
    std::vector<float> dataset = {
        0.0, 0.0, 0.0, 0.0,
        1.0, 1.0, 1.0, 1.0,
        10.0, 10.0, 10.0, 10.0,
        11.0, 11.0, 11.0, 11.0
    };
    std::string filename = "test_ivf_pq.bin";
    std::vector<int> devices = {0};

    // 1. Build and Save
    {
        ivf_pq_build_params_t bp = ivf_pq_build_params_default();
        bp.n_lists = 2;
        bp.m = 2;
        gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.start();
        index.build();
        index.save(filename);
        index.destroy();
    }

    // 2. Load and Search
    {
        ivf_pq_build_params_t bp = ivf_pq_build_params_default();
        bp.n_lists = 2;
        bp.m = 2;
        gpu_ivf_pq_t<float> index(filename, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.start();
        index.load(filename);
        
        std::vector<float> queries = {10.5, 10.5, 10.5, 10.5};
        ivf_pq_search_params_t sp = ivf_pq_search_params_default();
        sp.n_probes = 2;
        auto result = index.search(queries.data(), 1, dimension, 2, sp);
        
        ASSERT_EQ(result.neighbors.size(), (size_t)2);
        ASSERT_TRUE(result.neighbors[0] == 2 || result.neighbors[0] == 3);

        index.destroy();
    }

    std::remove(filename.c_str());
}

TEST(GpuIvfPqTest, ManualShardedSearch) {
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

    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 50;
    bp.m = 8;
    gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED);
    index.start();
    index.build();
    
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0);

    index.destroy();
}

TEST(GpuIvfPqTest, ManualShardedSearchWithIds) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    std::vector<int64_t> ids(count);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    for (size_t i = 0; i < count; ++i) ids[i] = (int64_t)(i + 20000);
    
    int dev_count = gpu_get_device_count();
    if (dev_count < 2) {
        TEST_LOG("Skipping ManualShardedSearchWithIds: Need at least 2 GPUs");
        return;
    }
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 50;
    bp.m = 8;
    gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED, ids.data());
    index.start();
    index.build();
    
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 20000);

    index.destroy();
}

TEST(GpuIvfPqTest, ManualShardedGetCenters) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    int dev_count = gpu_get_device_count();
    if (dev_count < 2) {
        TEST_LOG("Skipping ManualShardedGetCenters: Need at least 2 GPUs");
        return;
    }
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 50;
    bp.m = 8;
    gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED);
    index.start();
    index.build();
    
    auto centers = index.get_centers();
    // In sharded mode, get_centers returns centers from a SINGLE shard.
    // IVF-PQ codebook size is n_lists * pq_dim * pq_bits_dimension
    // For default 8 bits, pq_bits_dimension is 3. 
    // In this test: 50 * 8 * 3 = 1200
    ASSERT_EQ(centers.size(), (size_t)1200);

    index.destroy();
}

TEST(GpuIvfPqTest, ReplicatedModeSimulation) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    int dev_count = gpu_get_device_count();
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 100;
    bp.m = 8;
    gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_REPLICATED);
    index.start();
    index.build();
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0);

    index.destroy();
}

TEST(GpuIvfPqTest, ExtendWithoutHostIds) {
    const uint32_t dimension = 16;
    const uint64_t n_base = 100;
    const uint64_t n_ext  = 50;

    // Base dataset: vector i has all components = i
    std::vector<float> dataset(n_base * dimension);
    for (uint64_t i = 0; i < n_base; ++i)
        for (uint32_t j = 0; j < dimension; ++j)
            dataset[i * dimension + j] = (float)i;

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 10;
    bp.m = 8;
    gpu_ivf_pq_t<float> index(dataset.data(), n_base, dimension,
                              DistanceType_L2Expanded, bp, devices, 1,
                              DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    // Extended vectors: all components = 50.5 (within trained range [0..99])
    std::vector<float> ext(n_ext * dimension);
    for (uint64_t i = 0; i < n_ext; ++i)
        for (uint32_t j = 0; j < dimension; ++j)
            ext[i * dimension + j] = 50.5f;

    index.extend(ext.data(), n_ext, nullptr);

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 10;

    // Query near base: expect sequential ID 0
    std::vector<float> q0(dimension, 0.0f);
    auto r0 = index.search(q0.data(), 1, dimension, 1, sp);
    ASSERT_EQ(r0.neighbors[0], (int64_t)0);

    // Query exactly at extended set: expect sequential ID in [n_base, n_base+n_ext)
    // (PQ is approximate; any of the 50 identical extended vectors is valid)
    std::vector<float> q50(dimension, 50.5f);
    auto r500 = index.search(q50.data(), 1, dimension, 1, sp);
    ASSERT_GE(r500.neighbors[0], (int64_t)n_base);
    ASSERT_TRUE(r500.neighbors[0] < (int64_t)(n_base + n_ext));

    index.destroy();
}

TEST(GpuIvfPqTest, ExtendWithHostIds) {
    const uint32_t dimension = 16;
    const uint64_t n_base = 100;
    const uint64_t n_ext  = 50;

    std::vector<float> dataset(n_base * dimension);
    std::vector<int64_t> base_ids(n_base);
    for (uint64_t i = 0; i < n_base; ++i) {
        for (uint32_t j = 0; j < dimension; ++j)
            dataset[i * dimension + j] = (float)i;
        base_ids[i] = (int64_t)(1000 + i);  // external IDs 1000..1099
    }

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 10;
    bp.m = 8;
    gpu_ivf_pq_t<float> index(dataset.data(), n_base, dimension,
                              DistanceType_L2Expanded, bp, devices, 1,
                              DistributionMode_SINGLE_GPU, base_ids.data());
    index.start();
    index.build();

    std::vector<float> ext(n_ext * dimension);
    std::vector<int64_t> ext_ids(n_ext);
    for (uint64_t i = 0; i < n_ext; ++i) {
        for (uint32_t j = 0; j < dimension; ++j)
            ext[i * dimension + j] = 50.5f;  // within trained range [0..99]
        ext_ids[i] = (int64_t)(2000 + i);  // external IDs 2000..2049
    }
    index.extend(ext.data(), n_ext, ext_ids.data());

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 10;

    // Query near base: expect host ID 1000
    std::vector<float> q0(dimension, 0.0f);
    auto r0 = index.search(q0.data(), 1, dimension, 1, sp);
    ASSERT_EQ(r0.neighbors[0], (int64_t)1000);

    // Query exactly at extended set: expect host ID in [2000, 2050)
    std::vector<float> q50(dimension, 50.5f);
    auto r500 = index.search(q50.data(), 1, dimension, 1, sp);
    ASSERT_GE(r500.neighbors[0], (int64_t)2000);
    ASSERT_TRUE(r500.neighbors[0] < (int64_t)2050);

    index.destroy();
}

TEST(GpuIvfPqTest, ExtendReplicatedWithHostIds) {
    const uint32_t dimension = 16;
    const uint64_t n_base = 100;
    const uint64_t n_ext  = 50;

    int dev_count = gpu_get_device_count();
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    std::vector<float> dataset(n_base * dimension);
    std::vector<int64_t> base_ids(n_base);
    for (uint64_t i = 0; i < n_base; ++i) {
        for (uint32_t j = 0; j < dimension; ++j)
            dataset[i * dimension + j] = (float)i;
        base_ids[i] = (int64_t)(1000 + i);
    }

    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 10;
    bp.m = 8;
    gpu_ivf_pq_t<float> index(dataset.data(), n_base, dimension,
                              DistanceType_L2Expanded, bp, devices, 1,
                              DistributionMode_REPLICATED, base_ids.data());
    index.start();
    index.build();

    std::vector<float> ext(n_ext * dimension);
    std::vector<int64_t> ext_ids(n_ext);
    for (uint64_t i = 0; i < n_ext; ++i) {
        for (uint32_t j = 0; j < dimension; ++j)
            ext[i * dimension + j] = 50.5f;  // within trained range [0..99]
        ext_ids[i] = (int64_t)(2000 + i);
    }
    index.extend(ext.data(), n_ext, ext_ids.data());

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 10;

    index.destroy();
}

TEST(GpuIvfPqTest, ExtendShardedWithHostIds) {
    const uint32_t dimension = 2;
    const uint64_t n_base = 200;
    const uint64_t n_ext  = 50;

    int dev_count = gpu_get_device_count();
    if (dev_count < 2) return; // Need at least 2 GPUs for sharded test
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    std::vector<float> dataset(n_base * dimension);
    std::vector<int64_t> base_ids(n_base);
    for (uint64_t i = 0; i < n_base; ++i) {
        dataset[i * dimension]     = (float)(i % 100);
        dataset[i * dimension + 1] = (float)(i % 100);
        base_ids[i] = (int64_t)(1000 + i);
    }

    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 10;
    gpu_ivf_pq_t<float> index(dataset.data(), n_base, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SHARDED, base_ids.data());
    index.start();
    index.build();

    std::vector<float> ext(n_ext * dimension);
    std::vector<int64_t> ext_ids(n_ext);
    for (uint64_t i = 0; i < n_ext; ++i) {
        ext[i * dimension]     = 50.5f;
        ext[i * dimension + 1] = 50.5f;
        ext_ids[i] = (int64_t)(2000 + i);
    }
    index.extend(ext.data(), n_ext, ext_ids.data());

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 10;

    // Query exactly at extended set: expect host ID in [2000, 2050)
    std::vector<float> q50 = {50.5f, 50.5f};
    auto r = index.search(q50.data(), 1, dimension, 1, sp);
    ASSERT_GE(r.neighbors[0], (int64_t)2000);
    ASSERT_LT(r.neighbors[0], (int64_t)2050);

    index.destroy();
}

TEST(GpuIvfPqTest, ExtendShardedWithoutHostIds) {
    const uint32_t dimension = 2;
    const uint64_t n_base = 200;
    const uint64_t n_ext  = 50;

    int dev_count = gpu_get_device_count();
    if (dev_count < 2) return; // Need at least 2 GPUs for sharded test
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    std::vector<float> dataset(n_base * dimension);
    for (uint64_t i = 0; i < n_base; ++i) {
        dataset[i * dimension]     = (float)(i % 100);
        dataset[i * dimension + 1] = (float)(i % 100);
    }

    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 10;
    gpu_ivf_pq_t<float> index(dataset.data(), n_base, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SHARDED, nullptr);
    index.start();
    index.build();

    std::vector<float> ext(n_ext * dimension);
    for (uint64_t i = 0; i < n_ext; ++i) {
        ext[i * dimension]     = 50.5f;
        ext[i * dimension + 1] = 50.5f;
    }
    index.extend(ext.data(), n_ext, nullptr);

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 10;

    // Query exactly at extended set: expect sequential ID in [n_base, n_base+n_ext)
    std::vector<float> q50 = {50.5f, 50.5f};
    auto r = index.search(q50.data(), 1, dimension, 1, sp);
    ASSERT_GE(r.neighbors[0], (int64_t)n_base);
    ASSERT_LT(r.neighbors[0], (int64_t)(n_base + n_ext));

    index.destroy();
}
