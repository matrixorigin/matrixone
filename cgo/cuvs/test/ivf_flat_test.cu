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
#include "ivf_flat.hpp"
#include "helper.h"
#include "test_framework.hpp"
#include <cstdio>
#include <cstdlib>
#include <thread>

using namespace matrixone;

TEST(GpuIvfFlatTest, BasicLoadSearchAndCenters) {
    const uint32_t dimension = 2;
    const uint64_t count = 4;
    std::vector<float> dataset = {
        1.0, 1.0,
        1.1, 1.1,
        100.0, 100.0,
        101.0, 101.0
    };
    
    std::vector<int> devices = {0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 2;
    gpu_ivf_flat_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    // Verify centers
    auto centers = index.get_centers();
    ASSERT_EQ(centers.size(), (size_t)(2 * dimension));
    TEST_LOG("IVF-Flat Centers: " << centers[0] << ", " << centers[1]);

    std::vector<float> queries = {1.05, 1.05};
    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    sp.n_probes = 2;
    auto result = index.search(queries.data(), 1, dimension, 2, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)2);
    // Should be either 0 or 1
    ASSERT_TRUE(result.neighbors[0] == 0 || result.neighbors[0] == 1);

    index.destroy();
}

TEST(GpuIvfFlatTest, BasicLoadAndSearchWithIds) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    std::vector<int64_t> ids(count);
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j) dataset[i * dimension + j] = (float)rand() / RAND_MAX;
        ids[i] = (int64_t)(i + 1000);
    }
    
    std::vector<int> devices = {0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 100;
    gpu_ivf_flat_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU, ids.data());
    index.start();
    index.build();

    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 1000);

    index.destroy();
}

TEST(GpuIvfFlatTest, ParallelAddChunkWithOffset) {
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
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 100;
    gpu_ivf_flat_t<float> index(total_count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();

    std::thread t1([&]() { index.add_chunk(chunk1.data(), count_per_chunk, 0, ids1.data()); });
    std::thread t2([&]() { index.add_chunk(chunk2.data(), count_per_chunk, count_per_chunk, ids2.data()); });
    t1.join();
    t2.join();

    index.build();

    std::vector<float> queries(chunk2.begin(), chunk2.begin() + dimension);
    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors[0], (int64_t)count_per_chunk);

    index.destroy();
}

TEST(GpuIvfFlatTest, SaveAndLoadFromFile) {
    const uint32_t dimension = 2;
    const uint64_t count = 4;
    std::vector<float> dataset = {1.0, 1.0, 1.1, 1.1, 100.0, 100.0, 101.0, 101.0};
    std::string filename = "test_ivf_flat.bin";
    std::vector<int> devices = {0};

    // 1. Build and Save
    {
        ivf_flat_build_params_t bp = ivf_flat_build_params_default();
        bp.n_lists = 2;
        gpu_ivf_flat_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.start();
        index.build();
        index.save(filename);
        index.destroy();
    }

    // 2. Load and Search
    {
        ivf_flat_build_params_t bp = ivf_flat_build_params_default();
        bp.n_lists = 2;
        // Construct without loading immediately
        gpu_ivf_flat_t<float> index(filename, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.start(); // Start worker first
        index.load(filename); // Then load explicitly

        std::vector<float> queries = {100.5, 100.5};

        ivf_flat_search_params_t sp = ivf_flat_search_params_default();
        sp.n_probes = 2;
        auto result = index.search(queries.data(), 1, dimension, 2, sp);
        
        ASSERT_EQ(result.neighbors.size(), (size_t)2);
        ASSERT_TRUE(result.neighbors[0] == 2 || result.neighbors[0] == 3);

        index.destroy();
    }

    std::remove(filename.c_str());
}

TEST(GpuIvfFlatTest, ReplicatedModeSimulation) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    int dev_count = gpu_get_device_count();
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 10;
    gpu_ivf_flat_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_REPLICATED);
    index.start();
    index.build();
    
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0);

    index.destroy();
}

TEST(GpuIvfFlatTest, ReplicatedLoadSearch) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    std::string filename = "test_ivf_flat_replicated.bin";
    std::vector<int> single_device = {0};

    // 1. Build and Save on Single GPU
    {
        ivf_flat_build_params_t bp = ivf_flat_build_params_default();
        bp.n_lists = 10;
        gpu_ivf_flat_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, single_device, 1, DistributionMode_SINGLE_GPU);
        index.start();
        index.build();
        index.save(filename);
        index.destroy();
    }

    // 2. Load and Search in Replicated Mode (Multi-GPU)
    {
        int dev_count = gpu_get_device_count();
        if (dev_count > 4) dev_count = 4;
        std::vector<int> devices(dev_count);
        gpu_get_device_list(devices.data(), dev_count);

        ivf_flat_build_params_t bp = ivf_flat_build_params_default();
        bp.n_lists = 10;
        
        gpu_ivf_flat_t<float> index(filename, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_REPLICATED);
        index.start();
        index.load(filename);

        std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
        ivf_flat_search_params_t sp = ivf_flat_search_params_default();
        
        // Search multiple times to likely hit different GPUs/threads
        for (int i = 0; i < dev_count * 2; ++i) {
            auto result = index.search(queries.data(), 1, dimension, 5, sp);
            ASSERT_EQ(result.neighbors.size(), (size_t)5);
            ASSERT_EQ(result.neighbors[0], 0);
        }

        index.destroy();
    }

    std::remove(filename.c_str());
}

TEST(GpuIvfFlatTest, SetGetQuantizer) {
    const uint32_t dimension = 4;
    const uint64_t count = 10;
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 5;
    std::vector<int> devices = {0};
    
    gpu_ivf_flat_t<int8_t> index(count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    
    float min = -1.5f;
    float max = 2.5f;
    index.set_quantizer(min, max);
    
    float gMin = 0, gMax = 0;
    index.get_quantizer(&gMin, &gMax);
    
    ASSERT_EQ(min, gMin);
    ASSERT_EQ(max, gMax);
    
    index.destroy();
}

TEST(GpuIvfFlatTest, ManualShardedSearch) {
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

    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 50;
    gpu_ivf_flat_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED);
    index.start();
    index.build();
    
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0);

    index.destroy();
}

TEST(GpuIvfFlatTest, ManualShardedSearchWithIds) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    std::vector<int64_t> ids(count);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    for (size_t i = 0; i < count; ++i) ids[i] = (int64_t)(i + 10000);
    
    int dev_count = gpu_get_device_count();
    if (dev_count < 2) {
        TEST_LOG("Skipping ManualShardedSearchWithIds: Need at least 2 GPUs");
        return;
    }
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 50;
    gpu_ivf_flat_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED, ids.data());
    index.start();
    index.build();
    
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 10000);

    index.destroy();
}

TEST(GpuIvfFlatTest, ExtendWithoutHostIds) {
    const uint32_t dimension = 2;
    const uint64_t n_base = 100;
    const uint64_t n_ext  = 50;

    // Base dataset: vector i = [i, i]
    std::vector<float> dataset(n_base * dimension);
    for (uint64_t i = 0; i < n_base; ++i) {
        dataset[i * dimension]     = (float)i;
        dataset[i * dimension + 1] = (float)i;
    }

    std::vector<int> devices = {0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 10;
    gpu_ivf_flat_t<float> index(dataset.data(), n_base, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    // Extended vectors well separated: [500+i, 500+i]
    std::vector<float> ext(n_ext * dimension);
    for (uint64_t i = 0; i < n_ext; ++i) {
        ext[i * dimension]     = (float)(500 + i);
        ext[i * dimension + 1] = (float)(500 + i);
    }
    index.extend(ext.data(), n_ext, nullptr);

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    sp.n_probes = 10;

    // Query near base: expect sequential ID 0
    std::vector<float> q0 = {0.0f, 0.0f};
    auto r0 = index.search(q0.data(), 1, dimension, 1, sp);
    ASSERT_EQ(r0.neighbors[0], (int64_t)0);

    // Query near extended set: expect sequential ID n_base (first slot after build)
    std::vector<float> q500 = {500.0f, 500.0f};
    auto r500 = index.search(q500.data(), 1, dimension, 1, sp);
    ASSERT_EQ(r500.neighbors[0], (int64_t)n_base);

    index.destroy();
}

TEST(GpuIvfFlatTest, ExtendWithHostIds) {
    const uint32_t dimension = 2;
    const uint64_t n_base = 100;
    const uint64_t n_ext  = 50;

    std::vector<float> dataset(n_base * dimension);
    std::vector<int64_t> base_ids(n_base);
    for (uint64_t i = 0; i < n_base; ++i) {
        dataset[i * dimension]     = (float)i;
        dataset[i * dimension + 1] = (float)i;
        base_ids[i] = (int64_t)(1000 + i);  // external IDs 1000..1099
    }

    std::vector<int> devices = {0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 10;
    gpu_ivf_flat_t<float> index(dataset.data(), n_base, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SINGLE_GPU, base_ids.data());
    index.start();
    index.build();

    std::vector<float> ext(n_ext * dimension);
    std::vector<int64_t> ext_ids(n_ext);
    for (uint64_t i = 0; i < n_ext; ++i) {
        ext[i * dimension]     = (float)(500 + i);
        ext[i * dimension + 1] = (float)(500 + i);
        ext_ids[i] = (int64_t)(2000 + i);  // external IDs 2000..2049
    }
    index.extend(ext.data(), n_ext, ext_ids.data());

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    sp.n_probes = 10;

    // Query near base: expect host ID 1000
    std::vector<float> q0 = {0.0f, 0.0f};
    auto r0 = index.search(q0.data(), 1, dimension, 1, sp);
    ASSERT_EQ(r0.neighbors[0], (int64_t)1000);

    // Query near extended set: expect host ID 2000
    std::vector<float> q500 = {500.0f, 500.0f};
    auto r500 = index.search(q500.data(), 1, dimension, 1, sp);
    ASSERT_EQ(r500.neighbors[0], (int64_t)2000);

    index.destroy();
}

TEST(GpuIvfFlatTest, ExtendReplicatedWithHostIds) {
    const uint32_t dimension = 2;
    const uint64_t n_base = 100;
    const uint64_t n_ext  = 50;

    int dev_count = gpu_get_device_count();
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    std::vector<float> dataset(n_base * dimension);
    std::vector<int64_t> base_ids(n_base);
    for (uint64_t i = 0; i < n_base; ++i) {
        dataset[i * dimension]     = (float)i;
        dataset[i * dimension + 1] = (float)i;
        base_ids[i] = (int64_t)(1000 + i);
    }

    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 10;
    gpu_ivf_flat_t<float> index(dataset.data(), n_base, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_REPLICATED, base_ids.data());
    index.start();
    index.build();

    std::vector<float> ext(n_ext * dimension);
    std::vector<int64_t> ext_ids(n_ext);
    for (uint64_t i = 0; i < n_ext; ++i) {
        ext[i * dimension]     = (float)(500 + i);
        ext[i * dimension + 1] = (float)(500 + i);
        ext_ids[i] = (int64_t)(2000 + i);
    }
    index.extend(ext.data(), n_ext, ext_ids.data());

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    sp.n_probes = 10;

    std::vector<float> q500 = {500.0f, 500.0f};
    auto r = index.search(q500.data(), 1, dimension, 1, sp);
    ASSERT_EQ(r.neighbors[0], (int64_t)2000);

    index.destroy();
}

TEST(GpuIvfFlatTest, ExtendShardedWithHostIds) {
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
        dataset[i * dimension]     = (float)i;
        dataset[i * dimension + 1] = (float)i;
        base_ids[i] = (int64_t)(1000 + i);
    }

    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 10;
    gpu_ivf_flat_t<float> index(dataset.data(), n_base, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SHARDED, base_ids.data());
    index.start();
    index.build();

    std::vector<float> ext(n_ext * dimension);
    std::vector<int64_t> ext_ids(n_ext);
    for (uint64_t i = 0; i < n_ext; ++i) {
        ext[i * dimension]     = (float)(500 + i);
        ext[i * dimension + 1] = (float)(500 + i);
        ext_ids[i] = (int64_t)(2000 + i);
    }
    index.extend(ext.data(), n_ext, ext_ids.data());

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    sp.n_probes = 10;

    // Query near extended set: expect host ID 2000
    std::vector<float> q500 = {500.0f, 500.0f};
    auto r = index.search(q500.data(), 1, dimension, 1, sp);
    ASSERT_EQ(r.neighbors[0], (int64_t)2000);

    index.destroy();
}

TEST(GpuIvfFlatTest, ExtendShardedWithoutHostIds) {
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
        dataset[i * dimension]     = (float)i;
        dataset[i * dimension + 1] = (float)i;
    }

    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 10;
    gpu_ivf_flat_t<float> index(dataset.data(), n_base, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SHARDED, nullptr);
    index.start();
    index.build();

    std::vector<float> ext(n_ext * dimension);
    for (uint64_t i = 0; i < n_ext; ++i) {
        ext[i * dimension]     = (float)(500 + i);
        ext[i * dimension + 1] = (float)(500 + i);
    }
    index.extend(ext.data(), n_ext, nullptr);

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    sp.n_probes = 10;

    // Query near extended set: expect sequential ID n_base
    std::vector<float> q500 = {500.0f, 500.0f};
    auto r = index.search(q500.data(), 1, dimension, 1, sp);
    ASSERT_EQ(r.neighbors[0], (int64_t)n_base);

    index.destroy();
}

TEST(GpuIvfFlatTest, ManualShardedGetCenters) {
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

    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 50;
    gpu_ivf_flat_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED);
    index.start();
    index.build();

    // In sharded mode, each GPU built its own index with n_lists=50.
    // get_centers() returns centers from the "primary" or first available index it finds.
    auto centers = index.get_centers();
    ASSERT_EQ(centers.size(), (size_t)(bp.n_lists * dimension));

    index.destroy();
}

// Pre-filtered search tests — mirrors GpuCagraTest::FilteredSearch* pattern.
// IDs 0-2 are distinct close neighbors; 3..count-1 are far-away padding.
TEST(GpuIvfFlatTest, FilteredSearchIncludesOnlyAllowedCategories) {
    const uint32_t dimension = 3;
    const uint64_t count = 200;
    std::vector<float> dataset(count * dimension);
    dataset[0] = 1.0; dataset[1] = 2.0; dataset[2] = 3.0;  // ID 0, cat 10
    dataset[3] = 4.0; dataset[4] = 5.0; dataset[5] = 6.0;  // ID 1, cat 20
    dataset[6] = 7.0; dataset[7] = 8.0; dataset[8] = 9.0;  // ID 2, cat 30
    for (uint64_t i = 3; i < count; ++i)
        for (uint32_t j = 0; j < dimension; ++j)
            dataset[i * dimension + j] = 1e6f + (float)i;

    std::vector<int64_t> cats(count, 99);
    cats[0] = 10; cats[1] = 20; cats[2] = 30;

    std::vector<int> devices = {0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 4;
    gpu_ivf_flat_t<float> index(dataset.data(), count, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SINGLE_GPU);
    index.start();
    index.set_filter_columns("[{\"name\":\"cat\",\"type\":1}]", count);
    index.add_filter_chunk(0, cats.data(), count);
    index.build();

    std::vector<float> query = {1.0, 2.0, 3.0};
    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    sp.n_probes = 4;

    auto base = index.search_with_filter(query.data(), 1, dimension, 2, sp, "");
    ASSERT_EQ(base.neighbors[0], 0LL);
    ASSERT_EQ(base.neighbors[1], 1LL);

    auto filtered = index.search_with_filter(
        query.data(), 1, dimension, 2, sp,
        "[{\"col\":0,\"op\":\"!=\",\"val\":10}]");
    ASSERT_EQ(filtered.neighbors[0], 1LL);
    ASSERT_EQ(filtered.neighbors[1], 2LL);

    auto in_pred = index.search_with_filter(
        query.data(), 1, dimension, 1, sp,
        "[{\"col\":0,\"op\":\"in\",\"vals\":[30]}]");
    ASSERT_EQ(in_pred.neighbors[0], 2LL);

    index.destroy();
}

TEST(GpuIvfFlatTest, FilteredSearchCombinesWithDeleteBitset) {
    const uint32_t dimension = 3;
    const uint64_t count = 200;
    std::vector<float> dataset(count * dimension);
    dataset[0] = 1.0; dataset[1] = 2.0; dataset[2] = 3.0;
    dataset[3] = 4.0; dataset[4] = 5.0; dataset[5] = 6.0;
    dataset[6] = 7.0; dataset[7] = 8.0; dataset[8] = 9.0;
    for (uint64_t i = 3; i < count; ++i)
        for (uint32_t j = 0; j < dimension; ++j)
            dataset[i * dimension + j] = 1e6f + (float)i;

    std::vector<int64_t> cats(count, 99);
    cats[0] = 10; cats[1] = 20; cats[2] = 30;

    std::vector<int> devices = {0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 4;
    gpu_ivf_flat_t<float> index(dataset.data(), count, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SINGLE_GPU);
    index.start();
    index.set_filter_columns("[{\"name\":\"cat\",\"type\":1}]", count);
    index.add_filter_chunk(0, cats.data(), count);
    index.build();

    index.delete_id(1);

    std::vector<float> query = {1.0, 2.0, 3.0};
    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    sp.n_probes = 4;
    auto result = index.search_with_filter(
        query.data(), 1, dimension, 2, sp,
        "[{\"col\":0,\"op\":\"in\",\"vals\":[10, 20, 30]}]");

    ASSERT_EQ(result.neighbors[0], 0LL);
    ASSERT_EQ(result.neighbors[1], 2LL);

    index.destroy();
}

TEST(GpuIvfFlatTest, FilteredSearchEmptyPredsMatchesUnfiltered) {
    const uint32_t dimension = 3;
    const uint64_t count = 200;
    std::vector<float> dataset(count * dimension);
    dataset[0] = 1.0; dataset[1] = 2.0; dataset[2] = 3.0;
    dataset[3] = 4.0; dataset[4] = 5.0; dataset[5] = 6.0;
    dataset[6] = 7.0; dataset[7] = 8.0; dataset[8] = 9.0;
    for (uint64_t i = 3; i < count; ++i)
        for (uint32_t j = 0; j < dimension; ++j)
            dataset[i * dimension + j] = 1e6f + (float)i;

    std::vector<int64_t> cats(count, 99);
    cats[0] = 10; cats[1] = 20; cats[2] = 30;

    std::vector<int> devices = {0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 4;
    gpu_ivf_flat_t<float> index(dataset.data(), count, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SINGLE_GPU);
    index.start();
    index.set_filter_columns("[{\"name\":\"cat\",\"type\":1}]", count);
    index.add_filter_chunk(0, cats.data(), count);
    index.build();

    std::vector<float> query = {1.0, 2.0, 3.0};
    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    sp.n_probes = 4;
    auto unfiltered = index.search(query.data(), 1, dimension, 3, sp);
    auto empty_pred = index.search_with_filter(query.data(), 1, dimension, 3, sp, "");

    ASSERT_EQ(unfiltered.neighbors[0], empty_pred.neighbors[0]);
    ASSERT_EQ(unfiltered.neighbors[1], empty_pred.neighbors[1]);
    ASSERT_EQ(unfiltered.neighbors[2], empty_pred.neighbors[2]);

    index.destroy();
}
