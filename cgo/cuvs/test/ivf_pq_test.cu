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
#include <cuda_fp16.h>

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
    gpu_ivf_pq_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
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
    gpu_ivf_pq_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU, ids.data());
    index.start();
    index.build();

    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 2000);

    index.destroy();
}

// Native half (f16) build + search — validates the direct vecf16-base path
// (gpu_ivf_pq_t<half, half> native add_chunk/search, no quantizer). Linking this proves
// cuVS supports ivf_pq over half (unlike brute force over int8/uint8).
TEST(GpuIvfPqTest, BasicLoadAndSearchHalf) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<half> dataset(count * dimension);
    std::vector<int64_t> ids(count);
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j)
            dataset[i * dimension + j] = __float2half((float)rand() / RAND_MAX);
        ids[i] = (int64_t)(i + 2000);
    }

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 100;
    gpu_ivf_pq_t<half, half> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU, ids.data());
    index.start();
    index.build();

    // Query == row 0, so the nearest neighbour must be its id (2000).
    std::vector<half> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 2000);

    index.destroy();
}

// vecf16 base -> int8 storage via the native B(half)-source quantizer
// (add_chunk_quantize). Verifies the quantize-build path: train the half-source
// quantizer on the buffered vecf16 sample, transform half->int8, store as int8,
// and build a searchable int8 index. No f32 detour.
TEST(GpuIvfPqTest, HalfQuantizeToInt8Build) {
    const uint32_t dimension = 16;
    const uint64_t count = 2000;
    std::vector<half> dataset(count * dimension);
    std::vector<int64_t> ids(count);
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j)
            dataset[i * dimension + j] = __float2half((float)(rand() % 256) / 255.0f);
        ids[i] = (int64_t)(i + 5000);
    }

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 50;
    gpu_ivf_pq_t<half, int8_t> index(count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.add_chunk_quantize(dataset.data(), count, -1, ids.data());
    index.build();

    // The resulting int8 index is searchable with a native int8 query.
    std::vector<int8_t> q(dimension, 0);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 50;
    auto result = index.search(q.data(), 1, dimension, 5, sp);
    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    for (auto n : result.neighbors) {
        ASSERT_GE(n, (int64_t)5000);
        ASSERT_LT(n, (int64_t)(5000 + count));
    }

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
    gpu_ivf_pq_t<float, float> index(total_count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
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
        gpu_ivf_pq_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
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
        gpu_ivf_pq_t<float, float> index(filename, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
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
    gpu_ivf_pq_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED);
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
    gpu_ivf_pq_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED, ids.data());
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
    gpu_ivf_pq_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED);
    index.start();
    index.build();
    
    auto centers = index.get_centers();
    // In sharded mode, get_centers returns the cluster centers from a SINGLE
    // shard (n_lists rows × rot_dim cols, with the squared-norm/padding stripped
    // — see ivf_pq.hpp::get_centers and commit d4e0409d1). rot_dim is the
    // original dim rounded up to a multiple of pq_dim (m). For this test
    // dim=16, m=8, so rot_dim=16. Expected: n_lists * rot_dim = 50 * 16 = 800.
    ASSERT_EQ(centers.size(), (size_t)800);

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
    gpu_ivf_pq_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_REPLICATED);
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
    gpu_ivf_pq_t<float, float> index(dataset.data(), n_base, dimension,
                              DistanceType_L2Expanded, bp, devices, 1,
                              DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    // Extended vectors: all components = 500.5 (outside trained range [0..99])
    std::vector<float> ext(n_ext * dimension);
    for (uint64_t i = 0; i < n_ext; ++i)
        for (uint32_t j = 0; j < dimension; ++j)
            ext[i * dimension + j] = 500.5f;

    index.extend(ext.data(), n_ext, nullptr);

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 10;

    // PQ is approximate, and after extend the encoded extended vectors can
    // shift the per-list ranking enough that near-tied base vectors swap
    // top-1. Assert top-k membership rather than top-1 equality — that still
    // proves both base and extended vectors are searchable post-extend.
    const uint32_t k = 5;

    // Query near base: vector 0 should be in the top-k.
    std::vector<float> q0(dimension, 0.0f);
    auto r0 = index.search(q0.data(), 1, dimension, k, sp);
    bool found_zero = false;
    for (size_t i = 0; i < r0.neighbors.size(); ++i) {
        if (r0.neighbors[i] == (int64_t)0) { found_zero = true; break; }
    }
    ASSERT_TRUE(found_zero);

    // Query exactly at extended set: at least one of the top-k should be an
    // extended vector (IDs in [n_base, n_base + n_ext)). The 50 extended
    // vectors are identical so any of them is a valid hit.
    std::vector<float> q50(dimension, 500.5f);
    auto r500 = index.search(q50.data(), 1, dimension, k, sp);
    bool found_ext = false;
    for (size_t i = 0; i < r500.neighbors.size(); ++i) {
        int64_t id = r500.neighbors[i];
        if (id >= (int64_t)n_base && id < (int64_t)(n_base + n_ext)) {
            found_ext = true; break;
        }
    }
    ASSERT_TRUE(found_ext);

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
    gpu_ivf_pq_t<float, float> index(dataset.data(), n_base, dimension,
                              DistanceType_L2Expanded, bp, devices, 1,
                              DistributionMode_SINGLE_GPU, base_ids.data());
    index.start();
    index.build();

    std::vector<float> ext(n_ext * dimension);
    std::vector<int64_t> ext_ids(n_ext);
    for (uint64_t i = 0; i < n_ext; ++i) {
        for (uint32_t j = 0; j < dimension; ++j)
            ext[i * dimension + j] = 500.5f;  // outside trained range [0..99]
        ext_ids[i] = (int64_t)(2000 + i);  // external IDs 2000..2049
    }
    index.extend(ext.data(), n_ext, ext_ids.data());

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 10;

    // Query near base: expect host ID 1000
    std::vector<float> q0(dimension, 0.0f);
    auto r0 = index.search(q0.data(), 1, dimension, 1, sp);
    ASSERT_GE(r0.neighbors[0], (int64_t)1000);
    ASSERT_LE(r0.neighbors[0], (int64_t)1100);

    // Query exactly at extended set: expect host ID in [2000, 2050)
    std::vector<float> q50(dimension, 500.5f);
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
    gpu_ivf_pq_t<float, float> index(dataset.data(), n_base, dimension,
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
    gpu_ivf_pq_t<float, float> index(dataset.data(), n_base, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SHARDED, base_ids.data());
    index.start();
    index.build();

    std::vector<float> ext(n_ext * dimension);
    std::vector<int64_t> ext_ids(n_ext);
    for (uint64_t i = 0; i < n_ext; ++i) {
        ext[i * dimension]     = 500.5f;
        ext[i * dimension + 1] = 500.5f;
        ext_ids[i] = (int64_t)(2000 + i);
    }
    index.extend(ext.data(), n_ext, ext_ids.data());

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 10;

    // Query exactly at extended set: expect host ID in [2000, 2050)
    std::vector<float> q50 = {500.5f, 500.5f};
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
    gpu_ivf_pq_t<float, float> index(dataset.data(), n_base, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SHARDED, nullptr);
    index.start();
    index.build();

    std::vector<float> ext(n_ext * dimension);
    for (uint64_t i = 0; i < n_ext; ++i) {
        ext[i * dimension]     = 500.5f;
        ext[i * dimension + 1] = 500.5f;
    }
    index.extend(ext.data(), n_ext, nullptr);

    ASSERT_EQ((uint64_t)index.len(), n_base + n_ext);

    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 10;

    // Query exactly at extended set: expect sequential ID in [n_base, n_base+n_ext)
    std::vector<float> q50 = {500.5f, 500.5f};
    auto r = index.search(q50.data(), 1, dimension, 1, sp);
    ASSERT_GE(r.neighbors[0], (int64_t)n_base);
    ASSERT_LT(r.neighbors[0], (int64_t)(n_base + n_ext));

    index.destroy();
}

// Pre-filtered search tests — mirrors GpuCagraTest::FilteredSearch* pattern.
// IVF-PQ is a lossy approximate index, so tests assert set membership rather
// than exact top-k ordering. dimension=8 with m=4 gives pq_dim=8, pq_len=2.
TEST(GpuIvfPqTest, FilteredSearchExcludesForbiddenCategory) {
    const uint32_t dimension = 8;
    const uint64_t count = 200;
    std::vector<float> dataset(count * dimension);
    // ID 0, 1, 2 are the distinguishable close vectors.
    for (uint32_t j = 0; j < dimension; ++j) dataset[0 * dimension + j] = 1.0f;
    for (uint32_t j = 0; j < dimension; ++j) dataset[1 * dimension + j] = 2.0f;
    for (uint32_t j = 0; j < dimension; ++j) dataset[2 * dimension + j] = 3.0f;
    for (uint64_t i = 3; i < count; ++i)
        for (uint32_t j = 0; j < dimension; ++j)
            dataset[i * dimension + j] = 1e4f + (float)i;

    std::vector<int64_t> cats(count, 99);
    cats[0] = 10; cats[1] = 20; cats[2] = 30;

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 4;
    bp.m = 4;
    gpu_ivf_pq_t<float, float> index(dataset.data(), count, dimension,
                              DistanceType_L2Expanded, bp, devices, 1,
                              DistributionMode_SINGLE_GPU);
    index.start();
    index.set_filter_columns("[{\"name\":\"cat\",\"type\":1}]", count);
    index.add_filter_chunk(0, cats.data(), nullptr, count);
    index.build();

    std::vector<float> query(dimension, 1.0f);  // closest to ID 0
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 4;

    // Filter cat != 10 — ID 0 must NOT appear in any result slot.
    auto filtered = index.search_with_filter(
        query.data(), 1, dimension, 3, sp,
        "[{\"col\":0,\"op\":\"!=\",\"val\":10}]");
    for (size_t i = 0; i < filtered.neighbors.size(); ++i) {
        ASSERT_NE(filtered.neighbors[i], 0LL);
    }

    // IN [30] — top-1 must be ID 2 (the only cat=30 entry).
    auto in_pred = index.search_with_filter(
        query.data(), 1, dimension, 1, sp,
        "[{\"col\":0,\"op\":\"in\",\"vals\":[30]}]");
    ASSERT_EQ(in_pred.neighbors[0], 2LL);

    index.destroy();
}

TEST(GpuIvfPqTest, FilteredSearchCombinesWithDeleteBitset) {
    const uint32_t dimension = 8;
    const uint64_t count = 200;
    std::vector<float> dataset(count * dimension);
    for (uint32_t j = 0; j < dimension; ++j) dataset[0 * dimension + j] = 1.0f;
    for (uint32_t j = 0; j < dimension; ++j) dataset[1 * dimension + j] = 2.0f;
    for (uint32_t j = 0; j < dimension; ++j) dataset[2 * dimension + j] = 3.0f;
    for (uint64_t i = 3; i < count; ++i)
        for (uint32_t j = 0; j < dimension; ++j)
            dataset[i * dimension + j] = 1e4f + (float)i;

    std::vector<int64_t> cats(count, 99);
    cats[0] = 10; cats[1] = 20; cats[2] = 30;

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 4;
    bp.m = 4;
    gpu_ivf_pq_t<float, float> index(dataset.data(), count, dimension,
                              DistanceType_L2Expanded, bp, devices, 1,
                              DistributionMode_SINGLE_GPU);
    index.start();
    index.set_filter_columns("[{\"name\":\"cat\",\"type\":1}]", count);
    index.add_filter_chunk(0, cats.data(), nullptr, count);
    index.build();

    index.delete_id(1);

    std::vector<float> query(dimension, 1.0f);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 4;
    // limit=5 deliberately exceeds the number of valid rows (2: IDs 0 and 2;
    // ID 1 is soft-deleted). Exercises the host-side post-filter in
    // gpu_ivf_pq_t::apply_pq_post_filter_locked, which was added to defeat
    // cuVS IVF-PQ's bitset_filter padding quirk: without it, the 3 extra
    // result slots would be filled with filter-excluded (cat=99) rows rather
    // than the -1 sentinel.
    const uint32_t k = 5;
    auto result = index.search_with_filter(
        query.data(), 1, dimension, k, sp,
        "[{\"col\":0,\"op\":\"in\",\"vals\":[10, 20, 30]}]");
    ASSERT_EQ(result.neighbors.size(), (size_t)k);

    bool saw_0 = false, saw_2 = false;
    int valid_slots = 0;
    for (size_t i = 0; i < result.neighbors.size(); ++i) {
        ASSERT_NE(result.neighbors[i], 1LL);  // never the deleted ID
        if (result.neighbors[i] == -1) continue;
        ++valid_slots;
        // Only IDs that pass (cat IN {10,20,30}) AND are not deleted may appear.
        // A cat=99 ID leaking here would indicate the post-filter failed.
        ASSERT_TRUE(result.neighbors[i] == 0LL || result.neighbors[i] == 2LL);
        if (result.neighbors[i] == 0LL) saw_0 = true;
        if (result.neighbors[i] == 2LL) saw_2 = true;
    }
    ASSERT_TRUE(saw_0);
    ASSERT_TRUE(saw_2);
    ASSERT_EQ(valid_slots, 2);  // exactly the 2 filter-matching, non-deleted rows

    index.destroy();
}

TEST(GpuIvfPqTest, FilteredSearchEmptyPredsMatchesUnfiltered) {
    const uint32_t dimension = 8;
    const uint64_t count = 200;
    std::vector<float> dataset(count * dimension);
    for (uint32_t j = 0; j < dimension; ++j) dataset[0 * dimension + j] = 1.0f;
    for (uint32_t j = 0; j < dimension; ++j) dataset[1 * dimension + j] = 2.0f;
    for (uint32_t j = 0; j < dimension; ++j) dataset[2 * dimension + j] = 3.0f;
    for (uint64_t i = 3; i < count; ++i)
        for (uint32_t j = 0; j < dimension; ++j)
            dataset[i * dimension + j] = 1e4f + (float)i;

    std::vector<int64_t> cats(count, 99);
    cats[0] = 10; cats[1] = 20; cats[2] = 30;

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 4;
    bp.m = 4;
    gpu_ivf_pq_t<float, float> index(dataset.data(), count, dimension,
                              DistanceType_L2Expanded, bp, devices, 1,
                              DistributionMode_SINGLE_GPU);
    index.start();
    index.set_filter_columns("[{\"name\":\"cat\",\"type\":1}]", count);
    index.add_filter_chunk(0, cats.data(), nullptr, count);
    index.build();

    std::vector<float> query(dimension, 1.0f);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 4;
    auto unfiltered = index.search(query.data(), 1, dimension, 3, sp);
    auto empty_pred = index.search_with_filter(query.data(), 1, dimension, 3, sp, "");

    ASSERT_EQ(unfiltered.neighbors[0], empty_pred.neighbors[0]);
    ASSERT_EQ(unfiltered.neighbors[1], empty_pred.neighbors[1]);
    ASSERT_EQ(unfiltered.neighbors[2], empty_pred.neighbors[2]);

    index.destroy();
}

// Regression test for the k > index_size cuVS rejection that erictest/21
// surfaced via the Go-side over-fetch path (apply_indices.go:84 5× factor on
// small limits). search_internal now clamps to effective_k = min(limit, shard_sz)
// and pads the tail with (-1, FLT_MAX). See filter.hpp helpers.
TEST(GpuIvfPqTest, KExceedsIndexSizeClampsAndPads) {
    const uint32_t dimension = 8;
    const uint64_t count = 20;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j) {
            dataset[i * dimension + j] = static_cast<float>(i + 1);
        }
    }

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 2;
    bp.m = 2;
    bp.bits_per_code = 8;
    gpu_ivf_pq_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded,
                              bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    // Query near id=10 (vector value 11). limit=25 > count=20 — cuVS would
    // reject without the clamp.
    std::vector<float> query(dimension, 11.0f);
    const uint32_t limit = 25;
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 2;

    auto result = index.search(query.data(), 1, dimension, limit, sp);

    // Buffer shape preserved.
    ASSERT_EQ(result.neighbors.size(), (size_t)limit);
    ASSERT_EQ(result.distances.size(), (size_t)limit);

    // The first 20 slots should all be valid (in [0, 20)). cuVS may or may
    // not return them in true L2 order under PQ approximation, so we just
    // assert they're in range and unique.
    std::vector<int64_t> seen;
    for (uint32_t i = 0; i < count; ++i) {
        int64_t n = result.neighbors[i];
        ASSERT_GE(n, 0);
        ASSERT_LT(n, (int64_t)count);
        seen.push_back(n);
    }
    std::sort(seen.begin(), seen.end());
    for (uint32_t i = 1; i < count; ++i) {
        ASSERT_NE(seen[i], seen[i - 1]);
    }

    // Pad slots [20, 25) MUST be (-1, FLT_MAX).
    for (uint32_t i = count; i < limit; ++i) {
        ASSERT_EQ(result.neighbors[i], (int64_t)-1);
        ASSERT_EQ(result.distances[i], std::numeric_limits<float>::max());
    }

    index.destroy();
}

// Multi-query variant — exercises the per-row strided scatter in
// scatter_with_padding (production uses num_queries=1, so this is the only
// guard against the row-stride bug class). Each query row's tail slots must
// independently land at -1 / FLT_MAX.
TEST(GpuIvfPqTest, MultiQueryKExceedsIndexSize) {
    const uint32_t dimension = 8;
    const uint64_t count = 20;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j) {
            dataset[i * dimension + j] = static_cast<float>(i + 1);
        }
    }

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 2;
    bp.m = 2;
    bp.bits_per_code = 8;
    gpu_ivf_pq_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded,
                              bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    const uint64_t num_queries = 4;
    const uint32_t limit = 25;
    std::vector<float> queries(num_queries * dimension);
    for (uint64_t q = 0; q < num_queries; ++q) {
        const float v = static_cast<float>(2 * q + 3);  // targets ids ~2, 4, 6, 8
        for (uint32_t j = 0; j < dimension; ++j) {
            queries[q * dimension + j] = v;
        }
    }

    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    sp.n_probes = 2;

    auto result = index.search(queries.data(), num_queries, dimension, limit, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)(num_queries * limit));
    ASSERT_EQ(result.distances.size(), (size_t)(num_queries * limit));

    // Per-query row: first 20 slots valid in [0, 20), last 5 slots sentinel.
    for (uint64_t q = 0; q < num_queries; ++q) {
        for (uint32_t i = 0; i < count; ++i) {
            int64_t n = result.neighbors[q * limit + i];
            ASSERT_GE(n, 0);
            ASSERT_LT(n, (int64_t)count);
        }
        for (uint32_t i = count; i < limit; ++i) {
            ASSERT_EQ(result.neighbors[q * limit + i], (int64_t)-1);
            ASSERT_EQ(result.distances[q * limit + i], std::numeric_limits<float>::max());
        }
    }

    index.destroy();
}
