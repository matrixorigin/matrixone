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
    gpu_ivf_flat_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
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
    gpu_ivf_flat_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU, ids.data());
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
    gpu_ivf_flat_t<float, float> index(total_count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
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
        gpu_ivf_flat_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
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
        gpu_ivf_flat_t<float, float> index(filename, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
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
    gpu_ivf_flat_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_REPLICATED);
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
        gpu_ivf_flat_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, single_device, 1, DistributionMode_SINGLE_GPU);
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
        
        gpu_ivf_flat_t<float, float> index(filename, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_REPLICATED);
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
    
    gpu_ivf_flat_t<float, int8_t> index(count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    
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
    gpu_ivf_flat_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED);
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
    gpu_ivf_flat_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED, ids.data());
    index.start();
    index.build();
    
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 10000);

    index.destroy();
}

// ---------------------------------------------------------------------------
// Single-GPU "multi-GPU simulation". A duplicated device list [0,0] presents N
// logical GPUs on one physical device so REPLICATED / SHARDED can be exercised
// without real multi-GPU hardware. Per-rank index/dataset maps are keyed by
// logical rank, so the N copies coexist instead of colliding on device 0 —
// info() reports "ranks": N. nthread must be >= devices.size() so every rank's
// queue has a worker thread (else submit_all_devices would deadlock). These run
// on any host with >= 1 GPU, unlike the Manual* tests which need 2 real GPUs.
// ---------------------------------------------------------------------------

TEST(GpuIvfFlatTest, SimulatedReplicatedBuildSearch) {
    if (gpu_get_device_count() < 1) { TEST_LOG("Skipping SimulatedReplicatedBuildSearch (no GPU)"); return; }

    const uint32_t dim = 4; const uint64_t count = 16;
    std::vector<float> ds(count * dim); std::vector<int64_t> ids(count);
    for (uint64_t i = 0; i < count; ++i) { for (uint32_t j = 0; j < dim; ++j) ds[i*dim+j] = (float)(i+1); ids[i] = (int64_t)(i+100); }

    std::vector<int> sim2 = {0, 0};                 // 2 logical GPUs on physical device 0
    ivf_flat_build_params_t bp = ivf_flat_build_params_default(); bp.n_lists = 4;
    gpu_ivf_flat_t<float, float> index(ds.data(), count, dim, DistanceType_L2Expanded, bp, sim2, 2, DistributionMode_REPLICATED, ids.data());
    index.start(); index.build();

    ASSERT_TRUE(index.info().find("\"ranks\": 2") != std::string::npos);  // 2 replicas coexist

    ivf_flat_search_params_t sp = ivf_flat_search_params_default(); sp.n_probes = 4;
    for (int r : {0, 7, 15}) {
        std::vector<float> q(ds.begin()+r*dim, ds.begin()+(r+1)*dim);
        auto res = index.search(q.data(), 1, dim, 1, sp);
        ASSERT_EQ(res.neighbors.size(), (size_t)1);
        ASSERT_EQ(res.neighbors[0], (int64_t)(r+100));
    }
    index.destroy();
}

TEST(GpuIvfFlatTest, SimulatedShardedBuildSearch) {
    if (gpu_get_device_count() < 1) { TEST_LOG("Skipping SimulatedShardedBuildSearch (no GPU)"); return; }

    const uint32_t dim = 4; const uint64_t count = 64; // 2 shards of 32 (splitter rounds to a multiple of 32)
    std::vector<float> ds(count * dim); std::vector<int64_t> ids(count);
    for (uint64_t i = 0; i < count; ++i) { for (uint32_t j = 0; j < dim; ++j) ds[i*dim+j] = (float)(i+1); ids[i] = (int64_t)(i+100); }

    std::vector<int> sim2 = {0, 0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default(); bp.n_lists = 4;
    gpu_ivf_flat_t<float, float> index(ds.data(), count, dim, DistanceType_L2Expanded, bp, sim2, 2, DistributionMode_SHARDED, ids.data());
    index.start(); index.build();

    ASSERT_TRUE(index.info().find("\"ranks\": 2") != std::string::npos); // 2 shards coexist

    ivf_flat_search_params_t sp = ivf_flat_search_params_default(); sp.n_probes = 4;
    for (int r : {3, 33, 63}) { // rows spanning both shards
        std::vector<float> q(ds.begin()+r*dim, ds.begin()+(r+1)*dim);
        auto res = index.search(q.data(), 1, dim, 1, sp);
        ASSERT_EQ(res.neighbors.size(), (size_t)1);
        ASSERT_EQ(res.neighbors[0], (int64_t)(r+100));
    }
    index.destroy();
}

// SHARDED soft-delete under simulation — exercises the per-shard delete-bitset
// cache (gpu_index_base_t::device_shard_bitsets_ / acquire_delete_bitset_device).
// With the [0,0] device list both shards map to physical device 0; the cache must
// key by RANK, not dev_id. Pre-fix the two shards collided on a single device-0
// entry and one shard reused the other's bitset slice (and the version check then
// skipped re-syncing), so a deleted row in one shard was still returned. Here we
// delete one row in EACH shard and require both to be excluded from their own
// shard's results.
TEST(GpuIvfFlatTest, SimulatedShardedDeleteSearch) {
    if (gpu_get_device_count() < 1) { TEST_LOG("Skipping SimulatedShardedDeleteSearch (no GPU)"); return; }

    const uint32_t dim = 4; const uint64_t count = 64; // 2 shards of 32; offsets 0 / 32
    std::vector<float> ds(count * dim); std::vector<int64_t> ids(count);
    for (uint64_t i = 0; i < count; ++i) { for (uint32_t j = 0; j < dim; ++j) ds[i*dim+j] = (float)(i+1); ids[i] = (int64_t)(i+100); }

    std::vector<int> sim2 = {0, 0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default(); bp.n_lists = 4;
    gpu_ivf_flat_t<float, float> index(ds.data(), count, dim, DistanceType_L2Expanded, bp, sim2, 2, DistributionMode_SHARDED, ids.data());
    index.start(); index.build();
    ASSERT_TRUE(index.info().find("\"ranks\": 2") != std::string::npos);

    // Soft-delete one row per shard: row 10 (id 110) in shard 0 @offset 0, and
    // row 45 (id 145) in shard 1 @offset 32.
    index.delete_id(110);
    index.delete_id(145);

    ivf_flat_search_params_t sp = ivf_flat_search_params_default(); sp.n_probes = 4;

    // Probing a deleted row's own vector must NOT return that id — its shard's
    // delete must apply. Pre-fix, the colliding cache dropped one shard's delete
    // and returned the deleted id. The nearest surviving row (adjacent, +/-1) is
    // returned instead (row 9/11 resp. 44/46 are equidistant — accept either).
    for (int r : {10, 45}) {
        std::vector<float> q(ds.begin()+r*dim, ds.begin()+(r+1)*dim);
        auto res = index.search(q.data(), 1, dim, 1, sp);
        ASSERT_EQ(res.neighbors.size(), (size_t)1);
        ASSERT_NE(res.neighbors[0], (int64_t)(r+100));      // deleted id excluded
        int64_t d = res.neighbors[0] - (int64_t)(r+100);
        ASSERT_TRUE(d == 1 || d == -1);                     // adjacent surviving row
    }

    // Non-deleted rows (one per shard) still return themselves — confirms the
    // deletes did not corrupt the OTHER shard's bitset slice.
    for (int r : {3, 50}) {
        std::vector<float> q(ds.begin()+r*dim, ds.begin()+(r+1)*dim);
        auto res = index.search(q.data(), 1, dim, 1, sp);
        ASSERT_EQ(res.neighbors.size(), (size_t)1);
        ASSERT_EQ(res.neighbors[0], (int64_t)(r+100));
    }
    index.destroy();
}

// Concurrent EXTEND on the same physical device. extend() replicates to every
// rank via submit_all_devices, so under [0,0] two cuVS extends run on device 0
// at once — which raced and crashed pre-fix. The per-device build/extend mutex
// serializes them. Verifies both original and newly-extended rows are findable.
TEST(GpuIvfFlatTest, SimulatedReplicatedExtend) {
    if (gpu_get_device_count() < 1) { TEST_LOG("Skipping SimulatedReplicatedExtend (no GPU)"); return; }

    const uint32_t dim = 4; const uint64_t base = 16, n_ext = 8;
    std::vector<float> ds(base * dim); std::vector<int64_t> ids(base);
    for (uint64_t i = 0; i < base; ++i) { for (uint32_t j = 0; j < dim; ++j) ds[i*dim+j] = (float)(i+1); ids[i] = (int64_t)(i+100); }

    std::vector<int> sim2 = {0, 0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default(); bp.n_lists = 4;
    gpu_ivf_flat_t<float, float> index(ds.data(), base, dim, DistanceType_L2Expanded, bp, sim2, 2, DistributionMode_REPLICATED, ids.data());
    index.start(); index.build();
    ASSERT_TRUE(index.info().find("\"ranks\": 2") != std::string::npos);

    // Extended vectors use a disjoint value range so probes are unambiguous.
    std::vector<float> ext(n_ext * dim); std::vector<int64_t> ext_ids(n_ext);
    for (uint64_t i = 0; i < n_ext; ++i) { for (uint32_t j = 0; j < dim; ++j) ext[i*dim+j] = (float)(i+101); ext_ids[i] = (int64_t)(i+300); }
    index.extend(ext.data(), n_ext, ext_ids.data());

    ivf_flat_search_params_t sp = ivf_flat_search_params_default(); sp.n_probes = 4;
    { std::vector<float> q(ds.begin()+7*dim, ds.begin()+8*dim);   // original row 7 -> id 107
      auto r = index.search(q.data(), 1, dim, 1, sp); ASSERT_EQ(r.neighbors.size(), (size_t)1); ASSERT_EQ(r.neighbors[0], (int64_t)107); }
    { std::vector<float> q(ext.begin()+3*dim, ext.begin()+4*dim); // extended row 3 -> id 303
      auto r = index.search(q.data(), 1, dim, 1, sp); ASSERT_EQ(r.neighbors.size(), (size_t)1); ASSERT_EQ(r.neighbors[0], (int64_t)303); }
    index.destroy();
}

// Index files written under simulation must round-trip across modes: a
// SINGLE/REPLICATED build is interchangeable on load (one index.bin), while a
// SHARDED build (per-shard files) reloads as SHARDED. Verifies load_dir's
// target_mode handling on the files save_dir produced in simulation.
TEST(GpuIvfFlatTest, SimulatedSaveLoadAcrossModes) {
    if (gpu_get_device_count() < 1) { TEST_LOG("Skipping SimulatedSaveLoadAcrossModes (no GPU)"); return; }

    const uint32_t dim = 4; const uint64_t count = 16;
    std::vector<float> ds(count * dim); std::vector<int64_t> ids(count);
    for (uint64_t i = 0; i < count; ++i) { for (uint32_t j = 0; j < dim; ++j) ds[i*dim+j] = (float)(i+1); ids[i] = (int64_t)(i+100); }
    std::vector<int> sim2 = {0, 0}; std::vector<int> one = {0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default(); bp.n_lists = 4;
    ivf_flat_search_params_t sp = ivf_flat_search_params_default(); sp.n_probes = 4;

    auto probe = [&](gpu_ivf_flat_t<float, float>& idx, const std::vector<float>& data, const std::vector<int> rows) {
        for (int r : rows) {
            std::vector<float> q(data.begin()+r*dim, data.begin()+(r+1)*dim);
            auto res = idx.search(q.data(), 1, dim, 1, sp);
            ASSERT_EQ(res.neighbors.size(), (size_t)1);
            ASSERT_EQ(res.neighbors[0], (int64_t)(r+100));
        }
    };

    // (A) REPLICATED (simulated) build -> reload as REPLICATED and as SINGLE.
    std::string dirR = "/tmp/mo_sim_ivf_flat_rep";
    system(("rm -rf " + dirR).c_str());
    {
        gpu_ivf_flat_t<float, float> idx(ds.data(), count, dim, DistanceType_L2Expanded, bp, sim2, 2, DistributionMode_REPLICATED, ids.data());
        idx.start(); idx.build();
        ASSERT_TRUE(idx.info().find("\"ranks\": 2") != std::string::npos);
        idx.save_dir(dirR); idx.destroy();
    }
    {
        gpu_ivf_flat_t<float, float> idx(count, dim, DistanceType_L2Expanded, bp, sim2, 2, DistributionMode_REPLICATED);
        idx.start(); idx.load_dir(dirR, DistributionMode_REPLICATED);
        ASSERT_TRUE(idx.info().find("\"ranks\": 2") != std::string::npos);
        probe(idx, ds, {0, 9, 15}); idx.destroy();
    }
    {
        gpu_ivf_flat_t<float, float> idx(count, dim, DistanceType_L2Expanded, bp, one, 1, DistributionMode_SINGLE_GPU);
        idx.start(); idx.load_dir(dirR, DistributionMode_SINGLE_GPU);
        probe(idx, ds, {0, 9, 15}); idx.destroy();
    }

    // (B) SINGLE build -> fan out to REPLICATED on load.
    std::string dirS = "/tmp/mo_sim_ivf_flat_single";
    system(("rm -rf " + dirS).c_str());
    {
        gpu_ivf_flat_t<float, float> idx(ds.data(), count, dim, DistanceType_L2Expanded, bp, one, 1, DistributionMode_SINGLE_GPU, ids.data());
        idx.start(); idx.build(); idx.save_dir(dirS); idx.destroy();
    }
    {
        gpu_ivf_flat_t<float, float> idx(count, dim, DistanceType_L2Expanded, bp, sim2, 2, DistributionMode_REPLICATED);
        idx.start(); idx.load_dir(dirS, DistributionMode_REPLICATED);
        ASSERT_TRUE(idx.info().find("\"ranks\": 2") != std::string::npos);
        probe(idx, ds, {0, 9, 15}); idx.destroy();
    }

    // (C) SHARDED (simulated) build -> reload as SHARDED.
    const uint64_t scount = 64;
    std::vector<float> sds(scount * dim); std::vector<int64_t> sids(scount);
    for (uint64_t i = 0; i < scount; ++i) { for (uint32_t j = 0; j < dim; ++j) sds[i*dim+j] = (float)(i+1); sids[i] = (int64_t)(i+100); }
    std::string dirSh = "/tmp/mo_sim_ivf_flat_shard";
    system(("rm -rf " + dirSh).c_str());
    {
        gpu_ivf_flat_t<float, float> idx(sds.data(), scount, dim, DistanceType_L2Expanded, bp, sim2, 2, DistributionMode_SHARDED, sids.data());
        idx.start(); idx.build();
        ASSERT_TRUE(idx.info().find("\"ranks\": 2") != std::string::npos);
        idx.save_dir(dirSh); idx.destroy();
    }
    {
        gpu_ivf_flat_t<float, float> idx(scount, dim, DistanceType_L2Expanded, bp, sim2, 2, DistributionMode_SHARDED);
        idx.start(); idx.load_dir(dirSh, DistributionMode_SHARDED);
        ASSERT_TRUE(idx.info().find("\"ranks\": 2") != std::string::npos);
        probe(idx, sds, {3, 40, 63}); idx.destroy();
    }
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
    gpu_ivf_flat_t<float, float> index(dataset.data(), n_base, dimension,
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
    gpu_ivf_flat_t<float, float> index(dataset.data(), n_base, dimension,
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
    gpu_ivf_flat_t<float, float> index(dataset.data(), n_base, dimension,
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
    gpu_ivf_flat_t<float, float> index(dataset.data(), n_base, dimension,
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
    gpu_ivf_flat_t<float, float> index(dataset.data(), n_base, dimension,
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
    gpu_ivf_flat_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED);
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
    gpu_ivf_flat_t<float, float> index(dataset.data(), count, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SINGLE_GPU);
    index.start();
    index.set_filter_columns("[{\"name\":\"cat\",\"type\":1}]", count);
    index.add_filter_chunk(0, cats.data(), nullptr, count);
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
    gpu_ivf_flat_t<float, float> index(dataset.data(), count, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SINGLE_GPU);
    index.start();
    index.set_filter_columns("[{\"name\":\"cat\",\"type\":1}]", count);
    index.add_filter_chunk(0, cats.data(), nullptr, count);
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
    gpu_ivf_flat_t<float, float> index(dataset.data(), count, dimension,
                                DistanceType_L2Expanded, bp, devices, 1,
                                DistributionMode_SINGLE_GPU);
    index.start();
    index.set_filter_columns("[{\"name\":\"cat\",\"type\":1}]", count);
    index.add_filter_chunk(0, cats.data(), nullptr, count);
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

// k > index_size: cuVS rejects without the clamp. search_internal clamps to
// effective_k = min(limit, shard_sz) and pads (-1, FLT_MAX). See filter.hpp.
TEST(GpuIvfFlatTest, KExceedsIndexSizeClampsAndPads) {
    const uint32_t dimension = 8;
    const uint64_t count = 20;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j) {
            dataset[i * dimension + j] = static_cast<float>(i + 1);
        }
    }

    std::vector<int> devices = {0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 2;
    gpu_ivf_flat_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded,
                                bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    std::vector<float> query(dimension, 11.0f);
    const uint32_t limit = 25;
    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    sp.n_probes = 2;

    auto result = index.search(query.data(), 1, dimension, limit, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)limit);
    ASSERT_EQ(result.distances.size(), (size_t)limit);

    std::vector<int64_t> seen;
    for (uint32_t i = 0; i < count; ++i) {
        int64_t n = result.neighbors[i];
        ASSERT_GE(n, 0);
        ASSERT_LT(n, (int64_t)count);
        seen.push_back(n);
    }
    std::sort(seen.begin(), seen.end());
    for (uint32_t i = 1; i < count; ++i) ASSERT_NE(seen[i], seen[i - 1]);

    for (uint32_t i = count; i < limit; ++i) {
        ASSERT_EQ(result.neighbors[i], (int64_t)-1);
        ASSERT_EQ(result.distances[i], std::numeric_limits<float>::max());
    }

    index.destroy();
}

// Multi-query: each row's tail must independently land at sentinels (guards
// the per-row strided scatter in scatter_with_padding).
TEST(GpuIvfFlatTest, MultiQueryKExceedsIndexSize) {
    const uint32_t dimension = 8;
    const uint64_t count = 20;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j) {
            dataset[i * dimension + j] = static_cast<float>(i + 1);
        }
    }

    std::vector<int> devices = {0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 2;
    gpu_ivf_flat_t<float, float> index(dataset.data(), count, dimension, DistanceType_L2Expanded,
                                bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    const uint64_t num_queries = 4;
    const uint32_t limit = 25;
    std::vector<float> queries(num_queries * dimension);
    for (uint64_t q = 0; q < num_queries; ++q) {
        const float v = static_cast<float>(2 * q + 3);
        for (uint32_t j = 0; j < dimension; ++j) queries[q * dimension + j] = v;
    }

    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    sp.n_probes = 2;

    auto result = index.search(queries.data(), num_queries, dimension, limit, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)(num_queries * limit));
    ASSERT_EQ(result.distances.size(), (size_t)(num_queries * limit));

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
