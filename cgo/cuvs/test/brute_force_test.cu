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
#include "brute_force.hpp"
#include "test_framework.hpp"
#include <cstdio>
#include <cstdlib>
#include <cuda_fp16.h>

using namespace matrixone;

// --- Helper to convert float to half ---
static std::vector<half> float_to_half(const std::vector<float>& src) {
    std::vector<half> dst(src.size());
    for (size_t i = 0; i < src.size(); ++i) {
        dst[i] = __float2half(src[i]);
    }
    return dst;
}

// --- GpuBruteForceTest ---

TEST(GpuBruteForceTest, BasicLoadAndSearch) {
    const uint32_t dimension = 3;
    const uint64_t count = 2;
    std::vector<float> dataset = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0};
    
    gpu_brute_force_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, 1, 0);
    index.start();
    index.build();

    std::vector<float> queries = {1.0, 2.0, 3.0};
    auto result = index.search(queries.data(), 1, dimension, 1, brute_force_search_params_default());

    ASSERT_EQ(result.neighbors.size(), (size_t)1);
    ASSERT_EQ(result.neighbors[0], 0);
    ASSERT_EQ(result.distances[0], 0.0);

    index.destroy();
}

TEST(GpuBruteForceTest, BasicLoadAndSearchWithIds) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    std::vector<int64_t> ids(count);
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j) dataset[i * dimension + j] = (float)rand() / RAND_MAX;
        ids[i] = (int64_t)(i + 3000);
    }
    
    gpu_brute_force_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, 1, 0, ids.data());
    index.start();
    index.build();

    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    auto result = index.search(queries.data(), 1, dimension, 5, brute_force_search_params_default());

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 3000);

    index.destroy();
}

TEST(GpuBruteForceTest, ParallelAddChunkWithOffset) {
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

    gpu_brute_force_t<float> index(total_count, dimension, DistanceType_L2Expanded, 1, 0);
    index.start();

    #include <thread>
    std::thread t1([&]() { index.add_chunk(chunk1.data(), count_per_chunk, 0, ids1.data()); });
    std::thread t2([&]() { index.add_chunk(chunk2.data(), count_per_chunk, count_per_chunk, ids2.data()); });
    t1.join();
    t2.join();

    index.build();

    std::vector<float> queries(chunk2.begin(), chunk2.begin() + dimension);
    auto result = index.search(queries.data(), 1, dimension, 5, brute_force_search_params_default());

    ASSERT_EQ(result.neighbors[0], (int64_t)count_per_chunk);

    index.destroy();
}

TEST(GpuBruteForceTest, SearchWithMultipleQueries) {
    const uint32_t dimension = 4;
    const uint64_t count = 4;
    std::vector<float> dataset = {
        1.0, 0.0, 0.0, 0.0, // ID 0
        0.0, 1.0, 0.0, 0.0, // ID 1
        0.0, 0.0, 1.0, 0.0, // ID 2
        0.0, 0.0, 0.0, 1.0  // ID 3
    };
    
    gpu_brute_force_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, 1, 0);
    index.start();
    index.build();

    std::vector<float> queries = {
        1.0, 0.0, 0.0, 0.0, // Should match ID 0
        0.0, 0.0, 1.0, 0.0  // Should match ID 2
    };
    auto result = index.search(queries.data(), 2, dimension, 1, brute_force_search_params_default());

    ASSERT_EQ(result.neighbors.size(), (size_t)2);
    ASSERT_EQ(result.neighbors[0], 0);
    ASSERT_EQ(result.neighbors[1], 2);

    index.destroy();
}

TEST(GpuBruteForceTest, SearchWithFloat16) {
    const uint32_t dimension = 2;
    const uint64_t count = 2;
    std::vector<float> f_dataset = {1.0, 1.0, 2.0, 2.0};
    std::vector<half> h_dataset = float_to_half(f_dataset);
    
    gpu_brute_force_t<half> index(h_dataset.data(), count, dimension, DistanceType_L2Expanded, 1, 0);
    index.start();
    index.build();

    std::vector<float> f_queries = {1.0, 1.0};
    std::vector<half> h_queries = float_to_half(f_queries);
    auto result = index.search(h_queries.data(), 1, dimension, 1, brute_force_search_params_default());

    ASSERT_EQ(result.neighbors.size(), (size_t)1);
    ASSERT_EQ(result.neighbors[0], 0);
    ASSERT_EQ(result.distances[0], 0.0);

    index.destroy();
}

TEST(GpuBruteForceTest, SearchWithInnerProduct) {
    const uint32_t dimension = 2;
    const uint64_t count = 2;
    std::vector<float> dataset = {
        1.0, 0.0,
        0.0, 1.0
    };
    
    gpu_brute_force_t<float> index(dataset.data(), count, dimension, DistanceType_InnerProduct, 1, 0);
    index.start();
    index.build();

    std::vector<float> queries = {1.0, 0.0};
    auto result = index.search(queries.data(), 1, dimension, 2, brute_force_search_params_default());

    ASSERT_EQ(result.neighbors.size(), (size_t)2);
    ASSERT_EQ(result.neighbors[0], 0);
    ASSERT_EQ(result.neighbors[1], 1);
    
    // dot product should be 1.0 for exact match
    ASSERT_TRUE(std::abs(result.distances[0] - 1.0) < 1e-5);
    ASSERT_TRUE(std::abs(result.distances[1] - 0.0) < 1e-5);

    index.destroy();
}

TEST(GpuBruteForceTest, EmptyDataset) {
    const uint32_t dimension = 128;
    const uint64_t count = 0;
    
    gpu_brute_force_t<float> index(nullptr, count, dimension, DistanceType_L2Expanded, 1, 0);
    index.start();
    index.build();

    std::vector<float> queries(dimension, 0.0);
    auto result = index.search(queries.data(), 1, dimension, 5, brute_force_search_params_default());

    ASSERT_EQ(result.neighbors.size(), (size_t)0);

    index.destroy();
}

TEST(GpuBruteForceTest, LargeLimit) {
    const uint32_t dimension = 2;
    const uint64_t count = 5;
    std::vector<float> dataset(count * dimension, 1.0);
    
    gpu_brute_force_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, 1, 0);
    index.start();
    index.build();

    std::vector<float> queries(dimension, 1.0);
    uint32_t limit = 10;
    auto result = index.search(queries.data(), 1, dimension, limit, brute_force_search_params_default());

    ASSERT_EQ(result.neighbors.size(), (size_t)limit);
    for (int i = 0; i < 5; ++i) ASSERT_GE(result.neighbors[i], 0);
    
    // Neighbors > count might be filled with -1 (int64_t) or 4294967295 (if it was cast from uint32_t -1)
    for (int i = 5; i < 10; ++i) {
        int64_t nid = result.neighbors[i];
        ASSERT_TRUE(nid == -1 || nid == (int64_t)4294967295ULL || nid == (int64_t)0xFFFFFFFF);
    }

    index.destroy();
}

TEST(GpuBruteForceTest, SoftDeleteSearch) {
    const uint32_t dimension = 3;
    const uint64_t count = 3;
    std::vector<float> dataset = {
        1.0, 2.0, 3.0, // ID 0
        4.0, 5.0, 6.0, // ID 1
        7.0, 8.0, 9.0  // ID 2
    };
    
    gpu_brute_force_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, 1, 0);
    index.start();
    index.build();

    // 1. Initial search: point 1 should be the second closest to point 0
    std::vector<float> queries = {1.0, 2.0, 3.0};
    auto result1 = index.search(queries.data(), 1, dimension, 2, brute_force_search_params_default());
    ASSERT_EQ(result1.neighbors[0], 0);
    ASSERT_EQ(result1.neighbors[1], 1);

    // 2. Delete point 1 and search again: point 1 should be gone, point 2 should be the second neighbor
    index.delete_id(1);
    auto result2 = index.search(queries.data(), 1, dimension, 2, brute_force_search_params_default());
    ASSERT_EQ(result2.neighbors[0], 0);
    ASSERT_EQ(result2.neighbors[1], 2);

    // 3. Delete point 0 (the query point itself) and search: point 0 should be gone, point 2 should be first
    index.delete_id(0);
    auto result3 = index.search(queries.data(), 1, dimension, 1, brute_force_search_params_default());
    ASSERT_EQ(result3.neighbors[0], 2);

    index.destroy();
}

TEST(GpuBruteForceTest, SoftDeleteWithCustomIds) {
    const uint32_t dimension = 2;
    const uint64_t count = 3;
    std::vector<float> dataset = {10, 10, 20, 20, 30, 30};
    std::vector<int64_t> ids = {100, 200, 300};
    
    gpu_brute_force_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, 1, 0, ids.data());
    index.start();
    index.build();

    std::vector<float> query = {20, 20};
    auto res1 = index.search(query.data(), 1, dimension, 1, brute_force_search_params_default());
    ASSERT_EQ(res1.neighbors[0], 200);

    // Delete by custom ID
    index.delete_id(200);
    auto res2 = index.search(query.data(), 1, dimension, 1, brute_force_search_params_default());
    // Should now return the next closest point (100 or 300)
    ASSERT_TRUE(res2.neighbors[0] == 100 || res2.neighbors[0] == 300);
    ASSERT_NE(res2.neighbors[0], 200);

    index.destroy();
}

// --- CuvsWorkerTest ---

TEST(CuvsWorkerTest, BruteForceSearch) {
    uint32_t n_threads = 1;
    cuvs_worker_t worker(n_threads, std::vector<int>{0});
    worker.start();

    const uint32_t dimension = 128;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;

    gpu_brute_force_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, 1, 0);
    index.start();
    index.build();

    std::vector<float> queries = std::vector<float>(dataset.begin(), dataset.begin() + dimension);
    auto result = index.search(queries.data(), 1, dimension, 5, brute_force_search_params_default());

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0);

    index.destroy();
    worker.stop();
}

TEST(CuvsWorkerTest, ConcurrentSearches) {
    const uint32_t dimension = 16;
    const uint64_t count = 100;
    std::vector<float> dataset(count * dimension);
    // Use very distinct values to ensure unique neighbors
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j) {
            dataset[i * dimension + j] = (float)i * 100.0f + (float)j;
        }
    }

    gpu_brute_force_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, 4, 0);
    index.start();
    index.build();

    const int num_threads = 4;
    std::vector<std::future<void>> futures;
    for (int i = 0; i < num_threads; ++i) {
        futures.push_back(std::async(std::launch::async, [&index, dimension, &dataset, i]() {
            std::vector<float> query = std::vector<float>(dataset.begin() + i * dimension, dataset.begin() + (i + 1) * dimension);
            auto res = index.search(query.data(), 1, dimension, 1, brute_force_search_params_default());
            ASSERT_EQ(res.neighbors[0], (int64_t)i);
        }));
    }

    for (auto& f : futures) f.get();

    index.destroy();
}
