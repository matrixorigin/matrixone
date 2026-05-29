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
    ASSERT_TRUE(std::abs(result.distances[0] + 1.0) < 1e-5);
    ASSERT_TRUE(std::abs(result.distances[1] + 0.0) < 1e-5);

    index.destroy();
}

TEST(GpuBruteForceTest, EmptyDataset) {
    // Searching an empty (count==0) brute-force index is now an error:
    // there is nothing to search. search_async throws "index not loaded"
    // (the underlying brute_force_index is never constructed when count==0
    // — see brute_force.hpp build()). Callers must check the size of their
    // backing data before submitting a search.
    const uint32_t dimension = 128;
    const uint64_t count = 0;

    gpu_brute_force_t<float> index(nullptr, count, dimension, DistanceType_L2Expanded, 1, 0);
    index.start();
    index.build();

    std::vector<float> queries(dimension, 0.0);
    ASSERT_THROW(
        index.search(queries.data(), 1, dimension, 5, brute_force_search_params_default()),
        std::runtime_error);

    index.destroy();
}

TEST(GpuBruteForceTest, LargeLimit) {
    // Regression test for commit c7b7bf2a ("fix avoid neighbour MAX_INT32 junk
    // and return -1 for invalid neighbour id").
    //
    // When k > count, cuVS brute_force fills the trailing slots of the
    // neighbors buffer with sentinel/junk values (typically UINT32_MAX
    // cast through int64_t = 4294967295). Pre-fix, the empty-host_ids
    // branch in brute_force.hpp passed these through unchanged because
    // it skipped the id-mapping pass entirely. Post-fix, map_neighbor_id
    // is always invoked and bounds-checks raw against local_count, so
    // OOB sentinels collapse to -1.
    //
    // This test exercises the implicit-IDs path (no `ids.data()` passed
    // to the constructor) which is exactly the path commit c7b7bf2a
    // closed. Tightened from the original tolerant form that accepted
    // both -1 and the junk values — accepting the junk would silently
    // regress the fix.
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

    // Strict: OOB slots MUST be -1. If this fails with 4294967295 / 0xFFFFFFFF
    // it means the empty-host_ids branch is bypassing map_neighbor_id again.
    for (int i = 5; i < 10; ++i) {
        ASSERT_EQ(result.neighbors[i], (int64_t)-1);
    }

    index.destroy();
}

// Companion regression test for commit c7b7bf2a — same MAX_INT32 junk
// guard, but with EXPLICIT host_ids populated. The pre-fix code already
// ran the id-mapping pass when host_ids was non-empty, but it had no
// bounds-check on `raw`, so a sentinel raw of UINT32_MAX would have
// indexed wildly past host_ids.size() — typically a crash, but in
// release builds with relaxed bounds checking it could also leak garbage
// from adjacent memory. map_neighbor_id's `raw >= data_size` guard
// short-circuits this before the subscript.
TEST(GpuBruteForceTest, LargeLimitWithExplicitIds) {
    const uint32_t dimension = 2;
    const uint64_t count = 5;
    std::vector<float> dataset(count * dimension, 1.0);
    std::vector<int64_t> ids = {1000, 1001, 1002, 1003, 1004};

    gpu_brute_force_t<float> index(dataset.data(), count, dimension,
                                   DistanceType_L2Expanded, 1, 0, ids.data());
    index.start();
    index.build();

    std::vector<float> queries(dimension, 1.0);
    uint32_t limit = 10;
    auto result = index.search(queries.data(), 1, dimension, limit,
                               brute_force_search_params_default());

    ASSERT_EQ(result.neighbors.size(), (size_t)limit);
    // The first 5 slots are mapped through host_ids — must be in {1000..1004}.
    for (int i = 0; i < 5; ++i) {
        ASSERT_GE(result.neighbors[i], (int64_t)1000);
        ASSERT_LE(result.neighbors[i], (int64_t)1004);
    }
    // Trailing OOB slots: strict -1, never leaking adjacent host_ids slots
    // or raw sentinel values.
    for (int i = 5; i < 10; ++i) {
        ASSERT_EQ(result.neighbors[i], (int64_t)-1);
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

// k > index_size for brute_force. search_internal clamps to
// effective_k = min(limit, local_count) and pads (-1, FLT_MAX).
// brute_force is SINGLE_GPU only; no shard layer involved.
TEST(GpuBruteForceTest, KExceedsIndexSizeClampsAndPads) {
    const uint32_t dimension = 8;
    const uint64_t count = 20;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j) {
            dataset[i * dimension + j] = static_cast<float>(i + 1);
        }
    }

    gpu_brute_force_t<float> index(dataset.data(), count, dimension,
                                   DistanceType_L2Expanded, 1, 0);
    index.start();
    index.build();

    std::vector<float> query(dimension, 11.0f);
    const uint32_t limit = 25;

    auto result = index.search(query.data(), 1, dimension, limit,
                               brute_force_search_params_default());

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

// Multi-query for brute_force's int64_t neighbor scatter path.
TEST(GpuBruteForceTest, MultiQueryKExceedsIndexSize) {
    const uint32_t dimension = 8;
    const uint64_t count = 20;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < count; ++i) {
        for (size_t j = 0; j < dimension; ++j) {
            dataset[i * dimension + j] = static_cast<float>(i + 1);
        }
    }

    gpu_brute_force_t<float> index(dataset.data(), count, dimension,
                                   DistanceType_L2Expanded, 1, 0);
    index.start();
    index.build();

    const uint64_t num_queries = 4;
    const uint32_t limit = 25;
    std::vector<float> queries(num_queries * dimension);
    for (uint64_t q = 0; q < num_queries; ++q) {
        const float v = static_cast<float>(2 * q + 3);
        for (uint32_t j = 0; j < dimension; ++j) queries[q * dimension + j] = v;
    }

    auto result = index.search(queries.data(), num_queries, dimension, limit,
                               brute_force_search_params_default());

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
