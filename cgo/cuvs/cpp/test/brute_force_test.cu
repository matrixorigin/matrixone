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
    
    gpu_brute_force_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, 1, 0);
    index.load();

    std::vector<float> queries = {1.0, 2.0, 3.0};
    auto result = index.search(queries.data(), 1, dimension, 1);

    ASSERT_EQ(result.neighbors.size(), (size_t)1);
    ASSERT_EQ(result.neighbors[0], 0);
    ASSERT_EQ(result.distances[0], 0.0);

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
    
    gpu_brute_force_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, 1, 0);
    index.load();

    std::vector<float> queries = {
        1.0, 0.0, 0.0, 0.0, // Should match ID 0
        0.0, 0.0, 1.0, 0.0  // Should match ID 2
    };
    auto result = index.search(queries.data(), 2, dimension, 1);

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
    
    gpu_brute_force_t<half> index(h_dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, 1, 0);
    index.load();

    std::vector<float> f_queries = {1.0, 1.0};
    std::vector<half> h_queries = float_to_half(f_queries);
    auto result = index.search(h_queries.data(), 1, dimension, 1);

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
    
    gpu_brute_force_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::InnerProduct, 1, 0);
    index.load();

    std::vector<float> queries = {1.0, 0.0};
    auto result = index.search(queries.data(), 1, dimension, 2);

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
    
    gpu_brute_force_t<float> index(nullptr, count, dimension, cuvs::distance::DistanceType::L2Expanded, 1, 0);
    index.load();

    std::vector<float> queries(dimension, 0.0);
    auto result = index.search(queries.data(), 1, dimension, 5);

    ASSERT_EQ(result.neighbors.size(), (size_t)0);

    index.destroy();
}

TEST(GpuBruteForceTest, LargeLimit) {
    const uint32_t dimension = 2;
    const uint64_t count = 5;
    std::vector<float> dataset(count * dimension, 1.0);
    
    gpu_brute_force_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, 1, 0);
    index.load();

    std::vector<float> queries(dimension, 1.0);
    uint32_t limit = 10;
    auto result = index.search(queries.data(), 1, dimension, limit);

    ASSERT_EQ(result.neighbors.size(), (size_t)limit);
    for (int i = 0; i < 5; ++i) ASSERT_GE(result.neighbors[i], 0);
    for (int i = 5; i < 10; ++i) ASSERT_EQ(result.neighbors[i], -1);

    index.destroy();
}

// --- CuvsWorkerTest ---

TEST(CuvsWorkerTest, BruteForceSearch) {
    uint32_t n_threads = 1;
    cuvs_worker_t worker(n_threads);
    worker.start();

    const uint32_t dimension = 128;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;

    gpu_brute_force_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, 1, 0);
    index.load();

    std::vector<float> queries = std::vector<float>(dataset.begin(), dataset.begin() + dimension);
    auto result = index.search(queries.data(), 1, dimension, 5);

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

    gpu_brute_force_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, 4, 0);
    index.load();

    const int num_threads = 4;
    std::vector<std::future<void>> futures;
    for (int i = 0; i < num_threads; ++i) {
        futures.push_back(std::async(std::launch::async, [&index, dimension, &dataset, i]() {
            std::vector<float> query = std::vector<float>(dataset.begin() + i * dimension, dataset.begin() + (i + 1) * dimension);
            auto res = index.search(query.data(), 1, dimension, 1);
            ASSERT_EQ(res.neighbors[0], i);
        }));
    }

    for (auto& f : futures) f.get();

    index.destroy();
}
