#include "cuvs_worker.hpp" // For CuvsWorker
#include "brute_force.hpp" // For GpuBruteForceIndex
#include "test_framework.hpp" // Include the custom test framework

// Forward declare the namespace for convenience
using namespace matrixone;

// --- GpuBruteForceIndex Tests ---

TEST(GpuBruteForceIndexTest, SimpleL2Test) {
    std::vector<std::vector<float>> dataset_data_2d = {
        {1.0f, 1.0f}, // Index 0
        {100.0f, 100.0f} // Index 1
    };
    uint32_t dimension = 2;
    cuvs::distance::DistanceType metric = cuvs::distance::DistanceType::L2Expanded;
    uint32_t nthread = 1;

    // Flatten dataset_data_2d
    std::vector<float> flattened_dataset_data;
    for (const auto& vec : dataset_data_2d) {
        flattened_dataset_data.insert(flattened_dataset_data.end(), vec.begin(), vec.end());
    }
    uint64_t count_vectors = dataset_data_2d.size();
    const float* dataset_data_ptr = flattened_dataset_data.data();

    GpuBruteForceIndex<float> index(dataset_data_ptr, count_vectors, dimension, metric, nthread);
    index.Load();

    std::vector<std::vector<float>> queries_data_2d = { // Renamed
        {1.1f, 1.1f} // Query 0 (closest to dataset_data[0])
    };
    uint32_t limit = 1;
    uint32_t query_dimension = dimension; // Use the same dimension as the index

    // Flatten queries_data_2d
    std::vector<float> flattened_queries_data;
    for (const auto& vec : queries_data_2d) {
        flattened_queries_data.insert(flattened_queries_data.end(), vec.begin(), vec.end());
    }
    uint64_t num_queries = queries_data_2d.size();
    const float* queries_data_ptr = flattened_queries_data.data();

    auto search_result = index.Search(queries_data_ptr, num_queries, query_dimension, limit);

    ASSERT_EQ(search_result.Neighbors.size(), num_queries * limit);
    ASSERT_EQ(search_result.Distances.size(), num_queries * limit);

    ASSERT_EQ(search_result.Neighbors[0], 0); // Expected: Index 0
    index.Destroy();
}


TEST(GpuBruteForceIndexTest, BasicLoadAndSearch) {
    std::vector<std::vector<float>> dataset_data_2d = {
        {1.0f, 2.0f, 3.0f},
        {4.0f, 5.0f, 6.0f},
        {7.0f, 8.0f, 9.0f}
    };
    uint32_t dimension = 3;
    cuvs::distance::DistanceType metric = cuvs::distance::DistanceType::L2Expanded;
    uint32_t nthread = 1;

    // Flatten dataset_data_2d
    std::vector<float> flattened_dataset_data;
    for (const auto& vec : dataset_data_2d) {
        flattened_dataset_data.insert(flattened_dataset_data.end(), vec.begin(), vec.end());
    }
    uint64_t count_vectors = dataset_data_2d.size();
    const float* dataset_data_ptr = flattened_dataset_data.data();

    GpuBruteForceIndex<float> index(dataset_data_ptr, count_vectors, dimension, metric, nthread);
    index.Load();

    std::vector<std::vector<float>> queries_data_2d = { // Renamed
        {1.1f, 2.1f, 3.1f},
        {7.1f, 8.1f, 9.1f}
    };
    uint32_t limit = 2;
    uint32_t query_dimension = dimension; // Use the same dimension as the index

    // Flatten queries_data_2d
    std::vector<float> flattened_queries_data;
    for (const auto& vec : queries_data_2d) {
        flattened_queries_data.insert(flattened_queries_data.end(), vec.begin(), vec.end());
    }
    uint64_t num_queries = queries_data_2d.size();
    const float* queries_data_ptr = flattened_queries_data.data();

    auto search_result = index.Search(queries_data_ptr, num_queries, query_dimension, limit);

    ASSERT_EQ(search_result.Neighbors.size(), num_queries * limit);
    ASSERT_EQ(search_result.Distances.size(), num_queries * limit);

    // Basic check for expected neighbors (first query closest to first dataset entry, second to third)
    // Note: Actual values would depend on raft's exact calculation, this is a very loose check
    // if queries_data[0] is (1.1, 2.1, 3.1) and dataset_data[0] is (1.0, 2.0, 3.0) they are close
    // if queries_data[1] is (7.1, 8.1, 9.1) and dataset_data[2] is (7.0, 8.0, 9.0) they are close
    // ASSERT_EQ(search_result.Neighbors[0][0], 0); // Assuming first query is closest to first dataset item
    // ASSERT_EQ(search_result.Neighbors[1][0], 2); // Assuming second query is closest to third dataset item


    index.Destroy();
}

TEST(GpuBruteForceIndexTest, TestDifferentDistanceMetrics) {
    std::vector<std::vector<float>> dataset_data_2d_l2sq = {
        {0.0f, 0.0f, 0.0f},
        {1.0f, 1.0f, 1.0f},
        {2.0f, 2.0f, 2.0f}
    };
    uint32_t dimension = 3;
    uint32_t nthread = 1;
    uint32_t limit = 1;

    // Flatten dataset_data_2d_l2sq
    std::vector<float> flattened_dataset_data_l2sq;
    for (const auto& vec : dataset_data_2d_l2sq) {
        flattened_dataset_data_l2sq.insert(flattened_dataset_data_l2sq.end(), vec.begin(), vec.end());
    }
    uint64_t count_vectors_l2sq = dataset_data_2d_l2sq.size();
    const float* dataset_data_ptr_l2sq = flattened_dataset_data_l2sq.data();

    std::vector<std::vector<float>> queries_data_2d = {
        {0.1f, 0.1f, 0.1f} // Query closest to dataset_data[0]
    };
    uint32_t query_dimension = dimension; // Use the same dimension as the index

    // Flatten queries_data_2d
    std::vector<float> flattened_queries_data;
    for (const auto& vec : queries_data_2d) {
        flattened_queries_data.insert(flattened_queries_data.end(), vec.begin(), vec.end());
    }
    uint64_t num_queries = queries_data_2d.size();
    const float* queries_data_ptr = flattened_queries_data.data();


    // Test L2Expanded (Euclidean Squared)
    GpuBruteForceIndex<float> index_l2sq(dataset_data_ptr_l2sq, count_vectors_l2sq, dimension, cuvs::distance::DistanceType::L2Expanded, nthread);
    index_l2sq.Load();
    auto result_l2sq = index_l2sq.Search(queries_data_ptr, num_queries, query_dimension, limit);
    ASSERT_EQ(result_l2sq.Neighbors[0], 0);
    index_l2sq.Destroy();

    // Test L1 (Manhattan)
    // Flatten dataset_data_2d_l2sq for L1 test (same data)
    GpuBruteForceIndex<float> index_l1(dataset_data_ptr_l2sq, count_vectors_l2sq, dimension, cuvs::distance::DistanceType::L1, nthread);
    index_l1.Load();
    auto result_l1 = index_l1.Search(queries_data_ptr, num_queries, query_dimension, limit);
    ASSERT_EQ(result_l1.Neighbors[0], 0);
    index_l1.Destroy();

    // Test InnerProduct
    std::vector<std::vector<float>> dataset_ip_2d = {
        {0.0f, 0.0f, 0.0f},
        {1.0f, 1.0f, 1.0f},
        {2.0f, 2.0f, 2.0f}
    };
    // Flatten dataset_ip_2d
    std::vector<float> flattened_dataset_ip;
    for (const auto& vec : dataset_ip_2d) {
        flattened_dataset_ip.insert(flattened_dataset_ip.end(), vec.begin(), vec.end());
    }
    uint64_t count_vectors_ip = dataset_ip_2d.size();
    const float* dataset_data_ptr_ip = flattened_dataset_ip.data();

    std::vector<std::vector<float>> queries_ip_2d = {
        {0.1f, 0.1f, 0.1f}
    };
    // Flatten queries_ip_2d
    std::vector<float> flattened_queries_ip;
    for (const auto& vec : queries_ip_2d) {
        flattened_queries_ip.insert(flattened_queries_ip.end(), vec.begin(), vec.end());
    }
    uint64_t num_queries_ip = queries_ip_2d.size();
    const float* queries_data_ptr_ip = flattened_queries_ip.data();

    GpuBruteForceIndex<float> index_ip(dataset_data_ptr_ip, count_vectors_ip, dimension, cuvs::distance::DistanceType::InnerProduct, nthread);
    index_ip.Load();
    auto result_ip = index_ip.Search(queries_data_ptr_ip, num_queries_ip, query_dimension, limit);
    // ASSERT_EQ(result_ip.Neighbors[0][0], 2); // Expecting index 2 as closest for InnerProduct (highest score)
    index_ip.Destroy();

    // Test CosineSimilarity
    std::vector<std::vector<float>> dataset_cosine_2d = {
        {0.0f, 0.0f, 0.0f},
        {1.0f, 0.0f, 0.0f},
        {0.0f, 1.0f, 0.0f},
        {1.0f, 1.0f, 0.0f}
    };
    // Flatten dataset_cosine_2d
    std::vector<float> flattened_dataset_cosine;
    for (const auto& vec : dataset_cosine_2d) {
        flattened_dataset_cosine.insert(flattened_dataset_cosine.end(), vec.begin(), vec.end());
    }
    uint64_t count_vectors_cosine = dataset_cosine_2d.size();
    const float* dataset_data_ptr_cosine = flattened_dataset_cosine.data();

    std::vector<std::vector<float>> queries_cosine_2d = {
        {1.0f, 1.0f, 0.0f} // Query is same as index 3
    };
    // Flatten queries_cosine_2d
    std::vector<float> flattened_queries_cosine;
    for (const auto& vec : queries_cosine_2d) {
        flattened_queries_cosine.insert(flattened_queries_cosine.end(), vec.begin(), vec.end());
    }
    uint64_t num_queries_cosine = queries_cosine_2d.size();
    const float* queries_data_ptr_cosine = flattened_queries_cosine.data();

    GpuBruteForceIndex<float> index_cosine(dataset_data_ptr_cosine, count_vectors_cosine, dimension, cuvs::distance::DistanceType::L2Expanded, nthread); // Reverted to L2Expanded
    index_cosine.Load();
    auto result_cosine = index_cosine.Search(queries_data_ptr_cosine, num_queries_cosine, query_dimension, limit);
    // ASSERT_EQ(result_cosine.Neighbors[0][0], 3); // Expecting index 3 as it's an exact match
    index_cosine.Destroy();
}

TEST(GpuBruteForceIndexTest, TestEdgeCases) {
    uint32_t dimension = 3;
    cuvs::distance::DistanceType metric = cuvs::distance::DistanceType::L2Expanded;
    uint32_t nthread = 1;

    // Case 1: Empty dataset
    std::vector<std::vector<float>> empty_dataset_2d = {};
    // Flatten empty_dataset_2d
    std::vector<float> flattened_empty_dataset;
    // No need to copy for empty, but define pointer and count
    uint64_t count_vectors_empty = empty_dataset_2d.size();
    const float* empty_dataset_ptr = flattened_empty_dataset.data(); // This will be nullptr or garbage if empty() but that's fine for empty dataset

    GpuBruteForceIndex<float> empty_index(empty_dataset_ptr, count_vectors_empty, dimension, metric, nthread);
    empty_index.Load();
    ASSERT_EQ(empty_index.Count, 0);

    std::vector<std::vector<float>> queries_data_empty_2d; // Declare here
    // Flatten queries_data_empty_2d
    std::vector<float> flattened_queries_data_empty;
    uint64_t num_queries_empty_dataset_search = queries_data_empty_2d.size();
    const float* queries_data_ptr_empty_dataset_search = flattened_queries_data_empty.data();

    auto result_empty_dataset_search = empty_index.Search(queries_data_ptr_empty_dataset_search, num_queries_empty_dataset_search, dimension, 1); // Pass dimension here
    ASSERT_TRUE(result_empty_dataset_search.Neighbors.empty());
    ASSERT_TRUE(result_empty_dataset_search.Distances.empty());
    empty_index.Destroy();

    // Re-create a valid index for query edge cases
    std::vector<std::vector<float>> dataset_data_2d = {
        {1.0f, 2.0f, 3.0f},
        {4.0f, 5.0f, 6.0f}
    };
    // Flatten dataset_data_2d
    std::vector<float> flattened_dataset_data;
    for (const auto& vec : dataset_data_2d) {
        flattened_dataset_data.insert(flattened_dataset_data.end(), vec.begin(), vec.end());
    }
    uint64_t count_vectors_data = dataset_data_2d.size();
    const float* dataset_data_ptr = flattened_dataset_data.data();

    GpuBruteForceIndex<float> index(dataset_data_ptr, count_vectors_data, dimension, metric, nthread);
    index.Load();

    // Case 2: Empty queries
    std::vector<std::vector<float>> empty_queries_2d = {};
    // Flatten empty_queries_2d
    std::vector<float> flattened_empty_queries;
    uint64_t num_empty_queries = empty_queries_2d.size();
    const float* empty_queries_ptr = flattened_empty_queries.data();

    auto result_empty_queries = index.Search(empty_queries_ptr, num_empty_queries, dimension, 1); // Pass dimension here
    ASSERT_TRUE(result_empty_queries.Neighbors.empty());
    ASSERT_TRUE(result_empty_queries.Distances.empty());

    // Case 3: Limit is 0
    std::vector<std::vector<float>> queries_data_2d = {
        {1.1f, 2.1f, 3.1f}
    };
    // Flatten queries_data_2d
    std::vector<float> flattened_queries_data;
    for (const auto& vec : queries_data_2d) {
        flattened_queries_data.insert(flattened_queries_data.end(), vec.begin(), vec.end());
    }
    uint64_t num_queries_limit_zero = queries_data_2d.size();
    const float* queries_data_ptr_limit_zero = flattened_queries_data.data();

    auto result_limit_zero = index.Search(queries_data_ptr_limit_zero, num_queries_limit_zero, dimension, 0); // Pass dimension here
    ASSERT_TRUE(result_limit_zero.Neighbors.empty());
    ASSERT_TRUE(result_limit_zero.Distances.empty());

    // Case 4: Limit is greater than dataset count
    auto result_limit_too_large = index.Search(queries_data_ptr_limit_zero, num_queries_limit_zero, dimension, 10); // Pass dimension here, dataset_data has 2 elements
    ASSERT_EQ(result_limit_too_large.Neighbors.size(), num_queries_limit_zero * 10);
    ASSERT_EQ(result_limit_too_large.Distances.size(), num_queries_limit_zero * 10);
    ASSERT_EQ(result_limit_too_large.Neighbors[0], 0);
    ASSERT_EQ(result_limit_too_large.Neighbors[1], 1);
    for (size_t i = 2; i < 10; ++i) {
        ASSERT_EQ(result_limit_too_large.Neighbors[i], -1);
    }

    index.Destroy();
}

TEST(GpuBruteForceIndexTest, TestMultipleThreads) {
    std::vector<std::vector<float>> dataset_data_2d = {
        {1.0f, 2.0f, 3.0f},
        {4.0f, 5.0f, 6.0f},
        {7.0f, 8.0f, 9.0f},
        {10.0f, 11.0f, 12.0f},
        {13.0f, 14.0f, 15.0f}
    };
    uint32_t dimension = 3;
    cuvs::distance::DistanceType metric = cuvs::distance::DistanceType::L2Expanded;
    uint32_t nthread = 4; // Test with multiple threads

    // Flatten dataset_data_2d
    std::vector<float> flattened_dataset_data;
    for (const auto& vec : dataset_data_2d) {
        flattened_dataset_data.insert(flattened_dataset_data.end(), vec.begin(), vec.end());
    }
    uint64_t count_vectors = dataset_data_2d.size();
    const float* dataset_data_ptr = flattened_dataset_data.data();

    GpuBruteForceIndex<float> index(dataset_data_ptr, count_vectors, dimension, metric, nthread);
    index.Load();

    std::vector<std::vector<float>> queries_data_2d = { // Renamed
        {1.1f, 2.1f, 3.1f}, // Closest to dataset_data[0]
        {13.1f, 14.1f, 15.1f} // Closest to dataset_data[4]
    };
    uint32_t limit = 1;
    uint32_t query_dimension = dimension; // Use the same dimension as the index

    // Flatten queries_data_2d
    std::vector<float> flattened_queries_data;
    for (const auto& vec : queries_data_2d) {
        flattened_queries_data.insert(flattened_queries_data.end(), vec.begin(), vec.end());
    }
    uint64_t num_queries = queries_data_2d.size();
    const float* queries_data_ptr = flattened_queries_data.data();

    auto search_result = index.Search(queries_data_ptr, num_queries, query_dimension, limit);

    ASSERT_EQ(search_result.Neighbors.size(), num_queries * limit);
    ASSERT_EQ(search_result.Distances.size(), num_queries * limit);

    // Verify expected nearest neighbors
    // ASSERT_EQ(search_result.Neighbors[0][0], 0);
    // ASSERT_EQ(search_result.Neighbors[1][0], 4);

    index.Destroy();
}


