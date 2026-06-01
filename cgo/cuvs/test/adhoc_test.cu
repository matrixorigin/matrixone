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

#include "adhoc.hpp"
#include "helper.h"
#include "test_framework.hpp"
#include <cstdint>
#include <limits>
#include <vector>

using namespace matrixone;

// k > n_rows: cuVS would reject without the clamp. adhoc_brute_force_search
// clamps to effective_k = min(limit, n_rows) and pads the caller's
// (n_queries × limit) buffers with (-1, FLT_MAX). Mirrors the persistent-index
// path regression test (e.g. GpuBruteForceTest::KExceedsIndexSizeClampsAndPads).
TEST(AdhocBruteForceTest, KExceedsIndexSizeClampsAndPads) {
    const uint32_t dimension = 8;
    const uint64_t n_rows = 20;
    std::vector<float> dataset(n_rows * dimension);
    for (size_t i = 0; i < n_rows; ++i) {
        for (size_t j = 0; j < dimension; ++j) {
            dataset[i * dimension + j] = static_cast<float>(i + 1);
        }
    }

    std::vector<float> query(dimension, 11.0f);
    const uint32_t limit = 25;
    std::vector<int64_t> neighbors(limit);
    std::vector<float>   distances(limit);

    const auto& res = get_raft_resources();
    adhoc_brute_force_search<float>(res, dataset.data(), n_rows, dimension,
                                    query.data(), /*n_queries=*/1, limit,
                                    cuvs::distance::DistanceType::L2Expanded,
                                    neighbors.data(), distances.data());

    // First n_rows slots: valid permutation of [0, n_rows).
    std::vector<int64_t> seen;
    for (uint32_t i = 0; i < n_rows; ++i) {
        ASSERT_GE(neighbors[i], 0);
        ASSERT_LT(neighbors[i], (int64_t)n_rows);
        seen.push_back(neighbors[i]);
    }
    std::sort(seen.begin(), seen.end());
    for (uint32_t i = 1; i < n_rows; ++i) ASSERT_NE(seen[i], seen[i - 1]);

    // Tail [n_rows, limit) MUST be (-1, FLT_MAX).
    for (uint32_t i = n_rows; i < limit; ++i) {
        ASSERT_EQ(neighbors[i], (int64_t)-1);
        ASSERT_EQ(distances[i], std::numeric_limits<float>::max());
    }
}

// Multi-query: per-row tail sentinels (guards scatter_with_padding's row
// stride — adhoc is rarely called with n_queries > 1 in production, so this
// is the only structural guard).
TEST(AdhocBruteForceTest, MultiQueryKExceedsIndexSize) {
    const uint32_t dimension = 8;
    const uint64_t n_rows = 20;
    std::vector<float> dataset(n_rows * dimension);
    for (size_t i = 0; i < n_rows; ++i) {
        for (size_t j = 0; j < dimension; ++j) {
            dataset[i * dimension + j] = static_cast<float>(i + 1);
        }
    }

    const uint64_t n_queries = 4;
    const uint32_t limit = 25;
    std::vector<float> queries(n_queries * dimension);
    for (uint64_t q = 0; q < n_queries; ++q) {
        const float v = static_cast<float>(2 * q + 3);
        for (uint32_t j = 0; j < dimension; ++j) queries[q * dimension + j] = v;
    }

    std::vector<int64_t> neighbors(n_queries * limit);
    std::vector<float>   distances(n_queries * limit);

    const auto& res = get_raft_resources();
    adhoc_brute_force_search<float>(res, dataset.data(), n_rows, dimension,
                                    queries.data(), n_queries, limit,
                                    cuvs::distance::DistanceType::L2Expanded,
                                    neighbors.data(), distances.data());

    for (uint64_t q = 0; q < n_queries; ++q) {
        for (uint32_t i = 0; i < n_rows; ++i) {
            int64_t n = neighbors[q * limit + i];
            ASSERT_GE(n, 0);
            ASSERT_LT(n, (int64_t)n_rows);
        }
        for (uint32_t i = n_rows; i < limit; ++i) {
            ASSERT_EQ(neighbors[q * limit + i], (int64_t)-1);
            ASSERT_EQ(distances[q * limit + i], std::numeric_limits<float>::max());
        }
    }
}

// Empty dataset: effective_k == 0 path. Caller's buffers must come back
// all-sentinel; no GPU work issued.
TEST(AdhocBruteForceTest, EmptyDatasetReturnsSentinels) {
    const uint32_t dimension = 4;
    const uint64_t n_rows = 0;
    std::vector<float> dataset; // empty

    std::vector<float> query(dimension, 1.0f);
    const uint32_t limit = 5;
    std::vector<int64_t> neighbors(limit, /*garbage=*/77);
    std::vector<float>   distances(limit, 99.0f);

    const auto& res = get_raft_resources();
    adhoc_brute_force_search<float>(res, dataset.data(), n_rows, dimension,
                                    query.data(), /*n_queries=*/1, limit,
                                    cuvs::distance::DistanceType::L2Expanded,
                                    neighbors.data(), distances.data());

    for (uint32_t i = 0; i < limit; ++i) {
        ASSERT_EQ(neighbors[i], (int64_t)-1);
        ASSERT_EQ(distances[i], std::numeric_limits<float>::max());
    }
}

// Sanity: when limit < n_rows (the no-clamp path), behavior is unchanged.
// Asserts the fast path still returns correctly ordered top-k.
TEST(AdhocBruteForceTest, LimitLessThanNRowsUnchanged) {
    const uint32_t dimension = 4;
    const uint64_t n_rows = 50;
    std::vector<float> dataset(n_rows * dimension);
    for (size_t i = 0; i < n_rows; ++i) {
        for (size_t j = 0; j < dimension; ++j) {
            dataset[i * dimension + j] = static_cast<float>(i);
        }
    }

    std::vector<float> query(dimension, 10.0f);
    const uint32_t limit = 5;
    std::vector<int64_t> neighbors(limit);
    std::vector<float>   distances(limit);

    const auto& res = get_raft_resources();
    adhoc_brute_force_search<float>(res, dataset.data(), n_rows, dimension,
                                    query.data(), /*n_queries=*/1, limit,
                                    cuvs::distance::DistanceType::L2Expanded,
                                    neighbors.data(), distances.data());

    // Top-1 MUST be id=10 (exact match: dataset[10] = [10,10,10,10]).
    ASSERT_EQ(neighbors[0], (int64_t)10);
    ASSERT_NE(neighbors[1], (int64_t)-1);
    ASSERT_NE(neighbors[2], (int64_t)-1);
}
