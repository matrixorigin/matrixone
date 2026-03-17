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

#include "distance.hpp"
#include "test_framework.hpp"
#include <cstdio>
#include <cstdlib>
#include <cuda_fp16.h>
#include <vector>
#include <cmath>

using namespace matrixone;

#define ASSERT_NEAR(val1, val2, abs_error) ASSERT_TRUE(std::abs((val1) - (val2)) <= (abs_error))

TEST(PairwiseDistanceTest, BasicF32) {
    const uint32_t dimension = 3;
    const uint64_t n_x = 2;
    const uint64_t n_y = 2;

    std::vector<float> x = {
        1.0, 0.0, 0.0,
        0.0, 1.0, 0.0
    };
    std::vector<float> y = {
        1.0, 0.0, 0.0,
        0.0, 1.0, 0.0
    };

    std::vector<float> dist(n_x * n_y);
    const raft::resources& res = get_raft_resources();

    pairwise_distance<float>(res, x.data(), n_x, y.data(), n_y, dimension, cuvs::distance::DistanceType::L2Expanded, dist.data());

    // Expected results for L2Squared:
    // dist[0,0] = (1-1)^2 + (0-0)^2 + (0-0)^2 = 0
    // dist[0,1] = (1-0)^2 + (0-1)^2 + (0-0)^2 = 2
    // dist[1,0] = (0-1)^2 + (1-0)^2 + (0-0)^2 = 2
    // dist[1,1] = (0-0)^2 + (1-1)^2 + (0-0)^2 = 0

    ASSERT_NEAR(dist[0], 0.0f, 1e-5f);
    ASSERT_NEAR(dist[1], 2.0f, 1e-5f);
    ASSERT_NEAR(dist[2], 2.0f, 1e-5f);
    ASSERT_NEAR(dist[3], 0.0f, 1e-5f);
}

TEST(PairwiseDistanceTest, BasicF16) {
    const uint32_t dimension = 2;
    const uint64_t n_x = 1;
    const uint64_t n_y = 1;

    std::vector<half> x = {__float2half(1.0f), __float2half(2.0f)};
    std::vector<half> y = {__float2half(1.0f), __float2half(2.0f)};

    std::vector<float> dist(n_x * n_y);
    const raft::resources& res = get_raft_resources();

    pairwise_distance<half>(res, x.data(), n_x, y.data(), n_y, dimension, cuvs::distance::DistanceType::L2Expanded, dist.data());

    ASSERT_NEAR(dist[0], 0.0f, 1e-3f);
}

TEST(PairwiseDistanceTest, InnerProductF32) {
    const uint32_t dimension = 2;
    const uint64_t n_x = 2;
    const uint64_t n_y = 2;

    std::vector<float> x = {
        1.0, 0.0,
        0.0, 1.0
    };
    std::vector<float> y = {
        1.0, 0.0,
        0.0, 1.0
    };

    std::vector<float> dist(n_x * n_y);
    const raft::resources& res = get_raft_resources();

    pairwise_distance<float>(res, x.data(), n_x, y.data(), n_y, dimension, cuvs::distance::DistanceType::InnerProduct, dist.data());

    // Inner product:
    // dist[0,0] = 1*1 + 0*0 = 1
    // dist[0,1] = 1*0 + 0*1 = 0
    // dist[1,0] = 0*1 + 1*0 = 0
    // dist[1,1] = 0*0 + 1*1 = 1

    ASSERT_NEAR(dist[0], 1.0f, 1e-5f);
    ASSERT_NEAR(dist[1], 0.0f, 1e-5f);
    ASSERT_NEAR(dist[2], 0.0f, 1e-5f);
    ASSERT_NEAR(dist[3], 1.0f, 1e-5f);
}
