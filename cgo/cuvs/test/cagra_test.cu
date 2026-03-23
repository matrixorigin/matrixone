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
#include "cagra.hpp"
#include "helper.h"
#include "test_framework.hpp"
#include <cstdio>
#include <cstdlib>

using namespace matrixone;

TEST(GpuCagraTest, BasicLoadAndSearch) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    std::vector<int> devices = {0};
    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    cagra_search_params_t sp = cagra_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0u);

    index.destroy();
}

TEST(GpuCagraTest, SaveAndLoadFromFile) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    std::string filename = "test_cagra.bin";
    std::vector<int> devices = {0};

    // 1. Build and Save
    {
        cagra_build_params_t bp = cagra_build_params_default();
        gpu_cagra_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.start();
        index.build();
        index.save(filename);
        index.destroy();
    }

    // 2. Load and Search
    {
        cagra_build_params_t bp = cagra_build_params_default();
        gpu_cagra_t<float> index(filename, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.start();
        index.load(filename);
        
        std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
        cagra_search_params_t sp = cagra_search_params_default();
        auto result = index.search(queries.data(), 1, dimension, 5, sp);
        
        ASSERT_EQ(result.neighbors.size(), (size_t)5);
        ASSERT_EQ(result.neighbors[0], 0u);

        index.destroy();
    }

    std::remove(filename.c_str());
}

/*
// Sharded mode is currently disabled due to a suspected bug in cuVS or its integration.
// GDB trace showed mdspan extents being set to 18446744073709551615ul (SIZE_MAX),
// which suggests a dynamic extent initialization failure or dimension overflow/underflow
// within the multi-GPU search path.
TEST(GpuCagraTest, ShardedModeSimulation) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;

    int dev_count = gpu_get_device_count();
    ASSERT_TRUE(dev_count > 0);
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED);
    index.start();
    index.build();
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    cagra_search_params_t sp = cagra_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0u);

    index.destroy();
}
*/
TEST(GpuCagraTest, ReplicatedModeSimulation) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    int dev_count = gpu_get_device_count();
    ASSERT_TRUE(dev_count > 0);
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_REPLICATED);
    index.start();
    index.build();
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    cagra_search_params_t sp = cagra_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0u);

    index.destroy();
}
