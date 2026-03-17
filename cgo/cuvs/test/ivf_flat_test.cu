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
    gpu_ivf_flat_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
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
        gpu_ivf_flat_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.start();
        index.build();
        index.save(filename);
        index.destroy();
    }

    // 2. Load and Search
    {
        ivf_flat_build_params_t bp = ivf_flat_build_params_default();
        bp.n_lists = 2;
        gpu_ivf_flat_t<float> index(filename, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.start();
        index.build();

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

TEST(GpuIvfFlatTest, ShardedModeSimulation) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)i / dataset.size();
    
    std::vector<int> devices = {0}; 
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 5;
    gpu_ivf_flat_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_SHARDED);
    index.start();
    index.build();

    auto centers = index.get_centers();
    ASSERT_EQ(centers.size(), (size_t)(5 * dimension));

    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    sp.n_probes = 2;
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0u);

    index.destroy();
}

TEST(GpuIvfFlatTest, ReplicatedModeSimulation) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    int dev_count = gpu_get_device_count();
    ASSERT_TRUE(dev_count > 0);
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 10;
    gpu_ivf_flat_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_REPLICATED);
    index.start();
    index.build();
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_flat_search_params_t sp = ivf_flat_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0u);

    index.destroy();
}

TEST(GpuIvfFlatTest, SetGetQuantizer) {
    const uint32_t dimension = 4;
    const uint64_t count = 10;
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    std::vector<int> devices = {0};
    
    gpu_ivf_flat_t<int8_t> index(count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    
    float min = -1.5f;
    float max = 2.5f;
    index.set_quantizer(min, max);
    
    float gMin = 0, gMax = 0;
    index.get_quantizer(&gMin, &gMax);
    
    ASSERT_EQ(min, gMin);
    ASSERT_EQ(max, gMax);
    
    index.destroy();
}

