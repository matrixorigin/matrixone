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
    gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
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
        gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
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
        gpu_ivf_pq_t<float> index(filename, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
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

TEST(GpuIvfPqTest, BuildFromDataFile) {
    const uint32_t dimension = 8;
    const uint64_t count = 100;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) {
        dataset[i] = static_cast<float>(i % 10);
    }

    std::string data_filename = "test_dataset_pq.modf";
    {
        // Use our utility to save the dataset in MODF format
        raft::resources res;
        auto matrix = raft::make_host_matrix<float, int64_t>(count, dimension);
        std::copy(dataset.begin(), dataset.end(), matrix.data_handle());
        save_host_matrix(data_filename, matrix.view());
    }

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 10;
    bp.m = 4;

    gpu_ivf_pq_t<float> index(data_filename, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.start();
    index.build();

    ASSERT_EQ(index.get_dim(), dimension);
    ASSERT_EQ(index.count, static_cast<uint32_t>(count));

    std::vector<float> queries(dimension, 0.0f);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 1, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)1);
    
    index.destroy();
    std::remove(data_filename.c_str());
}

TEST(GpuIvfPqTest, ShardedModeSimulation) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    int dev_count = gpu_get_device_count();
    if (dev_count > 4) dev_count = 4;
    std::vector<int> devices(dev_count);
    gpu_get_device_list(devices.data(), dev_count);

    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 10 * dev_count;
    bp.m = 8;
    gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_SHARDED);
    index.start();
    index.build();
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0);

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
    bp.n_lists = 10;
    bp.m = 8;
    gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, DistanceType_L2Expanded, bp, devices, 1, DistributionMode_REPLICATED);
    index.start();
    index.build();
    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 5, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0);

    index.destroy();
}
