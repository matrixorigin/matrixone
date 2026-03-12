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
    gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.load();

    // Verify centers
    auto centers = index.get_centers();
    ASSERT_TRUE(centers.size() % index.get_n_list() == 0);
    ASSERT_EQ(centers.size(), (size_t)(index.get_n_list() * index.get_dim_ext()));

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
        gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.load();
        index.save(filename);
        index.destroy();
    }

    // 2. Load and Search
    {
        ivf_pq_build_params_t bp = ivf_pq_build_params_default();
        bp.n_lists = 2;
        bp.m = 2;
        gpu_ivf_pq_t<float> index(filename, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
        index.load();
        
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

    std::string data_filename = "test_dataset.modf";
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

    gpu_ivf_pq_t<float> index(data_filename, cuvs::distance::DistanceType::L2Expanded, bp, devices, 1, DistributionMode_SINGLE_GPU);
    index.load();

    ASSERT_EQ(index.get_dim(), dimension);
    ASSERT_EQ(index.count, static_cast<uint32_t>(count));

    std::vector<float> queries(dimension, 0.0f);
    ivf_pq_search_params_t sp = ivf_pq_search_params_default();
    auto result = index.search(queries.data(), 1, dimension, 1, sp);

    ASSERT_EQ(result.neighbors.size(), (size_t)1);
    
    index.destroy();
    std::remove(data_filename.c_str());
}
