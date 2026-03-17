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
#include "ivf_flat.hpp"
#include "ivf_pq.hpp"
#include "helper.h"
#include "test_framework.hpp"
#include <vector>
#include <thread>
#include <future>

using namespace matrixone;

TEST(DynamicBatchingTest, CagraConcurrentSearch) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)i / count;

    std::vector<int> devices = {0};
    cagra_build_params_t bp = cagra_build_params_default();
    gpu_cagra_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 8, DistributionMode_SINGLE_GPU);
    
    index.set_use_batching(true);
    index.start();
    index.build();

    const int num_threads = 8;
    std::vector<std::future<cagra_search_result_t>> futures;
    
    for (int i = 0; i < num_threads; ++i) {
        futures.push_back(std::async(std::launch::async, [&index, dimension, i]() {
            std::vector<float> query(dimension);
            for (uint32_t j = 0; j < dimension; ++j) query[j] = (float)i / 10.0f;
            cagra_search_params_t sp = cagra_search_params_default();
            return index.search(query.data(), 1, dimension, 5, sp);
        }));
    }

    for (auto& f : futures) {
        auto res = f.get();
        ASSERT_EQ(res.neighbors.size(), (size_t)5);
    }

    index.destroy();
}

TEST(DynamicBatchingTest, IvfFlatConcurrentSearch) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)i / count;

    std::vector<int> devices = {0};
    ivf_flat_build_params_t bp = ivf_flat_build_params_default();
    bp.n_lists = 10;
    gpu_ivf_flat_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 8, DistributionMode_SINGLE_GPU);
    
    index.set_use_batching(true);
    index.start();
    index.build();

    const int num_threads = 8;
    std::vector<std::future<ivf_flat_search_result_t>> futures;
    
    for (int i = 0; i < num_threads; ++i) {
        futures.push_back(std::async(std::launch::async, [&index, dimension, i]() {
            std::vector<float> query(dimension);
            for (uint32_t j = 0; j < dimension; ++j) query[j] = (float)i / 10.0f;
            ivf_flat_search_params_t sp = ivf_flat_search_params_default();
            return index.search(query.data(), 1, dimension, 5, sp);
        }));
    }

    for (auto& f : futures) {
        auto res = f.get();
        ASSERT_EQ(res.neighbors.size(), (size_t)5);
    }

    index.destroy();
}

TEST(DynamicBatchingTest, IvfPqConcurrentSearch) {
    const uint32_t dimension = 16;
    const uint64_t count = 1000;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)i / count;

    std::vector<int> devices = {0};
    ivf_pq_build_params_t bp = ivf_pq_build_params_default();
    bp.n_lists = 10;
    bp.m = 8;
    gpu_ivf_pq_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, bp, devices, 8, DistributionMode_SINGLE_GPU);
    
    index.set_use_batching(true);
    index.start();
    index.build();

    const int num_threads = 8;
    std::vector<std::future<ivf_pq_search_result_t>> futures;
    
    for (int i = 0; i < num_threads; ++i) {
        futures.push_back(std::async(std::launch::async, [&index, dimension, i]() {
            std::vector<float> query(dimension);
            for (uint32_t j = 0; j < dimension; ++j) query[j] = (float)i / 10.0f;
            ivf_pq_search_params_t sp = ivf_pq_search_params_default();
            return index.search(query.data(), 1, dimension, 5, sp);
        }));
    }

    for (auto& f : futures) {
        auto res = f.get();
        ASSERT_EQ(res.neighbors.size(), (size_t)5);
    }

    index.destroy();
}
