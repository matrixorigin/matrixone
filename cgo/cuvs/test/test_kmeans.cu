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
#include "kmeans.hpp"
#include <iostream>
#include <vector>
#include <chrono>
#include <random>

using namespace matrixone;

int main() {
    const uint32_t n_clusters = 17857;
    const uint32_t dimension = 1024;
    const uint64_t n_samples = 350000;

    std::cout << "Generating " << n_samples << " random samples with " << dimension << " dimensions..." << std::endl;
    
    std::vector<float> dataset(n_samples * dimension);
    std::mt19937 gen(42);
    std::uniform_real_distribution<float> dis(0.0, 1.0);
    for (size_t i = 0; i < dataset.size(); ++i) {
        dataset[i] = dis(gen);
    }

    std::cout << "Initializing KMeans with " << n_clusters << " clusters..." << std::endl;
    // Constructor: n_clusters, dimension, metric, max_iter, device_id, nthread
    // Using DistanceType_L2Expanded as it's common for KMeans
    gpu_kmeans_t<float> kmeans(n_clusters, dimension, DistanceType_L2Expanded, 20, 0, 1);
    kmeans.start();

    std::cout << "Performing KMeans fit..." << std::endl;
    auto start = std::chrono::high_resolution_clock::now();
    
    auto fit_res = kmeans.fit(dataset.data(), n_samples);
    
    auto end = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> diff = end - start;

    std::cout << "KMeans fit finished in " << diff.count() << " seconds." << std::endl;
    std::cout << "Inertia: " << fit_res.inertia << ", Iterations: " << fit_res.n_iter << std::endl;

    kmeans.destroy();
    return 0;
}
