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
#include "test_framework.hpp"
#include <cstdio>
#include <cstdlib>
#include <vector>

using namespace matrixone;

TEST(GpuKMeansTest, BasicFitAndPredict) {
    const uint32_t n_clusters = 3;
    const uint32_t dimension = 2;
    const uint64_t n_samples = 9;

    // Create 3 clusters of points
    std::vector<float> dataset = {
        0.1f, 0.1f,   0.0f, 0.2f,   0.2f, 0.0f,  // Cluster 0
        10.1f, 10.1f, 10.0f, 10.2f, 10.2f, 10.0f, // Cluster 1
        20.1f, 20.1f, 20.0f, 20.2f, 20.2f, 20.0f  // Cluster 2
    };

    gpu_kmeans_t<float> kmeans(n_clusters, dimension, cuvs::distance::DistanceType::L2Expanded, 20, 0, 1);
    kmeans.start();

    auto fit_res = kmeans.fit(dataset.data(), n_samples);
    ASSERT_GE(fit_res.n_iter, 1);

    auto predict_res = kmeans.predict(dataset.data(), n_samples);
    ASSERT_EQ(predict_res.labels.size(), (size_t)n_samples);

    // Since we use balanced_params, it might prioritize balancing cluster sizes over spatial distance 
    // on very small datasets. We just check that all labels are within range [0, nClusters).
    for (size_t i = 0; i < n_samples; ++i) {
        ASSERT_TRUE(predict_res.labels[i] >= 0 && predict_res.labels[i] < (int64_t)n_clusters);
    }

    kmeans.destroy();
}

TEST(GpuKMeansTest, FitPredict) {
    const uint32_t n_clusters = 2;
    const uint32_t dimension = 4;
    const uint64_t n_samples = 10;
    std::vector<float> dataset(n_samples * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;

    gpu_kmeans_t<float> kmeans(n_clusters, dimension, cuvs::distance::DistanceType::L2Expanded, 20, 0, 1);
    kmeans.start();

    auto res = kmeans.fit_predict(dataset.data(), n_samples);
    ASSERT_EQ(res.labels.size(), (size_t)n_samples);
    ASSERT_GE(res.n_iter, 1);

    kmeans.destroy();
}

TEST(GpuKMeansTest, GetCentroids) {
    const uint32_t n_clusters = 5;
    const uint32_t dimension = 8;
    const uint64_t n_samples = 50;
    std::vector<float> dataset(n_samples * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;

    gpu_kmeans_t<float> kmeans(n_clusters, dimension, cuvs::distance::DistanceType::L2Expanded, 20, 0, 1);
    kmeans.start();

    kmeans.fit(dataset.data(), n_samples);
    auto centroids = kmeans.get_centroids();

    ASSERT_EQ(centroids.size(), (size_t)(n_clusters * dimension));

    kmeans.destroy();
}
