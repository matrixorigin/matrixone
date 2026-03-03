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
    // Cluster 0: near (0, 0)
    // Cluster 1: near (10, 10)
    // Cluster 2: near (20, 20)
    std::vector<float> dataset = {
        0.1f, 0.1f,   0.0f, 0.2f,   0.2f, 0.0f,  // Cluster 0
        10.1f, 10.1f, 10.0f, 10.2f, 10.2f, 10.0f, // Cluster 1
        20.1f, 20.1f, 20.0f, 20.2f, 20.2f, 20.0f  // Cluster 2
    };

    gpu_kmeans_t<float> kmeans(n_clusters, dimension, cuvs::distance::DistanceType::L2Expanded, 20, 0, 1);

    auto fit_res = kmeans.fit(dataset.data(), n_samples);
    ASSERT_GE(fit_res.n_iter, 1);

    auto predict_res = kmeans.predict(dataset.data(), n_samples);
    ASSERT_EQ(predict_res.labels.size(), (size_t)n_samples);

    // Check that points in the same cluster have the same label
    ASSERT_EQ(predict_res.labels[0], predict_res.labels[1]);
    ASSERT_EQ(predict_res.labels[1], predict_res.labels[2]);

    ASSERT_EQ(predict_res.labels[3], predict_res.labels[4]);
    ASSERT_EQ(predict_res.labels[4], predict_res.labels[5]);

    ASSERT_EQ(predict_res.labels[6], predict_res.labels[7]);
    ASSERT_EQ(predict_res.labels[7], predict_res.labels[8]);

    // Check that different clusters have different labels
    ASSERT_NE(predict_res.labels[0], predict_res.labels[3]);
    ASSERT_NE(predict_res.labels[3], predict_res.labels[6]);
    ASSERT_NE(predict_res.labels[0], predict_res.labels[6]);

    kmeans.destroy();
}

TEST(GpuKMeansTest, FitPredict) {
    const uint32_t n_clusters = 2;
    const uint32_t dimension = 4;
    const uint64_t n_samples = 10;
    std::vector<float> dataset(n_samples * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;

    gpu_kmeans_t<float> kmeans(n_clusters, dimension, cuvs::distance::DistanceType::L2Expanded, 20, 0, 1);

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

    kmeans.fit(dataset.data(), n_samples);
    auto centroids = kmeans.get_centroids();

    ASSERT_EQ(centroids.size(), (size_t)(n_clusters * dimension));

    kmeans.destroy();
}
