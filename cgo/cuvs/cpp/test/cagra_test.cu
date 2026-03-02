#include "cuvs_worker.hpp"
#include "cagra.hpp"
#include "test_framework.hpp"
#include <cstdio>
#include <cstdlib>

using namespace matrixone;

TEST(GpuCagraTest, BasicLoadAndSearch) {
    const uint32_t dimension = 16;
    const uint64_t count = 100;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    std::vector<int> devices = {0};
    gpu_cagra_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, 32, 16, devices, 1);
    index.load();

    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    auto result = index.search(queries.data(), 1, dimension, 5, 16);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0);

    index.destroy();
}

TEST(GpuCagraTest, SaveAndLoadFromFile) {
    const uint32_t dimension = 16;
    const uint64_t count = 100;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    std::string filename = "test_cagra.bin";
    std::vector<int> devices = {0};

    // 1. Build and Save
    {
        gpu_cagra_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, 32, 16, devices, 1);
        index.load();
        index.save(filename);
        index.destroy();
    }

    // 2. Load and Search
    {
        gpu_cagra_t<float> index(filename, dimension, cuvs::distance::DistanceType::L2Expanded, devices, 1);
        index.load();
        
        std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
        auto result = index.search(queries.data(), 1, dimension, 5, 16);
        
        ASSERT_EQ(result.neighbors.size(), (size_t)5);
        ASSERT_EQ(result.neighbors[0], 0);

        index.destroy();
    }

    std::remove(filename.c_str());
}

TEST(GpuCagraTest, ShardedModeSimulation) {
    const uint32_t dimension = 16;
    const uint64_t count = 100;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) dataset[i] = (float)rand() / RAND_MAX;
    
    std::vector<int> devices = {0}; 
    gpu_cagra_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, 32, 16, devices, 1, true); // force_mg = true
    index.load();

    std::vector<float> queries(dataset.begin(), dataset.begin() + dimension);
    auto result = index.search(queries.data(), 1, dimension, 5, 16);

    ASSERT_EQ(result.neighbors.size(), (size_t)5);
    ASSERT_EQ(result.neighbors[0], 0);

    index.destroy();
}
