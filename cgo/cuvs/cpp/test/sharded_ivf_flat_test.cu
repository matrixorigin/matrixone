#include "cuvs_worker.hpp"
#include "sharded_ivf_flat.hpp"
#include "test_framework.hpp"
#include <cstdio>
#include <cstdlib>

using namespace matrixone;

TEST(GpuShardedIvfFlatIndexTest, BasicLoadSearchAndCenters) {
    uint32_t dimension = 16;
    uint64_t count = 100;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) {
        dataset[i] = static_cast<float>(rand()) / RAND_MAX;
    }

    uint32_t n_list = 5;
    uint32_t n_probes = 2;
    uint32_t nthread = 1;
    std::vector<int> devices = {0}; 

    GpuShardedIvfFlatIndex<float> index(dataset.data(), count, dimension, 
                                        cuvs::distance::DistanceType::L2Expanded, 
                                        n_list, devices, nthread);
    index.Load();

    // Verify Centers
    auto centers = index.GetCenters();
    ASSERT_EQ(centers.size(), (size_t)(n_list * dimension));
    TEST_LOG("Sharded centroids retrieved: " << centers.size() / dimension);

    // Verify Search
    std::vector<float> queries(dimension);
    for (size_t i = 0; i < dimension; ++i) queries[i] = dataset[i]; // Search for first vector
    
    auto result = index.Search(queries.data(), 1, dimension, 5, n_probes);
    
    ASSERT_EQ(result.Neighbors.size(), (size_t)5);
    ASSERT_EQ(result.Neighbors[0], 0); // Exact match

    index.Destroy();
}

TEST(GpuShardedIvfFlatIndexTest, SaveAndLoadFromFile) {
    uint32_t dimension = 16;
    uint64_t count = 100;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) {
        dataset[i] = static_cast<float>(rand()) / RAND_MAX;
    }

    uint32_t n_list = 5;
    uint32_t nthread = 1;
    std::vector<int> devices = {0};
    std::string filename = "test_sharded_ivf_flat.bin";

    // 1. Build and Save
    {
        GpuShardedIvfFlatIndex<float> index(dataset.data(), count, dimension, 
                                            cuvs::distance::DistanceType::L2Expanded, 
                                            n_list, devices, nthread);
        index.Load();
        index.Save(filename);
        index.Destroy();
    }

    // 2. Load from file and Search
    {
        GpuShardedIvfFlatIndex<float> index(filename, dimension, 
                                            cuvs::distance::DistanceType::L2Expanded, 
                                            devices, nthread);
        index.Load();
        
        ASSERT_EQ(index.Count, (uint32_t)100);
        ASSERT_EQ(index.NList, (uint32_t)5);

        std::vector<float> queries(dimension);
        for (size_t i = 0; i < dimension; ++i) queries[i] = dataset[i];

        auto result = index.Search(queries.data(), 1, dimension, 5, 2);
        
        ASSERT_EQ(result.Neighbors.size(), (size_t)5);
        ASSERT_EQ(result.Neighbors[0], 0);

        index.Destroy();
    }

    std::remove(filename.c_str());
}
