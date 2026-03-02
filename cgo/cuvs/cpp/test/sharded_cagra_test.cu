#include "cuvs_worker.hpp"
#include "sharded_cagra.hpp"
#include "test_framework.hpp"
#include <cstdio>
#include <cstdlib>

using namespace matrixone;

TEST(GpuShardedCagraIndexTest, BasicLoadAndSearch) {
    uint32_t dimension = 16;
    uint64_t count = 100;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) {
        dataset[i] = static_cast<float>(rand()) / RAND_MAX;
    }

    size_t intermediate_graph_degree = 64;
    size_t graph_degree = 32;
    uint32_t nthread = 1;
    std::vector<int> devices = {0};

    GpuShardedCagraIndex<float> index(dataset.data(), count, dimension, 
                                      cuvs::distance::DistanceType::L2Expanded, 
                                      intermediate_graph_degree, graph_degree, devices, nthread);
    index.Load();

    // Verify Search
    std::vector<float> queries(dimension);
    for (size_t i = 0; i < dimension; ++i) queries[i] = dataset[i]; 
    
    auto result = index.Search(queries.data(), 1, dimension, 5, 32);
    
    ASSERT_EQ(result.Neighbors.size(), (size_t)5);
    ASSERT_EQ(result.Neighbors[0], 0);

    index.Destroy();
}

TEST(GpuShardedCagraIndexTest, SaveAndLoadFromFile) {
    uint32_t dimension = 16;
    uint64_t count = 100;
    std::vector<float> dataset(count * dimension);
    for (size_t i = 0; i < dataset.size(); ++i) {
        dataset[i] = static_cast<float>(rand()) / RAND_MAX;
    }

    size_t intermediate_graph_degree = 64;
    size_t graph_degree = 32;
    uint32_t nthread = 1;
    std::vector<int> devices = {0};
    std::string filename = "test_sharded_cagra.bin";

    // 1. Build and Save
    {
        GpuShardedCagraIndex<float> index(dataset.data(), count, dimension, 
                                          cuvs::distance::DistanceType::L2Expanded, 
                                          intermediate_graph_degree, graph_degree, devices, nthread);
        index.Load();
        index.Save(filename);
        index.Destroy();
    }

    // 2. Load from file and Search
    {
        GpuShardedCagraIndex<float> index(filename, dimension, 
                                          cuvs::distance::DistanceType::L2Expanded, 
                                          devices, nthread);
        index.Load();
        
        ASSERT_EQ(index.Count, (uint32_t)100);
        ASSERT_EQ(index.GraphDegree, graph_degree);

        std::vector<float> queries(dimension);
        for (size_t i = 0; i < dimension; ++i) queries[i] = dataset[i];

        auto result = index.Search(queries.data(), 1, dimension, 5, 32);
        
        ASSERT_EQ(result.Neighbors.size(), (size_t)5);
        ASSERT_EQ(result.Neighbors[0], 0);

        index.Destroy();
    }

    std::remove(filename.c_str());
}
