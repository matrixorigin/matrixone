#include "cuvs_worker.hpp"
#include "ivf_flat.hpp"
#include "test_framework.hpp"
#include <cstdio> // For remove

using namespace matrixone;

TEST(GpuIvfFlatIndexTest, BasicLoadSearchAndCenters) {
    std::vector<float> dataset = {
        1.0f, 1.0f,
        1.1f, 1.1f,
        100.0f, 100.0f,
        101.0f, 101.0f
    };
    uint32_t dimension = 2;
    uint64_t count = 4;
    uint32_t n_list = 2;
    uint32_t n_probes = 2;
    uint32_t nthread = 1;

    GpuIvfFlatIndex<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, n_list, nthread);
    index.Load();

    // Verify Centers
    auto centers = index.GetCenters();
    ASSERT_EQ(centers.size(), (size_t)(n_list * dimension));
    TEST_LOG("Centroids retrieved: " << centers.size() / dimension);

    // Verify Search
    std::vector<float> queries = {1.05f, 1.05f};
    auto result = index.Search(queries.data(), 1, dimension, 2, n_probes);
    
    ASSERT_EQ(result.Neighbors.size(), (size_t)2);
    ASSERT_TRUE(result.Neighbors[0] == 0 || result.Neighbors[0] == 1);

    index.Destroy();
}

TEST(GpuIvfFlatIndexTest, SaveAndLoadFromFile) {
    std::vector<float> dataset = {
        1.0f, 1.0f,
        1.1f, 1.1f,
        100.0f, 100.0f,
        101.0f, 101.0f
    };
    uint32_t dimension = 2;
    uint64_t count = 4;
    uint32_t n_list = 2;
    uint32_t nthread = 1;
    std::string filename = "test_ivf_flat.bin";

    // 1. Build and Save
    {
        GpuIvfFlatIndex<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, n_list, nthread);
        index.Load();
        index.Save(filename);
        index.Destroy();
    }

    // 2. Load from file and Search
    {
        GpuIvfFlatIndex<float> index(filename, dimension, cuvs::distance::DistanceType::L2Expanded, nthread);
        index.Load();
        
        ASSERT_EQ(index.Count, (uint32_t)4);
        ASSERT_EQ(index.NList, (uint32_t)2);

        std::vector<float> queries = {100.5f, 100.5f};
        auto result = index.Search(queries.data(), 1, dimension, 2, 2);
        
        ASSERT_EQ(result.Neighbors.size(), (size_t)2);
        // Closest should be index 2 or 3 (100,100 or 101,101)
        ASSERT_TRUE(result.Neighbors[0] == 2 || result.Neighbors[0] == 3);

        auto centers = index.GetCenters();
        ASSERT_EQ(centers.size(), (size_t)(n_list * dimension));

        index.Destroy();
    }

    std::remove(filename.c_str());
}
