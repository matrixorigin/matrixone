#include "cuvs_worker.hpp"
#include "ivf_flat.hpp"
#include "test_framework.hpp"
#include <cstdio>
#include <cstdlib>

using namespace matrixone;

TEST(GpuIvfFlatIndexTest, BasicLoadSearchAndCenters) {
    const uint32_t dimension = 2;
    const uint64_t count = 4;
    std::vector<float> dataset = {
        1.0, 1.0,
        1.1, 1.1,
        100.0, 100.0,
        101.0, 101.0
    };
    
    gpu_ivf_flat_index_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, 2, 1, 0);
    index.load();

    // Verify centers
    auto centers = index.get_centers();
    ASSERT_EQ(centers.size(), (size_t)(2 * dimension));
    TEST_LOG("IVF-Flat Centers: " << centers[0] << ", " << centers[1]);

    std::vector<float> queries = {1.05, 1.05};
    auto result = index.search(queries.data(), 1, dimension, 2, 2);

    ASSERT_EQ(result.neighbors.size(), (size_t)2);
    // Should be either 0 or 1
    ASSERT_TRUE(result.neighbors[0] == 0 || result.neighbors[0] == 1);

    index.destroy();
}

TEST(GpuIvfFlatIndexTest, SaveAndLoadFromFile) {
    const uint32_t dimension = 2;
    const uint64_t count = 4;
    std::vector<float> dataset = {1.0, 1.0, 1.1, 1.1, 100.0, 100.0, 101.0, 101.0};
    std::string filename = "test_ivf_flat.bin";

    // 1. Build and Save
    {
        gpu_ivf_flat_index_t<float> index(dataset.data(), count, dimension, cuvs::distance::DistanceType::L2Expanded, 2, 1, 0);
        index.load();
        index.save(filename);
        index.destroy();
    }

    // 2. Load and Search
    {
        gpu_ivf_flat_index_t<float> index(filename, dimension, cuvs::distance::DistanceType::L2Expanded, 1, 0);
        index.load();
        
        std::vector<float> queries = {100.5, 100.5};
        auto result = index.search(queries.data(), 1, dimension, 2, 2);
        
        ASSERT_EQ(result.neighbors.size(), (size_t)2);
        ASSERT_TRUE(result.neighbors[0] == 2 || result.neighbors[0] == 3);

        index.destroy();
    }

    std::remove(filename.c_str());
}
