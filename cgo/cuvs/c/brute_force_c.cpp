#include "brute_force_c.h"
#include "../cpp/brute_force.hpp" // For C++ GpuBruteForceIndex
#include <iostream>    // For error logging
#include <stdexcept>   // For std::runtime_error
#include <vector>      // For std::vector
#include <algorithm>   // For std::copy
#include <cstdlib>     // For malloc, free
#include <limits>      // For std::numeric_limits
#include <cstring>     // For strcpy

// Helper to convert C enum to C++ enum
cuvs::distance::DistanceType convert_distance_type(CuvsDistanceTypeC metric_c) {
    switch (metric_c) {
        case DistanceType_L2Expanded: return cuvs::distance::DistanceType::L2Expanded;
        case DistanceType_L1: return cuvs::distance::DistanceType::L1;
        case DistanceType_InnerProduct: return cuvs::distance::DistanceType::InnerProduct;
        // Add other cases as needed
        default:
            std::cerr << "Error: Unknown distance type: " << metric_c << std::endl;
            throw std::runtime_error("Unknown distance type");
    }
}

// Constructor for GpuBruteForceIndex
GpuBruteForceIndexC GpuBruteForceIndex_New(const float* dataset_data, uint64_t count_vectors, uint32_t dimension, CuvsDistanceTypeC metric_c, uint32_t nthread) {
    try {
        cuvs::distance::DistanceType metric = convert_distance_type(metric_c);
        matrixone::GpuBruteForceIndex<float>* index = new matrixone::GpuBruteForceIndex<float>(dataset_data, count_vectors, dimension, metric, nthread);
        return static_cast<GpuBruteForceIndexC>(index);
    } catch (const std::exception& e) {
        std::cerr << "Error in GpuBruteForceIndex_New: " << e.what() << std::endl;
        return nullptr;
    }
}

// Loads the index to the GPU
void GpuBruteForceIndex_Load(GpuBruteForceIndexC index_c) {
    try {
        matrixone::GpuBruteForceIndex<float>* index = static_cast<matrixone::GpuBruteForceIndex<float>*>(index_c);
        if (index) {
            index->Load();
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in GpuBruteForceIndex_Load: " << e.what() << std::endl;
    }
}

// Performs a search operation
GpuBruteForceSearchResultC GpuBruteForceIndex_Search(GpuBruteForceIndexC index_c, const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, void* errmsg) {
    if (errmsg) {
        *(static_cast<char**>(errmsg)) = nullptr;
    }

    try {
        matrixone::GpuBruteForceIndex<float>* index = static_cast<matrixone::GpuBruteForceIndex<float>*>(index_c);
        if (index) {
            auto search_result = new matrixone::GpuBruteForceIndex<float>::SearchResult;
            *search_result = index->Search(queries_data, num_queries, query_dimension, limit);
            return static_cast<GpuBruteForceSearchResultC>(search_result);
        }
    } catch (const std::exception& e) {
        if (errmsg) {
            std::string err_str = "Error in GpuBruteForceIndex_Search: " + std::string(e.what());
            char* msg = (char*)malloc(err_str.length() + 1);
            if (msg) { // Check if malloc was successful
                std::strcpy(msg, err_str.c_str());
                *(static_cast<char**>(errmsg)) = msg;
            }
        } else {
            std::cerr << "Error in GpuBruteForceIndex_Search: " << e.what() << std::endl;
        }
    }
    return nullptr;
}

// Retrieves the results from a search operation
void GpuBruteForceIndex_GetResults(GpuBruteForceSearchResultC result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances) {
    if (!result_c) return;
    auto search_result = static_cast<matrixone::GpuBruteForceIndex<float>::SearchResult*>(result_c);

    for (uint64_t i = 0; i < num_queries; ++i) {
        uint64_t offset = i * limit;
        if (i < search_result->Neighbors.size()) {
            size_t found_k = search_result->Neighbors[i].size();
            std::copy(search_result->Neighbors[i].begin(), search_result->Neighbors[i].end(), neighbors + offset);
            std::copy(search_result->Distances[i].begin(), search_result->Distances[i].end(), distances + offset);

            // Pad the rest of the array if fewer than 'limit' neighbors are found
            for (size_t j = found_k; j < limit; ++j) {
                neighbors[offset + j] = -1;
                distances[offset + j] = std::numeric_limits<float>::infinity();
            }
        } else {
            // If the search returned fewer result sets than queries, pad the entire block
            for (size_t j = 0; j < limit; ++j) {
                neighbors[offset + j] = -1;
                distances[offset + j] = std::numeric_limits<float>::infinity();
            }
        }
    }
}

// Frees the memory for a GpuBruteForceSearchResultC object
void GpuBruteForceIndex_FreeSearchResult(GpuBruteForceSearchResultC result_c) {
    if (!result_c) return;
    auto search_result = static_cast<matrixone::GpuBruteForceIndex<float>::SearchResult*>(result_c);
    delete search_result;
}

// Destroys the GpuBruteForceIndex object and frees associated resources
void GpuBruteForceIndex_Destroy(GpuBruteForceIndexC index_c) {
    try {
        matrixone::GpuBruteForceIndex<float>* index = static_cast<matrixone::GpuBruteForceIndex<float>*>(index_c);
        if (index) {
            delete index;
        }
    } catch (const std::exception& e) {
        std::cerr << "Error in GpuBruteForceIndex_Destroy: " << e.what() << std::endl;
    }
}
