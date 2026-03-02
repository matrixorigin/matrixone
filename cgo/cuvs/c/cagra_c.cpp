#include "cagra_c.h"
#include "../cpp/cagra.hpp"
#include <iostream>
#include <stdexcept>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <cstring>

// Helper to set error message
static void set_errmsg_cagra(void* errmsg, const std::string& prefix, const std::exception& e) {
    if (errmsg) {
        std::string err_str = prefix + ": " + std::string(e.what());
        char* msg = (char*)malloc(err_str.length() + 1);
        if (msg) {
            std::strcpy(msg, err_str.c_str());
            *(static_cast<char**>(errmsg)) = msg;
        }
    } else {
        std::cerr << prefix << ": " << e.what() << std::endl;
    }
}

// Helper to convert C enum to C++ enum
static cuvs::distance::DistanceType convert_distance_type_cagra(CuvsDistanceTypeC metric_c) {
    switch (metric_c) {
        case DistanceType_L2Expanded: return cuvs::distance::DistanceType::L2Expanded;
        case DistanceType_L1: return cuvs::distance::DistanceType::L1;
        case DistanceType_InnerProduct: return cuvs::distance::DistanceType::InnerProduct;
        default:
            throw std::runtime_error("Unknown distance type");
    }
}

GpuCagraIndexC GpuCagraIndex_New(const float* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                 CuvsDistanceTypeC metric_c, size_t intermediate_graph_degree, 
                                 size_t graph_degree, uint32_t nthread, int device_id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_cagra(metric_c);
        auto* index = new matrixone::GpuCagraIndex<float>(dataset_data, count_vectors, dimension, metric, 
                                                          intermediate_graph_degree, graph_degree, nthread, device_id);
        return static_cast<GpuCagraIndexC>(index);
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_New", e);
        return nullptr;
    }
}

GpuCagraIndexC GpuCagraIndex_NewFromFile(const char* filename, uint32_t dimension, CuvsDistanceTypeC metric_c, 
                                         uint32_t nthread, int device_id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_cagra(metric_c);
        auto* index = new matrixone::GpuCagraIndex<float>(std::string(filename), dimension, metric, nthread, device_id);
        return static_cast<GpuCagraIndexC>(index);
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_NewFromFile", e);
        return nullptr;
    }
}

void GpuCagraIndex_Load(GpuCagraIndexC index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* index = static_cast<matrixone::GpuCagraIndex<float>*>(index_c);
        if (index) index->Load();
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_Load", e);
    }
}

void GpuCagraIndex_Save(GpuCagraIndexC index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* index = static_cast<matrixone::GpuCagraIndex<float>*>(index_c);
        if (index) index->Save(std::string(filename));
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_Save", e);
    }
}

GpuCagraSearchResultC GpuCagraIndex_Search(GpuCagraIndexC index_c, const float* queries_data, 
                                           uint64_t num_queries, uint32_t query_dimension, 
                                           uint32_t limit, size_t itopk_size, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* index = static_cast<matrixone::GpuCagraIndex<float>*>(index_c);
        if (index) {
            auto* search_result = new matrixone::GpuCagraIndex<float>::SearchResult;
            *search_result = index->Search(queries_data, num_queries, query_dimension, limit, itopk_size);
            return static_cast<GpuCagraSearchResultC>(search_result);
        }
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_Search", e);
    }
    return nullptr;
}

void GpuCagraIndex_GetResults(GpuCagraSearchResultC result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances) {
    if (!result_c) return;
    auto* search_result = static_cast<matrixone::GpuCagraIndex<float>::SearchResult*>(result_c);
    
    size_t total = num_queries * limit;
    if (search_result->Neighbors.size() >= total) {
        // Convert uint32_t to int64_t and handle sentinel (-1)
        for (size_t i = 0; i < total; ++i) {
            uint32_t n = search_result->Neighbors[i];
            if (n == static_cast<uint32_t>(-1)) {
                neighbors[i] = -1;
            } else {
                neighbors[i] = static_cast<int64_t>(n);
            }
        }
    } else {
        std::fill(neighbors, neighbors + total, -1);
    }

    if (search_result->Distances.size() >= total) {
        std::copy(search_result->Distances.begin(), search_result->Distances.begin() + total, distances);
    } else {
        std::fill(distances, distances + total, std::numeric_limits<float>::infinity());
    }
}

void GpuCagraIndex_FreeSearchResult(GpuCagraSearchResultC result_c) {
    if (!result_c) return;
    delete static_cast<matrixone::GpuCagraIndex<float>::SearchResult*>(result_c);
}

void GpuCagraIndex_Destroy(GpuCagraIndexC index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* index = static_cast<matrixone::GpuCagraIndex<float>*>(index_c);
        if (index) delete index;
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_Destroy", e);
    }
}
