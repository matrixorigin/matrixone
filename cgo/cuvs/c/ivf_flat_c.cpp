#include "ivf_flat_c.h"
#include "../cpp/ivf_flat.hpp"
#include <iostream>
#include <stdexcept>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <cstring>

// Helper to set error message
static void set_errmsg_ivf(void* errmsg, const std::string& prefix, const std::exception& e) {
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
static cuvs::distance::DistanceType convert_distance_type_ivf(CuvsDistanceTypeC metric_c) {
    switch (metric_c) {
        case DistanceType_L2Expanded: return cuvs::distance::DistanceType::L2Expanded;
        case DistanceType_L1: return cuvs::distance::DistanceType::L1;
        case DistanceType_InnerProduct: return cuvs::distance::DistanceType::InnerProduct;
        default:
            throw std::runtime_error("Unknown distance type");
    }
}

GpuIvfFlatIndexC GpuIvfFlatIndex_New(const float* dataset_data, uint64_t count_vectors, uint32_t dimension, CuvsDistanceTypeC metric_c, uint32_t n_list, uint32_t nthread, int device_id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_ivf(metric_c);
        auto* index = new matrixone::GpuIvfFlatIndex<float>(dataset_data, count_vectors, dimension, metric, n_list, nthread, device_id);
        return static_cast<GpuIvfFlatIndexC>(index);
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_New", e);
        return nullptr;
    }
}

GpuIvfFlatIndexC GpuIvfFlatIndex_NewFromFile(const char* filename, uint32_t dimension, CuvsDistanceTypeC metric_c, uint32_t nthread, int device_id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_ivf(metric_c);
        auto* index = new matrixone::GpuIvfFlatIndex<float>(std::string(filename), dimension, metric, nthread, device_id);
        return static_cast<GpuIvfFlatIndexC>(index);
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_NewFromFile", e);
        return nullptr;
    }
}

void GpuIvfFlatIndex_Load(GpuIvfFlatIndexC index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* index = static_cast<matrixone::GpuIvfFlatIndex<float>*>(index_c);
        if (index) index->Load();
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_Load", e);
    }
}

void GpuIvfFlatIndex_Save(GpuIvfFlatIndexC index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* index = static_cast<matrixone::GpuIvfFlatIndex<float>*>(index_c);
        if (index) index->Save(std::string(filename));
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_Save", e);
    }
}

GpuIvfFlatSearchResultC GpuIvfFlatIndex_Search(GpuIvfFlatIndexC index_c, const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, uint32_t n_probes, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* index = static_cast<matrixone::GpuIvfFlatIndex<float>*>(index_c);
        if (index) {
            auto* search_result = new matrixone::GpuIvfFlatIndex<float>::SearchResult;
            *search_result = index->Search(queries_data, num_queries, query_dimension, limit, n_probes);
            return static_cast<GpuIvfFlatSearchResultC>(search_result);
        }
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_Search", e);
    }
    return nullptr;
}

void GpuIvfFlatIndex_GetResults(GpuIvfFlatSearchResultC result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances) {
    if (!result_c) return;
    auto* search_result = static_cast<matrixone::GpuIvfFlatIndex<float>::SearchResult*>(result_c);
    
    size_t total = num_queries * limit;
    if (search_result->Neighbors.size() >= total) {
        std::copy(search_result->Neighbors.begin(), search_result->Neighbors.begin() + total, neighbors);
    } else {
        std::fill(neighbors, neighbors + total, -1);
    }

    if (search_result->Distances.size() >= total) {
        std::copy(search_result->Distances.begin(), search_result->Distances.begin() + total, distances);
    } else {
        std::fill(distances, distances + total, std::numeric_limits<float>::infinity());
    }
}

void GpuIvfFlatIndex_FreeSearchResult(GpuIvfFlatSearchResultC result_c) {
    if (!result_c) return;
    delete static_cast<matrixone::GpuIvfFlatIndex<float>::SearchResult*>(result_c);
}

void GpuIvfFlatIndex_Destroy(GpuIvfFlatIndexC index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* index = static_cast<matrixone::GpuIvfFlatIndex<float>*>(index_c);
        if (index) delete index;
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_Destroy", e);
    }
}

uint32_t GpuIvfFlatIndex_GetNList(GpuIvfFlatIndexC index_c) {
    auto* index = static_cast<matrixone::GpuIvfFlatIndex<float>*>(index_c);
    return index ? index->NList : 0;
}

void GpuIvfFlatIndex_GetCenters(GpuIvfFlatIndexC index_c, float* centers, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* index = static_cast<matrixone::GpuIvfFlatIndex<float>*>(index_c);
        if (index) {
            std::vector<float> host_centers = index->GetCenters();
            std::copy(host_centers.begin(), host_centers.end(), centers);
        }
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_GetCenters", e);
    }
}
