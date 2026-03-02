#include "brute_force_c.h"
#include "../cpp/brute_force.hpp"
#include <iostream>
#include <stdexcept>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <limits>
#include <cstring>

// Helper to set error message
static void set_errmsg(void* errmsg, const std::string& prefix, const std::exception& e) {
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
static cuvs::distance::DistanceType convert_distance_type(CuvsDistanceTypeC metric_c) {
    switch (metric_c) {
        case DistanceType_L2Expanded: return cuvs::distance::DistanceType::L2Expanded;
        case DistanceType_L1: return cuvs::distance::DistanceType::L1;
        case DistanceType_InnerProduct: return cuvs::distance::DistanceType::InnerProduct;
        case DistanceType_CosineSimilarity: return cuvs::distance::DistanceType::CosineExpanded;
        default:
            throw std::runtime_error("Unknown distance type");
    }
}

struct GpuBruteForceIndexAny {
    CuvsQuantizationC qtype;
    void* ptr;

    GpuBruteForceIndexAny(CuvsQuantizationC q, void* p) : qtype(q), ptr(p) {}
    ~GpuBruteForceIndexAny() {
        switch (qtype) {
            case Quantization_F32: delete static_cast<matrixone::GpuBruteForceIndex<float>*>(ptr); break;
            case Quantization_F16: delete static_cast<matrixone::GpuBruteForceIndex<half>*>(ptr); break;
            default: break;
        }
    }
};

GpuBruteForceIndexC GpuBruteForceIndex_New(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, CuvsDistanceTypeC metric_c, uint32_t nthread, int device_id, CuvsQuantizationC qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type(metric_c);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::GpuBruteForceIndex<float>(static_cast<const float*>(dataset_data), count_vectors, dimension, metric, nthread, device_id);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::GpuBruteForceIndex<half>(static_cast<const half*>(dataset_data), count_vectors, dimension, metric, nthread, device_id);
                break;
            default:
                throw std::runtime_error("Unsupported quantization type for Brute Force (Only F32 and F16 supported)");
        }
        return static_cast<GpuBruteForceIndexC>(new GpuBruteForceIndexAny(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in GpuBruteForceIndex_New", e);
        return nullptr;
    }
}

void GpuBruteForceIndex_Load(GpuBruteForceIndexC index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuBruteForceIndexAny*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::GpuBruteForceIndex<float>*>(any->ptr)->Load(); break;
            case Quantization_F16: static_cast<matrixone::GpuBruteForceIndex<half>*>(any->ptr)->Load(); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in GpuBruteForceIndex_Load", e);
    }
}

GpuBruteForceSearchResultC GpuBruteForceIndex_Search(GpuBruteForceIndexC index_c, const void* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuBruteForceIndexAny*>(index_c);
        void* result_ptr = nullptr;
        switch (any->qtype) {
            case Quantization_F32: {
                auto res = std::make_unique<matrixone::GpuBruteForceIndex<float>::SearchResult>();
                *res = static_cast<matrixone::GpuBruteForceIndex<float>*>(any->ptr)->Search(static_cast<const float*>(queries_data), num_queries, query_dimension, limit);
                result_ptr = res.release();
                break;
            }
            case Quantization_F16: {
                auto res = std::make_unique<matrixone::GpuBruteForceIndex<half>::SearchResult>();
                *res = static_cast<matrixone::GpuBruteForceIndex<half>*>(any->ptr)->Search(static_cast<const half*>(queries_data), num_queries, query_dimension, limit);
                result_ptr = res.release();
                break;
            }
            default: break;
        }
        return static_cast<GpuBruteForceSearchResultC>(result_ptr);
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in GpuBruteForceIndex_Search", e);
        return nullptr;
    }
}

void GpuBruteForceIndex_GetResults(GpuBruteForceSearchResultC result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances) {
    if (!result_c) return;
    auto* search_result = static_cast<matrixone::GpuBruteForceIndex<float>::SearchResult*>(result_c);

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

void GpuBruteForceIndex_FreeSearchResult(GpuBruteForceSearchResultC result_c) {
    if (!result_c) return;
    delete static_cast<matrixone::GpuBruteForceIndex<float>::SearchResult*>(result_c);
}

void GpuBruteForceIndex_Destroy(GpuBruteForceIndexC index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuBruteForceIndexAny*>(index_c);
        delete any;
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in GpuBruteForceIndex_Destroy", e);
    }
}
