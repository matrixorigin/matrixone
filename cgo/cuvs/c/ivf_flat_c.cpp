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
        case DistanceType_CosineSimilarity: return cuvs::distance::DistanceType::CosineExpanded;
        default:
            throw std::runtime_error("Unknown distance type");
    }
}

struct GpuIvfFlatIndexAny {
    CuvsQuantizationC qtype;
    void* ptr;

    GpuIvfFlatIndexAny(CuvsQuantizationC q, void* p) : qtype(q), ptr(p) {}
    ~GpuIvfFlatIndexAny() {
        switch (qtype) {
            case Quantization_F32: delete static_cast<matrixone::GpuIvfFlatIndex<float>*>(ptr); break;
            case Quantization_F16: delete static_cast<matrixone::GpuIvfFlatIndex<half>*>(ptr); break;
            case Quantization_INT8: delete static_cast<matrixone::GpuIvfFlatIndex<int8_t>*>(ptr); break;
            case Quantization_UINT8: delete static_cast<matrixone::GpuIvfFlatIndex<uint8_t>*>(ptr); break;
        }
    }
};

GpuIvfFlatIndexC GpuIvfFlatIndex_New(const float* dataset_data, uint64_t count_vectors, uint32_t dimension, CuvsDistanceTypeC metric_c, uint32_t n_list, uint32_t nthread, int device_id, void* errmsg) {
    return GpuIvfFlatIndex_NewUnsafe(dataset_data, count_vectors, dimension, metric_c, n_list, nthread, device_id, Quantization_F32, errmsg);
}

GpuIvfFlatIndexC GpuIvfFlatIndex_NewUnsafe(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, CuvsDistanceTypeC metric_c, uint32_t n_list, uint32_t nthread, int device_id, CuvsQuantizationC qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_ivf(metric_c);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::GpuIvfFlatIndex<float>(static_cast<const float*>(dataset_data), count_vectors, dimension, metric, n_list, nthread, device_id);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::GpuIvfFlatIndex<half>(static_cast<const half*>(dataset_data), count_vectors, dimension, metric, n_list, nthread, device_id);
                break;
            case Quantization_INT8:
                index_ptr = new matrixone::GpuIvfFlatIndex<int8_t>(static_cast<const int8_t*>(dataset_data), count_vectors, dimension, metric, n_list, nthread, device_id);
                break;
            case Quantization_UINT8:
                index_ptr = new matrixone::GpuIvfFlatIndex<uint8_t>(static_cast<const uint8_t*>(dataset_data), count_vectors, dimension, metric, n_list, nthread, device_id);
                break;
        }
        return static_cast<GpuIvfFlatIndexC>(new GpuIvfFlatIndexAny(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_NewUnsafe", e);
        return nullptr;
    }
}

GpuIvfFlatIndexC GpuIvfFlatIndex_NewFromFile(const char* filename, uint32_t dimension, CuvsDistanceTypeC metric_c, uint32_t nthread, int device_id, void* errmsg) {
    return GpuIvfFlatIndex_NewFromFileUnsafe(filename, dimension, metric_c, nthread, device_id, Quantization_F32, errmsg);
}

GpuIvfFlatIndexC GpuIvfFlatIndex_NewFromFileUnsafe(const char* filename, uint32_t dimension, CuvsDistanceTypeC metric_c, uint32_t nthread, int device_id, CuvsQuantizationC qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_ivf(metric_c);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::GpuIvfFlatIndex<float>(std::string(filename), dimension, metric, nthread, device_id);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::GpuIvfFlatIndex<half>(std::string(filename), dimension, metric, nthread, device_id);
                break;
            case Quantization_INT8:
                index_ptr = new matrixone::GpuIvfFlatIndex<int8_t>(std::string(filename), dimension, metric, nthread, device_id);
                break;
            case Quantization_UINT8:
                index_ptr = new matrixone::GpuIvfFlatIndex<uint8_t>(std::string(filename), dimension, metric, nthread, device_id);
                break;
        }
        return static_cast<GpuIvfFlatIndexC>(new GpuIvfFlatIndexAny(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_NewFromFileUnsafe", e);
        return nullptr;
    }
}

void GpuIvfFlatIndex_Load(GpuIvfFlatIndexC index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuIvfFlatIndexAny*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::GpuIvfFlatIndex<float>*>(any->ptr)->Load(); break;
            case Quantization_F16: static_cast<matrixone::GpuIvfFlatIndex<half>*>(any->ptr)->Load(); break;
            case Quantization_INT8: static_cast<matrixone::GpuIvfFlatIndex<int8_t>*>(any->ptr)->Load(); break;
            case Quantization_UINT8: static_cast<matrixone::GpuIvfFlatIndex<uint8_t>*>(any->ptr)->Load(); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_Load", e);
    }
}

void GpuIvfFlatIndex_Save(GpuIvfFlatIndexC index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuIvfFlatIndexAny*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::GpuIvfFlatIndex<float>*>(any->ptr)->Save(std::string(filename)); break;
            case Quantization_F16: static_cast<matrixone::GpuIvfFlatIndex<half>*>(any->ptr)->Save(std::string(filename)); break;
            case Quantization_INT8: static_cast<matrixone::GpuIvfFlatIndex<int8_t>*>(any->ptr)->Save(std::string(filename)); break;
            case Quantization_UINT8: static_cast<matrixone::GpuIvfFlatIndex<uint8_t>*>(any->ptr)->Save(std::string(filename)); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_Save", e);
    }
}

GpuIvfFlatSearchResultC GpuIvfFlatIndex_Search(GpuIvfFlatIndexC index_c, const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, uint32_t n_probes, void* errmsg) {
    return GpuIvfFlatIndex_SearchUnsafe(index_c, queries_data, num_queries, query_dimension, limit, n_probes, errmsg);
}

GpuIvfFlatSearchResultC GpuIvfFlatIndex_SearchUnsafe(GpuIvfFlatIndexC index_c, const void* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, uint32_t n_probes, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuIvfFlatIndexAny*>(index_c);
        void* result_ptr = nullptr;
        switch (any->qtype) {
            case Quantization_F32: {
                auto res = std::make_unique<matrixone::GpuIvfFlatIndex<float>::SearchResult>();
                *res = static_cast<matrixone::GpuIvfFlatIndex<float>*>(any->ptr)->Search(static_cast<const float*>(queries_data), num_queries, query_dimension, limit, n_probes);
                result_ptr = res.release();
                break;
            }
            case Quantization_F16: {
                auto res = std::make_unique<matrixone::GpuIvfFlatIndex<half>::SearchResult>();
                *res = static_cast<matrixone::GpuIvfFlatIndex<half>*>(any->ptr)->Search(static_cast<const half*>(queries_data), num_queries, query_dimension, limit, n_probes);
                result_ptr = res.release();
                break;
            }
            case Quantization_INT8: {
                auto res = std::make_unique<matrixone::GpuIvfFlatIndex<int8_t>::SearchResult>();
                *res = static_cast<matrixone::GpuIvfFlatIndex<int8_t>*>(any->ptr)->Search(static_cast<const int8_t*>(queries_data), num_queries, query_dimension, limit, n_probes);
                result_ptr = res.release();
                break;
            }
            case Quantization_UINT8: {
                auto res = std::make_unique<matrixone::GpuIvfFlatIndex<uint8_t>::SearchResult>();
                *res = static_cast<matrixone::GpuIvfFlatIndex<uint8_t>*>(any->ptr)->Search(static_cast<const uint8_t*>(queries_data), num_queries, query_dimension, limit, n_probes);
                result_ptr = res.release();
                break;
            }
        }
        return static_cast<GpuIvfFlatSearchResultC>(result_ptr);
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_SearchUnsafe", e);
        return nullptr;
    }
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
        auto* any = static_cast<GpuIvfFlatIndexAny*>(index_c);
        delete any;
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_Destroy", e);
    }
}

template <typename T>
static void copy_centers(void* ptr, float* centers) {
    auto host_centers = static_cast<matrixone::GpuIvfFlatIndex<T>*>(ptr)->GetCenters();
    for (size_t i = 0; i < host_centers.size(); ++i) {
        centers[i] = static_cast<float>(host_centers[i]);
    }
}

void GpuIvfFlatIndex_GetCenters(GpuIvfFlatIndexC index_c, float* centers, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuIvfFlatIndexAny*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: copy_centers<float>(any->ptr, centers); break;
            case Quantization_F16: copy_centers<half>(any->ptr, centers); break;
            case Quantization_INT8: copy_centers<int8_t>(any->ptr, centers); break;
            case Quantization_UINT8: copy_centers<uint8_t>(any->ptr, centers); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in GpuIvfFlatIndex_GetCenters", e);
    }
}

uint32_t GpuIvfFlatIndex_GetNList(GpuIvfFlatIndexC index_c) {
    auto* any = static_cast<GpuIvfFlatIndexAny*>(index_c);
    if (!any) return 0;
    switch (any->qtype) {
        case Quantization_F32: return static_cast<matrixone::GpuIvfFlatIndex<float>*>(any->ptr)->NList;
        case Quantization_F16: return static_cast<matrixone::GpuIvfFlatIndex<half>*>(any->ptr)->NList;
        case Quantization_INT8: return static_cast<matrixone::GpuIvfFlatIndex<int8_t>*>(any->ptr)->NList;
        case Quantization_UINT8: return static_cast<matrixone::GpuIvfFlatIndex<uint8_t>*>(any->ptr)->NList;
        default: return 0;
    }
}
