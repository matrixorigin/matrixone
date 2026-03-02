#include "sharded_ivf_flat_c.h"
#include "../cpp/sharded_ivf_flat.hpp"
#include <iostream>
#include <stdexcept>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <cstring>

// Helper to set error message
static void set_errmsg_sharded(void* errmsg, const std::string& prefix, const std::exception& e) {
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
static cuvs::distance::DistanceType convert_distance_type_sharded(CuvsDistanceTypeC metric_c) {
    switch (metric_c) {
        case DistanceType_L2Expanded: return cuvs::distance::DistanceType::L2Expanded;
        case DistanceType_L1: return cuvs::distance::DistanceType::L1;
        case DistanceType_InnerProduct: return cuvs::distance::DistanceType::InnerProduct;
        case DistanceType_CosineSimilarity: return cuvs::distance::DistanceType::CosineExpanded;
        default:
            throw std::runtime_error("Unknown distance type");
    }
}

struct GpuShardedIvfFlatIndexAny {
    CuvsQuantizationC qtype;
    void* ptr;

    GpuShardedIvfFlatIndexAny(CuvsQuantizationC q, void* p) : qtype(q), ptr(p) {}
    ~GpuShardedIvfFlatIndexAny() {
        switch (qtype) {
            case Quantization_F32: delete static_cast<matrixone::GpuShardedIvfFlatIndex<float>*>(ptr); break;
            case Quantization_F16: delete static_cast<matrixone::GpuShardedIvfFlatIndex<half>*>(ptr); break;
            case Quantization_INT8: delete static_cast<matrixone::GpuShardedIvfFlatIndex<int8_t>*>(ptr); break;
            case Quantization_UINT8: delete static_cast<matrixone::GpuShardedIvfFlatIndex<uint8_t>*>(ptr); break;
        }
    }
};

GpuShardedIvfFlatIndexC GpuShardedIvfFlatIndex_New(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                                  CuvsDistanceTypeC metric_c, uint32_t n_list, 
                                                  const int* devices, uint32_t num_devices, uint32_t nthread, CuvsQuantizationC qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_sharded(metric_c);
        std::vector<int> device_vec(devices, devices + num_devices);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::GpuShardedIvfFlatIndex<float>(static_cast<const float*>(dataset_data), count_vectors, dimension, metric, n_list, device_vec, nthread);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::GpuShardedIvfFlatIndex<half>(static_cast<const half*>(dataset_data), count_vectors, dimension, metric, n_list, device_vec, nthread);
                break;
            case Quantization_INT8:
                index_ptr = new matrixone::GpuShardedIvfFlatIndex<int8_t>(static_cast<const int8_t*>(dataset_data), count_vectors, dimension, metric, n_list, device_vec, nthread);
                break;
            case Quantization_UINT8:
                index_ptr = new matrixone::GpuShardedIvfFlatIndex<uint8_t>(static_cast<const uint8_t*>(dataset_data), count_vectors, dimension, metric, n_list, device_vec, nthread);
                break;
        }
        return static_cast<GpuShardedIvfFlatIndexC>(new GpuShardedIvfFlatIndexAny(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg_sharded(errmsg, "Error in GpuShardedIvfFlatIndex_New", e);
        return nullptr;
    }
}

GpuShardedIvfFlatIndexC GpuShardedIvfFlatIndex_NewFromFile(const char* filename, uint32_t dimension, 
                                                          CuvsDistanceTypeC metric_c, 
                                                          const int* devices, uint32_t num_devices, uint32_t nthread, CuvsQuantizationC qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_sharded(metric_c);
        std::vector<int> device_vec(devices, devices + num_devices);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::GpuShardedIvfFlatIndex<float>(std::string(filename), dimension, metric, device_vec, nthread);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::GpuShardedIvfFlatIndex<half>(std::string(filename), dimension, metric, device_vec, nthread);
                break;
            case Quantization_INT8:
                index_ptr = new matrixone::GpuShardedIvfFlatIndex<int8_t>(std::string(filename), dimension, metric, device_vec, nthread);
                break;
            case Quantization_UINT8:
                index_ptr = new matrixone::GpuShardedIvfFlatIndex<uint8_t>(std::string(filename), dimension, metric, device_vec, nthread);
                break;
        }
        return static_cast<GpuShardedIvfFlatIndexC>(new GpuShardedIvfFlatIndexAny(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg_sharded(errmsg, "Error in GpuShardedIvfFlatIndex_NewFromFile", e);
        return nullptr;
    }
}

void GpuShardedIvfFlatIndex_Load(GpuShardedIvfFlatIndexC index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuShardedIvfFlatIndexAny*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::GpuShardedIvfFlatIndex<float>*>(any->ptr)->Load(); break;
            case Quantization_F16: static_cast<matrixone::GpuShardedIvfFlatIndex<half>*>(any->ptr)->Load(); break;
            case Quantization_INT8: static_cast<matrixone::GpuShardedIvfFlatIndex<int8_t>*>(any->ptr)->Load(); break;
            case Quantization_UINT8: static_cast<matrixone::GpuShardedIvfFlatIndex<uint8_t>*>(any->ptr)->Load(); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_sharded(errmsg, "Error in GpuShardedIvfFlatIndex_Load", e);
    }
}

void GpuShardedIvfFlatIndex_Save(GpuShardedIvfFlatIndexC index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuShardedIvfFlatIndexAny*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::GpuShardedIvfFlatIndex<float>*>(any->ptr)->Save(std::string(filename)); break;
            case Quantization_F16: static_cast<matrixone::GpuShardedIvfFlatIndex<half>*>(any->ptr)->Save(std::string(filename)); break;
            case Quantization_INT8: static_cast<matrixone::GpuShardedIvfFlatIndex<int8_t>*>(any->ptr)->Save(std::string(filename)); break;
            case Quantization_UINT8: static_cast<matrixone::GpuShardedIvfFlatIndex<uint8_t>*>(any->ptr)->Save(std::string(filename)); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_sharded(errmsg, "Error in GpuShardedIvfFlatIndex_Save", e);
    }
}

GpuShardedIvfFlatSearchResultC GpuShardedIvfFlatIndex_Search(GpuShardedIvfFlatIndexC index_c, const void* queries_data, 
                                                            uint64_t num_queries, uint32_t query_dimension, 
                                                            uint32_t limit, uint32_t n_probes, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuShardedIvfFlatIndexAny*>(index_c);
        void* result_ptr = nullptr;
        switch (any->qtype) {
            case Quantization_F32: {
                auto res = std::make_unique<matrixone::GpuShardedIvfFlatIndex<float>::SearchResult>();
                *res = static_cast<matrixone::GpuShardedIvfFlatIndex<float>*>(any->ptr)->Search(static_cast<const float*>(queries_data), num_queries, query_dimension, limit, n_probes);
                result_ptr = res.release();
                break;
            }
            case Quantization_F16: {
                auto res = std::make_unique<matrixone::GpuShardedIvfFlatIndex<half>::SearchResult>();
                *res = static_cast<matrixone::GpuShardedIvfFlatIndex<half>*>(any->ptr)->Search(static_cast<const half*>(queries_data), num_queries, query_dimension, limit, n_probes);
                result_ptr = res.release();
                break;
            }
            case Quantization_INT8: {
                auto res = std::make_unique<matrixone::GpuShardedIvfFlatIndex<int8_t>::SearchResult>();
                *res = static_cast<matrixone::GpuShardedIvfFlatIndex<int8_t>*>(any->ptr)->Search(static_cast<const int8_t*>(queries_data), num_queries, query_dimension, limit, n_probes);
                result_ptr = res.release();
                break;
            }
            case Quantization_UINT8: {
                auto res = std::make_unique<matrixone::GpuShardedIvfFlatIndex<uint8_t>::SearchResult>();
                *res = static_cast<matrixone::GpuShardedIvfFlatIndex<uint8_t>*>(any->ptr)->Search(static_cast<const uint8_t*>(queries_data), num_queries, query_dimension, limit, n_probes);
                result_ptr = res.release();
                break;
            }
        }
        return static_cast<GpuShardedIvfFlatSearchResultC>(result_ptr);
    } catch (const std::exception& e) {
        set_errmsg_sharded(errmsg, "Error in GpuShardedIvfFlatIndex_Search", e);
        return nullptr;
    }
}

void GpuShardedIvfFlatIndex_GetResults(GpuShardedIvfFlatSearchResultC result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances) {
    if (!result_c) return;
    auto* search_result = static_cast<matrixone::GpuShardedIvfFlatIndex<float>::SearchResult*>(result_c);
    
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

void GpuShardedIvfFlatIndex_FreeSearchResult(GpuShardedIvfFlatSearchResultC result_c) {
    if (!result_c) return;
    delete static_cast<matrixone::GpuShardedIvfFlatIndex<float>::SearchResult*>(result_c);
}

void GpuShardedIvfFlatIndex_Destroy(GpuShardedIvfFlatIndexC index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuShardedIvfFlatIndexAny*>(index_c);
        delete any;
    } catch (const std::exception& e) {
        set_errmsg_sharded(errmsg, "Error in GpuShardedIvfFlatIndex_Destroy", e);
    }
}

template <typename T>
static void copy_centers_sharded(void* ptr, float* centers) {
    auto host_centers = static_cast<matrixone::GpuShardedIvfFlatIndex<T>*>(ptr)->GetCenters();
    for (size_t i = 0; i < host_centers.size(); ++i) {
        centers[i] = static_cast<float>(host_centers[i]);
    }
}

void GpuShardedIvfFlatIndex_GetCenters(GpuShardedIvfFlatIndexC index_c, float* centers, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuShardedIvfFlatIndexAny*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: copy_centers_sharded<float>(any->ptr, centers); break;
            case Quantization_F16: copy_centers_sharded<half>(any->ptr, centers); break;
            case Quantization_INT8: copy_centers_sharded<int8_t>(any->ptr, centers); break;
            case Quantization_UINT8: copy_centers_sharded<uint8_t>(any->ptr, centers); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_sharded(errmsg, "Error in GpuShardedIvfFlatIndex_GetCenters", e);
    }
}

uint32_t GpuShardedIvfFlatIndex_GetNList(GpuShardedIvfFlatIndexC index_c) {
    auto* any = static_cast<GpuShardedIvfFlatIndexAny*>(index_c);
    if (!any) return 0;
    switch (any->qtype) {
        case Quantization_F32: return static_cast<matrixone::GpuShardedIvfFlatIndex<float>*>(any->ptr)->NList;
        case Quantization_F16: return static_cast<matrixone::GpuShardedIvfFlatIndex<half>*>(any->ptr)->NList;
        case Quantization_INT8: return static_cast<matrixone::GpuShardedIvfFlatIndex<int8_t>*>(any->ptr)->NList;
        case Quantization_UINT8: return static_cast<matrixone::GpuShardedIvfFlatIndex<uint8_t>*>(any->ptr)->NList;
        default: return 0;
    }
}
