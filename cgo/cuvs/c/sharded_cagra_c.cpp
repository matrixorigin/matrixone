#include "sharded_cagra_c.h"
#include "../cpp/sharded_cagra.hpp"
#include <iostream>
#include <stdexcept>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <cstring>

// Helper to set error message
static void set_errmsg_sharded_cagra(void* errmsg, const std::string& prefix, const std::exception& e) {
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
static cuvs::distance::DistanceType convert_distance_type_sharded_cagra(CuvsDistanceTypeC metric_c) {
    switch (metric_c) {
        case DistanceType_L2Expanded: return cuvs::distance::DistanceType::L2Expanded;
        case DistanceType_L1: return cuvs::distance::DistanceType::L1;
        case DistanceType_InnerProduct: return cuvs::distance::DistanceType::InnerProduct;
        case DistanceType_CosineSimilarity: return cuvs::distance::DistanceType::CosineExpanded;
        default:
            throw std::runtime_error("Unknown distance type");
    }
}

struct GpuShardedCagraIndexAny {
    CuvsQuantizationC qtype;
    void* ptr;

    GpuShardedCagraIndexAny(CuvsQuantizationC q, void* p) : qtype(q), ptr(p) {}
    ~GpuShardedCagraIndexAny() {
        switch (qtype) {
            case Quantization_F32: delete static_cast<matrixone::GpuShardedCagraIndex<float>*>(ptr); break;
            case Quantization_F16: delete static_cast<matrixone::GpuShardedCagraIndex<half>*>(ptr); break;
            case Quantization_INT8: delete static_cast<matrixone::GpuShardedCagraIndex<int8_t>*>(ptr); break;
            case Quantization_UINT8: delete static_cast<matrixone::GpuShardedCagraIndex<uint8_t>*>(ptr); break;
        }
    }
};

GpuShardedCagraIndexC GpuShardedCagraIndex_New(const float* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                               CuvsDistanceTypeC metric_c, size_t intermediate_graph_degree, 
                                               size_t graph_degree, const int* devices, uint32_t num_devices, uint32_t nthread, void* errmsg) {
    return GpuShardedCagraIndex_NewUnsafe(dataset_data, count_vectors, dimension, metric_c, intermediate_graph_degree, graph_degree, devices, num_devices, nthread, Quantization_F32, errmsg);
}

GpuShardedCagraIndexC GpuShardedCagraIndex_NewUnsafe(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                                     CuvsDistanceTypeC metric_c, size_t intermediate_graph_degree, 
                                                     size_t graph_degree, const int* devices, uint32_t num_devices, uint32_t nthread, CuvsQuantizationC qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_sharded_cagra(metric_c);
        std::vector<int> device_vec(devices, devices + num_devices);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::GpuShardedCagraIndex<float>(static_cast<const float*>(dataset_data), count_vectors, dimension, metric, intermediate_graph_degree, graph_degree, device_vec, nthread);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::GpuShardedCagraIndex<half>(static_cast<const half*>(dataset_data), count_vectors, dimension, metric, intermediate_graph_degree, graph_degree, device_vec, nthread);
                break;
            case Quantization_INT8:
                index_ptr = new matrixone::GpuShardedCagraIndex<int8_t>(static_cast<const int8_t*>(dataset_data), count_vectors, dimension, metric, intermediate_graph_degree, graph_degree, device_vec, nthread);
                break;
            case Quantization_UINT8:
                index_ptr = new matrixone::GpuShardedCagraIndex<uint8_t>(static_cast<const uint8_t*>(dataset_data), count_vectors, dimension, metric, intermediate_graph_degree, graph_degree, device_vec, nthread);
                break;
        }
        return static_cast<GpuShardedCagraIndexC>(new GpuShardedCagraIndexAny(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg_sharded_cagra(errmsg, "Error in GpuShardedCagraIndex_NewUnsafe", e);
        return nullptr;
    }
}

GpuShardedCagraIndexC GpuShardedCagraIndex_NewFromFile(const char* filename, uint32_t dimension, 
                                                       CuvsDistanceTypeC metric_c, 
                                                       const int* devices, uint32_t num_devices, uint32_t nthread, void* errmsg) {
    return GpuShardedCagraIndex_NewFromFileUnsafe(filename, dimension, metric_c, devices, num_devices, nthread, Quantization_F32, errmsg);
}

GpuShardedCagraIndexC GpuShardedCagraIndex_NewFromFileUnsafe(const char* filename, uint32_t dimension, 
                                                             CuvsDistanceTypeC metric_c, 
                                                             const int* devices, uint32_t num_devices, uint32_t nthread, CuvsQuantizationC qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_sharded_cagra(metric_c);
        std::vector<int> device_vec(devices, devices + num_devices);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::GpuShardedCagraIndex<float>(std::string(filename), dimension, metric, device_vec, nthread);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::GpuShardedCagraIndex<half>(std::string(filename), dimension, metric, device_vec, nthread);
                break;
            case Quantization_INT8:
                index_ptr = new matrixone::GpuShardedCagraIndex<int8_t>(std::string(filename), dimension, metric, device_vec, nthread);
                break;
            case Quantization_UINT8:
                index_ptr = new matrixone::GpuShardedCagraIndex<uint8_t>(std::string(filename), dimension, metric, device_vec, nthread);
                break;
        }
        return static_cast<GpuShardedCagraIndexC>(new GpuShardedCagraIndexAny(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg_sharded_cagra(errmsg, "Error in GpuShardedCagraIndex_NewFromFileUnsafe", e);
        return nullptr;
    }
}

void GpuShardedCagraIndex_Load(GpuShardedCagraIndexC index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuShardedCagraIndexAny*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::GpuShardedCagraIndex<float>*>(any->ptr)->Load(); break;
            case Quantization_F16: static_cast<matrixone::GpuShardedCagraIndex<half>*>(any->ptr)->Load(); break;
            case Quantization_INT8: static_cast<matrixone::GpuShardedCagraIndex<int8_t>*>(any->ptr)->Load(); break;
            case Quantization_UINT8: static_cast<matrixone::GpuShardedCagraIndex<uint8_t>*>(any->ptr)->Load(); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_sharded_cagra(errmsg, "Error in GpuShardedCagraIndex_Load", e);
    }
}

void GpuShardedCagraIndex_Save(GpuShardedCagraIndexC index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuShardedCagraIndexAny*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::GpuShardedCagraIndex<float>*>(any->ptr)->Save(std::string(filename)); break;
            case Quantization_F16: static_cast<matrixone::GpuShardedCagraIndex<half>*>(any->ptr)->Save(std::string(filename)); break;
            case Quantization_INT8: static_cast<matrixone::GpuShardedCagraIndex<int8_t>*>(any->ptr)->Save(std::string(filename)); break;
            case Quantization_UINT8: static_cast<matrixone::GpuShardedCagraIndex<uint8_t>*>(any->ptr)->Save(std::string(filename)); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_sharded_cagra(errmsg, "Error in GpuShardedCagraIndex_Save", e);
    }
}

GpuShardedCagraSearchResultC GpuShardedCagraIndex_Search(GpuShardedCagraIndexC index_c, const float* queries_data, 
                                                         uint64_t num_queries, uint32_t query_dimension, 
                                                         uint32_t limit, size_t itopk_size, void* errmsg) {
    return GpuShardedCagraIndex_SearchUnsafe(index_c, queries_data, num_queries, query_dimension, limit, itopk_size, errmsg);
}

GpuShardedCagraSearchResultC GpuShardedCagraIndex_SearchUnsafe(GpuShardedCagraIndexC index_c, const void* queries_data, 
                                                               uint64_t num_queries, uint32_t query_dimension, 
                                                               uint32_t limit, size_t itopk_size, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuShardedCagraIndexAny*>(index_c);
        void* result_ptr = nullptr;
        switch (any->qtype) {
            case Quantization_F32: {
                auto res = std::make_unique<matrixone::GpuShardedCagraIndex<float>::SearchResult>();
                *res = static_cast<matrixone::GpuShardedCagraIndex<float>*>(any->ptr)->Search(static_cast<const float*>(queries_data), num_queries, query_dimension, limit, itopk_size);
                result_ptr = res.release();
                break;
            }
            case Quantization_F16: {
                auto res = std::make_unique<matrixone::GpuShardedCagraIndex<half>::SearchResult>();
                *res = static_cast<matrixone::GpuShardedCagraIndex<half>*>(any->ptr)->Search(static_cast<const half*>(queries_data), num_queries, query_dimension, limit, itopk_size);
                result_ptr = res.release();
                break;
            }
            case Quantization_INT8: {
                auto res = std::make_unique<matrixone::GpuShardedCagraIndex<int8_t>::SearchResult>();
                *res = static_cast<matrixone::GpuShardedCagraIndex<int8_t>*>(any->ptr)->Search(static_cast<const int8_t*>(queries_data), num_queries, query_dimension, limit, itopk_size);
                result_ptr = res.release();
                break;
            }
            case Quantization_UINT8: {
                auto res = std::make_unique<matrixone::GpuShardedCagraIndex<uint8_t>::SearchResult>();
                *res = static_cast<matrixone::GpuShardedCagraIndex<uint8_t>*>(any->ptr)->Search(static_cast<const uint8_t*>(queries_data), num_queries, query_dimension, limit, itopk_size);
                result_ptr = res.release();
                break;
            }
        }
        return static_cast<GpuShardedCagraSearchResultC>(result_ptr);
    } catch (const std::exception& e) {
        set_errmsg_sharded_cagra(errmsg, "Error in GpuShardedCagraIndex_SearchUnsafe", e);
        return nullptr;
    }
}

void GpuShardedCagraIndex_GetResults(GpuShardedCagraSearchResultC result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances) {
    if (!result_c) return;
    auto* search_result = static_cast<matrixone::GpuShardedCagraIndex<float>::SearchResult*>(result_c);
    
    size_t total = num_queries * limit;
    if (search_result->Neighbors.size() >= total) {
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

void GpuShardedCagraIndex_FreeSearchResult(GpuShardedCagraSearchResultC result_c) {
    if (!result_c) return;
    delete static_cast<matrixone::GpuShardedCagraIndex<float>::SearchResult*>(result_c);
}

void GpuShardedCagraIndex_Destroy(GpuShardedCagraIndexC index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuShardedCagraIndexAny*>(index_c);
        delete any;
    } catch (const std::exception& e) {
        set_errmsg_sharded_cagra(errmsg, "Error in GpuShardedCagraIndex_Destroy", e);
    }
}
