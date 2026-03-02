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
        case DistanceType_CosineSimilarity: return cuvs::distance::DistanceType::CosineExpanded;
        default:
            throw std::runtime_error("Unknown distance type");
    }
}

struct GpuCagraIndexAny {
    CuvsQuantizationC qtype;
    void* ptr;

    GpuCagraIndexAny(CuvsQuantizationC q, void* p) : qtype(q), ptr(p) {}
    ~GpuCagraIndexAny() {
        switch (qtype) {
            case Quantization_F32: delete static_cast<matrixone::GpuCagraIndex<float>*>(ptr); break;
            case Quantization_F16: delete static_cast<matrixone::GpuCagraIndex<half>*>(ptr); break;
            case Quantization_INT8: delete static_cast<matrixone::GpuCagraIndex<int8_t>*>(ptr); break;
            case Quantization_UINT8: delete static_cast<matrixone::GpuCagraIndex<uint8_t>*>(ptr); break;
        }
    }
};

GpuCagraIndexC GpuCagraIndex_New(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                 CuvsDistanceTypeC metric_c, size_t intermediate_graph_degree, 
                                 size_t graph_degree, uint32_t nthread, int device_id, CuvsQuantizationC qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_cagra(metric_c);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::GpuCagraIndex<float>(static_cast<const float*>(dataset_data), count_vectors, dimension, metric, intermediate_graph_degree, graph_degree, nthread, device_id);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::GpuCagraIndex<half>(static_cast<const half*>(dataset_data), count_vectors, dimension, metric, intermediate_graph_degree, graph_degree, nthread, device_id);
                break;
            case Quantization_INT8:
                index_ptr = new matrixone::GpuCagraIndex<int8_t>(static_cast<const int8_t*>(dataset_data), count_vectors, dimension, metric, intermediate_graph_degree, graph_degree, nthread, device_id);
                break;
            case Quantization_UINT8:
                index_ptr = new matrixone::GpuCagraIndex<uint8_t>(static_cast<const uint8_t*>(dataset_data), count_vectors, dimension, metric, intermediate_graph_degree, graph_degree, nthread, device_id);
                break;
        }
        return static_cast<GpuCagraIndexC>(new GpuCagraIndexAny(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_New", e);
        return nullptr;
    }
}

GpuCagraIndexC GpuCagraIndex_NewFromFile(const char* filename, uint32_t dimension, CuvsDistanceTypeC metric_c, 
                                         uint32_t nthread, int device_id, CuvsQuantizationC qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_cagra(metric_c);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::GpuCagraIndex<float>(std::string(filename), dimension, metric, nthread, device_id);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::GpuCagraIndex<half>(std::string(filename), dimension, metric, nthread, device_id);
                break;
            case Quantization_INT8:
                index_ptr = new matrixone::GpuCagraIndex<int8_t>(std::string(filename), dimension, metric, nthread, device_id);
                break;
            case Quantization_UINT8:
                index_ptr = new matrixone::GpuCagraIndex<uint8_t>(std::string(filename), dimension, metric, nthread, device_id);
                break;
        }
        return static_cast<GpuCagraIndexC>(new GpuCagraIndexAny(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_NewFromFile", e);
        return nullptr;
    }
}

void GpuCagraIndex_Load(GpuCagraIndexC index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuCagraIndexAny*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::GpuCagraIndex<float>*>(any->ptr)->Load(); break;
            case Quantization_F16: static_cast<matrixone::GpuCagraIndex<half>*>(any->ptr)->Load(); break;
            case Quantization_INT8: static_cast<matrixone::GpuCagraIndex<int8_t>*>(any->ptr)->Load(); break;
            case Quantization_UINT8: static_cast<matrixone::GpuCagraIndex<uint8_t>*>(any->ptr)->Load(); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_Load", e);
    }
}

void GpuCagraIndex_Save(GpuCagraIndexC index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuCagraIndexAny*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::GpuCagraIndex<float>*>(any->ptr)->Save(std::string(filename)); break;
            case Quantization_F16: static_cast<matrixone::GpuCagraIndex<half>*>(any->ptr)->Save(std::string(filename)); break;
            case Quantization_INT8: static_cast<matrixone::GpuCagraIndex<int8_t>*>(any->ptr)->Save(std::string(filename)); break;
            case Quantization_UINT8: static_cast<matrixone::GpuCagraIndex<uint8_t>*>(any->ptr)->Save(std::string(filename)); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_Save", e);
    }
}

GpuCagraSearchResultC GpuCagraIndex_Search(GpuCagraIndexC index_c, const void* queries_data, 
                                           uint64_t num_queries, uint32_t query_dimension, 
                                           uint32_t limit, size_t itopk_size, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuCagraIndexAny*>(index_c);
        void* result_ptr = nullptr;
        switch (any->qtype) {
            case Quantization_F32: {
                auto res = std::make_unique<matrixone::GpuCagraIndex<float>::SearchResult>();
                *res = static_cast<matrixone::GpuCagraIndex<float>*>(any->ptr)->Search(static_cast<const float*>(queries_data), num_queries, query_dimension, limit, itopk_size);
                result_ptr = res.release();
                break;
            }
            case Quantization_F16: {
                auto res = std::make_unique<matrixone::GpuCagraIndex<half>::SearchResult>();
                *res = static_cast<matrixone::GpuCagraIndex<half>*>(any->ptr)->Search(static_cast<const half*>(queries_data), num_queries, query_dimension, limit, itopk_size);
                result_ptr = res.release();
                break;
            }
            case Quantization_INT8: {
                auto res = std::make_unique<matrixone::GpuCagraIndex<int8_t>::SearchResult>();
                *res = static_cast<matrixone::GpuCagraIndex<int8_t>*>(any->ptr)->Search(static_cast<const int8_t*>(queries_data), num_queries, query_dimension, limit, itopk_size);
                result_ptr = res.release();
                break;
            }
            case Quantization_UINT8: {
                auto res = std::make_unique<matrixone::GpuCagraIndex<uint8_t>::SearchResult>();
                *res = static_cast<matrixone::GpuCagraIndex<uint8_t>*>(any->ptr)->Search(static_cast<const uint8_t*>(queries_data), num_queries, query_dimension, limit, itopk_size);
                result_ptr = res.release();
                break;
            }
        }
        return static_cast<GpuCagraSearchResultC>(result_ptr);
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_Search", e);
        return nullptr;
    }
}

void GpuCagraIndex_GetResults(GpuCagraSearchResultC result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances) {
    if (!result_c) return;
    auto* search_result = static_cast<matrixone::GpuCagraIndex<float>::SearchResult*>(result_c);
    
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

void GpuCagraIndex_FreeSearchResult(GpuCagraSearchResultC result_c) {
    if (!result_c) return;
    delete static_cast<matrixone::GpuCagraIndex<float>::SearchResult*>(result_c);
}

void GpuCagraIndex_Destroy(GpuCagraIndexC index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuCagraIndexAny*>(index_c);
        delete any;
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_Destroy", e);
    }
}

void GpuCagraIndex_Extend(GpuCagraIndexC index_c, const void* additional_data, uint64_t num_vectors, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<GpuCagraIndexAny*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::GpuCagraIndex<float>*>(any->ptr)->Extend(static_cast<const float*>(additional_data), num_vectors); break;
            case Quantization_F16: static_cast<matrixone::GpuCagraIndex<half>*>(any->ptr)->Extend(static_cast<const half*>(additional_data), num_vectors); break;
            case Quantization_INT8: static_cast<matrixone::GpuCagraIndex<int8_t>*>(any->ptr)->Extend(static_cast<const int8_t*>(additional_data), num_vectors); break;
            case Quantization_UINT8: static_cast<matrixone::GpuCagraIndex<uint8_t>*>(any->ptr)->Extend(static_cast<const uint8_t*>(additional_data), num_vectors); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_Extend", e);
    }
}

template <typename T>
static GpuCagraIndexC merge_cagra(GpuCagraIndexC* indices, uint32_t num_indices, uint32_t nthread, int device_id, CuvsQuantizationC qtype) {
    std::vector<matrixone::GpuCagraIndex<T>*> cpp_indices;
    for (uint32_t i = 0; i < num_indices; ++i) {
        cpp_indices.push_back(static_cast<matrixone::GpuCagraIndex<T>*>(static_cast<GpuCagraIndexAny*>(indices[i])->ptr));
    }
    auto merged = matrixone::GpuCagraIndex<T>::Merge(cpp_indices, nthread, device_id);
    return static_cast<GpuCagraIndexC>(new GpuCagraIndexAny(qtype, merged.release()));
}

GpuCagraIndexC GpuCagraIndex_Merge(GpuCagraIndexC* indices, uint32_t num_indices, uint32_t nthread, int device_id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    if (num_indices == 0) return nullptr;
    try {
        auto* first = static_cast<GpuCagraIndexAny*>(indices[0]);
        CuvsQuantizationC qtype = first->qtype;
        switch (qtype) {
            case Quantization_F32: return merge_cagra<float>(indices, num_indices, nthread, device_id, qtype);
            case Quantization_F16: return merge_cagra<half>(indices, num_indices, nthread, device_id, qtype);
            case Quantization_INT8: return merge_cagra<int8_t>(indices, num_indices, nthread, device_id, qtype);
            case Quantization_UINT8: return merge_cagra<uint8_t>(indices, num_indices, nthread, device_id, qtype);
            default: throw std::runtime_error("Unsupported quantization type for GpuCagraIndex_Merge");
        }
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in GpuCagraIndex_Merge", e);
        return nullptr;
    }
}
