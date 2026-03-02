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
static cuvs::distance::DistanceType convert_distance_type_cagra(distance_type_t metric_c) {
    switch (metric_c) {
        case DistanceType_L2Expanded: return cuvs::distance::DistanceType::L2Expanded;
        case DistanceType_L1: return cuvs::distance::DistanceType::L1;
        case DistanceType_InnerProduct: return cuvs::distance::DistanceType::InnerProduct;
        case DistanceType_CosineSimilarity: return cuvs::distance::DistanceType::CosineExpanded;
        default:
            throw std::runtime_error("Unknown distance type");
    }
}

struct gpu_cagra_index_any_t {
    quantization_t qtype;
    void* ptr;

    gpu_cagra_index_any_t(quantization_t q, void* p) : qtype(q), ptr(p) {}
    ~gpu_cagra_index_any_t() {
        switch (qtype) {
            case Quantization_F32: delete static_cast<matrixone::gpu_cagra_index_t<float>*>(ptr); break;
            case Quantization_F16: delete static_cast<matrixone::gpu_cagra_index_t<half>*>(ptr); break;
            case Quantization_INT8: delete static_cast<matrixone::gpu_cagra_index_t<int8_t>*>(ptr); break;
            case Quantization_UINT8: delete static_cast<matrixone::gpu_cagra_index_t<uint8_t>*>(ptr); break;
        }
    }
};

template <typename T>
static gpu_cagra_index_c merge_cagra(gpu_cagra_index_c* indices, uint32_t num_indices, uint32_t nthread, const std::vector<int>& device_vec, quantization_t qtype) {
    std::vector<matrixone::gpu_cagra_index_t<T>*> cpp_indices;
    for (uint32_t i = 0; i < num_indices; ++i) {
        cpp_indices.push_back(static_cast<matrixone::gpu_cagra_index_t<T>*>(static_cast<gpu_cagra_index_any_t*>(indices[i])->ptr));
    }
    auto merged = matrixone::gpu_cagra_index_t<T>::merge(cpp_indices, nthread, device_vec);
    return static_cast<gpu_cagra_index_c>(new gpu_cagra_index_any_t(qtype, merged.release()));
}

extern "C" {

gpu_cagra_index_c gpu_cagra_index_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                 distance_type_t metric_c, size_t intermediate_graph_degree, 
                                 size_t graph_degree, const int* devices, uint32_t num_devices, uint32_t nthread, quantization_t qtype, bool force_mg, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_cagra(metric_c);
        std::vector<int> device_vec(devices, devices + num_devices);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::gpu_cagra_index_t<float>(static_cast<const float*>(dataset_data), count_vectors, dimension, metric, intermediate_graph_degree, graph_degree, device_vec, nthread, force_mg);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::gpu_cagra_index_t<half>(static_cast<const half*>(dataset_data), count_vectors, dimension, metric, intermediate_graph_degree, graph_degree, device_vec, nthread, force_mg);
                break;
            case Quantization_INT8:
                index_ptr = new matrixone::gpu_cagra_index_t<int8_t>(static_cast<const int8_t*>(dataset_data), count_vectors, dimension, metric, intermediate_graph_degree, graph_degree, device_vec, nthread, force_mg);
                break;
            case Quantization_UINT8:
                index_ptr = new matrixone::gpu_cagra_index_t<uint8_t>(static_cast<const uint8_t*>(dataset_data), count_vectors, dimension, metric, intermediate_graph_degree, graph_degree, device_vec, nthread, force_mg);
                break;
        }
        return static_cast<gpu_cagra_index_c>(new gpu_cagra_index_any_t(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in gpu_cagra_index_new", e);
        return nullptr;
    }
}

gpu_cagra_index_c gpu_cagra_index_new_from_file(const char* filename, uint32_t dimension, distance_type_t metric_c, 
                                         const int* devices, uint32_t num_devices, uint32_t nthread, quantization_t qtype, bool force_mg, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_cagra(metric_c);
        std::vector<int> device_vec(devices, devices + num_devices);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::gpu_cagra_index_t<float>(std::string(filename), dimension, metric, device_vec, nthread, force_mg);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::gpu_cagra_index_t<half>(std::string(filename), dimension, metric, device_vec, nthread, force_mg);
                break;
            case Quantization_INT8:
                index_ptr = new matrixone::gpu_cagra_index_t<int8_t>(std::string(filename), dimension, metric, device_vec, nthread, force_mg);
                break;
            case Quantization_UINT8:
                index_ptr = new matrixone::gpu_cagra_index_t<uint8_t>(std::string(filename), dimension, metric, device_vec, nthread, force_mg);
                break;
        }
        return static_cast<gpu_cagra_index_c>(new gpu_cagra_index_any_t(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in gpu_cagra_index_new_from_file", e);
        return nullptr;
    }
}

void gpu_cagra_index_load(gpu_cagra_index_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_index_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_index_t<float>*>(any->ptr)->load(); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_index_t<half>*>(any->ptr)->load(); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_index_t<int8_t>*>(any->ptr)->load(); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_index_t<uint8_t>*>(any->ptr)->load(); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in gpu_cagra_index_load", e);
    }
}

void gpu_cagra_index_save(gpu_cagra_index_c index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_index_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_index_t<float>*>(any->ptr)->save(std::string(filename)); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_index_t<half>*>(any->ptr)->save(std::string(filename)); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_index_t<int8_t>*>(any->ptr)->save(std::string(filename)); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_index_t<uint8_t>*>(any->ptr)->save(std::string(filename)); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in gpu_cagra_index_save", e);
    }
}

gpu_cagra_search_result_c gpu_cagra_index_search(gpu_cagra_index_c index_c, const void* queries_data, 
                                           uint64_t num_queries, uint32_t query_dimension, 
                                           uint32_t limit, size_t itopk_size, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_index_any_t*>(index_c);
        void* result_ptr = nullptr;
        switch (any->qtype) {
            case Quantization_F32: {
                auto res = std::make_unique<matrixone::gpu_cagra_index_t<float>::search_result_t>();
                *res = static_cast<matrixone::gpu_cagra_index_t<float>*>(any->ptr)->search(static_cast<const float*>(queries_data), num_queries, query_dimension, limit, itopk_size);
                result_ptr = res.release();
                break;
            }
            case Quantization_F16: {
                auto res = std::make_unique<matrixone::gpu_cagra_index_t<half>::search_result_t>();
                *res = static_cast<matrixone::gpu_cagra_index_t<half>*>(any->ptr)->search(static_cast<const half*>(queries_data), num_queries, query_dimension, limit, itopk_size);
                result_ptr = res.release();
                break;
            }
            case Quantization_INT8: {
                auto res = std::make_unique<matrixone::gpu_cagra_index_t<int8_t>::search_result_t>();
                *res = static_cast<matrixone::gpu_cagra_index_t<int8_t>*>(any->ptr)->search(static_cast<const int8_t*>(queries_data), num_queries, query_dimension, limit, itopk_size);
                result_ptr = res.release();
                break;
            }
            case Quantization_UINT8: {
                auto res = std::make_unique<matrixone::gpu_cagra_index_t<uint8_t>::search_result_t>();
                *res = static_cast<matrixone::gpu_cagra_index_t<uint8_t>*>(any->ptr)->search(static_cast<const uint8_t*>(queries_data), num_queries, query_dimension, limit, itopk_size);
                result_ptr = res.release();
                break;
            }
        }
        return static_cast<gpu_cagra_search_result_c>(result_ptr);
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in gpu_cagra_index_search", e);
        return nullptr;
    }
}

void gpu_cagra_index_get_results(gpu_cagra_search_result_c result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances) {
    if (!result_c) return;
    auto* search_result = static_cast<matrixone::gpu_cagra_index_t<float>::search_result_t*>(result_c);
    
    size_t total = num_queries * limit;
    if (search_result->neighbors.size() >= total) {
        for (size_t i = 0; i < total; ++i) {
            uint32_t n = search_result->neighbors[i];
            if (n == static_cast<uint32_t>(-1)) {
                neighbors[i] = -1;
            } else {
                neighbors[i] = static_cast<int64_t>(n);
            }
        }
    } else {
        std::fill(neighbors, neighbors + total, -1);
    }

    if (search_result->distances.size() >= total) {
        std::copy(search_result->distances.begin(), search_result->distances.begin() + total, distances);
    } else {
        std::fill(distances, distances + total, std::numeric_limits<float>::infinity());
    }
}

void gpu_cagra_index_free_search_result(gpu_cagra_search_result_c result_c) {
    if (!result_c) return;
    delete static_cast<matrixone::gpu_cagra_index_t<float>::search_result_t*>(result_c);
}

void gpu_cagra_index_destroy(gpu_cagra_index_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_index_any_t*>(index_c);
        delete any;
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in gpu_cagra_index_destroy", e);
    }
}

void gpu_cagra_index_extend(gpu_cagra_index_c index_c, const void* additional_data, uint64_t num_vectors, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_index_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_index_t<float>*>(any->ptr)->extend(static_cast<const float*>(additional_data), num_vectors); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_index_t<half>*>(any->ptr)->extend(static_cast<const half*>(additional_data), num_vectors); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_index_t<int8_t>*>(any->ptr)->extend(static_cast<const int8_t*>(additional_data), num_vectors); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_index_t<uint8_t>*>(any->ptr)->extend(static_cast<const uint8_t*>(additional_data), num_vectors); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in gpu_cagra_index_extend", e);
    }
}

gpu_cagra_index_c gpu_cagra_index_merge(gpu_cagra_index_c* indices, uint32_t num_indices, uint32_t nthread, const int* devices, uint32_t num_devices, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    if (num_indices == 0) return nullptr;
    try {
        auto* first = static_cast<gpu_cagra_index_any_t*>(indices[0]);
        quantization_t qtype = first->qtype;
        std::vector<int> device_vec(devices, devices + num_devices);
        switch (qtype) {
            case Quantization_F32: return merge_cagra<float>(indices, num_indices, nthread, device_vec, qtype);
            case Quantization_F16: return merge_cagra<half>(indices, num_indices, nthread, device_vec, qtype);
            case Quantization_INT8: return merge_cagra<int8_t>(indices, num_indices, nthread, device_vec, qtype);
            case Quantization_UINT8: return merge_cagra<uint8_t>(indices, num_indices, nthread, device_vec, qtype);
            default: throw std::runtime_error("Unsupported quantization type for gpu_cagra_index_merge");
        }
    } catch (const std::exception& e) {
        set_errmsg_cagra(errmsg, "Error in gpu_cagra_index_merge", e);
        return nullptr;
    }
}

} // extern "C"
