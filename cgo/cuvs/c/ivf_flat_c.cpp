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
static cuvs::distance::DistanceType convert_distance_type_ivf(distance_type_t metric_c) {
    switch (metric_c) {
        case DistanceType_L2Expanded: return cuvs::distance::DistanceType::L2Expanded;
        case DistanceType_L1: return cuvs::distance::DistanceType::L1;
        case DistanceType_InnerProduct: return cuvs::distance::DistanceType::InnerProduct;
        case DistanceType_CosineSimilarity: return cuvs::distance::DistanceType::CosineExpanded;
        default:
            throw std::runtime_error("Unknown distance type");
    }
}

struct gpu_ivf_flat_any_t {
    quantization_t qtype;
    void* ptr;

    gpu_ivf_flat_any_t(quantization_t q, void* p) : qtype(q), ptr(p) {}
    ~gpu_ivf_flat_any_t() {
        switch (qtype) {
            case Quantization_F32: delete static_cast<matrixone::gpu_ivf_flat_t<float>*>(ptr); break;
            case Quantization_F16: delete static_cast<matrixone::gpu_ivf_flat_t<half>*>(ptr); break;
            case Quantization_INT8: delete static_cast<matrixone::gpu_ivf_flat_t<int8_t>*>(ptr); break;
            case Quantization_UINT8: delete static_cast<matrixone::gpu_ivf_flat_t<uint8_t>*>(ptr); break;
        }
    }
};

template <typename T>
static void copy_centers(void* ptr, float* centers) {
    auto host_centers = static_cast<matrixone::gpu_ivf_flat_t<T>*>(ptr)->get_centers();
    for (size_t i = 0; i < host_centers.size(); ++i) {
        centers[i] = static_cast<float>(host_centers[i]);
    }
}

extern "C" {

gpu_ivf_flat_c gpu_ivf_flat_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, distance_type_t metric_c, uint32_t n_list, const int* devices, uint32_t num_devices, uint32_t nthread, quantization_t qtype, bool force_mg, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_ivf(metric_c);
        std::vector<int> device_vec(devices, devices + num_devices);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::gpu_ivf_flat_t<float>(static_cast<const float*>(dataset_data), count_vectors, dimension, metric, n_list, device_vec, nthread, force_mg);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::gpu_ivf_flat_t<half>(static_cast<const half*>(dataset_data), count_vectors, dimension, metric, n_list, device_vec, nthread, force_mg);
                break;
            case Quantization_INT8:
                index_ptr = new matrixone::gpu_ivf_flat_t<int8_t>(static_cast<const int8_t*>(dataset_data), count_vectors, dimension, metric, n_list, device_vec, nthread, force_mg);
                break;
            case Quantization_UINT8:
                index_ptr = new matrixone::gpu_ivf_flat_t<uint8_t>(static_cast<const uint8_t*>(dataset_data), count_vectors, dimension, metric, n_list, device_vec, nthread, force_mg);
                break;
        }
        return static_cast<gpu_ivf_flat_c>(new gpu_ivf_flat_any_t(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in gpu_ivf_flat_new", e);
        return nullptr;
    }
}

gpu_ivf_flat_c gpu_ivf_flat_new_from_file(const char* filename, uint32_t dimension, distance_type_t metric_c, const int* devices, uint32_t num_devices, uint32_t nthread, quantization_t qtype, bool force_mg, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_ivf(metric_c);
        std::vector<int> device_vec(devices, devices + num_devices);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::gpu_ivf_flat_t<float>(std::string(filename), dimension, metric, device_vec, nthread, force_mg);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::gpu_ivf_flat_t<half>(std::string(filename), dimension, metric, device_vec, nthread, force_mg);
                break;
            case Quantization_INT8:
                index_ptr = new matrixone::gpu_ivf_flat_t<int8_t>(std::string(filename), dimension, metric, device_vec, nthread, force_mg);
                break;
            case Quantization_UINT8:
                index_ptr = new matrixone::gpu_ivf_flat_t<uint8_t>(std::string(filename), dimension, metric, device_vec, nthread, force_mg);
                break;
        }
        return static_cast<gpu_ivf_flat_c>(new gpu_ivf_flat_any_t(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in gpu_ivf_flat_new_from_file", e);
        return nullptr;
    }
}

void gpu_ivf_flat_load(gpu_ivf_flat_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_ivf_flat_t<float>*>(any->ptr)->load(); break;
            case Quantization_F16: static_cast<matrixone::gpu_ivf_flat_t<half>*>(any->ptr)->load(); break;
            case Quantization_INT8: static_cast<matrixone::gpu_ivf_flat_t<int8_t>*>(any->ptr)->load(); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_ivf_flat_t<uint8_t>*>(any->ptr)->load(); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in gpu_ivf_flat_load", e);
    }
}

void gpu_ivf_flat_save(gpu_ivf_flat_c index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_ivf_flat_t<float>*>(any->ptr)->save(std::string(filename)); break;
            case Quantization_F16: static_cast<matrixone::gpu_ivf_flat_t<half>*>(any->ptr)->save(std::string(filename)); break;
            case Quantization_INT8: static_cast<matrixone::gpu_ivf_flat_t<int8_t>*>(any->ptr)->save(std::string(filename)); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_ivf_flat_t<uint8_t>*>(any->ptr)->save(std::string(filename)); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in gpu_ivf_flat_save", e);
    }
}

gpu_ivf_flat_search_result_c gpu_ivf_flat_search(gpu_ivf_flat_c index_c, const void* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, uint32_t n_probes, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        void* result_ptr = nullptr;
        switch (any->qtype) {
            case Quantization_F32: {
                auto res = std::make_unique<matrixone::gpu_ivf_flat_t<float>::search_result_t>();
                *res = static_cast<matrixone::gpu_ivf_flat_t<float>*>(any->ptr)->search(static_cast<const float*>(queries_data), num_queries, query_dimension, limit, n_probes);
                result_ptr = res.release();
                break;
            }
            case Quantization_F16: {
                auto res = std::make_unique<matrixone::gpu_ivf_flat_t<half>::search_result_t>();
                *res = static_cast<matrixone::gpu_ivf_flat_t<half>*>(any->ptr)->search(static_cast<const half*>(queries_data), num_queries, query_dimension, limit, n_probes);
                result_ptr = res.release();
                break;
            }
            case Quantization_INT8: {
                auto res = std::make_unique<matrixone::gpu_ivf_flat_t<int8_t>::search_result_t>();
                *res = static_cast<matrixone::gpu_ivf_flat_t<int8_t>*>(any->ptr)->search(static_cast<const int8_t*>(queries_data), num_queries, query_dimension, limit, n_probes);
                result_ptr = res.release();
                break;
            }
            case Quantization_UINT8: {
                auto res = std::make_unique<matrixone::gpu_ivf_flat_t<uint8_t>::search_result_t>();
                *res = static_cast<matrixone::gpu_ivf_flat_t<uint8_t>*>(any->ptr)->search(static_cast<const uint8_t*>(queries_data), num_queries, query_dimension, limit, n_probes);
                result_ptr = res.release();
                break;
            }
        }
        return static_cast<gpu_ivf_flat_search_result_c>(result_ptr);
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in gpu_ivf_flat_search", e);
        return nullptr;
    }
}

void gpu_ivf_flat_get_results(gpu_ivf_flat_search_result_c result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances) {
    if (!result_c) return;
    auto* search_result = static_cast<matrixone::gpu_ivf_flat_t<float>::search_result_t*>(result_c);
    
    size_t total = num_queries * limit;
    if (search_result->neighbors.size() >= total) {
        std::copy(search_result->neighbors.begin(), search_result->neighbors.begin() + total, neighbors);
    } else {
        std::fill(neighbors, neighbors + total, -1);
    }

    if (search_result->distances.size() >= total) {
        std::copy(search_result->distances.begin(), search_result->distances.begin() + total, distances);
    } else {
        std::fill(distances, distances + total, std::numeric_limits<float>::infinity());
    }
}

void gpu_ivf_flat_free_search_result(gpu_ivf_flat_search_result_c result_c) {
    if (!result_c) return;
    delete static_cast<matrixone::gpu_ivf_flat_t<float>::search_result_t*>(result_c);
}

void gpu_ivf_flat_destroy(gpu_ivf_flat_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        delete any;
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in gpu_ivf_flat_destroy", e);
    }
}

void gpu_ivf_flat_get_centers(gpu_ivf_flat_c index_c, float* centers, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: copy_centers<float>(any->ptr, centers); break;
            case Quantization_F16: copy_centers<half>(any->ptr, centers); break;
            case Quantization_INT8: copy_centers<int8_t>(any->ptr, centers); break;
            case Quantization_UINT8: copy_centers<uint8_t>(any->ptr, centers); break;
        }
    } catch (const std::exception& e) {
        set_errmsg_ivf(errmsg, "Error in gpu_ivf_flat_get_centers", e);
    }
}

uint32_t gpu_ivf_flat_get_n_list(gpu_ivf_flat_c index_c) {
    auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
    if (!any) return 0;
    switch (any->qtype) {
        case Quantization_F32: return static_cast<matrixone::gpu_ivf_flat_t<float>*>(any->ptr)->n_list;
        case Quantization_F16: return static_cast<matrixone::gpu_ivf_flat_t<half>*>(any->ptr)->n_list;
        case Quantization_INT8: return static_cast<matrixone::gpu_ivf_flat_t<int8_t>*>(any->ptr)->n_list;
        case Quantization_UINT8: return static_cast<matrixone::gpu_ivf_flat_t<uint8_t>*>(any->ptr)->n_list;
        default: return 0;
    }
}

} // extern "C"
