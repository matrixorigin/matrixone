#include "ivf_flat_c.h"
#include "ivf_flat.hpp"
#include <iostream>
#include <stdexcept>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <cstring>

// Helper to set error message
static void set_errmsg_ivf_flat(void* errmsg, const std::string& prefix, const std::exception& e) {
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
static cuvs::distance::DistanceType convert_distance_type_ivf_flat(distance_type_t metric_c) {
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
            default: break;
        }
    }
};

extern "C" {

gpu_ivf_flat_c gpu_ivf_flat_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                 distance_type_t metric_c, ivf_flat_build_params_t build_params,
                                 const int* devices, int device_count, uint32_t nthread, 
                                 distribution_mode_t dist_mode, quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_ivf_flat(metric_c);
        std::vector<int> devs(devices, devices + device_count);
        void* ivf_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                ivf_ptr = new matrixone::gpu_ivf_flat_t<float>(static_cast<const float*>(dataset_data), count_vectors, dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_F16:
                ivf_ptr = new matrixone::gpu_ivf_flat_t<half>(static_cast<const half*>(dataset_data), count_vectors, dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_INT8:
                ivf_ptr = new matrixone::gpu_ivf_flat_t<int8_t>(static_cast<const int8_t*>(dataset_data), count_vectors, dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_UINT8:
                ivf_ptr = new matrixone::gpu_ivf_flat_t<uint8_t>(static_cast<const uint8_t*>(dataset_data), count_vectors, dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            default:
                throw std::runtime_error("Unsupported quantization type for IVF-Flat");
        }
        return static_cast<gpu_ivf_flat_c>(new gpu_ivf_flat_any_t(qtype, ivf_ptr));
    } catch (const std::exception& e) {
        set_errmsg_ivf_flat(errmsg, "Error in gpu_ivf_flat_new", e);
        return nullptr;
    }
}

gpu_ivf_flat_c gpu_ivf_flat_load_file(const char* filename, uint32_t dimension, distance_type_t metric_c,
                                      ivf_flat_build_params_t build_params,
                                      const int* devices, int device_count, uint32_t nthread, 
                                      distribution_mode_t dist_mode, quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_ivf_flat(metric_c);
        std::vector<int> devs(devices, devices + device_count);
        void* ivf_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                ivf_ptr = new matrixone::gpu_ivf_flat_t<float>(std::string(filename), dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_F16:
                ivf_ptr = new matrixone::gpu_ivf_flat_t<half>(std::string(filename), dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_INT8:
                ivf_ptr = new matrixone::gpu_ivf_flat_t<int8_t>(std::string(filename), dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_UINT8:
                ivf_ptr = new matrixone::gpu_ivf_flat_t<uint8_t>(std::string(filename), dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            default:
                throw std::runtime_error("Unsupported quantization type for IVF-Flat");
        }
        return static_cast<gpu_ivf_flat_c>(new gpu_ivf_flat_any_t(qtype, ivf_ptr));
    } catch (const std::exception& e) {
        set_errmsg_ivf_flat(errmsg, "Error in gpu_ivf_flat_load_file", e);
        return nullptr;
    }
}

void gpu_ivf_flat_destroy(gpu_ivf_flat_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        delete any;
    } catch (const std::exception& e) {
        set_errmsg_ivf_flat(errmsg, "Error in gpu_ivf_flat_destroy", e);
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
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg_ivf_flat(errmsg, "Error in gpu_ivf_flat_load", e);
    }
}

void gpu_ivf_flat_save(gpu_ivf_flat_c index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_ivf_flat_t<float>*>(any->ptr)->save(filename); break;
            case Quantization_F16: static_cast<matrixone::gpu_ivf_flat_t<half>*>(any->ptr)->save(filename); break;
            case Quantization_INT8: static_cast<matrixone::gpu_ivf_flat_t<int8_t>*>(any->ptr)->save(filename); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_ivf_flat_t<uint8_t>*>(any->ptr)->save(filename); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg_ivf_flat(errmsg, "Error in gpu_ivf_flat_save", e);
    }
}

gpu_ivf_flat_search_res_t gpu_ivf_flat_search(gpu_ivf_flat_c index_c, const void* queries_data, uint64_t num_queries, 
                                              uint32_t query_dimension, uint32_t limit, 
                                              ivf_flat_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_ivf_flat_search_res_t res = {nullptr};
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: {
                auto* cpp_res = new matrixone::gpu_ivf_flat_t<float>::search_result_t();
                *cpp_res = static_cast<matrixone::gpu_ivf_flat_t<float>*>(any->ptr)->search(static_cast<const float*>(queries_data), num_queries, query_dimension, limit, search_params);
                res.result_ptr = static_cast<gpu_ivf_flat_result_c>(cpp_res);
                break;
            }
            case Quantization_F16: {
                auto* cpp_res = new matrixone::gpu_ivf_flat_t<half>::search_result_t();
                *cpp_res = static_cast<matrixone::gpu_ivf_flat_t<half>*>(any->ptr)->search(static_cast<const half*>(queries_data), num_queries, query_dimension, limit, search_params);
                res.result_ptr = static_cast<gpu_ivf_flat_result_c>(cpp_res);
                break;
            }
            case Quantization_INT8: {
                auto* cpp_res = new matrixone::gpu_ivf_flat_t<int8_t>::search_result_t();
                *cpp_res = static_cast<matrixone::gpu_ivf_flat_t<int8_t>*>(any->ptr)->search(static_cast<const int8_t*>(queries_data), num_queries, query_dimension, limit, search_params);
                res.result_ptr = static_cast<gpu_ivf_flat_result_c>(cpp_res);
                break;
            }
            case Quantization_UINT8: {
                auto* cpp_res = new matrixone::gpu_ivf_flat_t<uint8_t>::search_result_t();
                *cpp_res = static_cast<matrixone::gpu_ivf_flat_t<uint8_t>*>(any->ptr)->search(static_cast<const uint8_t*>(queries_data), num_queries, query_dimension, limit, search_params);
                res.result_ptr = static_cast<gpu_ivf_flat_result_c>(cpp_res);
                break;
            }
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg_ivf_flat(errmsg, "Error in gpu_ivf_flat_search", e);
    }
    return res;
}

void gpu_ivf_flat_get_neighbors(gpu_ivf_flat_result_c result_c, uint64_t total_elements, int64_t* neighbors) {
    if (!result_c) return;
    // Using float's search_result_t is safe as neighbors is always int64_t
    auto* neighbors_vec = &static_cast<matrixone::gpu_ivf_flat_t<float>::search_result_t*>(result_c)->neighbors;
    if (neighbors_vec->size() >= total_elements) {
        std::copy(neighbors_vec->begin(), neighbors_vec->begin() + total_elements, neighbors);
    }
}

void gpu_ivf_flat_get_distances(gpu_ivf_flat_result_c result_c, uint64_t total_elements, float* distances) {
    if (!result_c) return;
    // Using float's search_result_t is safe as distances is always float
    auto* distances_vec = &static_cast<matrixone::gpu_ivf_flat_t<float>::search_result_t*>(result_c)->distances;
    if (distances_vec->size() >= total_elements) {
        std::copy(distances_vec->begin(), distances_vec->begin() + total_elements, distances);
    }
}

void gpu_ivf_flat_free_result(gpu_ivf_flat_result_c result_c) {
    if (!result_c) return;
    delete static_cast<matrixone::gpu_ivf_flat_t<float>::search_result_t*>(result_c);
}

void gpu_ivf_flat_get_centers(gpu_ivf_flat_c index_c, float* centers, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        if (any->qtype == Quantization_F32) {
            auto host_centers = static_cast<matrixone::gpu_ivf_flat_t<float>*>(any->ptr)->get_centers();
            std::copy(host_centers.begin(), host_centers.end(), centers);
        } else if (any->qtype == Quantization_F16) {
            auto host_centers = static_cast<matrixone::gpu_ivf_flat_t<half>*>(any->ptr)->get_centers();
            for (size_t i = 0; i < host_centers.size(); ++i) centers[i] = (float)host_centers[i];
        } else if (any->qtype == Quantization_INT8) {
            auto host_centers = static_cast<matrixone::gpu_ivf_flat_t<int8_t>*>(any->ptr)->get_centers();
            for (size_t i = 0; i < host_centers.size(); ++i) centers[i] = (float)host_centers[i];
        } else if (any->qtype == Quantization_UINT8) {
            auto host_centers = static_cast<matrixone::gpu_ivf_flat_t<uint8_t>*>(any->ptr)->get_centers();
            for (size_t i = 0; i < host_centers.size(); ++i) centers[i] = (float)host_centers[i];
        }
    } catch (const std::exception& e) {
        set_errmsg_ivf_flat(errmsg, "Error in gpu_ivf_flat_get_centers", e);
    }
}

uint32_t gpu_ivf_flat_get_n_list(gpu_ivf_flat_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<matrixone::gpu_ivf_flat_t<float>*>(any->ptr)->get_n_list();
        case Quantization_F16: return static_cast<matrixone::gpu_ivf_flat_t<half>*>(any->ptr)->get_n_list();
        case Quantization_INT8: return static_cast<matrixone::gpu_ivf_flat_t<int8_t>*>(any->ptr)->get_n_list();
        case Quantization_UINT8: return static_cast<matrixone::gpu_ivf_flat_t<uint8_t>*>(any->ptr)->get_n_list();
        default: return 0;
    }
}

} // extern "C"
