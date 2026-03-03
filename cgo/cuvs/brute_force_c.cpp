#include "brute_force_c.h"
#include "brute_force.hpp"
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
static cuvs::distance::DistanceType convert_distance_type(distance_type_t metric_c) {
    switch (metric_c) {
        case DistanceType_L2Expanded: return cuvs::distance::DistanceType::L2Expanded;
        case DistanceType_L1: return cuvs::distance::DistanceType::L1;
        case DistanceType_InnerProduct: return cuvs::distance::DistanceType::InnerProduct;
        case DistanceType_CosineSimilarity: return cuvs::distance::DistanceType::CosineExpanded;
        default:
            throw std::runtime_error("Unknown distance type");
    }
}

struct gpu_brute_force_any_t {
    quantization_t qtype;
    void* ptr;

    gpu_brute_force_any_t(quantization_t q, void* p) : qtype(q), ptr(p) {}
    ~gpu_brute_force_any_t() {
        switch (qtype) {
            case Quantization_F32: delete static_cast<matrixone::gpu_brute_force_t<float>*>(ptr); break;
            case Quantization_F16: delete static_cast<matrixone::gpu_brute_force_t<half>*>(ptr); break;
            default: break;
        }
    }
};

extern "C" {

gpu_brute_force_c gpu_brute_force_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, distance_type_t metric_c, uint32_t nthread, int device_id, quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type(metric_c);
        void* index_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::gpu_brute_force_t<float>(static_cast<const float*>(dataset_data), count_vectors, dimension, metric, nthread, device_id);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::gpu_brute_force_t<half>(static_cast<const half*>(dataset_data), count_vectors, dimension, metric, nthread, device_id);
                break;
            default:
                throw std::runtime_error("Unsupported quantization type for brute force (only f32 and f16 supported)");
        }
        return static_cast<gpu_brute_force_c>(new gpu_brute_force_any_t(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_new", e);
        return nullptr;
    }
}

void gpu_brute_force_load(gpu_brute_force_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_brute_force_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_brute_force_t<float>*>(any->ptr)->load(); break;
            case Quantization_F16: static_cast<matrixone::gpu_brute_force_t<half>*>(any->ptr)->load(); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_load", e);
    }
}

gpu_brute_force_search_result_c gpu_brute_force_search(gpu_brute_force_c index_c, const void* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_brute_force_any_t*>(index_c);
        void* result_ptr = nullptr;
        switch (any->qtype) {
            case Quantization_F32: {
                auto res = std::make_unique<matrixone::gpu_brute_force_t<float>::search_result_t>();
                *res = static_cast<matrixone::gpu_brute_force_t<float>*>(any->ptr)->search(static_cast<const float*>(queries_data), num_queries, query_dimension, limit);
                result_ptr = res.release();
                break;
            }
            case Quantization_F16: {
                auto res = std::make_unique<matrixone::gpu_brute_force_t<half>::search_result_t>();
                *res = static_cast<matrixone::gpu_brute_force_t<half>*>(any->ptr)->search(static_cast<const half*>(queries_data), num_queries, query_dimension, limit);
                result_ptr = res.release();
                break;
            }
            default: break;
        }
        return static_cast<gpu_brute_force_search_result_c>(result_ptr);
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_search", e);
        return nullptr;
    }
}

void gpu_brute_force_get_results(gpu_brute_force_search_result_c result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances) {
    if (!result_c) return;
    auto* search_result = static_cast<matrixone::gpu_brute_force_t<float>::search_result_t*>(result_c);

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

void gpu_brute_force_free_search_result(gpu_brute_force_search_result_c result_c) {
    if (!result_c) return;
    delete static_cast<matrixone::gpu_brute_force_t<float>::search_result_t*>(result_c);
}

void gpu_brute_force_destroy(gpu_brute_force_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_brute_force_any_t*>(index_c);
        delete any;
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_destroy", e);
    }
}

} // extern "C"

namespace matrixone {
template class gpu_brute_force_t<float>;
template class gpu_brute_force_t<half>;
}
