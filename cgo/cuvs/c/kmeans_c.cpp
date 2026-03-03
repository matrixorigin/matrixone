#include "kmeans_c.h"
#include "../cpp/kmeans.hpp"
#include <iostream>
#include <stdexcept>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <cstring>

// Helper to set error message
static void set_errmsg_kmeans(void* errmsg, const std::string& prefix, const std::exception& e) {
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
static cuvs::distance::DistanceType convert_distance_type_kmeans(distance_type_t metric_c) {
    switch (metric_c) {
        case DistanceType_L2Expanded: return cuvs::distance::DistanceType::L2Expanded;
        case DistanceType_L1: return cuvs::distance::DistanceType::L1;
        case DistanceType_InnerProduct: return cuvs::distance::DistanceType::InnerProduct;
        case DistanceType_CosineSimilarity: return cuvs::distance::DistanceType::CosineExpanded;
        default:
            throw std::runtime_error("Unknown distance type");
    }
}

struct gpu_kmeans_any_t {
    quantization_t qtype;
    void* ptr;

    gpu_kmeans_any_t(quantization_t q, void* p) : qtype(q), ptr(p) {}
    ~gpu_kmeans_any_t() {
        switch (qtype) {
            case Quantization_F32: delete static_cast<matrixone::gpu_kmeans_t<float>*>(ptr); break;
            case Quantization_F16: delete static_cast<matrixone::gpu_kmeans_t<half>*>(ptr); break;
            case Quantization_INT8: delete static_cast<matrixone::gpu_kmeans_t<int8_t>*>(ptr); break;
            case Quantization_UINT8: delete static_cast<matrixone::gpu_kmeans_t<uint8_t>*>(ptr); break;
            default: break;
        }
    }
};

extern "C" {

gpu_kmeans_c gpu_kmeans_new(uint32_t n_clusters, uint32_t dimension, distance_type_t metric_c,
                            int max_iter, int device_id, uint32_t nthread, 
                            quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = convert_distance_type_kmeans(metric_c);
        void* kmeans_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                kmeans_ptr = new matrixone::gpu_kmeans_t<float>(n_clusters, dimension, metric, max_iter, device_id, nthread);
                break;
            case Quantization_F16:
                kmeans_ptr = new matrixone::gpu_kmeans_t<half>(n_clusters, dimension, metric, max_iter, device_id, nthread);
                break;
            case Quantization_INT8:
                kmeans_ptr = new matrixone::gpu_kmeans_t<int8_t>(n_clusters, dimension, metric, max_iter, device_id, nthread);
                break;
            case Quantization_UINT8:
                kmeans_ptr = new matrixone::gpu_kmeans_t<uint8_t>(n_clusters, dimension, metric, max_iter, device_id, nthread);
                break;
            default:
                throw std::runtime_error("Unsupported quantization type for KMeans");
        }
        return static_cast<gpu_kmeans_c>(new gpu_kmeans_any_t(qtype, kmeans_ptr));
    } catch (const std::exception& e) {
        set_errmsg_kmeans(errmsg, "Error in gpu_kmeans_new", e);
        return nullptr;
    }
}

void gpu_kmeans_destroy(gpu_kmeans_c kmeans_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        delete any;
    } catch (const std::exception& e) {
        set_errmsg_kmeans(errmsg, "Error in gpu_kmeans_destroy", e);
    }
}

gpu_kmeans_fit_res_t gpu_kmeans_fit(gpu_kmeans_c kmeans_c, const void* X_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_kmeans_fit_res_t res = {0.0f, 0};
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        switch (any->qtype) {
            case Quantization_F32: {
                auto cpp_res = static_cast<matrixone::gpu_kmeans_t<float>*>(any->ptr)->fit(static_cast<const float*>(X_data), n_samples);
                res.inertia = cpp_res.inertia;
                res.n_iter = cpp_res.n_iter;
                break;
            }
            case Quantization_F16: {
                auto cpp_res = static_cast<matrixone::gpu_kmeans_t<half>*>(any->ptr)->fit(static_cast<const half*>(X_data), n_samples);
                res.inertia = cpp_res.inertia;
                res.n_iter = cpp_res.n_iter;
                break;
            }
            case Quantization_INT8: {
                auto cpp_res = static_cast<matrixone::gpu_kmeans_t<int8_t>*>(any->ptr)->fit(static_cast<const int8_t*>(X_data), n_samples);
                res.inertia = cpp_res.inertia;
                res.n_iter = cpp_res.n_iter;
                break;
            }
            case Quantization_UINT8: {
                auto cpp_res = static_cast<matrixone::gpu_kmeans_t<uint8_t>*>(any->ptr)->fit(static_cast<const uint8_t*>(X_data), n_samples);
                res.inertia = cpp_res.inertia;
                res.n_iter = cpp_res.n_iter;
                break;
            }
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg_kmeans(errmsg, "Error in gpu_kmeans_fit", e);
    }
    return res;
}

gpu_kmeans_predict_res_t gpu_kmeans_predict(gpu_kmeans_c kmeans_c, const void* X_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_kmeans_predict_res_t res = {nullptr, 0.0f};
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        switch (any->qtype) {
            case Quantization_F32: {
                auto* cpp_res = new matrixone::gpu_kmeans_t<float>::predict_result_t();
                *cpp_res = static_cast<matrixone::gpu_kmeans_t<float>*>(any->ptr)->predict(static_cast<const float*>(X_data), n_samples);
                res.result_ptr = static_cast<gpu_kmeans_result_c>(cpp_res);
                res.inertia = cpp_res->inertia;
                break;
            }
            case Quantization_F16: {
                auto* cpp_res = new matrixone::gpu_kmeans_t<half>::predict_result_t();
                *cpp_res = static_cast<matrixone::gpu_kmeans_t<half>*>(any->ptr)->predict(static_cast<const half*>(X_data), n_samples);
                res.result_ptr = static_cast<gpu_kmeans_result_c>(cpp_res);
                res.inertia = cpp_res->inertia;
                break;
            }
            case Quantization_INT8: {
                auto* cpp_res = new matrixone::gpu_kmeans_t<int8_t>::predict_result_t();
                *cpp_res = static_cast<matrixone::gpu_kmeans_t<int8_t>*>(any->ptr)->predict(static_cast<const int8_t*>(X_data), n_samples);
                res.result_ptr = static_cast<gpu_kmeans_result_c>(cpp_res);
                res.inertia = cpp_res->inertia;
                break;
            }
            case Quantization_UINT8: {
                auto* cpp_res = new matrixone::gpu_kmeans_t<uint8_t>::predict_result_t();
                *cpp_res = static_cast<matrixone::gpu_kmeans_t<uint8_t>*>(any->ptr)->predict(static_cast<const uint8_t*>(X_data), n_samples);
                res.result_ptr = static_cast<gpu_kmeans_result_c>(cpp_res);
                res.inertia = cpp_res->inertia;
                break;
            }
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg_kmeans(errmsg, "Error in gpu_kmeans_predict", e);
    }
    return res;
}

gpu_kmeans_fit_predict_res_t gpu_kmeans_fit_predict(gpu_kmeans_c kmeans_c, const void* X_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_kmeans_fit_predict_res_t res = {nullptr, 0.0f, 0};
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        switch (any->qtype) {
            case Quantization_F32: {
                auto* cpp_res = new matrixone::gpu_kmeans_t<float>::fit_predict_result_t();
                *cpp_res = static_cast<matrixone::gpu_kmeans_t<float>*>(any->ptr)->fit_predict(static_cast<const float*>(X_data), n_samples);
                res.result_ptr = static_cast<gpu_kmeans_result_c>(cpp_res);
                res.inertia = cpp_res->inertia;
                res.n_iter = cpp_res->n_iter;
                break;
            }
            case Quantization_F16: {
                auto* cpp_res = new matrixone::gpu_kmeans_t<half>::fit_predict_result_t();
                *cpp_res = static_cast<matrixone::gpu_kmeans_t<half>*>(any->ptr)->fit_predict(static_cast<const half*>(X_data), n_samples);
                res.result_ptr = static_cast<gpu_kmeans_result_c>(cpp_res);
                res.inertia = cpp_res->inertia;
                res.n_iter = cpp_res->n_iter;
                break;
            }
            case Quantization_INT8: {
                auto* cpp_res = new matrixone::gpu_kmeans_t<int8_t>::fit_predict_result_t();
                *cpp_res = static_cast<matrixone::gpu_kmeans_t<int8_t>*>(any->ptr)->fit_predict(static_cast<const int8_t*>(X_data), n_samples);
                res.result_ptr = static_cast<gpu_kmeans_result_c>(cpp_res);
                res.inertia = cpp_res->inertia;
                res.n_iter = cpp_res->n_iter;
                break;
            }
            case Quantization_UINT8: {
                auto* cpp_res = new matrixone::gpu_kmeans_t<uint8_t>::fit_predict_result_t();
                *cpp_res = static_cast<matrixone::gpu_kmeans_t<uint8_t>*>(any->ptr)->fit_predict(static_cast<const uint8_t*>(X_data), n_samples);
                res.result_ptr = static_cast<gpu_kmeans_result_c>(cpp_res);
                res.inertia = cpp_res->inertia;
                res.n_iter = cpp_res->n_iter;
                break;
            }
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg_kmeans(errmsg, "Error in gpu_kmeans_fit_predict", e);
    }
    return res;
}

void gpu_kmeans_get_labels(gpu_kmeans_result_c result_c, uint64_t n_samples, int64_t* labels) {
    if (!result_c) return;
    // Both predict_result_t and fit_predict_result_t have labels as their first member
    auto* labels_vec = &static_cast<matrixone::gpu_kmeans_t<float>::predict_result_t*>(result_c)->labels;
    if (labels_vec->size() >= n_samples) {
        std::copy(labels_vec->begin(), labels_vec->begin() + n_samples, labels);
    }
}

void gpu_kmeans_free_result(gpu_kmeans_result_c result_c) {
    if (!result_c) return;
    delete static_cast<matrixone::gpu_kmeans_t<float>::predict_result_t*>(result_c);
}

void gpu_kmeans_get_centroids(gpu_kmeans_c kmeans_c, void* centroids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        switch (any->qtype) {
            case Quantization_F32: {
                auto host_centroids = static_cast<matrixone::gpu_kmeans_t<float>*>(any->ptr)->get_centroids();
                std::copy(host_centroids.begin(), host_centroids.end(), static_cast<float*>(centroids));
                break;
            }
            case Quantization_F16: {
                auto host_centroids = static_cast<matrixone::gpu_kmeans_t<half>*>(any->ptr)->get_centroids();
                std::copy(host_centroids.begin(), host_centroids.end(), static_cast<float*>(centroids));
                break;
            }
            case Quantization_INT8: {
                auto host_centroids = static_cast<matrixone::gpu_kmeans_t<int8_t>*>(any->ptr)->get_centroids();
                std::copy(host_centroids.begin(), host_centroids.end(), static_cast<float*>(centroids));
                break;
            }
            case Quantization_UINT8: {
                auto host_centroids = static_cast<matrixone::gpu_kmeans_t<uint8_t>*>(any->ptr)->get_centroids();
                std::copy(host_centroids.begin(), host_centroids.end(), static_cast<float*>(centroids));
                break;
            }
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg_kmeans(errmsg, "Error in gpu_kmeans_get_centroids", e);
    }
}

} // extern "C"
