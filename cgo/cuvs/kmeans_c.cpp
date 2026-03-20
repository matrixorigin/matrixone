/* 
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * K-Means C Wrapper Implementation
 * Supported data types (via quantization_t): Quantization_F32, Quantization_F16, Quantization_INT8, Quantization_UINT8
 */

#include "kmeans_c.h"
#include "kmeans.hpp"
#include <iostream>
#include <vector>
#include <cstring>

using namespace matrixone;

struct gpu_kmeans_any_t {
    quantization_t qtype;
    void* ptr;

    gpu_kmeans_any_t(quantization_t q, void* p) : qtype(q), ptr(p) {}
    ~gpu_kmeans_any_t() {
        switch (qtype) {
            case Quantization_F32: delete static_cast<gpu_kmeans_t<float>*>(ptr); break;
            case Quantization_F16: delete static_cast<gpu_kmeans_t<half>*>(ptr); break;
            case Quantization_INT8: delete static_cast<gpu_kmeans_t<int8_t>*>(ptr); break;
            case Quantization_UINT8: delete static_cast<gpu_kmeans_t<uint8_t>*>(ptr); break;
            default: break;
        }
    }
};

extern "C" {

gpu_kmeans_c gpu_kmeans_new(uint32_t n_clusters, uint32_t dimension, distance_type_t metric_c,
                            int max_iter, int device_id, uint32_t nthread, 
                            quantization_t qtype, void* errmsg) {
    void* ptr = nullptr;
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        switch (qtype) {
            case Quantization_F32:
                ptr = new gpu_kmeans_t<float>(n_clusters, dimension, metric_c, max_iter, device_id, nthread);
                break;
            case Quantization_F16:
                ptr = new gpu_kmeans_t<half>(n_clusters, dimension, metric_c, max_iter, device_id, nthread);
                break;
            case Quantization_INT8:
                ptr = new gpu_kmeans_t<int8_t>(n_clusters, dimension, metric_c, max_iter, device_id, nthread);
                break;
            case Quantization_UINT8:
                ptr = new gpu_kmeans_t<uint8_t>(n_clusters, dimension, metric_c, max_iter, device_id, nthread);
                break;
            default: return nullptr;
        }
        if (ptr) static_cast<gpu_index_base_t<float, kmeans_build_params_t>*>(ptr)->start();
        return static_cast<gpu_kmeans_c>(new gpu_kmeans_any_t(qtype, ptr));
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_kmeans_new", e.what());
    }
    return nullptr;
}

void gpu_kmeans_destroy(gpu_kmeans_c kmeans_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        delete static_cast<gpu_kmeans_any_t*>(kmeans_c);
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_kmeans_destroy", e.what());
    }
}

void gpu_kmeans_start(gpu_kmeans_c kmeans_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_kmeans_t<float>*>(any->ptr)->start(); break;
            case Quantization_F16: static_cast<gpu_kmeans_t<half>*>(any->ptr)->start(); break;
            case Quantization_INT8: static_cast<gpu_kmeans_t<int8_t>*>(any->ptr)->start(); break;
            case Quantization_UINT8: static_cast<gpu_kmeans_t<uint8_t>*>(any->ptr)->start(); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_kmeans_start", e.what());
    }
}

void gpu_kmeans_train_quantizer(gpu_kmeans_c kmeans_c, const float* train_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_kmeans_t<float>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            case Quantization_F16: static_cast<gpu_kmeans_t<half>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            case Quantization_INT8: static_cast<gpu_kmeans_t<int8_t>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            case Quantization_UINT8: static_cast<gpu_kmeans_t<uint8_t>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_kmeans_train_quantizer", e.what());
    }
}

void gpu_kmeans_set_quantizer(gpu_kmeans_c kmeans_c, float min, float max, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_kmeans_t<float>*>(any->ptr)->set_quantizer(min, max); break;
            case Quantization_F16: static_cast<gpu_kmeans_t<half>*>(any->ptr)->set_quantizer(min, max); break;
            case Quantization_INT8: static_cast<gpu_kmeans_t<int8_t>*>(any->ptr)->set_quantizer(min, max); break;
            case Quantization_UINT8: static_cast<gpu_kmeans_t<uint8_t>*>(any->ptr)->set_quantizer(min, max); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_kmeans_set_quantizer", e.what());
    }
}

void gpu_kmeans_get_quantizer(gpu_kmeans_c kmeans_c, float* min, float* max, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_kmeans_t<float>*>(any->ptr)->get_quantizer(min, max); break;
            case Quantization_F16: static_cast<gpu_kmeans_t<half>*>(any->ptr)->get_quantizer(min, max); break;
            case Quantization_INT8: static_cast<gpu_kmeans_t<int8_t>*>(any->ptr)->get_quantizer(min, max); break;
            case Quantization_UINT8: static_cast<gpu_kmeans_t<uint8_t>*>(any->ptr)->get_quantizer(min, max); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_kmeans_get_quantizer", e.what());
    }
}

gpu_kmeans_fit_res_t gpu_kmeans_fit(gpu_kmeans_c kmeans_c, const void* X_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_kmeans_fit_res_t result = {0.0f, 0};
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        kmeans_result_t res;
        switch (any->qtype) {
            case Quantization_F32: res = static_cast<gpu_kmeans_t<float>*>(any->ptr)->fit(static_cast<const float*>(X_data), n_samples); break;
            case Quantization_F16: res = static_cast<gpu_kmeans_t<half>*>(any->ptr)->fit(static_cast<const half*>(X_data), n_samples); break;
            case Quantization_INT8: res = static_cast<gpu_kmeans_t<int8_t>*>(any->ptr)->fit(static_cast<const int8_t*>(X_data), n_samples); break;
            case Quantization_UINT8: res = static_cast<gpu_kmeans_t<uint8_t>*>(any->ptr)->fit(static_cast<const uint8_t*>(X_data), n_samples); break;
            default: break;
        }
        result.inertia = res.inertia;
        result.n_iter = (int)res.n_iter;
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_kmeans_fit", e.what());
    }
    return result;
}

gpu_kmeans_predict_res_t gpu_kmeans_predict(gpu_kmeans_c kmeans_c, const void* X_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_kmeans_predict_res_t result = {nullptr, 0.0f};
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        auto* cpp_res = new kmeans_result_t();
        switch (any->qtype) {
            case Quantization_F32: *cpp_res = static_cast<gpu_kmeans_t<float>*>(any->ptr)->predict(static_cast<const float*>(X_data), n_samples); break;
            case Quantization_F16: *cpp_res = static_cast<gpu_kmeans_t<half>*>(any->ptr)->predict(static_cast<const half*>(X_data), n_samples); break;
            case Quantization_INT8: *cpp_res = static_cast<gpu_kmeans_t<int8_t>*>(any->ptr)->predict(static_cast<const int8_t*>(X_data), n_samples); break;
            case Quantization_UINT8: *cpp_res = static_cast<gpu_kmeans_t<uint8_t>*>(any->ptr)->predict(static_cast<const uint8_t*>(X_data), n_samples); break;
            default: break;
        }
        result.result_ptr = static_cast<gpu_kmeans_result_c>(cpp_res);
        result.inertia = cpp_res->inertia;
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_kmeans_predict", e.what());
    }
    return result;
}

gpu_kmeans_predict_res_t gpu_kmeans_predict_float(gpu_kmeans_c kmeans_c, const float* X_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_kmeans_predict_res_t result = {nullptr, 0.0f};
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        auto* cpp_res = new kmeans_result_t();
        switch (any->qtype) {
            case Quantization_F32: *cpp_res = static_cast<gpu_kmeans_t<float>*>(any->ptr)->predict_float(X_data, n_samples); break;
            case Quantization_F16: *cpp_res = static_cast<gpu_kmeans_t<half>*>(any->ptr)->predict_float(X_data, n_samples); break;
            case Quantization_INT8: *cpp_res = static_cast<gpu_kmeans_t<int8_t>*>(any->ptr)->predict_float(X_data, n_samples); break;
            case Quantization_UINT8: *cpp_res = static_cast<gpu_kmeans_t<uint8_t>*>(any->ptr)->predict_float(X_data, n_samples); break;
            default: break;
        }
        result.result_ptr = static_cast<gpu_kmeans_result_c>(cpp_res);
        result.inertia = cpp_res->inertia;
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_kmeans_predict_float", e.what());
    }
    return result;
}

gpu_kmeans_fit_predict_res_t gpu_kmeans_fit_predict(gpu_kmeans_c kmeans_c, const void* X_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_kmeans_fit_predict_res_t result = {nullptr, 0.0f, 0};
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        auto* cpp_res = new kmeans_result_t();
        switch (any->qtype) {
            case Quantization_F32: *cpp_res = static_cast<gpu_kmeans_t<float>*>(any->ptr)->fit_predict(static_cast<const float*>(X_data), n_samples); break;
            case Quantization_F16: *cpp_res = static_cast<gpu_kmeans_t<half>*>(any->ptr)->fit_predict(static_cast<const half*>(X_data), n_samples); break;
            case Quantization_INT8: *cpp_res = static_cast<gpu_kmeans_t<int8_t>*>(any->ptr)->fit_predict(static_cast<const int8_t*>(X_data), n_samples); break;
            case Quantization_UINT8: *cpp_res = static_cast<gpu_kmeans_t<uint8_t>*>(any->ptr)->fit_predict(static_cast<const uint8_t*>(X_data), n_samples); break;
            default: break;
        }
        result.result_ptr = static_cast<gpu_kmeans_result_c>(cpp_res);
        result.inertia = cpp_res->inertia;
        result.n_iter = (int)cpp_res->n_iter;
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_kmeans_fit_predict", e.what());
    }
    return result;
}

gpu_kmeans_fit_predict_res_t gpu_kmeans_fit_predict_float(gpu_kmeans_c kmeans_c, const float* X_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_kmeans_fit_predict_res_t result = {nullptr, 0.0f, 0};
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        auto* cpp_res = new kmeans_result_t();
        switch (any->qtype) {
            case Quantization_F32: *cpp_res = static_cast<gpu_kmeans_t<float>*>(any->ptr)->fit_predict_float(X_data, n_samples); break;
            case Quantization_F16: *cpp_res = static_cast<gpu_kmeans_t<half>*>(any->ptr)->fit_predict_float(X_data, n_samples); break;
            case Quantization_INT8: *cpp_res = static_cast<gpu_kmeans_t<int8_t>*>(any->ptr)->fit_predict_float(X_data, n_samples); break;
            case Quantization_UINT8: *cpp_res = static_cast<gpu_kmeans_t<uint8_t>*>(any->ptr)->fit_predict_float(X_data, n_samples); break;
            default: break;
        }
        result.result_ptr = static_cast<gpu_kmeans_result_c>(cpp_res);
        result.inertia = cpp_res->inertia;
        result.n_iter = (int)cpp_res->n_iter;
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_kmeans_fit_predict_float", e.what());
    }
    return result;
}

void gpu_kmeans_get_labels(gpu_kmeans_result_c result_c, uint64_t n_samples, int64_t* labels) {
    if (!result_c) return;
    auto* labels_vec = &static_cast<kmeans_result_t*>(result_c)->labels;
    if (labels_vec->size() >= n_samples) {
        std::copy(labels_vec->begin(), labels_vec->begin() + n_samples, labels);
    }
}

void gpu_kmeans_free_result(gpu_kmeans_result_c result_c) {
    if (!result_c) return;
    delete static_cast<kmeans_result_t*>(result_c);
}

void gpu_kmeans_get_centroids(gpu_kmeans_c kmeans_c, void* centroids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        switch (any->qtype) {
            case Quantization_F32: {
                auto host_centroids = static_cast<gpu_kmeans_t<float>*>(any->ptr)->get_centroids();
                if (!host_centroids.empty()) std::copy(host_centroids.begin(), host_centroids.end(), static_cast<float*>(centroids));
                break;
            }
            case Quantization_F16: {
                auto host_centroids = static_cast<gpu_kmeans_t<half>*>(any->ptr)->get_centroids();
                if (!host_centroids.empty()) std::copy(host_centroids.begin(), host_centroids.end(), static_cast<half*>(centroids));
                break;
            }
            case Quantization_INT8: {
                auto host_centroids = static_cast<gpu_kmeans_t<int8_t>*>(any->ptr)->get_centroids();
                if (!host_centroids.empty()) std::copy(host_centroids.begin(), host_centroids.end(), static_cast<int8_t*>(centroids));
                break;
            }
            case Quantization_UINT8: {
                auto host_centroids = static_cast<gpu_kmeans_t<uint8_t>*>(any->ptr)->get_centroids();
                if (!host_centroids.empty()) std::copy(host_centroids.begin(), host_centroids.end(), static_cast<uint8_t*>(centroids));
                break;
            }
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_kmeans_get_centroids", e.what());
    }
}

char* gpu_kmeans_info(gpu_kmeans_c kmeans_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    if (!kmeans_c) return nullptr;
    try {
        auto* any = static_cast<gpu_kmeans_any_t*>(kmeans_c);
        std::string info;
        switch (any->qtype) {
            case Quantization_F32: info = static_cast<gpu_kmeans_t<float>*>(any->ptr)->info(); break;
            case Quantization_F16: info = static_cast<gpu_kmeans_t<half>*>(any->ptr)->info(); break;
            case Quantization_INT8: info = static_cast<gpu_kmeans_t<int8_t>*>(any->ptr)->info(); break;
            case Quantization_UINT8: info = static_cast<gpu_kmeans_t<uint8_t>*>(any->ptr)->info(); break;
            default: return nullptr;
        }
        return strdup(info.c_str());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_kmeans_info", e.what());
        return nullptr;
    }
}

} // extern "C"

namespace matrixone {
template class gpu_kmeans_t<float>;
template class gpu_kmeans_t<half>;
template class gpu_kmeans_t<int8_t>;
template class gpu_kmeans_t<uint8_t>;
} // namespace matrixone
