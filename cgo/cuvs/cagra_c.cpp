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

#include "cagra_c.h"
#include "cagra.hpp"
#include <iostream>
#include <stdexcept>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <limits>
#include <cstring>

struct gpu_cagra_any_t {
    quantization_t qtype;
    void* ptr;

    gpu_cagra_any_t(quantization_t q, void* p) : qtype(q), ptr(p) {}
    ~gpu_cagra_any_t() {
        switch (qtype) {
            case Quantization_F32: delete static_cast<matrixone::gpu_cagra_t<float>*>(ptr); break;
            case Quantization_F16: delete static_cast<matrixone::gpu_cagra_t<half>*>(ptr); break;
            case Quantization_INT8: delete static_cast<matrixone::gpu_cagra_t<int8_t>*>(ptr); break;
            case Quantization_UINT8: delete static_cast<matrixone::gpu_cagra_t<uint8_t>*>(ptr); break;
            default: break;
        }
    }
};

extern "C" {

gpu_cagra_c gpu_cagra_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, distance_type_t metric_c,
                                 cagra_build_params_t build_params,
                                 const int* devices, int device_count, uint32_t nthread, 
                                 distribution_mode_t dist_mode, quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = matrixone::convert_distance_type(metric_c);
        std::vector<int> devs(devices, devices + device_count);
        void* cagra_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                cagra_ptr = new matrixone::gpu_cagra_t<float>(static_cast<const float*>(dataset_data), count_vectors, dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_F16:
                cagra_ptr = new matrixone::gpu_cagra_t<half>(static_cast<const half*>(dataset_data), count_vectors, dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_INT8:
                cagra_ptr = new matrixone::gpu_cagra_t<int8_t>(static_cast<const int8_t*>(dataset_data), count_vectors, dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_UINT8:
                cagra_ptr = new matrixone::gpu_cagra_t<uint8_t>(static_cast<const uint8_t*>(dataset_data), count_vectors, dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            default:
                throw std::runtime_error("Unsupported quantization type for CAGRA");
        }
        return static_cast<gpu_cagra_c>(new gpu_cagra_any_t(qtype, cagra_ptr));
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_new", e.what());
        return nullptr;
    }
}

gpu_cagra_c gpu_cagra_new_empty(uint64_t total_count, uint32_t dimension, distance_type_t metric_c,
                                      cagra_build_params_t build_params,
                                      const int* devices, int device_count, uint32_t nthread, 
                                      distribution_mode_t dist_mode, quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = matrixone::convert_distance_type(metric_c);
        std::vector<int> devs(devices, devices + device_count);
        void* cagra_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                cagra_ptr = new matrixone::gpu_cagra_t<float>(total_count, dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_F16:
                cagra_ptr = new matrixone::gpu_cagra_t<half>(total_count, dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_INT8:
                cagra_ptr = new matrixone::gpu_cagra_t<int8_t>(total_count, dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_UINT8:
                cagra_ptr = new matrixone::gpu_cagra_t<uint8_t>(total_count, dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            default:
                throw std::runtime_error("Unsupported quantization type for CAGRA");
        }
        return static_cast<gpu_cagra_c>(new gpu_cagra_any_t(qtype, cagra_ptr));
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_new_empty", e.what());
        return nullptr;
    }
}

void gpu_cagra_add_chunk(gpu_cagra_c index_c, const void* chunk_data, uint64_t chunk_count, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->add_chunk(static_cast<const float*>(chunk_data), chunk_count); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->add_chunk(static_cast<const half*>(chunk_data), chunk_count); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->add_chunk(static_cast<const int8_t*>(chunk_data), chunk_count); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->add_chunk(static_cast<const uint8_t*>(chunk_data), chunk_count); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_add_chunk", e.what());
    }
}

void gpu_cagra_add_chunk_float(gpu_cagra_c index_c, const float* chunk_data, uint64_t chunk_count, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_add_chunk_float", e.what());
    }
}

void gpu_cagra_train_quantizer(gpu_cagra_c index_c, const float* train_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_train_quantizer", e.what());
    }
}

void gpu_cagra_set_per_thread_device(gpu_cagra_c index_c, bool enable, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->set_per_thread_device(enable); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->set_per_thread_device(enable); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->set_per_thread_device(enable); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->set_per_thread_device(enable); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_set_per_thread_device", e.what());
    }
}

void gpu_cagra_set_use_batching(gpu_cagra_c index_c, bool enable, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->set_use_batching(enable); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->set_use_batching(enable); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->set_use_batching(enable); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->set_use_batching(enable); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_set_use_batching", e.what());
    }
}

void gpu_cagra_set_quantizer(gpu_cagra_c index_c, float min, float max, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->set_quantizer(min, max); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->set_quantizer(min, max); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->set_quantizer(min, max); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->set_quantizer(min, max); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_set_quantizer", e.what());
    }
}

void gpu_cagra_get_quantizer(gpu_cagra_c index_c, float* min, float* max, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->get_quantizer(min, max); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->get_quantizer(min, max); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->get_quantizer(min, max); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->get_quantizer(min, max); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_get_quantizer", e.what());
    }
}

gpu_cagra_c gpu_cagra_load_file(const char* filename, uint32_t dimension, distance_type_t metric_c,
                                      cagra_build_params_t build_params,
                                      const int* devices, int device_count, uint32_t nthread, 
                                      distribution_mode_t dist_mode, quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cuvs::distance::DistanceType metric = matrixone::convert_distance_type(metric_c);
        std::vector<int> devs(devices, devices + device_count);
        void* cagra_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                cagra_ptr = new matrixone::gpu_cagra_t<float>(std::string(filename), dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_F16:
                cagra_ptr = new matrixone::gpu_cagra_t<half>(std::string(filename), dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_INT8:
                cagra_ptr = new matrixone::gpu_cagra_t<int8_t>(std::string(filename), dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_UINT8:
                cagra_ptr = new matrixone::gpu_cagra_t<uint8_t>(std::string(filename), dimension, metric, build_params, devs, nthread, dist_mode);
                break;
            default:
                throw std::runtime_error("Unsupported quantization type for CAGRA");
        }
        return static_cast<gpu_cagra_c>(new gpu_cagra_any_t(qtype, cagra_ptr));
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_load_file", e.what());
        return nullptr;
    }
}

void gpu_cagra_destroy(gpu_cagra_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        delete any;
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_destroy", e.what());
    }
}

void gpu_cagra_start(gpu_cagra_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->start(); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->start(); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->start(); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->start(); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_start", e.what());
    }
}

void gpu_cagra_build(gpu_cagra_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->build(); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->build(); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->build(); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->build(); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_build", e.what());
    }
}

void gpu_cagra_save(gpu_cagra_c index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->save(filename); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->save(filename); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->save(filename); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->save(filename); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_save", e.what());
    }
}

gpu_cagra_search_res_t gpu_cagra_search(gpu_cagra_c index_c, const void* queries_data, uint64_t num_queries, 
                                            uint32_t query_dimension, uint32_t limit, 
                                            cagra_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_cagra_search_res_t res = {nullptr};
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        auto* cpp_res = new matrixone::cagra_search_result_t();
        switch (any->qtype) {
            case Quantization_F32: 
                *cpp_res = static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->search(static_cast<const float*>(queries_data), num_queries, query_dimension, limit, search_params);
                break;
            case Quantization_F16: 
                *cpp_res = static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->search(static_cast<const half*>(queries_data), num_queries, query_dimension, limit, search_params);
                break;
            case Quantization_INT8: 
                *cpp_res = static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->search(static_cast<const int8_t*>(queries_data), num_queries, query_dimension, limit, search_params);
                break;
            case Quantization_UINT8: 
                *cpp_res = static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->search(static_cast<const uint8_t*>(queries_data), num_queries, query_dimension, limit, search_params);
                break;
            default: break;
        }
        res.result_ptr = static_cast<gpu_cagra_result_c>(cpp_res);
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_search", e.what());
    }
    return res;
}

gpu_cagra_search_res_t gpu_cagra_search_float(gpu_cagra_c index_c, const float* queries_data, uint64_t num_queries, 
                                                  uint32_t query_dimension, uint32_t limit, 
                                                  cagra_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_cagra_search_res_t res = {nullptr};
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        auto* cpp_res = new matrixone::cagra_search_result_t();
        switch (any->qtype) {
            case Quantization_F32: 
                *cpp_res = static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, search_params);
                break;
            case Quantization_F16: 
                *cpp_res = static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, search_params);
                break;
            case Quantization_INT8: 
                *cpp_res = static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, search_params);
                break;
            case Quantization_UINT8: 
                *cpp_res = static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, search_params);
                break;
            default: break;
        }
        res.result_ptr = static_cast<gpu_cagra_result_c>(cpp_res);
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_search_float", e.what());
    }
    return res;
}

void gpu_cagra_get_neighbors(gpu_cagra_result_c result_c, uint64_t total_elements, int64_t* neighbors) {
    if (!result_c) return;
    auto* neighbors_vec = &static_cast<matrixone::cagra_search_result_t*>(result_c)->neighbors;
    if (neighbors_vec->size() >= total_elements) {
        std::copy(neighbors_vec->begin(), neighbors_vec->begin() + total_elements, neighbors);
    }
}

void gpu_cagra_get_distances(gpu_cagra_result_c result_c, uint64_t total_elements, float* distances) {
    if (!result_c) return;
    auto* distances_vec = &static_cast<matrixone::cagra_search_result_t*>(result_c)->distances;
    if (distances_vec->size() >= total_elements) {
        std::copy(distances_vec->begin(), distances_vec->begin() + total_elements, distances);
    }
}

void gpu_cagra_free_result(gpu_cagra_result_c result_c) {
    if (!result_c) return;
    delete static_cast<matrixone::cagra_search_result_t*>(result_c);
}

uint32_t gpu_cagra_cap(gpu_cagra_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_cagra_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->cap();
        case Quantization_F16: return static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->cap();
        case Quantization_INT8: return static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->cap();
        case Quantization_UINT8: return static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->cap();
        default: return 0;
    }
}

uint32_t gpu_cagra_len(gpu_cagra_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_cagra_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->len();
        case Quantization_F16: return static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->len();
        case Quantization_INT8: return static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->len();
        case Quantization_UINT8: return static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->len();
        default: return 0;
    }
}

char* gpu_cagra_info(gpu_cagra_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    if (!index_c) return nullptr;
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        std::string info;
        switch (any->qtype) {
            case Quantization_F32: info = static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->info(); break;
            case Quantization_F16: info = static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->info(); break;
            case Quantization_INT8: info = static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->info(); break;
            case Quantization_UINT8: info = static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->info(); break;
            default: return nullptr;
        }
        return strdup(info.c_str());
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_info", e.what());
        return nullptr;
    }
}

void gpu_cagra_extend(gpu_cagra_c index_c, const void* additional_data, uint64_t num_vectors, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_cagra_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_cagra_t<float>*>(any->ptr)->extend(static_cast<const float*>(additional_data), num_vectors); break;
            case Quantization_F16: static_cast<matrixone::gpu_cagra_t<half>*>(any->ptr)->extend(static_cast<const half*>(additional_data), num_vectors); break;
            case Quantization_INT8: static_cast<matrixone::gpu_cagra_t<int8_t>*>(any->ptr)->extend(static_cast<const int8_t*>(additional_data), num_vectors); break;
            case Quantization_UINT8: static_cast<matrixone::gpu_cagra_t<uint8_t>*>(any->ptr)->extend(static_cast<const uint8_t*>(additional_data), num_vectors); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_extend", e.what());
    }
}

gpu_cagra_c gpu_cagra_merge(gpu_cagra_c* indices_c, int count, uint32_t nthread, const int* devices, int device_count, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        if (count <= 0) return nullptr;
        std::vector<int> devs(devices, devices + device_count);
        auto* first_any = static_cast<gpu_cagra_any_t*>(indices_c[0]);
        quantization_t qtype = first_any->qtype;

        void* merged_ptr = nullptr;
        switch (qtype) {
            case Quantization_F32: {
                std::vector<matrixone::gpu_cagra_t<float>*> indices;
                for (int i = 0; i < count; ++i) indices.push_back(static_cast<matrixone::gpu_cagra_t<float>*>(static_cast<gpu_cagra_any_t*>(indices_c[i])->ptr));
                merged_ptr = matrixone::gpu_cagra_t<float>::merge(indices, nthread, devs).release();
                break;
            }
            case Quantization_F16: {
                std::vector<matrixone::gpu_cagra_t<half>*> indices;
                for (int i = 0; i < count; ++i) indices.push_back(static_cast<matrixone::gpu_cagra_t<half>*>(static_cast<gpu_cagra_any_t*>(indices_c[i])->ptr));
                merged_ptr = matrixone::gpu_cagra_t<half>::merge(indices, nthread, devs).release();
                break;
            }
            case Quantization_INT8: {
                std::vector<matrixone::gpu_cagra_t<int8_t>*> indices;
                for (int i = 0; i < count; ++i) indices.push_back(static_cast<matrixone::gpu_cagra_t<int8_t>*>(static_cast<gpu_cagra_any_t*>(indices_c[i])->ptr));
                merged_ptr = matrixone::gpu_cagra_t<int8_t>::merge(indices, nthread, devs).release();
                break;
            }
            case Quantization_UINT8: {
                std::vector<matrixone::gpu_cagra_t<uint8_t>*> indices;
                for (int i = 0; i < count; ++i) indices.push_back(static_cast<matrixone::gpu_cagra_t<uint8_t>*>(static_cast<gpu_cagra_any_t*>(indices_c[i])->ptr));
                merged_ptr = matrixone::gpu_cagra_t<uint8_t>::merge(indices, nthread, devs).release();
                break;
            }
            default: break;
        }
        return static_cast<gpu_cagra_c>(new gpu_cagra_any_t(qtype, merged_ptr));
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_cagra_merge", e.what());
        return nullptr;
    }
}

} // extern "C"

namespace matrixone {
template class gpu_cagra_t<float>;
template class gpu_cagra_t<half>;
template class gpu_cagra_t<int8_t>;
template class gpu_cagra_t<uint8_t>;
} // namespace matrixone
