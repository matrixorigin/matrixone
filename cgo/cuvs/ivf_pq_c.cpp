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
 * IVF-PQ C Wrapper Implementation
 * Supported data types (via quantization_t): Quantization_F32, Quantization_F16, Quantization_INT8, Quantization_UINT8
 */

#include "ivf_pq_c.h"
#include "ivf_pq.hpp"
#include <iostream>
#include <vector>
#include <cstring>

using namespace matrixone;

struct gpu_ivf_pq_any_t {
    quantization_t qtype;
    void* ptr;

    gpu_ivf_pq_any_t(quantization_t q, void* p) : qtype(q), ptr(p) {}
    ~gpu_ivf_pq_any_t() {
        switch (qtype) {
            case Quantization_F32: {
                auto* p = static_cast<gpu_ivf_pq_t<float>*>(ptr);
                p->destroy();
                delete p;
                break;
            }
            case Quantization_F16: {
                auto* p = static_cast<gpu_ivf_pq_t<half>*>(ptr);
                p->destroy();
                delete p;
                break;
            }
            case Quantization_INT8: {
                auto* p = static_cast<gpu_ivf_pq_t<int8_t>*>(ptr);
                p->destroy();
                delete p;
                break;
            }
            case Quantization_UINT8: {
                auto* p = static_cast<gpu_ivf_pq_t<uint8_t>*>(ptr);
                p->destroy();
                delete p;
                break;
            }
            default: break;
        }
    }
};

extern "C" {

gpu_ivf_pq_c gpu_ivf_pq_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                               distance_type_t metric_c, ivf_pq_build_params_t build_params,
                               const int* devices, int device_count, uint32_t nthread, 
                               distribution_mode_t dist_mode, quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::vector<int> devs(devices, devices + device_count);
        void* ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                ptr = new gpu_ivf_pq_t<float>(static_cast<const float*>(dataset_data), count_vectors, dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_F16:
                ptr = new gpu_ivf_pq_t<half>(static_cast<const half*>(dataset_data), count_vectors, dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_INT8:
                ptr = new gpu_ivf_pq_t<int8_t>(static_cast<const int8_t*>(dataset_data), count_vectors, dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_UINT8:
                ptr = new gpu_ivf_pq_t<uint8_t>(static_cast<const uint8_t*>(dataset_data), count_vectors, dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            default: return nullptr;
        }
        static_cast<gpu_index_base_t<float, ivf_pq_build_params_t>*>(ptr)->start();
        return static_cast<gpu_ivf_pq_c>(new gpu_ivf_pq_any_t(qtype, ptr));
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_new", e.what());
    }
    return nullptr;
}

gpu_ivf_pq_c gpu_ivf_pq_new_from_data_file(const char* data_filename, distance_type_t metric_c, 
                                                ivf_pq_build_params_t build_params,
                                                const int* devices, int device_count, uint32_t nthread, 
                                                distribution_mode_t dist_mode, quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::vector<int> devs(devices, devices + device_count);
        void* ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                ptr = new gpu_ivf_pq_t<float>(std::string(data_filename), metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_F16:
                ptr = new gpu_ivf_pq_t<half>(std::string(data_filename), metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_INT8:
                ptr = new gpu_ivf_pq_t<int8_t>(std::string(data_filename), metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_UINT8:
                ptr = new gpu_ivf_pq_t<uint8_t>(std::string(data_filename), metric_c, build_params, devs, nthread, dist_mode);
                break;
            default: return nullptr;
        }
        static_cast<gpu_index_base_t<float, ivf_pq_build_params_t>*>(ptr)->start();
        return static_cast<gpu_ivf_pq_c>(new gpu_ivf_pq_any_t(qtype, ptr));
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_new_from_data_file", e.what());
    }
    return nullptr;
}

gpu_ivf_pq_c gpu_ivf_pq_new_empty(uint64_t total_count, uint32_t dimension, distance_type_t metric_c, 
                                         ivf_pq_build_params_t build_params,
                                         const int* devices, int device_count, uint32_t nthread, 
                                         distribution_mode_t dist_mode, quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::vector<int> devs(devices, devices + device_count);
        void* ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                ptr = new gpu_ivf_pq_t<float>(total_count, dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_F16:
                ptr = new gpu_ivf_pq_t<half>(total_count, dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_INT8:
                ptr = new gpu_ivf_pq_t<int8_t>(total_count, dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_UINT8:
                ptr = new gpu_ivf_pq_t<uint8_t>(total_count, dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            default: return nullptr;
        }
        static_cast<gpu_index_base_t<float, ivf_pq_build_params_t>*>(ptr)->start();
        return static_cast<gpu_ivf_pq_c>(new gpu_ivf_pq_any_t(qtype, ptr));
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_new_empty", e.what());
    }
    return nullptr;
}

gpu_ivf_pq_c gpu_ivf_pq_load_file(const char* filename, uint32_t dimension, distance_type_t metric_c,
                                    ivf_pq_build_params_t build_params,
                                    const int* devices, int device_count, uint32_t nthread, 
                                    distribution_mode_t dist_mode, quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::vector<int> devs(devices, devices + device_count);
        void* ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                ptr = new gpu_ivf_pq_t<float>(std::string(filename), dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_F16:
                ptr = new gpu_ivf_pq_t<half>(std::string(filename), dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_INT8:
                ptr = new gpu_ivf_pq_t<int8_t>(std::string(filename), dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_UINT8:
                ptr = new gpu_ivf_pq_t<uint8_t>(std::string(filename), dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            default: return nullptr;
        }
        static_cast<gpu_index_base_t<float, ivf_pq_build_params_t>*>(ptr)->start();
        return static_cast<gpu_ivf_pq_c>(new gpu_ivf_pq_any_t(qtype, ptr));
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_load_file", e.what());
    }
    return nullptr;
}

void gpu_ivf_pq_destroy(gpu_ivf_pq_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        delete static_cast<gpu_ivf_pq_any_t*>(index_c);
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_destroy", e.what());
    }
}

void gpu_ivf_pq_start(gpu_ivf_pq_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->start(); break;
            case Quantization_F16: static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->start(); break;
            case Quantization_INT8: static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->start(); break;
            case Quantization_UINT8: static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->start(); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_start", e.what());
    }
}

void gpu_ivf_pq_build(gpu_ivf_pq_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->build(); break;
            case Quantization_F16: static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->build(); break;
            case Quantization_INT8: static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->build(); break;
            case Quantization_UINT8: static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->build(); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_build", e.what());
    }
}

void gpu_ivf_pq_add_chunk(gpu_ivf_pq_c index_c, const void* chunk_data, uint64_t chunk_count, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->add_chunk(static_cast<const float*>(chunk_data), chunk_count); break;
            case Quantization_F16: static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->add_chunk(static_cast<const half*>(chunk_data), chunk_count); break;
            case Quantization_INT8: static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->add_chunk(static_cast<const int8_t*>(chunk_data), chunk_count); break;
            case Quantization_UINT8: static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->add_chunk(static_cast<const uint8_t*>(chunk_data), chunk_count); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_add_chunk", e.what());
    }
}

void gpu_ivf_pq_add_chunk_float(gpu_ivf_pq_c index_c, const float* chunk_data, uint64_t chunk_count, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count); break;
            case Quantization_F16: static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count); break;
            case Quantization_INT8: static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count); break;
            case Quantization_UINT8: static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_add_chunk_float", e.what());
    }
}

void gpu_ivf_pq_train_quantizer(gpu_ivf_pq_c index_c, const float* train_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            case Quantization_F16: static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            case Quantization_INT8: static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            case Quantization_UINT8: static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_train_quantizer", e.what());
    }
}

void gpu_ivf_pq_set_per_thread_device(gpu_ivf_pq_c index_c, bool enable, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->set_per_thread_device(enable); break;
            case Quantization_F16: static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->set_per_thread_device(enable); break;
            case Quantization_INT8: static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->set_per_thread_device(enable); break;
            case Quantization_UINT8: static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->set_per_thread_device(enable); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_set_per_thread_device", e.what());
    }
}

void gpu_ivf_pq_set_use_batching(gpu_ivf_pq_c index_c, bool enable, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->set_use_batching(enable); break;
            case Quantization_F16: static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->set_use_batching(enable); break;
            case Quantization_INT8: static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->set_use_batching(enable); break;
            case Quantization_UINT8: static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->set_use_batching(enable); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_set_use_batching", e.what());
    }
}

void gpu_ivf_pq_set_quantizer(gpu_ivf_pq_c index_c, float min, float max, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->set_quantizer(min, max); break;
            case Quantization_F16: static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->set_quantizer(min, max); break;
            case Quantization_INT8: static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->set_quantizer(min, max); break;
            case Quantization_UINT8: static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->set_quantizer(min, max); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_set_quantizer", e.what());
    }
}

void gpu_ivf_pq_get_quantizer(gpu_ivf_pq_c index_c, float* min, float* max, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->get_quantizer(min, max); break;
            case Quantization_F16: static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->get_quantizer(min, max); break;
            case Quantization_INT8: static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->get_quantizer(min, max); break;
            case Quantization_UINT8: static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->get_quantizer(min, max); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_get_quantizer", e.what());
    }
}

void gpu_ivf_pq_save(gpu_ivf_pq_c index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->save(filename); break;
            case Quantization_F16: static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->save(filename); break;
            case Quantization_INT8: static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->save(filename); break;
            case Quantization_UINT8: static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->save(filename); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_save", e.what());
    }
}

gpu_ivf_pq_search_res_t gpu_ivf_pq_search(gpu_ivf_pq_c index_c, const void* queries_data, uint64_t num_queries, 
                                             uint32_t query_dimension, uint32_t limit, 
                                             ivf_pq_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_ivf_pq_search_res_t result = {nullptr};
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        auto* cpp_res = new ivf_pq_search_result_t();
        switch (any->qtype) {
            case Quantization_F32: *cpp_res = static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->search(static_cast<const float*>(queries_data), num_queries, query_dimension, limit, search_params); break;
            case Quantization_F16: *cpp_res = static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->search(static_cast<const half*>(queries_data), num_queries, query_dimension, limit, search_params); break;
            case Quantization_INT8: *cpp_res = static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->search(static_cast<const int8_t*>(queries_data), num_queries, query_dimension, limit, search_params); break;
            case Quantization_UINT8: *cpp_res = static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->search(static_cast<const uint8_t*>(queries_data), num_queries, query_dimension, limit, search_params); break;
            default: break;
        }
        result.result_ptr = static_cast<gpu_ivf_pq_result_c>(cpp_res);
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_search", e.what());
    }
    return result;
}

gpu_ivf_pq_search_res_t gpu_ivf_pq_search_float(gpu_ivf_pq_c index_c, const float* queries_data, uint64_t num_queries, 
                                                   uint32_t query_dimension, uint32_t limit, 
                                                   ivf_pq_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_ivf_pq_search_res_t result = {nullptr};
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        auto* cpp_res = new ivf_pq_search_result_t();
        switch (any->qtype) {
            case Quantization_F32: *cpp_res = static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, search_params); break;
            case Quantization_F16: *cpp_res = static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, search_params); break;
            case Quantization_INT8: *cpp_res = static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, search_params); break;
            case Quantization_UINT8: *cpp_res = static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, search_params); break;
            default: break;
        }
        result.result_ptr = static_cast<gpu_ivf_pq_result_c>(cpp_res);
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_search_float", e.what());
    }
    return result;
}

void gpu_ivf_pq_get_neighbors(gpu_ivf_pq_result_c result_c, uint64_t total_elements, int64_t* neighbors) {
    if (!result_c) return;
    auto* neighbors_vec = &static_cast<ivf_pq_search_result_t*>(result_c)->neighbors;
    if (neighbors_vec->size() >= total_elements) {
        std::copy(neighbors_vec->begin(), neighbors_vec->begin() + total_elements, neighbors);
    }
}

void gpu_ivf_pq_get_distances(gpu_ivf_pq_result_c result_c, uint64_t total_elements, float* distances) {
    if (!result_c) return;
    auto* distances_vec = &static_cast<ivf_pq_search_result_t*>(result_c)->distances;
    if (distances_vec->size() >= total_elements) {
        std::copy(distances_vec->begin(), distances_vec->begin() + total_elements, distances);
    }
}

void gpu_ivf_pq_free_result(gpu_ivf_pq_result_c result_c) {
    if (!result_c) return;
    delete static_cast<ivf_pq_search_result_t*>(result_c);
}

uint32_t gpu_ivf_pq_cap(gpu_ivf_pq_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->cap();
        case Quantization_F16: return static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->cap();
        case Quantization_INT8: return static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->cap();
        case Quantization_UINT8: return static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->cap();
        default: return 0;
    }
}

uint32_t gpu_ivf_pq_len(gpu_ivf_pq_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->len();
        case Quantization_F16: return static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->len();
        case Quantization_INT8: return static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->len();
        case Quantization_UINT8: return static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->len();
        default: return 0;
    }
}

char* gpu_ivf_pq_info(gpu_ivf_pq_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    if (!index_c) return nullptr;
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        std::string info;
        switch (any->qtype) {
            case Quantization_F32: info = static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->info(); break;
            case Quantization_F16: info = static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->info(); break;
            case Quantization_INT8: info = static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->info(); break;
            case Quantization_UINT8: info = static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->info(); break;
            default: return nullptr;
        }
        return strdup(info.c_str());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_info", e.what());
        return nullptr;
    }
}

void gpu_ivf_pq_get_centers(gpu_ivf_pq_c index_c, void* centers, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: {
                auto host_centers = static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->get_centers();
                if (!host_centers.empty()) std::copy(host_centers.begin(), host_centers.end(), static_cast<float*>(centers));
                break;
            }
            case Quantization_F16: {
                auto host_centers = static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->get_centers();
                if (!host_centers.empty()) std::copy(host_centers.begin(), host_centers.end(), static_cast<half*>(centers));
                break;
            }
            case Quantization_INT8: {
                auto host_centers = static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->get_centers();
                if (!host_centers.empty()) std::copy(host_centers.begin(), host_centers.end(), static_cast<int8_t*>(centers));
                break;
            }
            case Quantization_UINT8: {
                auto host_centers = static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->get_centers();
                if (!host_centers.empty()) std::copy(host_centers.begin(), host_centers.end(), static_cast<uint8_t*>(centers));
                break;
            }
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_pq_get_centers", e.what());
    }
}

uint32_t gpu_ivf_pq_get_n_list(gpu_ivf_pq_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->get_n_list();
        case Quantization_F16: return static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->get_n_list();
        case Quantization_INT8: return static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->get_n_list();
        case Quantization_UINT8: return static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->get_n_list();
        default: return 0;
    }
}

uint32_t gpu_ivf_pq_get_dim(gpu_ivf_pq_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->get_dim();
        case Quantization_F16: return static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->get_dim();
        case Quantization_INT8: return static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->get_dim();
        case Quantization_UINT8: return static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->get_dim();
        default: return 0;
    }
}

uint32_t gpu_ivf_pq_get_rot_dim(gpu_ivf_pq_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->get_rot_dim();
        case Quantization_F16: return static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->get_rot_dim();
        case Quantization_INT8: return static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->get_rot_dim();
        case Quantization_UINT8: return static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->get_rot_dim();
        default: return 0;
    }
}

uint32_t gpu_ivf_pq_get_dim_ext(gpu_ivf_pq_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->get_dim_ext();
        case Quantization_F16: return static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->get_dim_ext();
        case Quantization_INT8: return static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->get_dim_ext();
        case Quantization_UINT8: return static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->get_dim_ext();
        default: return 0;
    }
}

void gpu_ivf_pq_get_dataset(gpu_ivf_pq_c index_c, void* out_data) {
    // This is for debugging, we just copy the host dataset if it exists
    if (!index_c) return;
    auto* any = static_cast<gpu_ivf_pq_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: {
            auto& ds = static_cast<gpu_ivf_pq_t<float>*>(any->ptr)->flattened_host_dataset;
            if (!ds.empty()) std::copy(ds.begin(), ds.end(), static_cast<float*>(out_data));
            break;
        }
        case Quantization_F16: {
            auto& ds = static_cast<gpu_ivf_pq_t<half>*>(any->ptr)->flattened_host_dataset;
            if (!ds.empty()) std::copy(ds.begin(), ds.end(), static_cast<half*>(out_data));
            break;
        }
        case Quantization_INT8: {
            auto& ds = static_cast<gpu_ivf_pq_t<int8_t>*>(any->ptr)->flattened_host_dataset;
            if (!ds.empty()) std::copy(ds.begin(), ds.end(), static_cast<int8_t*>(out_data));
            break;
        }
        case Quantization_UINT8: {
            auto& ds = static_cast<gpu_ivf_pq_t<uint8_t>*>(any->ptr)->flattened_host_dataset;
            if (!ds.empty()) std::copy(ds.begin(), ds.end(), static_cast<uint8_t*>(out_data));
            break;
        }
        default: break;
    }
}

} // extern "C"

namespace matrixone {
template class gpu_ivf_pq_t<float>;
template class gpu_ivf_pq_t<half>;
template class gpu_ivf_pq_t<int8_t>;
template class gpu_ivf_pq_t<uint8_t>;
} // namespace matrixone
