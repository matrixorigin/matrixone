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
 * Brute-Force C Wrapper Implementation
 * Supported data types (via quantization_t): Quantization_F32, Quantization_F16
 */

#include "brute_force_c.h"
#include "brute_force.hpp"
#include <iostream>
#include <stdexcept>
#include <vector>
#include <algorithm>
#include <cstdlib>
#include <limits>
#include <cstring>

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
    void* index_ptr = nullptr;
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::gpu_brute_force_t<float>(static_cast<const float*>(dataset_data), count_vectors, dimension, metric_c, nthread, device_id);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::gpu_brute_force_t<half>(static_cast<const half*>(dataset_data), count_vectors, dimension, metric_c, nthread, device_id);
                break;
            default:
                throw std::runtime_error("Unsupported quantization type for brute force (only f32 and f16 supported)");
        }
        if (index_ptr) static_cast<matrixone::gpu_index_base_t<float, brute_force_build_params_t>*>(index_ptr)->start();
        return static_cast<gpu_brute_force_c>(new gpu_brute_force_any_t(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_new", e.what());
        return nullptr;
    }
}

gpu_brute_force_c gpu_brute_force_new_empty(uint64_t total_count, uint32_t dimension, distance_type_t metric_c, uint32_t nthread, int device_id, quantization_t qtype, void* errmsg) {
    void* index_ptr = nullptr;
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        switch (qtype) {
            case Quantization_F32:
                index_ptr = new matrixone::gpu_brute_force_t<float>(total_count, dimension, metric_c, nthread, device_id);
                break;
            case Quantization_F16:
                index_ptr = new matrixone::gpu_brute_force_t<half>(total_count, dimension, metric_c, nthread, device_id);
                break;
            default:
                throw std::runtime_error("Unsupported quantization type for brute force (only f32 and f16 supported)");
        }
        if (index_ptr) static_cast<matrixone::gpu_index_base_t<float, brute_force_build_params_t>*>(index_ptr)->start();
        return static_cast<gpu_brute_force_c>(new gpu_brute_force_any_t(qtype, index_ptr));
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_new_empty", e.what());
        return nullptr;
    }
}

void gpu_brute_force_start(gpu_brute_force_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_brute_force_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_brute_force_t<float>*>(any->ptr)->start(); break;
            case Quantization_F16: static_cast<matrixone::gpu_brute_force_t<half>*>(any->ptr)->start(); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_start", e.what());
    }
}

void gpu_brute_force_build(gpu_brute_force_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_brute_force_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_brute_force_t<float>*>(any->ptr)->build(); break;
            case Quantization_F16: static_cast<matrixone::gpu_brute_force_t<half>*>(any->ptr)->build(); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_build", e.what());
    }
}

void gpu_brute_force_add_chunk(gpu_brute_force_c index_c, const void* chunk_data, uint64_t chunk_count, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_brute_force_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_brute_force_t<float>*>(any->ptr)->add_chunk(static_cast<const float*>(chunk_data), chunk_count); break;
            case Quantization_F16: static_cast<matrixone::gpu_brute_force_t<half>*>(any->ptr)->add_chunk(static_cast<const half*>(chunk_data), chunk_count); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_add_chunk", e.what());
    }
}

void gpu_brute_force_add_chunk_float(gpu_brute_force_c index_c, const float* chunk_data, uint64_t chunk_count, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_brute_force_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<matrixone::gpu_brute_force_t<float>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count); break;
            case Quantization_F16: static_cast<matrixone::gpu_brute_force_t<half>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count); break;
            default: break;
        }
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_add_chunk_float", e.what());
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
                *res = static_cast<matrixone::gpu_brute_force_t<float>*>(any->ptr)->search(static_cast<const float*>(queries_data), num_queries, query_dimension, limit, brute_force_search_params_default());
                result_ptr = res.release();
                break;
            }
            case Quantization_F16: {
                auto res = std::make_unique<matrixone::gpu_brute_force_t<half>::search_result_t>();
                *res = static_cast<matrixone::gpu_brute_force_t<half>*>(any->ptr)->search(static_cast<const half*>(queries_data), num_queries, query_dimension, limit, brute_force_search_params_default());
                result_ptr = res.release();
                break;
            }
            default: break;
        }
        return static_cast<gpu_brute_force_search_result_c>(result_ptr);
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_search", e.what());
        return nullptr;
    }
}

gpu_brute_force_search_result_c gpu_brute_force_search_float(gpu_brute_force_c index_c, const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_brute_force_any_t*>(index_c);
        void* result_ptr = nullptr;
        switch (any->qtype) {
            case Quantization_F32: {
                auto res = std::make_unique<matrixone::gpu_brute_force_t<float>::search_result_t>();
                *res = static_cast<matrixone::gpu_brute_force_t<float>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, brute_force_search_params_default());
                result_ptr = res.release();
                break;
            }
            case Quantization_F16: {
                auto res = std::make_unique<matrixone::gpu_brute_force_t<half>::search_result_t>();
                *res = static_cast<matrixone::gpu_brute_force_t<half>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, brute_force_search_params_default());
                result_ptr = res.release();
                break;
            }
            default: break;
        }
        return static_cast<gpu_brute_force_search_result_c>(result_ptr);
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_search_float", e.what());
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

uint32_t gpu_brute_force_cap(gpu_brute_force_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_brute_force_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<matrixone::gpu_brute_force_t<float>*>(any->ptr)->cap();
        case Quantization_F16: return static_cast<matrixone::gpu_brute_force_t<half>*>(any->ptr)->cap();
        default: return 0;
    }
}

uint32_t gpu_brute_force_len(gpu_brute_force_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_brute_force_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<matrixone::gpu_brute_force_t<float>*>(any->ptr)->len();
        case Quantization_F16: return static_cast<matrixone::gpu_brute_force_t<half>*>(any->ptr)->len();
        default: return 0;
    }
}

char* gpu_brute_force_info(gpu_brute_force_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    if (!index_c) return nullptr;
    try {
        auto* any = static_cast<gpu_brute_force_any_t*>(index_c);
        std::string info;
        switch (any->qtype) {
            case Quantization_F32: info = static_cast<matrixone::gpu_brute_force_t<float>*>(any->ptr)->info(); break;
            case Quantization_F16: info = static_cast<matrixone::gpu_brute_force_t<half>*>(any->ptr)->info(); break;
            default: return nullptr;
        }
        return strdup(info.c_str());
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_info", e.what());
        return nullptr;
    }
}

void gpu_brute_force_destroy(gpu_brute_force_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_brute_force_any_t*>(index_c);
        delete any;
    } catch (const std::exception& e) {
        set_errmsg(errmsg, "Error in gpu_brute_force_destroy", e.what());
    }
}

} // extern "C"

namespace matrixone {
template class gpu_brute_force_t<float>;
template class gpu_brute_force_t<half>;
}
