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
 * CAGRA C Wrapper Implementation
 *
 * Two type axes via quantization_t:
 *   btype = base / query / quantizer-SOURCE element type (Quantization_F32 or F16)
 *   qtype = storage element type (Quantization_F32, F16, INT8, UINT8)
 *
 * Wired (btype, qtype) combinations:
 *   F32 base: F32, F16, INT8, UINT8 storage
 *   F16 base: F16, INT8, UINT8 storage
 * Any other combination throws "unsupported (base,storage) type combination".
 */

#include "cagra_c.h"
#include "cagra.hpp"
#include <iostream>
#include <vector>
#include <cstring>
#include <memory>
#include <limits>
#include <algorithm>
#include <type_traits>

using namespace matrixone;

struct gpu_cagra_any_t {
    quantization_t btype;   // base / query / quantizer-source element type
    quantization_t qtype;   // storage element type
    void* ptr;

    gpu_cagra_any_t(quantization_t b, quantization_t q, void* p)
        : btype(b), qtype(q), ptr(p) {}
    ~gpu_cagra_any_t();
};

// Static dispatch: resolves the concrete gpu_cagra_t<B,T> for (btype,qtype) and
// invokes fn with a typed pointer. fn is a generic lambda; recover B/Q inside it
// via decltype(idx)::base_type / ::storage_type. Throws on unsupported combos.
template <typename Fn>
static auto cagra_dispatch(const gpu_cagra_any_t* a, Fn&& fn) {
    switch (a->btype) {
    case Quantization_F32:
        switch (a->qtype) {
        case Quantization_F32:   return fn(static_cast<gpu_cagra_t<float, float  >*>(a->ptr));
        case Quantization_F16:   return fn(static_cast<gpu_cagra_t<float, half   >*>(a->ptr));
        case Quantization_INT8:  return fn(static_cast<gpu_cagra_t<float, int8_t >*>(a->ptr));
        case Quantization_UINT8: return fn(static_cast<gpu_cagra_t<float, uint8_t>*>(a->ptr));
        default: break;
        }
        break;
    case Quantization_F16:
        switch (a->qtype) {
        case Quantization_F16:   return fn(static_cast<gpu_cagra_t<half, half   >*>(a->ptr));
        case Quantization_INT8:  return fn(static_cast<gpu_cagra_t<half, int8_t >*>(a->ptr));
        case Quantization_UINT8: return fn(static_cast<gpu_cagra_t<half, uint8_t>*>(a->ptr));
        default: break;
        }
        break;
    default: break;
    }
    throw std::runtime_error("gpu_cagra: unsupported (base,storage) type combination");
}

gpu_cagra_any_t::~gpu_cagra_any_t() {
    if (!ptr) return;
    try {
        cagra_dispatch(this, [](auto* idx) {
            idx->destroy();
            delete idx;
        });
    } catch (...) {
        // unsupported combo never gets a live ptr — nothing to free
    }
}

// Construct a new gpu_cagra_t<B,T> for the wired (btype,qtype) combos.
// Maker is a generic lambda invoked as maker(static type tag) -> void*; it
// receives a null typed pointer purely to recover B and Q.
template <typename Maker>
static void* cagra_construct(quantization_t btype, quantization_t qtype, Maker&& maker) {
    switch (btype) {
    case Quantization_F32:
        switch (qtype) {
        case Quantization_F32:   return maker(static_cast<gpu_cagra_t<float, float  >*>(nullptr));
        case Quantization_F16:   return maker(static_cast<gpu_cagra_t<float, half   >*>(nullptr));
        case Quantization_INT8:  return maker(static_cast<gpu_cagra_t<float, int8_t >*>(nullptr));
        case Quantization_UINT8: return maker(static_cast<gpu_cagra_t<float, uint8_t>*>(nullptr));
        default: break;
        }
        break;
    case Quantization_F16:
        switch (qtype) {
        case Quantization_F16:   return maker(static_cast<gpu_cagra_t<half, half   >*>(nullptr));
        case Quantization_INT8:  return maker(static_cast<gpu_cagra_t<half, int8_t >*>(nullptr));
        case Quantization_UINT8: return maker(static_cast<gpu_cagra_t<half, uint8_t>*>(nullptr));
        default: break;
        }
        break;
    default: break;
    }
    throw std::runtime_error("gpu_cagra: unsupported (base,storage) type combination");
}

extern "C" {

gpu_cagra_c gpu_cagra_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension,
                            distance_type_t metric_c, cagra_build_params_t build_params,
                            const int* devices, int device_count, uint32_t nthread,
                            distribution_mode_t dist_mode, quantization_t btype, quantization_t qtype,
                            const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::vector<int> devs(devices, devices + device_count);
        void* ptr = cagra_construct(btype, qtype, [&](auto* tag) -> void* {
            using B = typename std::remove_pointer_t<decltype(tag)>::base_type;
            using Q = typename std::remove_pointer_t<decltype(tag)>::storage_type;
            // The dataset-providing constructor takes storage-typed (Q) data and
            // copies it directly into flattened_host_dataset (no quantization here;
            // quantization happens via add_chunk_quantize / add_chunk_float).
            return new gpu_cagra_t<B, Q>(static_cast<const Q*>(dataset_data), count_vectors, dimension, metric_c, build_params, devs, nthread, dist_mode, ids);
        });
        return static_cast<gpu_cagra_c>(new gpu_cagra_any_t(btype, qtype, ptr));
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_new", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_new", "unknown C++ exception");
    }
    return nullptr;
}

gpu_cagra_c gpu_cagra_new_empty(uint64_t total_count, uint32_t dimension, distance_type_t metric_c,
                                     cagra_build_params_t build_params,
                                     const int* devices, int device_count, uint32_t nthread,
                                     distribution_mode_t dist_mode, quantization_t btype, quantization_t qtype,
                                     const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::vector<int> devs(devices, devices + device_count);
        void* ptr = cagra_construct(btype, qtype, [&](auto* tag) -> void* {
            using B = typename std::remove_pointer_t<decltype(tag)>::base_type;
            using Q = typename std::remove_pointer_t<decltype(tag)>::storage_type;
            return new gpu_cagra_t<B, Q>(total_count, dimension, metric_c, build_params, devs, nthread, dist_mode, ids);
        });
        return static_cast<gpu_cagra_c>(new gpu_cagra_any_t(btype, qtype, ptr));
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_new_empty", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_new_empty", "unknown C++ exception");
    }
    return nullptr;
}

gpu_cagra_c gpu_cagra_load_file(const char* filename, uint32_t dimension, distance_type_t metric_c,
                                 cagra_build_params_t build_params,
                                 const int* devices, int device_count, uint32_t nthread,
                                 distribution_mode_t dist_mode, quantization_t btype, quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::vector<int> devs(devices, devices + device_count);
        void* ptr = cagra_construct(btype, qtype, [&](auto* tag) -> void* {
            using B = typename std::remove_pointer_t<decltype(tag)>::base_type;
            using Q = typename std::remove_pointer_t<decltype(tag)>::storage_type;
            return new gpu_cagra_t<B, Q>(std::string(filename), dimension, metric_c, build_params, devs, nthread, dist_mode);
        });
        return static_cast<gpu_cagra_c>(new gpu_cagra_any_t(btype, qtype, ptr));
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_load_file", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_load_file", "unknown C++ exception");
    }
    return nullptr;
}

void gpu_cagra_destroy(gpu_cagra_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        delete static_cast<gpu_cagra_any_t*>(index_c);
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_destroy", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_destroy", "unknown C++ exception");
    }
}

void gpu_cagra_start(gpu_cagra_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [](auto* idx) { idx->start(); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_start", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_start", "unknown C++ exception");
    }
}

void gpu_cagra_build(gpu_cagra_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [](auto* idx) { idx->build(); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_build", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_build", "unknown C++ exception");
    }
}

void gpu_cagra_add_chunk(gpu_cagra_c index_c, const void* chunk_data, uint64_t chunk_count, const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            idx->add_chunk(static_cast<const Q*>(chunk_data), chunk_count, -1, ids);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_add_chunk", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_add_chunk", "unknown C++ exception");
    }
}

void gpu_cagra_add_chunk_float(gpu_cagra_c index_c, const float* chunk_data, uint64_t chunk_count, const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) {
            idx->add_chunk_float(chunk_data, chunk_count, -1, ids);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_add_chunk_float", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_add_chunk_float", "unknown C++ exception");
    }
}

void gpu_cagra_add_chunk_quantize(gpu_cagra_c index_c, const void* base_data, uint64_t chunk_count, const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            idx->add_chunk_quantize(static_cast<const B*>(base_data), chunk_count, -1, ids);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_add_chunk_quantize", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_add_chunk_quantize", "unknown C++ exception");
    }
}

void gpu_cagra_quantize_query(gpu_cagra_c index_c, const void* base_data, uint64_t num_queries, void* out, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            idx->quantize_query(static_cast<const B*>(base_data), num_queries, static_cast<Q*>(out));
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_quantize_query", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_quantize_query", "unknown C++ exception");
    }
}

void gpu_cagra_train_quantizer(gpu_cagra_c index_c, const void* train_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            idx->train_quantizer(static_cast<const B*>(train_data), n_samples);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_train_quantizer", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_train_quantizer", "unknown C++ exception");
    }
}

void gpu_cagra_set_batch_window(gpu_cagra_c index_c, int64_t window_us, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) { idx->set_batch_window(window_us); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_set_batch_window", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_set_batch_window", "unknown C++ exception");
    }
}

void gpu_cagra_set_dynb_conservative_dispatch(gpu_cagra_c index_c, bool enable, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) { idx->set_dynb_conservative_dispatch(enable); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_set_dynb_conservative_dispatch", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_set_dynb_conservative_dispatch", "unknown C++ exception");
    }
}

void gpu_cagra_set_quantizer(gpu_cagra_c index_c, float min, float max, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) { idx->set_quantizer(min, max); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_set_quantizer", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_set_quantizer", "unknown C++ exception");
    }
}

void gpu_cagra_get_quantizer(gpu_cagra_c index_c, float* min, float* max, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) { idx->get_quantizer(min, max); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_get_quantizer", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_get_quantizer", "unknown C++ exception");
    }
}

void gpu_cagra_save(gpu_cagra_c index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) { idx->save(filename); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_save", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_save", "unknown C++ exception");
    }
}

void gpu_cagra_save_dir(gpu_cagra_c index_c, const char* dir, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) { idx->save_dir(dir); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_save_dir", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_save_dir", "unknown C++ exception");
    }
}

void gpu_cagra_delete_id(gpu_cagra_c index_c, int64_t id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) { idx->delete_id(id); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_delete_id", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_delete_id", "unknown C++ exception");
    }
}

void gpu_cagra_load_dir(gpu_cagra_c index_c, const char* dir,
                         distribution_mode_t target_mode, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) { idx->load_dir(dir, target_mode); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_load_dir", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_load_dir", "unknown C++ exception");
    }
}

gpu_cagra_search_res_t gpu_cagra_search(gpu_cagra_c index_c, const void* queries_data, uint64_t num_queries,
                                            uint32_t query_dimension, uint32_t limit,
                                            cagra_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_cagra_search_res_t result = {nullptr};
    try {
        auto cpp_res = std::make_unique<cagra_search_result_t>();
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            *cpp_res = idx->search(static_cast<const Q*>(queries_data), num_queries, query_dimension, limit, search_params);
        });
        result.result_ptr = static_cast<gpu_cagra_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search", "unknown C++ exception");
    }
    return result;
}

gpu_cagra_search_res_t gpu_cagra_search_float(gpu_cagra_c index_c, const float* queries_data, uint64_t num_queries,
                                                uint32_t query_dimension, uint32_t limit,
                                                cagra_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_cagra_search_res_t result = {nullptr};
    try {
        auto cpp_res = std::make_unique<cagra_search_result_t>();
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) {
            *cpp_res = idx->search_float(queries_data, num_queries, query_dimension, limit, search_params);
        });
        result.result_ptr = static_cast<gpu_cagra_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_float", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_float", "unknown C++ exception");
    }
    return result;
}

uint64_t gpu_cagra_search_async(gpu_cagra_c index_c, const void* queries_data, uint64_t num_queries,
                                   uint32_t query_dimension, uint32_t limit,
                                   cagra_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        return cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) -> uint64_t {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            return idx->search_async(static_cast<const Q*>(queries_data), num_queries, query_dimension, limit, search_params);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_async", e.what());
        return 0;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_async", "unknown C++ exception");
        return 0;
    }
}

uint64_t gpu_cagra_search_float_async(gpu_cagra_c index_c, const float* queries_data, uint64_t num_queries,
                                         uint32_t query_dimension, uint32_t limit,
                                         cagra_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        return cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) -> uint64_t {
            return idx->search_float_async(queries_data, num_queries, query_dimension, limit, search_params);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_float_async", e.what());
        return 0;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_float_async", "unknown C++ exception");
        return 0;
    }
}

gpu_cagra_search_res_t gpu_cagra_search_wait(gpu_cagra_c index_c, uint64_t job_id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_cagra_search_res_t result = {nullptr};
    try {
        auto cpp_res = std::make_unique<cagra_search_result_t>();
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) {
            *cpp_res = idx->search_wait(job_id);
        });
        result.result_ptr = static_cast<gpu_cagra_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_wait", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_wait", "unknown C++ exception");
    }
    return result;
}
void gpu_cagra_get_neighbors(gpu_cagra_result_c result_c, uint64_t total_elements, int64_t* neighbors) {
    try {
        if (!neighbors || total_elements == 0) return;
        if (!result_c) {
            // No result_t: caller buffer must not be left uninitialized.
            std::fill(neighbors, neighbors + total_elements, static_cast<int64_t>(-1));
            return;
        }
        auto* neighbors_vec = &static_cast<cagra_search_result_t*>(result_c)->neighbors;
        uint64_t n_copy = std::min<uint64_t>(neighbors_vec->size(), total_elements);
        std::copy(neighbors_vec->begin(), neighbors_vec->begin() + n_copy, neighbors);
        // Sentinel-fill the tail: caller asked for total_elements but vec was shorter.
        // -1 matches map_neighbor_id's OOB sentinel (index_base.hpp).
        std::fill(neighbors + n_copy, neighbors + total_elements, static_cast<int64_t>(-1));
    } catch (...) {
        matrixone::log_err("gpu_cagra_get_neighbors: unknown C++ exception (swallowed)");
    }
}

void gpu_cagra_get_distances(gpu_cagra_result_c result_c, uint64_t total_elements, float* distances) {
    try {
        if (!distances || total_elements == 0) return;
        if (!result_c) {
            std::fill(distances, distances + total_elements, std::numeric_limits<float>::max());
            return;
        }
        auto* distances_vec = &static_cast<cagra_search_result_t*>(result_c)->distances;
        uint64_t n_copy = std::min<uint64_t>(distances_vec->size(), total_elements);
        std::copy(distances_vec->begin(), distances_vec->begin() + n_copy, distances);
        // Sentinel-fill the tail to match brute_force_c.cpp convention.
        std::fill(distances + n_copy, distances + total_elements, std::numeric_limits<float>::max());
    } catch (...) {
        matrixone::log_err("gpu_cagra_get_distances: unknown C++ exception (swallowed)");
    }
}

void gpu_cagra_free_result(gpu_cagra_result_c result_c) {
    try {
        if (!result_c) return;
        delete static_cast<cagra_search_result_t*>(result_c);
    } catch (...) {
        matrixone::log_err("gpu_cagra_free_result: unknown C++ exception (swallowed)");
    }
}

uint64_t gpu_cagra_cap(gpu_cagra_c index_c) {
    try {
        if (!index_c) return 0;
        return cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [](auto* idx) -> uint64_t { return idx->cap(); });
    } catch (...) {
        return 0;
    }
}

uint64_t gpu_cagra_len(gpu_cagra_c index_c) {
    try {
        if (!index_c) return 0;
        return cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [](auto* idx) -> uint64_t { return idx->len(); });
    } catch (...) {
        return 0;
    }
}

// Returns a heap-allocated, NUL-terminated JSON string of the index's
// INCLUDE column metadata in the same shape gpu_cagra_set_filter_columns
// consumes:
//   [{"name":"price","type":2},{"name":"cat","type":1}]
// Returns an empty string for indexes built without INCLUDE columns; never
// returns NULL on success. Caller frees with free().
char* gpu_cagra_get_filter_col_meta_json(gpu_cagra_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    if (!index_c) return strdup("");
    try {
        std::string json = cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [](auto* idx) -> std::string {
            return matrixone::format_filter_col_meta(idx->filter_host_.columns);
        });
        return strdup(json.c_str());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_get_filter_col_meta_json", e.what());
        return strdup("");
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_get_filter_col_meta_json", "unknown C++ exception");
        return strdup("");
    }
}

char* gpu_cagra_info(gpu_cagra_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    if (!index_c) return nullptr;
    try {
        std::string info = cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [](auto* idx) -> std::string { return idx->info(); });
        return strdup(info.c_str());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_info", e.what());
        return nullptr;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_info", "unknown C++ exception");
        return nullptr;
    }
}

void gpu_cagra_extend(gpu_cagra_c index_c, const void* additional_data, uint64_t num_vectors,
                      const int64_t* new_ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            idx->extend(static_cast<const Q*>(additional_data), num_vectors, new_ids);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_extend", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_extend", "unknown C++ exception");
    }
}

gpu_cagra_c gpu_cagra_merge(gpu_cagra_c* indices_c, int num_indices, uint32_t nthread, const int* devices, int device_count, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        if (num_indices <= 0) return nullptr;
        auto* first = static_cast<gpu_cagra_any_t*>(indices_c[0]);
        std::vector<int> devs(devices, devices + device_count);
        void* merged_ptr = cagra_construct(first->btype, first->qtype, [&](auto* tag) -> void* {
            using B = typename std::remove_pointer_t<decltype(tag)>::base_type;
            using Q = typename std::remove_pointer_t<decltype(tag)>::storage_type;
            std::vector<gpu_index_base_t<B, Q, cagra_build_params_t, int64_t>*> base_indices;
            for (int i = 0; i < num_indices; ++i) {
                base_indices.push_back(static_cast<gpu_cagra_t<B, Q>*>(static_cast<gpu_cagra_any_t*>(indices_c[i])->ptr));
            }
            return gpu_cagra_t<B, Q>::merge(base_indices, nthread, devs).release();
        });
        return static_cast<gpu_cagra_c>(new gpu_cagra_any_t(first->btype, first->qtype, merged_ptr));
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_merge", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_merge", "unknown C++ exception");
    }
    return nullptr;
}

// ---------- Pre-filter API ----------

void gpu_cagra_set_filter_columns(gpu_cagra_c index_c, const char* col_meta_json,
                                   uint64_t total_count, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::string s = col_meta_json ? col_meta_json : "";
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) {
            idx->set_filter_columns(s, total_count);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_set_filter_columns", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_set_filter_columns", "unknown C++ exception");
    }
}

void gpu_cagra_add_filter_chunk(gpu_cagra_c index_c, uint32_t col_idx,
                                 const void* data, const uint32_t* null_bitmap,
                                 uint64_t nrows, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) {
            idx->add_filter_chunk(col_idx, data, null_bitmap, nrows);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_add_filter_chunk", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_add_filter_chunk", "unknown C++ exception");
    }
}

gpu_cagra_search_res_t gpu_cagra_search_with_filter(gpu_cagra_c index_c, const void* queries_data,
                                                     uint64_t num_queries, uint32_t query_dimension,
                                                     uint32_t limit, cagra_search_params_t sp,
                                                     const char* preds_json, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_cagra_search_res_t result = {nullptr};
    try {
        auto cpp_res = std::make_unique<cagra_search_result_t>();
        std::string preds = preds_json ? preds_json : "";
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            *cpp_res = idx->search_with_filter(static_cast<const Q*>(queries_data), num_queries, query_dimension, limit, sp, preds);
        });
        result.result_ptr = static_cast<gpu_cagra_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_with_filter", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_with_filter", "unknown C++ exception");
    }
    return result;
}

gpu_cagra_search_res_t gpu_cagra_search_float_with_filter(gpu_cagra_c index_c, const float* queries_data,
                                                           uint64_t num_queries, uint32_t query_dimension,
                                                           uint32_t limit, cagra_search_params_t sp,
                                                           const char* preds_json, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_cagra_search_res_t result = {nullptr};
    try {
        auto cpp_res = std::make_unique<cagra_search_result_t>();
        std::string preds = preds_json ? preds_json : "";
        cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) {
            *cpp_res = idx->search_float_with_filter(queries_data, num_queries, query_dimension, limit, sp, preds);
        });
        result.result_ptr = static_cast<gpu_cagra_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_float_with_filter", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_float_with_filter", "unknown C++ exception");
    }
    return result;
}

uint64_t gpu_cagra_search_float_with_filter_async(gpu_cagra_c index_c, const float* queries_data,
                                                   uint64_t num_queries, uint32_t query_dimension,
                                                   uint32_t limit, cagra_search_params_t sp,
                                                   const char* preds_json, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::string preds = preds_json ? preds_json : "";
        return cagra_dispatch(static_cast<gpu_cagra_any_t*>(index_c), [&](auto* idx) -> uint64_t {
            return idx->search_float_with_filter_async(queries_data, num_queries, query_dimension, limit, sp, preds);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_float_with_filter_async", e.what());
        return 0;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_cagra_search_float_with_filter_async", "unknown C++ exception");
        return 0;
    }
}

} // extern "C"

namespace matrixone {
template class gpu_cagra_t<float, float>;
template class gpu_cagra_t<float, half>;
template class gpu_cagra_t<float, int8_t>;
template class gpu_cagra_t<float, uint8_t>;
template class gpu_cagra_t<half, half>;
template class gpu_cagra_t<half, int8_t>;
template class gpu_cagra_t<half, uint8_t>;
} // namespace matrixone
