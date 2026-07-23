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
 * IVF-Flat C Wrapper Implementation
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

#include "ivf_flat_c.h"
#include "ivf_flat.hpp"
#include <iostream>
#include <vector>
#include <cstring>
#include <memory>
#include <limits>
#include <algorithm>
#include <type_traits>

using namespace matrixone;

struct gpu_ivf_flat_any_t {
    quantization_t btype;   // base / query / quantizer-source element type
    quantization_t qtype;   // storage element type
    void* ptr;

    gpu_ivf_flat_any_t(quantization_t b, quantization_t q, void* p)
        : btype(b), qtype(q), ptr(p) {}
    ~gpu_ivf_flat_any_t();
};

// Static dispatch: resolves the concrete gpu_ivf_flat_t<B,Q> for (btype,qtype) and
// invokes fn with a typed pointer. fn is a generic lambda; recover B/Q inside it
// via decltype(idx)::base_type / ::storage_type. Throws on unsupported combos.
template <typename Fn>
static auto ivf_flat_dispatch(const gpu_ivf_flat_any_t* a, Fn&& fn) {
    switch (a->btype) {
    case Quantization_F32:
        switch (a->qtype) {
        case Quantization_F32:   return fn(static_cast<gpu_ivf_flat_t<float, float  >*>(a->ptr));
        case Quantization_F16:   return fn(static_cast<gpu_ivf_flat_t<float, half   >*>(a->ptr));
        case Quantization_INT8:  return fn(static_cast<gpu_ivf_flat_t<float, int8_t >*>(a->ptr));
        case Quantization_UINT8: return fn(static_cast<gpu_ivf_flat_t<float, uint8_t>*>(a->ptr));
        default: break;
        }
        break;
    case Quantization_F16:
        switch (a->qtype) {
        case Quantization_F16:   return fn(static_cast<gpu_ivf_flat_t<half, half   >*>(a->ptr));
        case Quantization_INT8:  return fn(static_cast<gpu_ivf_flat_t<half, int8_t >*>(a->ptr));
        case Quantization_UINT8: return fn(static_cast<gpu_ivf_flat_t<half, uint8_t>*>(a->ptr));
        default: break;
        }
        break;
    default: break;
    }
    throw std::runtime_error("gpu_ivf_flat: unsupported (base,storage) type combination");
}

gpu_ivf_flat_any_t::~gpu_ivf_flat_any_t() {
    if (!ptr) return;
    try {
        ivf_flat_dispatch(this, [](auto* idx) {
            idx->destroy();
            delete idx;
        });
    } catch (...) {
        // unsupported combo never gets a live ptr — nothing to free
    }
}

// Construct a new gpu_ivf_flat_t<B,Q> for the wired (btype,qtype) combos.
// Maker is a generic lambda invoked as maker(static type tag) -> void*; it
// receives a null typed pointer purely to recover B and Q.
template <typename Maker>
static void* ivf_flat_construct(quantization_t btype, quantization_t qtype, Maker&& maker) {
    switch (btype) {
    case Quantization_F32:
        switch (qtype) {
        case Quantization_F32:   return maker(static_cast<gpu_ivf_flat_t<float, float  >*>(nullptr));
        case Quantization_F16:   return maker(static_cast<gpu_ivf_flat_t<float, half   >*>(nullptr));
        case Quantization_INT8:  return maker(static_cast<gpu_ivf_flat_t<float, int8_t >*>(nullptr));
        case Quantization_UINT8: return maker(static_cast<gpu_ivf_flat_t<float, uint8_t>*>(nullptr));
        default: break;
        }
        break;
    case Quantization_F16:
        switch (qtype) {
        case Quantization_F16:   return maker(static_cast<gpu_ivf_flat_t<half, half   >*>(nullptr));
        case Quantization_INT8:  return maker(static_cast<gpu_ivf_flat_t<half, int8_t >*>(nullptr));
        case Quantization_UINT8: return maker(static_cast<gpu_ivf_flat_t<half, uint8_t>*>(nullptr));
        default: break;
        }
        break;
    default: break;
    }
    throw std::runtime_error("gpu_ivf_flat: unsupported (base,storage) type combination");
}

extern "C" {

gpu_ivf_flat_c gpu_ivf_flat_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension,
                                 distance_type_t metric_c, ivf_flat_build_params_t build_params,
                                 const int* devices, int device_count, uint32_t nthread,
                                 distribution_mode_t dist_mode, quantization_t btype, quantization_t qtype,
                                 const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::vector<int> devs(devices, devices + device_count);
        std::unique_ptr<gpu_ivf_flat_any_t> holder(new gpu_ivf_flat_any_t(btype, qtype, nullptr));
        holder->ptr = ivf_flat_construct(btype, qtype, [&](auto* tag) -> void* {
            using B = typename std::remove_pointer_t<decltype(tag)>::base_type;
            using Q = typename std::remove_pointer_t<decltype(tag)>::storage_type;
            // The dataset-providing constructor takes storage-typed (Q) data.
            return new gpu_ivf_flat_t<B, Q>(static_cast<const Q*>(dataset_data), count_vectors, dimension, metric_c, build_params, devs, nthread, dist_mode, ids);
        });
        return static_cast<gpu_ivf_flat_c>(holder.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_new", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_new", "unknown C++ exception");
    }
    return nullptr;
}

gpu_ivf_flat_c gpu_ivf_flat_new_empty(uint64_t total_count, uint32_t dimension, distance_type_t metric_c,
                                        ivf_flat_build_params_t build_params,
                                        const int* devices, int device_count, uint32_t nthread,
                                        distribution_mode_t dist_mode, quantization_t btype, quantization_t qtype,
                                        const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::vector<int> devs(devices, devices + device_count);
        std::unique_ptr<gpu_ivf_flat_any_t> holder(new gpu_ivf_flat_any_t(btype, qtype, nullptr));
        holder->ptr = ivf_flat_construct(btype, qtype, [&](auto* tag) -> void* {
            using B = typename std::remove_pointer_t<decltype(tag)>::base_type;
            using Q = typename std::remove_pointer_t<decltype(tag)>::storage_type;
            return new gpu_ivf_flat_t<B, Q>(total_count, dimension, metric_c, build_params, devs, nthread, dist_mode, ids);
        });
        return static_cast<gpu_ivf_flat_c>(holder.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_new_empty", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_new_empty", "unknown C++ exception");
    }
    return nullptr;
}

gpu_ivf_flat_c gpu_ivf_flat_load_file(const char* filename, uint32_t dimension, distance_type_t metric_c,
                                      ivf_flat_build_params_t build_params,
                                      const int* devices, int device_count, uint32_t nthread,
                                      distribution_mode_t dist_mode, quantization_t btype, quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::vector<int> devs(devices, devices + device_count);
        std::unique_ptr<gpu_ivf_flat_any_t> holder(new gpu_ivf_flat_any_t(btype, qtype, nullptr));
        holder->ptr = ivf_flat_construct(btype, qtype, [&](auto* tag) -> void* {
            using B = typename std::remove_pointer_t<decltype(tag)>::base_type;
            using Q = typename std::remove_pointer_t<decltype(tag)>::storage_type;
            return new gpu_ivf_flat_t<B, Q>(std::string(filename), dimension, metric_c, build_params, devs, nthread, dist_mode);
        });
        return static_cast<gpu_ivf_flat_c>(holder.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_load_file", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_load_file", "unknown C++ exception");
    }
    return nullptr;
}

void gpu_ivf_flat_destroy(gpu_ivf_flat_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        delete static_cast<gpu_ivf_flat_any_t*>(index_c);
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_destroy", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_destroy", "unknown C++ exception");
    }
}

void gpu_ivf_flat_start(gpu_ivf_flat_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [](auto* idx) { idx->start(); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_start", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_start", "unknown C++ exception");
    }
}

void gpu_ivf_flat_build(gpu_ivf_flat_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [](auto* idx) { idx->build(); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_build", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_build", "unknown C++ exception");
    }
}

void gpu_ivf_flat_extend(gpu_ivf_flat_c index_c, const void* new_data, uint64_t n_rows,
                         const int64_t* new_ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            idx->extend(static_cast<const Q*>(new_data), n_rows, new_ids);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_extend", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_extend", "unknown C++ exception");
    }
}

void gpu_ivf_flat_extend_float(gpu_ivf_flat_c index_c, const float* new_data, uint64_t n_rows,
                               const int64_t* new_ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            idx->extend_float(new_data, n_rows, new_ids);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_extend_float", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_extend_float", "unknown C++ exception");
    }
}

void gpu_ivf_flat_add_chunk(gpu_ivf_flat_c index_c, const void* chunk_data, uint64_t chunk_count, const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            idx->add_chunk(static_cast<const Q*>(chunk_data), chunk_count, -1, ids);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_add_chunk", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_add_chunk", "unknown C++ exception");
    }
}

void gpu_ivf_flat_add_chunk_float(gpu_ivf_flat_c index_c, const float* chunk_data, uint64_t chunk_count, const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            idx->add_chunk_float(chunk_data, chunk_count, -1, ids);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_add_chunk_float", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_add_chunk_float", "unknown C++ exception");
    }
}

void gpu_ivf_flat_add_chunk_quantize(gpu_ivf_flat_c index_c, const void* base_data, uint64_t chunk_count, const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            idx->add_chunk_quantize(static_cast<const B*>(base_data), chunk_count, -1, ids);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_add_chunk_quantize", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_add_chunk_quantize", "unknown C++ exception");
    }
}

void gpu_ivf_flat_train_quantizer(gpu_ivf_flat_c index_c, const float* train_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            // train_quantizer takes base-typed (B) data. The C ABI hands us
            // float32 (every existing caller has an F32 base); for an F16 base
            // convert the host buffer to half first so all instantiations are
            // both compilable and correct.
            if constexpr (std::is_same_v<B, float>) {
                idx->train_quantizer(train_data, n_samples);
            } else {
                std::vector<B> conv(static_cast<size_t>(n_samples) * idx->dimension);
                matrixone::cast_float_to_half_host(train_data, conv.data(), conv.size());
                idx->train_quantizer(conv.data(), n_samples);
            }
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_train_quantizer", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_train_quantizer", "unknown C++ exception");
    }
}

void gpu_ivf_flat_set_batch_window(gpu_ivf_flat_c index_c, int64_t window_us, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) { idx->set_batch_window(window_us); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_set_batch_window", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_set_batch_window", "unknown C++ exception");
    }
}

void gpu_ivf_flat_set_dynb_conservative_dispatch(gpu_ivf_flat_c index_c, bool enable, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) { idx->set_dynb_conservative_dispatch(enable); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_set_dynb_conservative_dispatch", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_set_dynb_conservative_dispatch", "unknown C++ exception");
    }
}

void gpu_ivf_flat_set_quantizer(gpu_ivf_flat_c index_c, float min, float max, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) { idx->set_quantizer(min, max); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_set_quantizer", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_set_quantizer", "unknown C++ exception");
    }
}

void gpu_ivf_flat_get_quantizer(gpu_ivf_flat_c index_c, float* min, float* max, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) { idx->get_quantizer(min, max); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_get_quantizer", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_get_quantizer", "unknown C++ exception");
    }
}

void gpu_ivf_flat_save(gpu_ivf_flat_c index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) { idx->save(filename); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_save", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_save", "unknown C++ exception");
    }
}

void gpu_ivf_flat_save_dir(gpu_ivf_flat_c index_c, const char* dir, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) { idx->save_dir(dir); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_save_dir", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_save_dir", "unknown C++ exception");
    }
}

void gpu_ivf_flat_delete_id(gpu_ivf_flat_c index_c, int64_t id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) { idx->delete_id(id); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_delete_id", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_delete_id", "unknown C++ exception");
    }
}

void gpu_ivf_flat_load_dir(gpu_ivf_flat_c index_c, const char* dir,
                            distribution_mode_t target_mode, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) { idx->load_dir(dir, target_mode); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_load_dir", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_load_dir", "unknown C++ exception");
    }
}

gpu_ivf_flat_search_res_t gpu_ivf_flat_search(gpu_ivf_flat_c index_c, const void* queries_data, uint64_t num_queries,
                                                uint32_t query_dimension, uint32_t limit,
                                                ivf_flat_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_ivf_flat_search_res_t result = {nullptr};
    try {
        auto cpp_res = std::make_unique<ivf_flat_search_result_t>();
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            *cpp_res = idx->search(static_cast<const Q*>(queries_data), num_queries, query_dimension, limit, search_params);
        });
        result.result_ptr = static_cast<gpu_ivf_flat_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search", "unknown C++ exception");
    }
    return result;
}

gpu_ivf_flat_search_res_t gpu_ivf_flat_search_quantize(gpu_ivf_flat_c index_c, const void* queries_data, uint64_t num_queries,
                                                    uint32_t query_dimension, uint32_t limit,
                                                    ivf_flat_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_ivf_flat_search_res_t result = {nullptr};
    try {
        auto cpp_res = std::make_unique<ivf_flat_search_result_t>();
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            *cpp_res = idx->search_quantize(static_cast<const B*>(queries_data), num_queries, query_dimension, limit, search_params);
        });
        result.result_ptr = static_cast<gpu_ivf_flat_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_quantize", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_quantize", "unknown C++ exception");
    }
    return result;
}

uint64_t gpu_ivf_flat_search_async(gpu_ivf_flat_c index_c, const void* queries_data, uint64_t num_queries,
                                     uint32_t query_dimension, uint32_t limit,
                                     ivf_flat_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        return ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) -> uint64_t {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            return idx->search_async(static_cast<const Q*>(queries_data), num_queries, query_dimension, limit, search_params);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_async", e.what());
        return 0;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_async", "unknown C++ exception");
        return 0;
    }
}

uint64_t gpu_ivf_flat_search_quantize_async(gpu_ivf_flat_c index_c, const void* queries_data, uint64_t num_queries,
                                           uint32_t query_dimension, uint32_t limit,
                                           ivf_flat_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        return ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) -> uint64_t {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            return idx->search_quantize_async(static_cast<const B*>(queries_data), num_queries, query_dimension, limit, search_params);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_quantize_async", e.what());
        return 0;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_quantize_async", "unknown C++ exception");
        return 0;
    }
}

gpu_ivf_flat_search_res_t gpu_ivf_flat_search_wait(gpu_ivf_flat_c index_c, uint64_t job_id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_ivf_flat_search_res_t result = {nullptr};
    try {
        auto cpp_res = std::make_unique<ivf_flat_search_result_t>();
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            *cpp_res = idx->search_wait(job_id);
        });
        result.result_ptr = static_cast<gpu_ivf_flat_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_wait", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_wait", "unknown C++ exception");
    }
    return result;
}


void gpu_ivf_flat_get_neighbors(gpu_ivf_flat_result_c result_c, uint64_t total_elements, int64_t* neighbors) {
    try {
        if (!neighbors || total_elements == 0) return;
        if (!result_c) {
            // No result_t: caller buffer must not be left uninitialized.
            std::fill(neighbors, neighbors + total_elements, static_cast<int64_t>(-1));
            return;
        }
        auto* neighbors_vec = &static_cast<ivf_flat_search_result_t*>(result_c)->neighbors;
        uint64_t n_copy = std::min<uint64_t>(neighbors_vec->size(), total_elements);
        std::copy(neighbors_vec->begin(), neighbors_vec->begin() + n_copy, neighbors);
        // Sentinel-fill the tail: caller asked for total_elements but vec was shorter.
        // -1 matches map_neighbor_id's OOB sentinel (index_base.hpp).
        std::fill(neighbors + n_copy, neighbors + total_elements, static_cast<int64_t>(-1));
    } catch (...) {
        matrixone::log_err("gpu_ivf_flat_get_neighbors: unknown C++ exception (swallowed)");
    }
}

void gpu_ivf_flat_get_distances(gpu_ivf_flat_result_c result_c, uint64_t total_elements, float* distances) {
    try {
        if (!distances || total_elements == 0) return;
        if (!result_c) {
            std::fill(distances, distances + total_elements, std::numeric_limits<float>::max());
            return;
        }
        auto* distances_vec = &static_cast<ivf_flat_search_result_t*>(result_c)->distances;
        uint64_t n_copy = std::min<uint64_t>(distances_vec->size(), total_elements);
        std::copy(distances_vec->begin(), distances_vec->begin() + n_copy, distances);
        // Sentinel-fill the tail to match brute_force_c.cpp convention.
        std::fill(distances + n_copy, distances + total_elements, std::numeric_limits<float>::max());
    } catch (...) {
        matrixone::log_err("gpu_ivf_flat_get_distances: unknown C++ exception (swallowed)");
    }
}

void gpu_ivf_flat_free_result(gpu_ivf_flat_result_c result_c) {
    try {
        if (!result_c) return;
        delete static_cast<ivf_flat_search_result_t*>(result_c);
    } catch (...) {
        matrixone::log_err("gpu_ivf_flat_free_result: unknown C++ exception (swallowed)");
    }
}

uint64_t gpu_ivf_flat_cap(gpu_ivf_flat_c index_c) {
    try {
        if (!index_c) return 0;
        return ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [](auto* idx) -> uint64_t { return idx->cap(); });
    } catch (...) {
        return 0;
    }
}

uint64_t gpu_ivf_flat_len(gpu_ivf_flat_c index_c) {
    try {
        if (!index_c) return 0;
        return ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [](auto* idx) -> uint64_t { return idx->len(); });
    } catch (...) {
        return 0;
    }
}

char* gpu_ivf_flat_info(gpu_ivf_flat_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    if (!index_c) return nullptr;
    try {
        std::string info = ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [](auto* idx) -> std::string { return idx->info(); });
        return strdup(info.c_str());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_info", e.what());
        return nullptr;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_info", "unknown C++ exception");
        return nullptr;
    }
}

void gpu_ivf_flat_get_centers(gpu_ivf_flat_c index_c, void* centers, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            auto host_centers = idx->get_centers();
            if (!host_centers.empty()) std::copy(host_centers.begin(), host_centers.end(), static_cast<Q*>(centers));
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_get_centers", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_get_centers", "unknown C++ exception");
    }
}

uint32_t gpu_ivf_flat_get_n_list(gpu_ivf_flat_c index_c) {
    try {
        if (!index_c) return 0;
        return ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [](auto* idx) -> uint32_t { return idx->get_n_list(); });
    } catch (...) {
        return 0;
    }
}

// ---------- Pre-filter API ----------

void gpu_ivf_flat_set_filter_columns(gpu_ivf_flat_c index_c, const char* col_meta_json,
                                      uint64_t total_count, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::string s = col_meta_json ? col_meta_json : "";
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            idx->set_filter_columns(s, total_count);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_set_filter_columns", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_set_filter_columns", "unknown C++ exception");
    }
}

void gpu_ivf_flat_add_filter_chunk(gpu_ivf_flat_c index_c, uint32_t col_idx,
                                    const void* data, const uint32_t* null_bitmap,
                                    uint64_t nrows, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            idx->add_filter_chunk(col_idx, data, null_bitmap, nrows);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_add_filter_chunk", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_add_filter_chunk", "unknown C++ exception");
    }
}

gpu_ivf_flat_search_res_t gpu_ivf_flat_search_with_filter(gpu_ivf_flat_c index_c, const void* queries_data,
                                                           uint64_t num_queries, uint32_t query_dimension,
                                                           uint32_t limit, ivf_flat_search_params_t sp,
                                                           const char* preds_json, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_ivf_flat_search_res_t result = {nullptr};
    try {
        auto cpp_res = std::make_unique<ivf_flat_search_result_t>();
        std::string preds = preds_json ? preds_json : "";
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            *cpp_res = idx->search_with_filter(static_cast<const Q*>(queries_data), num_queries, query_dimension, limit, sp, preds);
        });
        result.result_ptr = static_cast<gpu_ivf_flat_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_with_filter", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_with_filter", "unknown C++ exception");
    }
    return result;
}

gpu_ivf_flat_search_res_t gpu_ivf_flat_search_quantize_with_filter(gpu_ivf_flat_c index_c, const void* queries_data,
                                                                 uint64_t num_queries, uint32_t query_dimension,
                                                                 uint32_t limit, ivf_flat_search_params_t sp,
                                                                 const char* preds_json, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_ivf_flat_search_res_t result = {nullptr};
    try {
        auto cpp_res = std::make_unique<ivf_flat_search_result_t>();
        std::string preds = preds_json ? preds_json : "";
        ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            *cpp_res = idx->search_quantize_with_filter(static_cast<const B*>(queries_data), num_queries, query_dimension, limit, sp, preds);
        });
        result.result_ptr = static_cast<gpu_ivf_flat_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_quantize_with_filter", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_quantize_with_filter", "unknown C++ exception");
    }
    return result;
}

uint64_t gpu_ivf_flat_search_quantize_with_filter_async(gpu_ivf_flat_c index_c, const void* queries_data,
                                                      uint64_t num_queries, uint32_t query_dimension,
                                                      uint32_t limit, ivf_flat_search_params_t sp,
                                                      const char* preds_json, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::string preds = preds_json ? preds_json : "";
        return ivf_flat_dispatch(static_cast<gpu_ivf_flat_any_t*>(index_c), [&](auto* idx) -> uint64_t {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            return idx->search_quantize_with_filter_async(static_cast<const B*>(queries_data), num_queries, query_dimension, limit, sp, preds);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_quantize_with_filter_async", e.what());
        return 0;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_quantize_with_filter_async", "unknown C++ exception");
        return 0;
    }
}

} // extern "C"

namespace matrixone {
template class gpu_ivf_flat_t<float, float>;
template class gpu_ivf_flat_t<float, half>;
template class gpu_ivf_flat_t<float, int8_t>;
template class gpu_ivf_flat_t<float, uint8_t>;
template class gpu_ivf_flat_t<half, half>;
template class gpu_ivf_flat_t<half, int8_t>;
template class gpu_ivf_flat_t<half, uint8_t>;
} // namespace matrixone
