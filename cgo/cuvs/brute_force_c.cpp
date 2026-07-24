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
 *
 * Two type axes via quantization_t:
 *   btype = base / query / quantizer-SOURCE element type (Quantization_F32 or F16)
 *   qtype = storage element type (Quantization_F32 or F16)
 *
 * Wired (btype, qtype) combinations:
 *   F32 base: F32, F16 storage
 *   F16 base: F16 storage
 * Any other combination throws "unsupported (base,storage) type combination".
 *
 * NOTE: unlike CAGRA, cuVS brute_force only provides build/search for
 * index<float,float> and index<half,float> — there is NO int8_t/uint8_t
 * storage path. So the INT8/UINT8 qtype combos are intentionally omitted
 * here (instantiating them would fail to bind cuvs::brute_force::build/search).
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
#include <memory>
#include <type_traits>

using namespace matrixone;

struct gpu_brute_force_any_t {
    quantization_t btype;   // base / query / quantizer-source element type
    quantization_t qtype;   // storage element type
    void* ptr;

    gpu_brute_force_any_t(quantization_t b, quantization_t q, void* p)
        : btype(b), qtype(q), ptr(p) {}
    ~gpu_brute_force_any_t();
};

// Static dispatch: resolves the concrete gpu_brute_force_t<B,T> for (btype,qtype)
// and invokes fn with a typed pointer. fn is a generic lambda; recover B/Q inside
// it via decltype(idx)::base_type / ::storage_type. Throws on unsupported combos.
template <typename Fn>
static auto brute_force_dispatch(const gpu_brute_force_any_t* a, Fn&& fn) {
    switch (a->btype) {
    case Quantization_F32:
        switch (a->qtype) {
        case Quantization_F32:   return fn(static_cast<gpu_brute_force_t<float, float>*>(a->ptr));
        case Quantization_F16:   return fn(static_cast<gpu_brute_force_t<float, half >*>(a->ptr));
        default: break;
        }
        break;
    case Quantization_F16:
        switch (a->qtype) {
        case Quantization_F16:   return fn(static_cast<gpu_brute_force_t<half, half>*>(a->ptr));
        default: break;
        }
        break;
    default: break;
    }
    throw std::runtime_error("gpu_brute_force: unsupported (base,storage) type combination");
}

gpu_brute_force_any_t::~gpu_brute_force_any_t() {
    if (!ptr) return;
    try {
        brute_force_dispatch(this, [](auto* idx) {
            delete idx;
        });
    } catch (...) {
        // unsupported combo never gets a live ptr — nothing to free
    }
}

extern "C" {

gpu_brute_force_c gpu_brute_force_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, distance_type_t metric_c, uint32_t nthread, int device_id, quantization_t btype, quantization_t qtype, const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        // Construct the right gpu_brute_force_t<B,T>; the native build
        // constructor takes storage-typed (T) data.
        std::unique_ptr<gpu_brute_force_any_t> holder(new gpu_brute_force_any_t(btype, qtype, nullptr));
        holder->ptr = brute_force_dispatch(holder.get(), [&](auto* tag) -> void* {
            using B = typename std::remove_pointer_t<decltype(tag)>::base_type;
            using T = typename std::remove_pointer_t<decltype(tag)>::storage_type;
            return new gpu_brute_force_t<B, T>(static_cast<const T*>(dataset_data), count_vectors, dimension, metric_c, nthread, device_id, ids);
        });
        return static_cast<gpu_brute_force_c>(holder.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_new", e.what());
        return nullptr;
    } catch (...) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_new", "unknown C++ exception");
        return nullptr;
    }
}

gpu_brute_force_c gpu_brute_force_new_empty(uint64_t total_count, uint32_t dimension, distance_type_t metric_c, uint32_t nthread, int device_id, quantization_t btype, quantization_t qtype, const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::unique_ptr<gpu_brute_force_any_t> holder(new gpu_brute_force_any_t(btype, qtype, nullptr));
        holder->ptr = brute_force_dispatch(holder.get(), [&](auto* tag) -> void* {
            using B = typename std::remove_pointer_t<decltype(tag)>::base_type;
            using T = typename std::remove_pointer_t<decltype(tag)>::storage_type;
            return new gpu_brute_force_t<B, T>(total_count, dimension, metric_c, nthread, device_id, ids);
        });
        return static_cast<gpu_brute_force_c>(holder.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_new_empty", e.what());
        return nullptr;
    } catch (...) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_new_empty", "unknown C++ exception");
        return nullptr;
    }
}

void gpu_brute_force_start(gpu_brute_force_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [](auto* idx) { idx->start(); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_start", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_start", "unknown C++ exception");
    }
}

void gpu_brute_force_build(gpu_brute_force_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [](auto* idx) { idx->build(); });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_build", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_build", "unknown C++ exception");
    }
}

void gpu_brute_force_add_chunk(gpu_brute_force_c index_c, const void* chunk_data, uint64_t chunk_count, const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [&](auto* idx) {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            idx->add_chunk(static_cast<const Q*>(chunk_data), chunk_count, -1, ids);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_add_chunk", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_add_chunk", "unknown C++ exception");
    }
}

// Base-typed (B) add: the add counterpart of search_quantize. queries_data is in
// the base element type B; add_chunk_quantize converts B -> storage T (native
// store when B==T, f32->f16 cast for (float,half), learned SQ for 1-byte). Used
// by the CDC overflow build to store base vectors at the index's storage type.
void gpu_brute_force_add_chunk_quantize(gpu_brute_force_c index_c, const void* chunk_data, uint64_t chunk_count, const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [&](auto* idx) {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            idx->add_chunk_quantize(static_cast<const B*>(chunk_data), chunk_count, -1, ids);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_add_chunk_quantize", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_add_chunk_quantize", "unknown C++ exception");
    }
}

gpu_brute_force_search_result_c gpu_brute_force_search(gpu_brute_force_c index_c, const void* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto cpp_res = std::make_unique<brute_force_search_result_t>();
        brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [&](auto* idx) {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            *cpp_res = idx->search(static_cast<const Q*>(queries_data), num_queries, query_dimension, limit, brute_force_search_params_default());
        });
        return static_cast<gpu_brute_force_search_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_search", e.what());
        return nullptr;
    } catch (...) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_search", "unknown C++ exception");
        return nullptr;
    }
}

gpu_brute_force_search_result_c gpu_brute_force_search_quantize(gpu_brute_force_c index_c, const void* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto cpp_res = std::make_unique<brute_force_search_result_t>();
        brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [&](auto* idx) {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            *cpp_res = idx->search_quantize(static_cast<const B*>(queries_data), num_queries, query_dimension, limit, brute_force_search_params_default());
        });
        return static_cast<gpu_brute_force_search_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_search_quantize", e.what());
        return nullptr;
    } catch (...) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_search_quantize", "unknown C++ exception");
        return nullptr;
    }
}

uint64_t gpu_brute_force_search_async(gpu_brute_force_c index_c, const void* queries_data, uint64_t num_queries,
                                         uint32_t query_dimension, uint32_t limit, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        brute_force_search_params_t search_params;
        return brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [&](auto* idx) -> uint64_t {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            return idx->search_async(static_cast<const Q*>(queries_data), num_queries, query_dimension, limit, search_params);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_async", e.what());
        return 0;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_async", "unknown C++ exception");
        return 0;
    }
}

uint64_t gpu_brute_force_search_quantize_async(gpu_brute_force_c index_c, const void* queries_data, uint64_t num_queries,
                                              uint32_t query_dimension, uint32_t limit, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        brute_force_search_params_t search_params;
        return brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [&](auto* idx) -> uint64_t {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            return idx->search_quantize_async(static_cast<const B*>(queries_data), num_queries, query_dimension, limit, search_params);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_quantize_async", e.what());
        return 0;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_quantize_async", "unknown C++ exception");
        return 0;
    }
}

gpu_brute_force_search_result_c gpu_brute_force_search_wait(gpu_brute_force_c index_c, uint64_t job_id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto cpp_res = std::make_unique<brute_force_search_result_t>();
        brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [&](auto* idx) {
            *cpp_res = idx->search_wait(job_id);
        });
        return static_cast<gpu_brute_force_search_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_wait", e.what());
        return nullptr;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_wait", "unknown C++ exception");
        return nullptr;
    }
}

void gpu_brute_force_get_results(gpu_brute_force_search_result_c result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances) {
    try {
        if (!result_c) return;
        auto* search_result = static_cast<brute_force_search_result_t*>(result_c);

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
    } catch (...) {
        matrixone::log_err("gpu_brute_force_get_results: unknown C++ exception (swallowed)");
    }
}

void gpu_brute_force_free_search_result(gpu_brute_force_search_result_c result_c) {
    try {
        if (!result_c) return;
        delete static_cast<brute_force_search_result_t*>(result_c);
    } catch (...) {
        matrixone::log_err("gpu_brute_force_free_search_result: unknown C++ exception (swallowed)");
    }
}

uint64_t gpu_brute_force_cap(gpu_brute_force_c index_c) {
    try {
        if (!index_c) return 0;
        return brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [](auto* idx) -> uint64_t { return idx->cap(); });
    } catch (...) {
        return 0;
    }
}

uint64_t gpu_brute_force_len(gpu_brute_force_c index_c) {
    try {
        if (!index_c) return 0;
        return brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [](auto* idx) -> uint64_t { return idx->len(); });
    } catch (...) {
        return 0;
    }
}

char* gpu_brute_force_info(gpu_brute_force_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    if (!index_c) return nullptr;
    try {
        std::string info = brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [](auto* idx) -> std::string { return idx->info(); });
        return strdup(info.c_str());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_info", e.what());
        return nullptr;
    } catch (...) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_info", "unknown C++ exception");
        return nullptr;
    }
}

void gpu_brute_force_destroy(gpu_brute_force_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        delete static_cast<gpu_brute_force_any_t*>(index_c);
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_destroy", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_brute_force_destroy", "unknown C++ exception");
    }
}

// ---------- Filter wrappers ----------

void gpu_brute_force_set_filter_columns(gpu_brute_force_c index_c, const char* col_meta_json,
                                        uint64_t total_count, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::string meta = col_meta_json ? col_meta_json : "";
        brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [&](auto* idx) {
            idx->set_filter_columns(meta, total_count);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_set_filter_columns", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_set_filter_columns", "unknown C++ exception");
    }
}

void gpu_brute_force_add_filter_chunk(gpu_brute_force_c index_c, uint32_t col_idx,
                                      const void* data, const uint32_t* null_bitmap,
                                      uint64_t nrows, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [&](auto* idx) {
            idx->add_filter_chunk(col_idx, data, null_bitmap, nrows);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_add_filter_chunk", e.what());
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_add_filter_chunk", "unknown C++ exception");
    }
}

gpu_brute_force_search_result_c gpu_brute_force_search_with_filter(gpu_brute_force_c index_c,
                                                                    const void* queries_data,
                                                                    uint64_t num_queries, uint32_t query_dimension,
                                                                    uint32_t limit, const char* preds_json,
                                                                    void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        brute_force_search_params_t sp;
        std::string preds = preds_json ? preds_json : "";
        auto cpp_res = std::make_unique<brute_force_search_result_t>();
        brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [&](auto* idx) {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            *cpp_res = idx->search_with_filter(static_cast<const Q*>(queries_data), num_queries, query_dimension, limit, sp, preds);
        });
        return static_cast<gpu_brute_force_search_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_with_filter", e.what());
        return nullptr;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_with_filter", "unknown C++ exception");
        return nullptr;
    }
}

gpu_brute_force_search_result_c gpu_brute_force_search_quantize_with_filter(gpu_brute_force_c index_c,
                                                                          const void* queries_data,
                                                                          uint64_t num_queries, uint32_t query_dimension,
                                                                          uint32_t limit, const char* preds_json,
                                                                          void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        brute_force_search_params_t sp;
        std::string preds = preds_json ? preds_json : "";
        auto cpp_res = std::make_unique<brute_force_search_result_t>();
        brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [&](auto* idx) {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            *cpp_res = idx->search_quantize_with_filter(static_cast<const B*>(queries_data), num_queries, query_dimension, limit, sp, preds);
        });
        return static_cast<gpu_brute_force_search_result_c>(cpp_res.release());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_quantize_with_filter", e.what());
        return nullptr;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_quantize_with_filter", "unknown C++ exception");
        return nullptr;
    }
}

uint64_t gpu_brute_force_search_quantize_with_filter_async(gpu_brute_force_c index_c,
                                                         const void* queries_data,
                                                         uint64_t num_queries, uint32_t query_dimension,
                                                         uint32_t limit, const char* preds_json,
                                                         void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        brute_force_search_params_t sp;
        std::string preds = preds_json ? preds_json : "";
        return brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [&](auto* idx) -> uint64_t {
            using B = typename std::remove_pointer_t<decltype(idx)>::base_type;
            return idx->search_quantize_with_filter_async(static_cast<const B*>(queries_data), num_queries, query_dimension, limit, sp, preds);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_quantize_with_filter_async", e.what());
        return 0;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_quantize_with_filter_async", "unknown C++ exception");
        return 0;
    }
}

// Native-typed (T) async filtered search. queries_data is in the index storage
// type T; no quantization/widening. Returns a job_id collected with
// gpu_brute_force_search_wait. Lets the filtered overflow stay native.
uint64_t gpu_brute_force_search_with_filter_async(gpu_brute_force_c index_c,
                                                  const void* queries_data,
                                                  uint64_t num_queries, uint32_t query_dimension,
                                                  uint32_t limit, const char* preds_json,
                                                  void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        brute_force_search_params_t sp;
        std::string preds = preds_json ? preds_json : "";
        return brute_force_dispatch(static_cast<gpu_brute_force_any_t*>(index_c), [&](auto* idx) -> uint64_t {
            using Q = typename std::remove_pointer_t<decltype(idx)>::storage_type;
            return idx->search_with_filter_async(static_cast<const Q*>(queries_data), num_queries, query_dimension, limit, sp, preds);
        });
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_with_filter_async", e.what());
        return 0;
    } catch (...) {
        matrixone::set_errmsg(errmsg, "Error in gpu_brute_force_search_with_filter_async", "unknown C++ exception");
        return 0;
    }
}

} // extern "C"

namespace matrixone {
template class gpu_brute_force_t<float, float>;
template class gpu_brute_force_t<float, half>;
template class gpu_brute_force_t<half, half>;
}
