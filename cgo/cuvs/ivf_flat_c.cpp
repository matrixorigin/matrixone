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
 * Supported data types (via quantization_t): Quantization_F32, Quantization_F16, Quantization_INT8, Quantization_UINT8
 */

#include "ivf_flat_c.h"
#include "ivf_flat.hpp"
#include <iostream>
#include <vector>
#include <cstring>

using namespace matrixone;

struct gpu_ivf_flat_any_t {
    quantization_t qtype;
    void* ptr;

    gpu_ivf_flat_any_t(quantization_t q, void* p) : qtype(q), ptr(p) {}
    ~gpu_ivf_flat_any_t() {
        switch (qtype) {
            case Quantization_F32: {
                auto* p = static_cast<gpu_ivf_flat_t<float>*>(ptr);
                p->destroy();
                delete p;
                break;
            }
            case Quantization_F16: {
                auto* p = static_cast<gpu_ivf_flat_t<half>*>(ptr);
                p->destroy();
                delete p;
                break;
            }
            case Quantization_INT8: {
                auto* p = static_cast<gpu_ivf_flat_t<int8_t>*>(ptr);
                p->destroy();
                delete p;
                break;
            }
            case Quantization_UINT8: {
                auto* p = static_cast<gpu_ivf_flat_t<uint8_t>*>(ptr);
                p->destroy();
                delete p;
                break;
            }
            default: break;
        }
    }
};

extern "C" {

gpu_ivf_flat_c gpu_ivf_flat_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                 distance_type_t metric_c, ivf_flat_build_params_t build_params,
                                 const int* devices, int device_count, uint32_t nthread, 
                                 distribution_mode_t dist_mode, quantization_t qtype, 
                                 const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::vector<int> devs(devices, devices + device_count);
        void* ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                ptr = new gpu_ivf_flat_t<float>(static_cast<const float*>(dataset_data), count_vectors, dimension, metric_c, build_params, devs, nthread, dist_mode, ids);
                break;
            case Quantization_F16:
                ptr = new gpu_ivf_flat_t<half>(static_cast<const half*>(dataset_data), count_vectors, dimension, metric_c, build_params, devs, nthread, dist_mode, ids);
                break;
            case Quantization_INT8:
                ptr = new gpu_ivf_flat_t<int8_t>(static_cast<const int8_t*>(dataset_data), count_vectors, dimension, metric_c, build_params, devs, nthread, dist_mode, ids);
                break;
            case Quantization_UINT8:
                ptr = new gpu_ivf_flat_t<uint8_t>(static_cast<const uint8_t*>(dataset_data), count_vectors, dimension, metric_c, build_params, devs, nthread, dist_mode, ids);
                break;
            default: return nullptr;
        }
        static_cast<gpu_index_base_t<float, ivf_flat_build_params_t>*>(ptr)->start();
        return static_cast<gpu_ivf_flat_c>(new gpu_ivf_flat_any_t(qtype, ptr));
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_new", e.what());
    }
    return nullptr;
}

gpu_ivf_flat_c gpu_ivf_flat_new_empty(uint64_t total_count, uint32_t dimension, distance_type_t metric_c, 
                                        ivf_flat_build_params_t build_params,
                                        const int* devices, int device_count, uint32_t nthread, 
                                        distribution_mode_t dist_mode, quantization_t qtype, 
                                        const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::vector<int> devs(devices, devices + device_count);
        void* ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                ptr = new gpu_ivf_flat_t<float>(total_count, dimension, metric_c, build_params, devs, nthread, dist_mode, ids);
                break;
            case Quantization_F16:
                ptr = new gpu_ivf_flat_t<half>(total_count, dimension, metric_c, build_params, devs, nthread, dist_mode, ids);
                break;
            case Quantization_INT8:
                ptr = new gpu_ivf_flat_t<int8_t>(total_count, dimension, metric_c, build_params, devs, nthread, dist_mode, ids);
                break;
            case Quantization_UINT8:
                ptr = new gpu_ivf_flat_t<uint8_t>(total_count, dimension, metric_c, build_params, devs, nthread, dist_mode, ids);
                break;
            default: return nullptr;
        }        static_cast<gpu_index_base_t<float, ivf_flat_build_params_t>*>(ptr)->start();
        return static_cast<gpu_ivf_flat_c>(new gpu_ivf_flat_any_t(qtype, ptr));
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_new_empty", e.what());
    }
    return nullptr;
}

gpu_ivf_flat_c gpu_ivf_flat_load_file(const char* filename, uint32_t dimension, distance_type_t metric_c,
                                      ivf_flat_build_params_t build_params,
                                      const int* devices, int device_count, uint32_t nthread, 
                                      distribution_mode_t dist_mode, quantization_t qtype, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        std::vector<int> devs(devices, devices + device_count);
        void* ptr = nullptr;
        switch (qtype) {
            case Quantization_F32:
                ptr = new gpu_ivf_flat_t<float>(std::string(filename), dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_F16:
                ptr = new gpu_ivf_flat_t<half>(std::string(filename), dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_INT8:
                ptr = new gpu_ivf_flat_t<int8_t>(std::string(filename), dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            case Quantization_UINT8:
                ptr = new gpu_ivf_flat_t<uint8_t>(std::string(filename), dimension, metric_c, build_params, devs, nthread, dist_mode);
                break;
            default: return nullptr;
        }
        static_cast<gpu_index_base_t<float, ivf_flat_build_params_t>*>(ptr)->start();
        return static_cast<gpu_ivf_flat_c>(new gpu_ivf_flat_any_t(qtype, ptr));
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_load_file", e.what());
    }
    return nullptr;
}

void gpu_ivf_flat_destroy(gpu_ivf_flat_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        delete static_cast<gpu_ivf_flat_any_t*>(index_c);
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_destroy", e.what());
    }
}

void gpu_ivf_flat_start(gpu_ivf_flat_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->start(); break;
            case Quantization_F16: static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->start(); break;
            case Quantization_INT8: static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->start(); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->start(); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_start", e.what());
    }
}

void gpu_ivf_flat_build(gpu_ivf_flat_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->build(); break;
            case Quantization_F16: static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->build(); break;
            case Quantization_INT8: static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->build(); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->build(); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_build", e.what());
    }
}

void gpu_ivf_flat_extend(gpu_ivf_flat_c index_c, const void* new_data, uint64_t n_rows,
                         const int64_t* new_ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32:  static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->extend(static_cast<const float*>(new_data), n_rows, new_ids); break;
            case Quantization_F16:  static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->extend(static_cast<const half*>(new_data), n_rows, new_ids); break;
            case Quantization_INT8: static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->extend(static_cast<const int8_t*>(new_data), n_rows, new_ids); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->extend(static_cast<const uint8_t*>(new_data), n_rows, new_ids); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_extend", e.what());
    }
}

void gpu_ivf_flat_extend_float(gpu_ivf_flat_c index_c, const float* new_data, uint64_t n_rows,
                               const int64_t* new_ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32:  static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->extend_float(new_data, n_rows, new_ids); break;
            case Quantization_F16:  static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->extend_float(new_data, n_rows, new_ids); break;
            case Quantization_INT8: static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->extend_float(new_data, n_rows, new_ids); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->extend_float(new_data, n_rows, new_ids); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_extend_float", e.what());
    }
}

void gpu_ivf_flat_add_chunk(gpu_ivf_flat_c index_c, const void* chunk_data, uint64_t chunk_count, const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->add_chunk(static_cast<const float*>(chunk_data), chunk_count, -1, ids); break;
            case Quantization_F16: static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->add_chunk(static_cast<const half*>(chunk_data), chunk_count, -1, ids); break;
            case Quantization_INT8: static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->add_chunk(static_cast<const int8_t*>(chunk_data), chunk_count, -1, ids); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->add_chunk(static_cast<const uint8_t*>(chunk_data), chunk_count, -1, ids); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_add_chunk", e.what());
    }
}

void gpu_ivf_flat_add_chunk_float(gpu_ivf_flat_c index_c, const float* chunk_data, uint64_t chunk_count, const int64_t* ids, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count, -1, ids); break;
            case Quantization_F16: static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count, -1, ids); break;
            case Quantization_INT8: static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count, -1, ids); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->add_chunk_float(chunk_data, chunk_count, -1, ids); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_add_chunk_float", e.what());
    }
}

void gpu_ivf_flat_train_quantizer(gpu_ivf_flat_c index_c, const float* train_data, uint64_t n_samples, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            case Quantization_F16: static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            case Quantization_INT8: static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->train_quantizer(train_data, n_samples); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_train_quantizer", e.what());
    }
}

void gpu_ivf_flat_set_per_thread_device(gpu_ivf_flat_c index_c, bool enable, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->set_per_thread_device(enable); break;
            case Quantization_F16: static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->set_per_thread_device(enable); break;
            case Quantization_INT8: static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->set_per_thread_device(enable); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->set_per_thread_device(enable); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_set_per_thread_device", e.what());
    }
}

void gpu_ivf_flat_set_use_batching(gpu_ivf_flat_c index_c, bool enable, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->set_use_batching(enable); break;
            case Quantization_F16: static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->set_use_batching(enable); break;
            case Quantization_INT8: static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->set_use_batching(enable); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->set_use_batching(enable); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_set_use_batching", e.what());
    }
}

void gpu_ivf_flat_set_quantizer(gpu_ivf_flat_c index_c, float min, float max, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->set_quantizer(min, max); break;
            case Quantization_F16: static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->set_quantizer(min, max); break;
            case Quantization_INT8: static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->set_quantizer(min, max); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->set_quantizer(min, max); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_set_quantizer", e.what());
    }
}

void gpu_ivf_flat_get_quantizer(gpu_ivf_flat_c index_c, float* min, float* max, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->get_quantizer(min, max); break;
            case Quantization_F16: static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->get_quantizer(min, max); break;
            case Quantization_INT8: static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->get_quantizer(min, max); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->get_quantizer(min, max); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_get_quantizer", e.what());
    }
}

void gpu_ivf_flat_save(gpu_ivf_flat_c index_c, const char* filename, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->save(filename); break;
            case Quantization_F16: static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->save(filename); break;
            case Quantization_INT8: static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->save(filename); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->save(filename); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg,
 "Error in gpu_ivf_flat_save", e.what());
    }
}

void gpu_ivf_flat_save_dir(gpu_ivf_flat_c index_c, const char* dir, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32:   static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->save_dir(dir); break;
            case Quantization_F16:   static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->save_dir(dir); break;
            case Quantization_INT8:  static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->save_dir(dir); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->save_dir(dir); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_save_dir", e.what());
    }
}

void gpu_ivf_flat_delete_id(gpu_ivf_flat_c index_c, int64_t id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32:   static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->delete_id(id); break;
            case Quantization_F16:   static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->delete_id(id); break;
            case Quantization_INT8:  static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->delete_id(id); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->delete_id(id); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_delete_id", e.what());
    }
}

void gpu_ivf_flat_load_dir(gpu_ivf_flat_c index_c, const char* dir, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32:   static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->load_dir(dir); break;
            case Quantization_F16:   static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->load_dir(dir); break;
            case Quantization_INT8:  static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->load_dir(dir); break;
            case Quantization_UINT8: static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->load_dir(dir); break;
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_load_dir", e.what());
    }
}

gpu_ivf_flat_search_res_t gpu_ivf_flat_search(gpu_ivf_flat_c index_c, const void* queries_data, uint64_t num_queries, 
                                                uint32_t query_dimension, uint32_t limit, 
                                                ivf_flat_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_ivf_flat_search_res_t result = {nullptr};
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        auto* cpp_res = new ivf_flat_search_result_t();
        switch (any->qtype) {
            case Quantization_F32: *cpp_res = static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->search(static_cast<const float*>(queries_data), num_queries, query_dimension, limit, search_params); break;
            case Quantization_F16: *cpp_res = static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->search(static_cast<const half*>(queries_data), num_queries, query_dimension, limit, search_params); break;
            case Quantization_INT8: *cpp_res = static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->search(static_cast<const int8_t*>(queries_data), num_queries, query_dimension, limit, search_params); break;
            case Quantization_UINT8: *cpp_res = static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->search(static_cast<const uint8_t*>(queries_data), num_queries, query_dimension, limit, search_params); break;
            default: break;
        }
        result.result_ptr = static_cast<gpu_ivf_flat_result_c>(cpp_res);
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_search", e.what());
    }
    return result;
}

gpu_ivf_flat_search_res_t gpu_ivf_flat_search_float(gpu_ivf_flat_c index_c, const float* queries_data, uint64_t num_queries, 
                                                    uint32_t query_dimension, uint32_t limit, 
                                                    ivf_flat_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_ivf_flat_search_res_t result = {nullptr};
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        auto* cpp_res = new ivf_flat_search_result_t();
        switch (any->qtype) {
            case Quantization_F32: *cpp_res = static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, search_params); break;
            case Quantization_F16: *cpp_res = static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, search_params); break;
            case Quantization_INT8: *cpp_res = static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, search_params); break;
            case Quantization_UINT8: *cpp_res = static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->search_float(queries_data, num_queries, query_dimension, limit, search_params); break;
            default: break;
        }
        result.result_ptr = static_cast<gpu_ivf_flat_result_c>(cpp_res);
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_float", e.what());
    }
    return result;
}

uint64_t gpu_ivf_flat_search_async(gpu_ivf_flat_c index_c, const void* queries_data, uint64_t num_queries, 
                                     uint32_t query_dimension, uint32_t limit, 
                                     ivf_flat_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32:   return static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->search_async(static_cast<const float*>(queries_data), num_queries, query_dimension, limit, search_params);
            case Quantization_F16:   return static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->search_async(static_cast<const half*>(queries_data), num_queries, query_dimension, limit, search_params);
            case Quantization_INT8:  return static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->search_async(static_cast<const int8_t*>(queries_data), num_queries, query_dimension, limit, search_params);
            case Quantization_UINT8: return static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->search_async(static_cast<const uint8_t*>(queries_data), num_queries, query_dimension, limit, search_params);
            default: return 0;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_async", e.what());
        return 0;
    }
}

uint64_t gpu_ivf_flat_search_float_async(gpu_ivf_flat_c index_c, const float* queries_data, uint64_t num_queries, 
                                           uint32_t query_dimension, uint32_t limit, 
                                           ivf_flat_search_params_t search_params, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32:   return static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->search_float_async(queries_data, num_queries, query_dimension, limit, search_params);
            case Quantization_F16:   return static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->search_float_async(queries_data, num_queries, query_dimension, limit, search_params);
            case Quantization_INT8:  return static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->search_float_async(queries_data, num_queries, query_dimension, limit, search_params);
            case Quantization_UINT8: return static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->search_float_async(queries_data, num_queries, query_dimension, limit, search_params);
            default: return 0;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_float_async", e.what());
        return 0;
    }
}

gpu_ivf_flat_search_res_t gpu_ivf_flat_search_wait(gpu_ivf_flat_c index_c, uint64_t job_id, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    gpu_ivf_flat_search_res_t result = {nullptr};
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        auto* cpp_res = new ivf_flat_search_result_t();
        switch (any->qtype) {
            case Quantization_F32: *cpp_res = static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->search_wait(job_id); break;
            case Quantization_F16: *cpp_res = static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->search_wait(job_id); break;
            case Quantization_INT8: *cpp_res = static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->search_wait(job_id); break;
            case Quantization_UINT8: *cpp_res = static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->search_wait(job_id); break;
            default: break;
        }
        result.result_ptr = static_cast<gpu_ivf_flat_result_c>(cpp_res);
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, "Error in gpu_ivf_flat_search_wait", e.what());
    }
    return result;
}


void gpu_ivf_flat_get_neighbors(gpu_ivf_flat_result_c result_c, uint64_t total_elements, int64_t* neighbors) {
    if (!result_c) return;
    auto* neighbors_vec = &static_cast<ivf_flat_search_result_t*>(result_c)->neighbors;
    if (neighbors_vec->size() >= total_elements) {
        std::copy(neighbors_vec->begin(), neighbors_vec->begin() + total_elements, neighbors);
    }
}

void gpu_ivf_flat_get_distances(gpu_ivf_flat_result_c result_c, uint64_t total_elements, float* distances) {
    if (!result_c) return;
    auto* distances_vec = &static_cast<ivf_flat_search_result_t*>(result_c)->distances;
    if (distances_vec->size() >= total_elements) {
        std::copy(distances_vec->begin(), distances_vec->begin() + total_elements, distances);
    }
}

void gpu_ivf_flat_free_result(gpu_ivf_flat_result_c result_c) {
    if (!result_c) return;
    delete static_cast<ivf_flat_search_result_t*>(result_c);
}

uint32_t gpu_ivf_flat_cap(gpu_ivf_flat_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->cap();
        case Quantization_F16: return static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->cap();
        case Quantization_INT8: return static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->cap();
        case Quantization_UINT8: return static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->cap();
        default: return 0;
    }
}

uint32_t gpu_ivf_flat_len(gpu_ivf_flat_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->len();
        case Quantization_F16: return static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->len();
        case Quantization_INT8: return static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->len();
        case Quantization_UINT8: return static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->len();
        default: return 0;
    }
}

char* gpu_ivf_flat_info(gpu_ivf_flat_c index_c, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    if (!index_c) return nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        std::string info;
        switch (any->qtype) {
            case Quantization_F32: info = static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->info(); break;
            case Quantization_F16: info = static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->info(); break;
            case Quantization_INT8: info = static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->info(); break;
            case Quantization_UINT8: info = static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->info(); break;
            default: return nullptr;
        }
        return strdup(info.c_str());
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_info", e.what());
        return nullptr;
    }
}

void gpu_ivf_flat_get_centers(gpu_ivf_flat_c index_c, void* centers, void* errmsg) {
    if (errmsg) *(static_cast<char**>(errmsg)) = nullptr;
    try {
        auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
        switch (any->qtype) {
            case Quantization_F32: {
                auto host_centers = static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->get_centers();
                if (!host_centers.empty()) std::copy(host_centers.begin(), host_centers.end(), static_cast<float*>(centers));
                break;
            }
            case Quantization_F16: {
                auto host_centers = static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->get_centers();
                if (!host_centers.empty()) std::copy(host_centers.begin(), host_centers.end(), static_cast<half*>(centers));
                break;
            }
            case Quantization_INT8: {
                auto host_centers = static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->get_centers();
                if (!host_centers.empty()) std::copy(host_centers.begin(), host_centers.end(), static_cast<int8_t*>(centers));
                break;
            }
            case Quantization_UINT8: {
                auto host_centers = static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->get_centers();
                if (!host_centers.empty()) std::copy(host_centers.begin(), host_centers.end(), static_cast<uint8_t*>(centers));
                break;
            }
            default: break;
        }
    } catch (const std::exception& e) {
        matrixone::set_errmsg(errmsg, 
 "Error in gpu_ivf_flat_get_centers", e.what());
    }
}

uint32_t gpu_ivf_flat_get_n_list(gpu_ivf_flat_c index_c) {
    if (!index_c) return 0;
    auto* any = static_cast<gpu_ivf_flat_any_t*>(index_c);
    switch (any->qtype) {
        case Quantization_F32: return static_cast<gpu_ivf_flat_t<float>*>(any->ptr)->get_n_list();
        case Quantization_F16: return static_cast<gpu_ivf_flat_t<half>*>(any->ptr)->get_n_list();
        case Quantization_INT8: return static_cast<gpu_ivf_flat_t<int8_t>*>(any->ptr)->get_n_list();
        case Quantization_UINT8: return static_cast<gpu_ivf_flat_t<uint8_t>*>(any->ptr)->get_n_list();
        default: return 0;
    }
}

} // extern "C"

namespace matrixone {
template class gpu_ivf_flat_t<float>;
template class gpu_ivf_flat_t<half>;
template class gpu_ivf_flat_t<int8_t>;
template class gpu_ivf_flat_t<uint8_t>;
} // namespace matrixone
