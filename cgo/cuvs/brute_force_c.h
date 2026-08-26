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

#ifndef BRUTE_FORCE_C_H
#define BRUTE_FORCE_C_H

#include "helper.h"
#include "cuvs_types.h"

#ifdef __cplusplus
extern "C" {
#endif

// Opaque pointer to the C++ gpu_brute_force_t object
typedef void* gpu_brute_force_c;

// Opaque pointer to the C++ search result object
typedef void* gpu_brute_force_search_result_c;

// Constructor for gpu_brute_force_t
gpu_brute_force_c gpu_brute_force_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, distance_type_t metric, uint32_t nthread, int device_id, quantization_t btype, quantization_t qtype, const int64_t* ids, void* errmsg);

// Constructor for an empty index (pre-allocates)
gpu_brute_force_c gpu_brute_force_new_empty(uint64_t total_count, uint32_t dimension, distance_type_t metric, uint32_t nthread, int device_id, quantization_t btype, quantization_t qtype, const int64_t* ids, void* errmsg);

// Starts the worker and initializes resources
void gpu_brute_force_start(gpu_brute_force_c index_c, void* errmsg);

// Builds the index (loads the dataset to the GPU)
void gpu_brute_force_build(gpu_brute_force_c index_c, void* errmsg);

// Add chunk of data (same type as index quantization)
void gpu_brute_force_add_chunk(gpu_brute_force_c index_c, const void* chunk_data, uint64_t chunk_count, const int64_t* ids, void* errmsg);

// Add chunk of base-typed (B) data; converts B -> storage T (the add counterpart
// of search_quantize: native store when B==T, f32->f16 cast, or learned SQ for 1-byte).
void gpu_brute_force_add_chunk_quantize(gpu_brute_force_c index_c, const void* chunk_data, uint64_t chunk_count, const int64_t* ids, void* errmsg);

// Performs a search operation
gpu_brute_force_search_result_c gpu_brute_force_search(gpu_brute_force_c index_c, const void* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, void* errmsg);

// Performs a search operation with base-typed (B) queries; quantizes B -> storage T internally.
gpu_brute_force_search_result_c gpu_brute_force_search_quantize(gpu_brute_force_c index_c, const void* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, void* errmsg);

// Asynchronous search functions
uint64_t gpu_brute_force_search_async(gpu_brute_force_c index_c, const void* queries_data, uint64_t num_queries,
                                         uint32_t query_dimension, uint32_t limit, void* errmsg);

uint64_t gpu_brute_force_search_quantize_async(gpu_brute_force_c index_c, const void* queries_data, uint64_t num_queries,
                                              uint32_t query_dimension, uint32_t limit, void* errmsg);

gpu_brute_force_search_result_c gpu_brute_force_search_wait(gpu_brute_force_c index_c, uint64_t job_id, void* errmsg);

// Retrieves the results from a search operation
void gpu_brute_force_get_results(gpu_brute_force_search_result_c result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances);

// Frees the memory for a gpu_brute_force_search_result_c object
void gpu_brute_force_free_search_result(gpu_brute_force_search_result_c result_c);

// ---------- Pre-filter (INCLUDE columns) ----------
// See cagra_c.h for JSON format details.
void gpu_brute_force_set_filter_columns(gpu_brute_force_c index_c, const char* col_meta_json,
                                        uint64_t total_count, void* errmsg);

// null_bitmap: LSB-first bits where 1 = row is NULL; NULL pointer = dense.
void gpu_brute_force_add_filter_chunk(gpu_brute_force_c index_c, uint32_t col_idx,
                                      const void* data, const uint32_t* null_bitmap,
                                      uint64_t nrows, void* errmsg);

// Filtered search variants. preds_json is a JSON predicate array;
// passing NULL or "" yields unfiltered behavior (delegates to the deletes-only
// fast path internally if any rows are tombstoned).
gpu_brute_force_search_result_c gpu_brute_force_search_with_filter(gpu_brute_force_c index_c,
                                                                    const void* queries_data,
                                                                    uint64_t num_queries, uint32_t query_dimension,
                                                                    uint32_t limit, const char* preds_json,
                                                                    void* errmsg);

gpu_brute_force_search_result_c gpu_brute_force_search_quantize_with_filter(gpu_brute_force_c index_c,
                                                                          const void* queries_data,
                                                                          uint64_t num_queries, uint32_t query_dimension,
                                                                          uint32_t limit, const char* preds_json,
                                                                          void* errmsg);

// Async variant of gpu_brute_force_search_quantize_with_filter. Returns a job_id
// that is collected with the existing gpu_brute_force_search_wait.
uint64_t gpu_brute_force_search_quantize_with_filter_async(gpu_brute_force_c index_c,
                                                         const void* queries_data,
                                                         uint64_t num_queries, uint32_t query_dimension,
                                                         uint32_t limit, const char* preds_json,
                                                         void* errmsg);

// Native-typed (T) async variant of gpu_brute_force_search_with_filter: the
// query stays in the index element type T (f32 or f16), no widening. Returns a
// job_id collected with gpu_brute_force_search_wait. Lets the filtered overflow
// stay native half.
uint64_t gpu_brute_force_search_with_filter_async(gpu_brute_force_c index_c,
                                                  const void* queries_data,
                                                  uint64_t num_queries, uint32_t query_dimension,
                                                  uint32_t limit, const char* preds_json,
                                                  void* errmsg);

// Returns the capacity of the index buffer
uint64_t gpu_brute_force_cap(gpu_brute_force_c index_c);

// Returns the current number of vectors in the index
uint64_t gpu_brute_force_len(gpu_brute_force_c index_c);

// Returns info about the index as a JSON string
char* gpu_brute_force_info(gpu_brute_force_c index_c, void* errmsg);

// Destroys the gpu_brute_force_t object and frees associated resources
void gpu_brute_force_destroy(gpu_brute_force_c index_c, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // BRUTE_FORCE_C_H
