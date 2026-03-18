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

#ifdef __cplusplus
extern "C" {
#endif

// Opaque pointer to the C++ gpu_brute_force_t object
typedef void* gpu_brute_force_c;

// Opaque pointer to the C++ search result object
typedef void* gpu_brute_force_search_result_c;

// Constructor for gpu_brute_force_t
gpu_brute_force_c gpu_brute_force_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, distance_type_t metric, uint32_t nthread, int device_id, quantization_t qtype, void* errmsg);

// Constructor for an empty index (pre-allocates)
gpu_brute_force_c gpu_brute_force_new_empty(uint64_t total_count, uint32_t dimension, distance_type_t metric, uint32_t nthread, int device_id, quantization_t qtype, void* errmsg);

// Starts the worker and initializes resources
void gpu_brute_force_start(gpu_brute_force_c index_c, void* errmsg);

// Builds the index (loads the dataset to the GPU)
void gpu_brute_force_build(gpu_brute_force_c index_c, void* errmsg);

// Add chunk of data (same type as index quantization)
void gpu_brute_force_add_chunk(gpu_brute_force_c index_c, const void* chunk_data, uint64_t chunk_count, void* errmsg);

// Add chunk of data (from float, with on-the-fly conversion if needed)
void gpu_brute_force_add_chunk_float(gpu_brute_force_c index_c, const float* chunk_data, uint64_t chunk_count, void* errmsg);

// Performs a search operation
gpu_brute_force_search_result_c gpu_brute_force_search(gpu_brute_force_c index_c, const void* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, void* errmsg);

// Performs a search operation with float32 queries
gpu_brute_force_search_result_c gpu_brute_force_search_float(gpu_brute_force_c index_c, const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, void* errmsg);

// Retrieves the results from a search operation
void gpu_brute_force_get_results(gpu_brute_force_search_result_c result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances);

// Frees the memory for a gpu_brute_force_search_result_c object
void gpu_brute_force_free_search_result(gpu_brute_force_search_result_c result_c);

// Returns the capacity of the index buffer
uint32_t gpu_brute_force_cap(gpu_brute_force_c index_c);

// Returns the current number of vectors in the index
uint32_t gpu_brute_force_len(gpu_brute_force_c index_c);

// Prints info about the index
void gpu_brute_force_info(gpu_brute_force_c index_c, void* errmsg);

// Destroys the gpu_brute_force_t object and frees associated resources
void gpu_brute_force_destroy(gpu_brute_force_c index_c, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // BRUTE_FORCE_C_H
