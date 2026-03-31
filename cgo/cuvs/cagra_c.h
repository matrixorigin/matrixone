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

#ifndef CAGRA_C_H
#define CAGRA_C_H

#include "helper.h"
#include "cuvs_types.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque pointer to the C++ gpu_cagra_t object
typedef void* gpu_cagra_c;

// Opaque pointer to the C++ CAGRA search result object
typedef void* gpu_cagra_result_c;

// Constructor for building from dataset
gpu_cagra_c gpu_cagra_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                            distance_type_t metric, cagra_build_params_t build_params,
                            const int* devices, int device_count, uint32_t nthread, 
                            distribution_mode_t dist_mode, quantization_t qtype, 
                            const uint32_t* ids, void* errmsg);

// Constructor for loading from file
gpu_cagra_c gpu_cagra_load_file(const char* filename, uint32_t dimension, distance_type_t metric,
                                 cagra_build_params_t build_params,
                                 const int* devices, int device_count, uint32_t nthread, 
                                 distribution_mode_t dist_mode, quantization_t qtype, void* errmsg);

// Destructor
void gpu_cagra_destroy(gpu_cagra_c index_c, void* errmsg);

// Start function (initializes worker and resources)
void gpu_cagra_start(gpu_cagra_c index_c, void* errmsg);

// Build function (actually triggers the build/load logic)
void gpu_cagra_build(gpu_cagra_c index_c, void* errmsg);

// Constructor for an empty index (pre-allocates)
gpu_cagra_c gpu_cagra_new_empty(uint64_t total_count, uint32_t dimension, distance_type_t metric, 
                                     cagra_build_params_t build_params,
                                     const int* devices, int device_count, uint32_t nthread, 
                                     distribution_mode_t dist_mode, quantization_t qtype, 
                                     const uint32_t* ids, void* errmsg);

// Add chunk of data (same type as index quantization)
void gpu_cagra_add_chunk(gpu_cagra_c index_c, const void* chunk_data, uint64_t chunk_count, void* errmsg);

// Add chunk of data (from float, with on-the-fly quantization if needed)
void gpu_cagra_add_chunk_float(gpu_cagra_c index_c, const float* chunk_data, uint64_t chunk_count, void* errmsg);

// Trains the scalar quantizer (if T is 1-byte)
void gpu_cagra_train_quantizer(gpu_cagra_c index_c, const float* train_data, uint64_t n_samples, void* errmsg);

void gpu_cagra_set_per_thread_device(gpu_cagra_c index_c, bool enable, void* errmsg);
void gpu_cagra_set_use_batching(gpu_cagra_c index_c, bool enable, void* errmsg);

void gpu_cagra_set_quantizer(gpu_cagra_c index_c, float min, float max, void* errmsg);
void gpu_cagra_get_quantizer(gpu_cagra_c index_c, float* min, float* max, void* errmsg);

// Destructor


void gpu_cagra_save(gpu_cagra_c index_c, const char* filename, void* errmsg);

// Save all components (index, IDs, quantizer, bitset) to a directory + manifest.json
void gpu_cagra_save_dir(gpu_cagra_c index_c, const char* dir, void* errmsg);

// Delete ID from index (soft delete via bitset)
void gpu_cagra_delete_id(gpu_cagra_c index_c, uint32_t id, void* errmsg);

// Load all components from a directory previously written by gpu_cagra_save_dir.
// The index must have been created (e.g. via gpu_cagra_new_empty) and started before calling this.
void gpu_cagra_load_dir(gpu_cagra_c index_c, const char* dir, void* errmsg);

// Search function
typedef struct {
    gpu_cagra_result_c result_ptr;
} gpu_cagra_search_res_t;

gpu_cagra_search_res_t gpu_cagra_search(gpu_cagra_c index_c, const void* queries_data, uint64_t num_queries, 
                                            uint32_t query_dimension, uint32_t limit, 
                                            cagra_search_params_t search_params, void* errmsg);

gpu_cagra_search_res_t gpu_cagra_search_float(gpu_cagra_c index_c, const float* queries_data, uint64_t num_queries, 
                                                uint32_t query_dimension, uint32_t limit, 
                                                cagra_search_params_t search_params, void* errmsg);

// Asynchronous search functions
uint64_t gpu_cagra_search_async(gpu_cagra_c index_c, const void* queries_data, uint64_t num_queries, 
                                   uint32_t query_dimension, uint32_t limit, 
                                   cagra_search_params_t search_params, void* errmsg);

uint64_t gpu_cagra_search_float_async(gpu_cagra_c index_c, const float* queries_data, uint64_t num_queries, 
                                         uint32_t query_dimension, uint32_t limit, 
                                         cagra_search_params_t search_params, void* errmsg);

gpu_cagra_search_res_t gpu_cagra_search_wait(gpu_cagra_c index_c, uint64_t job_id, void* errmsg);

// Get results from result object

void gpu_cagra_get_neighbors(gpu_cagra_result_c result_c, uint64_t total_elements, uint32_t* neighbors);
void gpu_cagra_get_distances(gpu_cagra_result_c result_c, uint64_t total_elements, float* distances);

// Free result object
void gpu_cagra_free_result(gpu_cagra_result_c result_c);

// Returns the capacity of the index buffer
uint32_t gpu_cagra_cap(gpu_cagra_c index_c);

// Returns the current number of vectors in the index
uint32_t gpu_cagra_len(gpu_cagra_c index_c);

// Returns info about the index as a JSON string
char* gpu_cagra_info(gpu_cagra_c index_c, void* errmsg);

// Extend function
// new_ids may be NULL to auto-assign sequential IDs starting from current index size
void gpu_cagra_extend(gpu_cagra_c index_c, const void* additional_data, uint64_t num_vectors,
                      const uint32_t* new_ids, void* errmsg);

// Merge function
gpu_cagra_c gpu_cagra_merge(gpu_cagra_c* indices_c, int num_indices, uint32_t nthread, const int* devices, int device_count, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // CAGRA_C_H
