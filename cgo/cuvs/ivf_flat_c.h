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

#ifndef IVF_FLAT_C_H
#define IVF_FLAT_C_H

#include "helper.h"
#include "cuvs_types.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque pointer to the C++ gpu_ivf_flat_t object
typedef void* gpu_ivf_flat_c;

// Opaque pointer to the C++ IVF-Flat search result object
typedef void* gpu_ivf_flat_result_c;

// Constructor for building from dataset
gpu_ivf_flat_c gpu_ivf_flat_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                 distance_type_t metric, ivf_flat_build_params_t build_params,
                                 const int* devices, int device_count, uint32_t nthread, 
                                 distribution_mode_t dist_mode, quantization_t qtype, void* errmsg);

// Constructor for loading from file
gpu_ivf_flat_c gpu_ivf_flat_load_file(const char* filename, uint32_t dimension, distance_type_t metric,
                                      ivf_flat_build_params_t build_params,
                                      const int* devices, int device_count, uint32_t nthread, 
                                      distribution_mode_t dist_mode, quantization_t qtype, void* errmsg);

// Destructor
void gpu_ivf_flat_destroy(gpu_ivf_flat_c index_c, void* errmsg);

// Start function (initializes worker and resources)
void gpu_ivf_flat_start(gpu_ivf_flat_c index_c, void* errmsg);

// Build function (actually triggers the build/load logic)
void gpu_ivf_flat_build(gpu_ivf_flat_c index_c, void* errmsg);

// Constructor for an empty index (pre-allocates)
gpu_ivf_flat_c gpu_ivf_flat_new_empty(uint64_t total_count, uint32_t dimension, distance_type_t metric, 
                                           ivf_flat_build_params_t build_params,
                                           const int* devices, int device_count, uint32_t nthread, 
                                           distribution_mode_t dist_mode, quantization_t qtype, void* errmsg);

// Add chunk of data (same type as index quantization)
void gpu_ivf_flat_add_chunk(gpu_ivf_flat_c index_c, const void* chunk_data, uint64_t chunk_count, void* errmsg);

// Extend an already-built index with new vectors (same type as index quantization)
// new_ids may be NULL to auto-assign sequential IDs starting from current index size
void gpu_ivf_flat_extend(gpu_ivf_flat_c index_c, const void* new_data, uint64_t n_rows,
                         const int64_t* new_ids, void* errmsg);

// Extend an already-built index with float32 vectors (quantized on-the-fly if needed)
void gpu_ivf_flat_extend_float(gpu_ivf_flat_c index_c, const float* new_data, uint64_t n_rows,
                               const int64_t* new_ids, void* errmsg);

// Add chunk of data (from float, with on-the-fly quantization if needed)
void gpu_ivf_flat_add_chunk_float(gpu_ivf_flat_c index_c, const float* chunk_data, uint64_t chunk_count, void* errmsg);

// Trains the scalar quantizer (if T is 1-byte)
void gpu_ivf_flat_train_quantizer(gpu_ivf_flat_c index_c, const float* train_data, uint64_t n_samples, void* errmsg);

void gpu_ivf_flat_set_per_thread_device(gpu_ivf_flat_c index_c, bool enable, void* errmsg);
void gpu_ivf_flat_set_use_batching(gpu_ivf_flat_c index_c, bool enable, void* errmsg);

void gpu_ivf_flat_set_quantizer(gpu_ivf_flat_c index_c, float min, float max, void* errmsg);
void gpu_ivf_flat_get_quantizer(gpu_ivf_flat_c index_c, float* min, float* max, void* errmsg);

// Destructor

void gpu_ivf_flat_save(gpu_ivf_flat_c index_c, const char* filename, void* errmsg);

// Save all components (index, IDs, quantizer, bitset) to a directory + manifest.json
void gpu_ivf_flat_save_dir(gpu_ivf_flat_c index_c, const char* dir, void* errmsg);

// Load all components from a directory previously written by gpu_ivf_flat_save_dir.
// The index must have been created and started before calling this.
void gpu_ivf_flat_load_dir(gpu_ivf_flat_c index_c, const char* dir, void* errmsg);

// Search function
typedef struct {
    gpu_ivf_flat_result_c result_ptr;
} gpu_ivf_flat_search_res_t;

gpu_ivf_flat_search_res_t gpu_ivf_flat_search(gpu_ivf_flat_c index_c, const void* queries_data, uint64_t num_queries, 
                                                uint32_t query_dimension, uint32_t limit, 
                                                ivf_flat_search_params_t search_params, void* errmsg);

gpu_ivf_flat_search_res_t gpu_ivf_flat_search_float(gpu_ivf_flat_c index_c, const float* queries_data, uint64_t num_queries, 
                                                      uint32_t query_dimension, uint32_t limit, 
                                                      ivf_flat_search_params_t search_params, void* errmsg);
// Get results from result object
void gpu_ivf_flat_get_neighbors(gpu_ivf_flat_result_c result_c, uint64_t total_elements, int64_t* neighbors);
void gpu_ivf_flat_get_distances(gpu_ivf_flat_result_c result_c, uint64_t total_elements, float* distances);

// Free result object
void gpu_ivf_flat_free_result(gpu_ivf_flat_result_c result_c);

// Returns the capacity of the index buffer
uint32_t gpu_ivf_flat_cap(gpu_ivf_flat_c index_c);

// Returns the current number of vectors in the index
uint32_t gpu_ivf_flat_len(gpu_ivf_flat_c index_c);

// Returns info about the index as a JSON string
char* gpu_ivf_flat_info(gpu_ivf_flat_c index_c, void* errmsg);

// Gets the trained centroids
void gpu_ivf_flat_get_centers(gpu_ivf_flat_c index_c, void* centers, void* errmsg);

// Gets the number of lists (centroids)
uint32_t gpu_ivf_flat_get_n_list(gpu_ivf_flat_c index_c);

#ifdef __cplusplus
}
#endif

#endif // IVF_FLAT_C_H
