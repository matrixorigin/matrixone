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

#ifndef IVF_PQ_C_H
#define IVF_PQ_C_H

#include "helper.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque pointer to the C++ gpu_ivf_pq_t object
typedef void* gpu_ivf_pq_c;

// Opaque pointer to the C++ IVF-PQ search result object
typedef void* gpu_ivf_pq_result_c;

// Constructor for building from dataset
gpu_ivf_pq_c gpu_ivf_pq_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                 distance_type_t metric, ivf_pq_build_params_t build_params,
                                 const int* devices, int device_count, uint32_t nthread, 
                                 distribution_mode_t dist_mode, quantization_t qtype, void* errmsg);

// Constructor for building from MODF datafile
gpu_ivf_pq_c gpu_ivf_pq_new_from_data_file(const char* data_filename, distance_type_t metric, 
                                                ivf_pq_build_params_t build_params,
                                                const int* devices, int device_count, uint32_t nthread, 
                                                distribution_mode_t dist_mode, quantization_t qtype, void* errmsg);

// Constructor for loading from file
gpu_ivf_pq_c gpu_ivf_pq_load_file(const char* filename, uint32_t dimension, distance_type_t metric,
                                      ivf_pq_build_params_t build_params,
                                      const int* devices, int device_count, uint32_t nthread, 
                                      distribution_mode_t dist_mode, quantization_t qtype, void* errmsg);

// Constructor for an empty index (pre-allocates)
gpu_ivf_pq_c gpu_ivf_pq_new_empty(uint64_t total_count, uint32_t dimension, distance_type_t metric, 
                                       ivf_pq_build_params_t build_params,
                                       const int* devices, int device_count, uint32_t nthread, 
                                       distribution_mode_t dist_mode, quantization_t qtype, void* errmsg);

// Add chunk of data (same type as index quantization)
void gpu_ivf_pq_add_chunk(gpu_ivf_pq_c index_c, const void* chunk_data, uint64_t chunk_count, uint64_t row_offset, void* errmsg);

// Add chunk of data (from float, with on-the-fly quantization if needed)
void gpu_ivf_pq_add_chunk_float(gpu_ivf_pq_c index_c, const float* chunk_data, uint64_t chunk_count, uint64_t row_offset, void* errmsg);

// Destructor
void gpu_ivf_pq_destroy(gpu_ivf_pq_c index_c, void* errmsg);

// Start function (initializes worker and resources)
void gpu_ivf_pq_start(gpu_ivf_pq_c index_c, void* errmsg);

// Load function (actually triggers the build/load logic)
void gpu_ivf_pq_load(gpu_ivf_pq_c index_c, void* errmsg);

// Save function
void gpu_ivf_pq_save(gpu_ivf_pq_c index_c, const char* filename, void* errmsg);

// Search function
typedef struct {
    gpu_ivf_pq_result_c result_ptr;
} gpu_ivf_pq_search_res_t;

gpu_ivf_pq_search_res_t gpu_ivf_pq_search(gpu_ivf_pq_c index_c, const void* queries_data, uint64_t num_queries, 
                                              uint32_t query_dimension, uint32_t limit, 
                                              ivf_pq_search_params_t search_params, void* errmsg);

// Get results from result object
void gpu_ivf_pq_get_neighbors(gpu_ivf_pq_result_c result_c, uint64_t total_elements, int64_t* neighbors);
void gpu_ivf_pq_get_distances(gpu_ivf_pq_result_c result_c, uint64_t total_elements, float* distances);

// Free result object
void gpu_ivf_pq_free_result(gpu_ivf_pq_result_c result_c);

// Gets the trained centroids
void gpu_ivf_pq_get_centers(gpu_ivf_pq_c index_c, float* centers, void* errmsg);

// Gets the number of lists (centroids)
uint32_t gpu_ivf_pq_get_n_list(gpu_ivf_pq_c index_c);

// Gets the dimension of the index
uint32_t gpu_ivf_pq_get_dim(gpu_ivf_pq_c index_c);

// Gets the rotated dimension of the index (dimension used for centers)
uint32_t gpu_ivf_pq_get_rot_dim(gpu_ivf_pq_c index_c);

// Gets the extended dimension of the index (including norms and padding)
uint32_t gpu_ivf_pq_get_dim_ext(gpu_ivf_pq_c index_c);

// Gets the flattened dataset (for debugging)
void gpu_ivf_pq_get_dataset(gpu_ivf_pq_c index_c, void* out_data);

#ifdef __cplusplus
}
#endif

#endif // IVF_PQ_C_H
