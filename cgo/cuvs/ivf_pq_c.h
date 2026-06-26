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
#include "cuvs_types.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque pointer to the C++ gpu_ivf_pq_t object
typedef void* gpu_ivf_pq_c;

// Opaque pointer to the C++ IVF-PQ search result object
typedef void* gpu_ivf_pq_result_c;

// btype = base/query/quantizer-source element type (Quantization_F32 or F16).
// qtype = storage element type. Wired combos: F32 base {F32,F16,INT8,UINT8};
// F16 base {F16,INT8,UINT8}. Other combinations set errmsg and return NULL.

// Constructor for building from dataset
gpu_ivf_pq_c gpu_ivf_pq_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension,
                                 distance_type_t metric, ivf_pq_build_params_t build_params,
                                 const int* devices, int device_count, uint32_t nthread,
                                 distribution_mode_t dist_mode, quantization_t btype, quantization_t qtype,
                                 const int64_t* ids, void* errmsg);

// Constructor for building from MODF datafile
gpu_ivf_pq_c gpu_ivf_pq_new_from_data_file(const char* data_filename, distance_type_t metric,
                                                ivf_pq_build_params_t build_params,
                                                const int* devices, int device_count, uint32_t nthread,
                                                distribution_mode_t dist_mode, quantization_t btype, quantization_t qtype, void* errmsg);

// Constructor for loading from file
gpu_ivf_pq_c gpu_ivf_pq_load_file(const char* filename, uint32_t dimension, distance_type_t metric,
                                      ivf_pq_build_params_t build_params,
                                      const int* devices, int device_count, uint32_t nthread,
                                      distribution_mode_t dist_mode, quantization_t btype, quantization_t qtype, void* errmsg);

// Constructor for an empty index (pre-allocates)
gpu_ivf_pq_c gpu_ivf_pq_new_empty(uint64_t total_count, uint32_t dimension, distance_type_t metric,
                                       ivf_pq_build_params_t build_params,
                                       const int* devices, int device_count, uint32_t nthread,
                                       distribution_mode_t dist_mode, quantization_t btype, quantization_t qtype,
                                       const int64_t* ids, void* errmsg);

// Add chunk of data (same type as index quantization)
void gpu_ivf_pq_add_chunk(gpu_ivf_pq_c index_c, const void* chunk_data, uint64_t chunk_count, const int64_t* ids, void* errmsg);

// Extend an already-built index with new vectors (same type as index quantization)
// new_ids may be NULL to auto-assign sequential IDs starting from current index size
void gpu_ivf_pq_extend(gpu_ivf_pq_c index_c, const void* new_data, uint64_t n_rows,
                       const int64_t* new_ids, void* errmsg);

// Extend an already-built index with float32 vectors (quantized on-the-fly if needed)
void gpu_ivf_pq_extend_float(gpu_ivf_pq_c index_c, const float* new_data, uint64_t n_rows,
                             const int64_t* new_ids, void* errmsg);

// Add chunk of data (from float, with on-the-fly quantization if needed)
void gpu_ivf_pq_add_chunk_float(gpu_ivf_pq_c index_c, const float* chunk_data, uint64_t chunk_count, const int64_t* ids, void* errmsg);

// Add chunk of base-typed (B) data, quantizing natively to a 1-byte storage type
// (int8/uint8) via the B-source quantizer. base_data is a host buffer of
// chunk_count*dimension B elements (passed as raw bytes; B = btype). Requires int8/uint8 storage.
void gpu_ivf_pq_add_chunk_quantize(gpu_ivf_pq_c index_c, const void* base_data, uint64_t chunk_count, const int64_t* ids, void* errmsg);

// Quantize a base-typed (B) query to the 1-byte storage type via the B-source
// quantizer, writing num_queries*dimension bytes into out. The caller then runs
// the normal native search with the quantized query. Requires int8/uint8 storage.
void gpu_ivf_pq_quantize_query(gpu_ivf_pq_c index_c, const void* base_data, uint64_t num_queries, void* out, void* errmsg);

// Trains the scalar quantizer (if storage is 1-byte). train_data is a host buffer
// of n_samples*dimension B elements (B = btype), passed as raw bytes.
void gpu_ivf_pq_train_quantizer(gpu_ivf_pq_c index_c, const void* train_data, uint64_t n_samples, void* errmsg);

void gpu_ivf_pq_set_batch_window(gpu_ivf_pq_c index_c, int64_t window_us, void* errmsg);
void gpu_ivf_pq_set_dynb_conservative_dispatch(gpu_ivf_pq_c index_c, bool enable, void* errmsg);

void gpu_ivf_pq_set_quantizer(gpu_ivf_pq_c index_c, float min, float max, void* errmsg);
void gpu_ivf_pq_get_quantizer(gpu_ivf_pq_c index_c, float* min, float* max, void* errmsg);

// Destructor
void gpu_ivf_pq_destroy(gpu_ivf_pq_c index_c, void* errmsg);

// Start function (initializes worker and resources)
void gpu_ivf_pq_start(gpu_ivf_pq_c index_c, void* errmsg);

// Build function (actually triggers the build/load logic)
void gpu_ivf_pq_build(gpu_ivf_pq_c index_c, void* errmsg);

// Save function
void gpu_ivf_pq_save(gpu_ivf_pq_c index_c, const char* filename, void* errmsg);

// Save all components (index, IDs, quantizer, bitset) to a directory + manifest.json
void gpu_ivf_pq_save_dir(gpu_ivf_pq_c index_c, const char* dir, void* errmsg);

// Delete ID from index (soft delete via bitset)
void gpu_ivf_pq_delete_id(gpu_ivf_pq_c index_c, int64_t id, void* errmsg);

// Load all components from a directory previously written by gpu_ivf_pq_save_dir.
// target_mode overrides the distribution mode at load time (e.g. load a SINGLE_GPU
// .tar as REPLICATED to broadcast the index to all GPUs).
// The index must have been created and started before calling this.
void gpu_ivf_pq_load_dir(gpu_ivf_pq_c index_c, const char* dir,
                          distribution_mode_t target_mode, void* errmsg);

// Search function
typedef struct {
    gpu_ivf_pq_result_c result_ptr;
} gpu_ivf_pq_search_res_t;

gpu_ivf_pq_search_res_t gpu_ivf_pq_search(gpu_ivf_pq_c index_c, const void* queries_data, uint64_t num_queries, 
                                              uint32_t query_dimension, uint32_t limit, 
                                              ivf_pq_search_params_t search_params, void* errmsg);

// Quantize search: query in the BASE element type B (float or half); the index
// converts it to storage type T (copy / quantize / f32->f16 cast) internally.
gpu_ivf_pq_search_res_t gpu_ivf_pq_search_quantize(gpu_ivf_pq_c index_c, const void* queries_data, uint64_t num_queries,
                                                   uint32_t query_dimension, uint32_t limit,
                                                   ivf_pq_search_params_t search_params, void* errmsg);

// Asynchronous search functions
uint64_t gpu_ivf_pq_search_async(gpu_ivf_pq_c index_c, const void* queries_data, uint64_t num_queries,
                                    uint32_t query_dimension, uint32_t limit,
                                    ivf_pq_search_params_t search_params, void* errmsg);

uint64_t gpu_ivf_pq_search_quantize_async(gpu_ivf_pq_c index_c, const void* queries_data, uint64_t num_queries,
                                          uint32_t query_dimension, uint32_t limit,
                                          ivf_pq_search_params_t search_params, void* errmsg);

gpu_ivf_pq_search_res_t gpu_ivf_pq_search_wait(gpu_ivf_pq_c index_c, uint64_t job_id, void* errmsg);

// Get results from result object

void gpu_ivf_pq_get_neighbors(gpu_ivf_pq_result_c result_c, uint64_t total_elements, int64_t* neighbors);
void gpu_ivf_pq_get_distances(gpu_ivf_pq_result_c result_c, uint64_t total_elements, float* distances);

// Free result object
void gpu_ivf_pq_free_result(gpu_ivf_pq_result_c result_c);

// Returns the capacity of the index buffer
uint64_t gpu_ivf_pq_cap(gpu_ivf_pq_c index_c);

// Returns the current number of vectors in the index
uint64_t gpu_ivf_pq_len(gpu_ivf_pq_c index_c);

// Returns info about the index as a JSON string
char* gpu_ivf_pq_info(gpu_ivf_pq_c index_c, void* errmsg);

// Returns a heap-allocated NUL-terminated JSON string of the index's
// INCLUDE column metadata in the same shape gpu_ivf_pq_set_filter_columns
// consumes. Returns "" for indexes built without INCLUDE columns. Caller
// frees with free().
char* gpu_ivf_pq_get_filter_col_meta_json(gpu_ivf_pq_c index_c, void* errmsg);

// Gets the trained centroids. `count` is the number of elements the caller's
// `centers` buffer can hold; the copy is clamped to it (the index has
// n_lists * rot_dim center coords — size it with gpu_ivf_pq_get_n_list /
// gpu_ivf_pq_get_rot_dim).
void gpu_ivf_pq_get_centers(gpu_ivf_pq_c index_c, void* centers, uint64_t count, void* errmsg);

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

// ---------- Pre-filter (INCLUDE columns) ----------
// See cagra_c.h for JSON format details.
void gpu_ivf_pq_set_filter_columns(gpu_ivf_pq_c index_c, const char* col_meta_json,
                                    uint64_t total_count, void* errmsg);

// null_bitmap: LSB-first bits where 1 = row is NULL; NULL pointer = dense.
void gpu_ivf_pq_add_filter_chunk(gpu_ivf_pq_c index_c, uint32_t col_idx,
                                  const void* data, const uint32_t* null_bitmap,
                                  uint64_t nrows, void* errmsg);

gpu_ivf_pq_search_res_t gpu_ivf_pq_search_with_filter(gpu_ivf_pq_c index_c, const void* queries_data,
                                                       uint64_t num_queries, uint32_t query_dimension,
                                                       uint32_t limit, ivf_pq_search_params_t search_params,
                                                       const char* preds_json, void* errmsg);

// Query in the BASE element type B (float or half); converted to storage T internally.
gpu_ivf_pq_search_res_t gpu_ivf_pq_search_quantize_with_filter(gpu_ivf_pq_c index_c, const void* queries_data,
                                                             uint64_t num_queries, uint32_t query_dimension,
                                                             uint32_t limit, ivf_pq_search_params_t search_params,
                                                             const char* preds_json, void* errmsg);

// Async variant of gpu_ivf_pq_search_quantize_with_filter. Returns a job_id that
// is collected with the existing gpu_ivf_pq_search_wait. Lets multi-index
// callers fan out filtered searches across shards in parallel.
uint64_t gpu_ivf_pq_search_quantize_with_filter_async(gpu_ivf_pq_c index_c, const void* queries_data,
                                                    uint64_t num_queries, uint32_t query_dimension,
                                                    uint32_t limit, ivf_pq_search_params_t search_params,
                                                    const char* preds_json, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // IVF_PQ_C_H
