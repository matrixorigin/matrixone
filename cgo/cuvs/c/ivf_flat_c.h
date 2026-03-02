#ifndef IVF_FLAT_C_H
#define IVF_FLAT_C_H

#include "helper.h"

#ifdef __cplusplus
extern "C" {
#endif

// Opaque pointer to the C++ gpu_ivf_flat_index_t object
typedef void* gpu_ivf_flat_index_c;

// Opaque pointer to the C++ IVF search result object
typedef void* gpu_ivf_flat_search_result_c;

// Constructor for building from dataset
gpu_ivf_flat_index_c gpu_ivf_flat_index_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, distance_type_t metric, uint32_t n_list, uint32_t nthread, int device_id, quantization_t qtype, void* errmsg);

// Constructor for loading from file
gpu_ivf_flat_index_c gpu_ivf_flat_index_new_from_file(const char* filename, uint32_t dimension, distance_type_t metric, uint32_t nthread, int device_id, quantization_t qtype, void* errmsg);

// Loads the index to the GPU (either builds or loads from file depending on constructor)
void gpu_ivf_flat_index_load(gpu_ivf_flat_index_c index_c, void* errmsg);

// Saves the index to file
void gpu_ivf_flat_index_save(gpu_ivf_flat_index_c index_c, const char* filename, void* errmsg);

// Performs a search operation
gpu_ivf_flat_search_result_c gpu_ivf_flat_index_search(gpu_ivf_flat_index_c index_c, const void* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, uint32_t n_probes, void* errmsg);

// Retrieves the results from a search operation
void gpu_ivf_flat_index_get_results(gpu_ivf_flat_search_result_c result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances);

// Frees the memory for a gpu_ivf_flat_search_result_c object
void gpu_ivf_flat_index_free_search_result(gpu_ivf_flat_search_result_c result_c);

// Destroys the gpu_ivf_flat_index_t object
void gpu_ivf_flat_index_destroy(gpu_ivf_flat_index_c index_c, void* errmsg);

// Gets the centroids after build
// centers: Pre-allocated array of size n_list * dimension
void gpu_ivf_flat_index_get_centers(gpu_ivf_flat_index_c index_c, float* centers, void* errmsg);

// Gets the number of lists (centroids)
uint32_t gpu_ivf_flat_index_get_n_list(gpu_ivf_flat_index_c index_c);

#ifdef __cplusplus
}
#endif

#endif // IVF_FLAT_C_H
