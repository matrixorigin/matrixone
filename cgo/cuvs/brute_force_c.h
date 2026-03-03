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

// Loads the index to the GPU
void gpu_brute_force_load(gpu_brute_force_c index_c, void* errmsg);

// Performs a search operation
gpu_brute_force_search_result_c gpu_brute_force_search(gpu_brute_force_c index_c, const void* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, void* errmsg);

// Retrieves the results from a search operation
void gpu_brute_force_get_results(gpu_brute_force_search_result_c result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances);

// Frees the memory for a gpu_brute_force_search_result_c object
void gpu_brute_force_free_search_result(gpu_brute_force_search_result_c result_c);

// Destroys the gpu_brute_force_t object and frees associated resources
void gpu_brute_force_destroy(gpu_brute_force_c index_c, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // BRUTE_FORCE_C_H
