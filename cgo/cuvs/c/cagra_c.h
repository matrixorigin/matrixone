#ifndef CAGRA_C_H
#define CAGRA_C_H

#include "helper.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* gpu_cagra_index_c;
typedef void* gpu_cagra_search_result_c;

// Constructor for building from dataset. 
// devices: pointer to array of device IDs. If num_devices > 1, sharded mode is used.
gpu_cagra_index_c gpu_cagra_index_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                 distance_type_t metric, size_t intermediate_graph_degree, 
                                 size_t graph_degree, const int* devices, uint32_t num_devices, uint32_t nthread, quantization_t qtype, bool force_mg, void* errmsg);

// Constructor for loading from file
gpu_cagra_index_c gpu_cagra_index_new_from_file(const char* filename, uint32_t dimension, distance_type_t metric, 
                                         const int* devices, uint32_t num_devices, uint32_t nthread, quantization_t qtype, bool force_mg, void* errmsg);

void gpu_cagra_index_load(gpu_cagra_index_c index_c, void* errmsg);

void gpu_cagra_index_save(gpu_cagra_index_c index_c, const char* filename, void* errmsg);

// Performs search
gpu_cagra_search_result_c gpu_cagra_index_search(gpu_cagra_index_c index_c, const void* queries_data, 
                                           uint64_t num_queries, uint32_t query_dimension, 
                                           uint32_t limit, size_t itopk_size, void* errmsg);

// Retrieves the results from a search operation (converts uint32_t neighbors to int64_t)
void gpu_cagra_index_get_results(gpu_cagra_search_result_c result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances);

void gpu_cagra_index_free_search_result(gpu_cagra_search_result_c result_c);

void gpu_cagra_index_destroy(gpu_cagra_index_c index_c, void* errmsg);

// Extends the index with new vectors (only supported for single-GPU)
void gpu_cagra_index_extend(gpu_cagra_index_c index_c, const void* additional_data, uint64_t num_vectors, void* errmsg);

// Merges multiple single-GPU indices into one
gpu_cagra_index_c gpu_cagra_index_merge(gpu_cagra_index_c* indices, uint32_t num_indices, uint32_t nthread, const int* devices, uint32_t num_devices, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // CAGRA_C_H
