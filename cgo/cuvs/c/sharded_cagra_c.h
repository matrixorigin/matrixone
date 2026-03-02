#ifndef SHARDED_CAGRA_C_H
#define SHARDED_CAGRA_C_H

#include "helper.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void* gpu_sharded_cagra_index_c;
typedef void* gpu_sharded_cagra_search_result_c;

// Constructor for building from dataset across multiple GPUs
gpu_sharded_cagra_index_c gpu_sharded_cagra_index_new(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                               distance_type_t metric, size_t intermediate_graph_degree, 
                                               size_t graph_degree, const int* devices, uint32_t num_devices, uint32_t nthread, quantization_t qtype, void* errmsg);

// Constructor for loading from file (multi-GPU)
gpu_sharded_cagra_index_c gpu_sharded_cagra_index_new_from_file(const char* filename, uint32_t dimension, 
                                                       distance_type_t metric, 
                                                       const int* devices, uint32_t num_devices, uint32_t nthread, quantization_t qtype, void* errmsg);

void gpu_sharded_cagra_index_load(gpu_sharded_cagra_index_c index_c, void* errmsg);

void gpu_sharded_cagra_index_save(gpu_sharded_cagra_index_c index_c, const char* filename, void* errmsg);

// Performs search
gpu_sharded_cagra_search_result_c gpu_sharded_cagra_index_search(gpu_sharded_cagra_index_c index_c, const void* queries_data, 
                                                         uint64_t num_queries, uint32_t query_dimension, 
                                                         uint32_t limit, size_t itopk_size, void* errmsg);

void gpu_sharded_cagra_index_get_results(gpu_sharded_cagra_search_result_c result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances);

void gpu_sharded_cagra_index_free_search_result(gpu_sharded_cagra_search_result_c result_c);

void gpu_sharded_cagra_index_destroy(gpu_sharded_cagra_index_c index_c, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // SHARDED_CAGRA_C_H
