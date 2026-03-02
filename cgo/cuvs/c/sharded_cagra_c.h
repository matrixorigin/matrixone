#ifndef SHARDED_CAGRA_C_H
#define SHARDED_CAGRA_C_H

#include "helper.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void* GpuShardedCagraIndexC;
typedef void* GpuShardedCagraSearchResultC;

// Constructor for building from dataset across multiple GPUs (Float32 specific)
GpuShardedCagraIndexC GpuShardedCagraIndex_New(const float* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                               CuvsDistanceTypeC metric, size_t intermediate_graph_degree, 
                                               size_t graph_degree, const int* devices, uint32_t num_devices, uint32_t nthread, void* errmsg);

// Constructor for building from dataset (Generic/Unsafe)
GpuShardedCagraIndexC GpuShardedCagraIndex_NewUnsafe(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                                     CuvsDistanceTypeC metric, size_t intermediate_graph_degree, 
                                                     size_t graph_degree, const int* devices, uint32_t num_devices, uint32_t nthread, CuvsQuantizationC qtype, void* errmsg);

// Constructor for loading from file (multi-GPU) (Float32 specific)
GpuShardedCagraIndexC GpuShardedCagraIndex_NewFromFile(const char* filename, uint32_t dimension, 
                                                       CuvsDistanceTypeC metric, 
                                                       const int* devices, uint32_t num_devices, uint32_t nthread, void* errmsg);

// Constructor for loading from file (Generic/Unsafe)
GpuShardedCagraIndexC GpuShardedCagraIndex_NewFromFileUnsafe(const char* filename, uint32_t dimension, 
                                                             CuvsDistanceTypeC metric, 
                                                             const int* devices, uint32_t num_devices, uint32_t nthread, CuvsQuantizationC qtype, void* errmsg);

void GpuShardedCagraIndex_Load(GpuShardedCagraIndexC index_c, void* errmsg);

void GpuShardedCagraIndex_Save(GpuShardedCagraIndexC index_c, const char* filename, void* errmsg);

// Performs search (Float32 specific)
GpuShardedCagraSearchResultC GpuShardedCagraIndex_Search(GpuShardedCagraIndexC index_c, const float* queries_data, 
                                                         uint64_t num_queries, uint32_t query_dimension, 
                                                         uint32_t limit, size_t itopk_size, void* errmsg);

// Performs search (Generic/Unsafe)
GpuShardedCagraSearchResultC GpuShardedCagraIndex_SearchUnsafe(GpuShardedCagraIndexC index_c, const void* queries_data, 
                                                               uint64_t num_queries, uint32_t query_dimension, 
                                                               uint32_t limit, size_t itopk_size, void* errmsg);

void GpuShardedCagraIndex_GetResults(GpuShardedCagraSearchResultC result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances);

void GpuShardedCagraIndex_FreeSearchResult(GpuShardedCagraSearchResultC result_c);

void GpuShardedCagraIndex_Destroy(GpuShardedCagraIndexC index_c, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // SHARDED_CAGRA_C_H
