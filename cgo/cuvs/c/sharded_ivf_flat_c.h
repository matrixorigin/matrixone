#ifndef SHARDED_IVF_FLAT_C_H
#define SHARDED_IVF_FLAT_C_H

#include "helper.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef void* GpuShardedIvfFlatIndexC;
typedef void* GpuShardedIvfFlatSearchResultC;

// Constructor for building from dataset across multiple GPUs (Float32 specific)
GpuShardedIvfFlatIndexC GpuShardedIvfFlatIndex_New(const float* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                                  CuvsDistanceTypeC metric, uint32_t n_list, 
                                                  const int* devices, uint32_t num_devices, uint32_t nthread, void* errmsg);

// Constructor for building from dataset (Generic/Unsafe)
GpuShardedIvfFlatIndexC GpuShardedIvfFlatIndex_NewUnsafe(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                                        CuvsDistanceTypeC metric, uint32_t n_list, 
                                                        const int* devices, uint32_t num_devices, uint32_t nthread, CuvsQuantizationC qtype, void* errmsg);

// Constructor for loading from file (multi-GPU) (Float32 specific)
GpuShardedIvfFlatIndexC GpuShardedIvfFlatIndex_NewFromFile(const char* filename, uint32_t dimension, 
                                                          CuvsDistanceTypeC metric, 
                                                          const int* devices, uint32_t num_devices, uint32_t nthread, void* errmsg);

// Constructor for loading from file (Generic/Unsafe)
GpuShardedIvfFlatIndexC GpuShardedIvfFlatIndex_NewFromFileUnsafe(const char* filename, uint32_t dimension, 
                                                                CuvsDistanceTypeC metric, 
                                                                const int* devices, uint32_t num_devices, uint32_t nthread, CuvsQuantizationC qtype, void* errmsg);

void GpuShardedIvfFlatIndex_Load(GpuShardedIvfFlatIndexC index_c, void* errmsg);

void GpuShardedIvfFlatIndex_Save(GpuShardedIvfFlatIndexC index_c, const char* filename, void* errmsg);

// Performs search (Float32 specific)
GpuShardedIvfFlatSearchResultC GpuShardedIvfFlatIndex_Search(GpuShardedIvfFlatIndexC index_c, const float* queries_data, 
                                                            uint64_t num_queries, uint32_t query_dimension, 
                                                            uint32_t limit, uint32_t n_probes, void* errmsg);

// Performs search (Generic/Unsafe)
GpuShardedIvfFlatSearchResultC GpuShardedIvfFlatIndex_SearchUnsafe(GpuShardedIvfFlatIndexC index_c, const void* queries_data, 
                                                                  uint64_t num_queries, uint32_t query_dimension, 
                                                                  uint32_t limit, uint32_t n_probes, void* errmsg);

void GpuShardedIvfFlatIndex_GetResults(GpuShardedIvfFlatSearchResultC result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances);

void GpuShardedIvfFlatIndex_FreeSearchResult(GpuShardedIvfFlatSearchResultC result_c);

void GpuShardedIvfFlatIndex_Destroy(GpuShardedIvfFlatIndexC index_c, void* errmsg);

void GpuShardedIvfFlatIndex_GetCenters(GpuShardedIvfFlatIndexC index_c, float* centers, void* errmsg);

uint32_t GpuShardedIvfFlatIndex_GetNList(GpuShardedIvfFlatIndexC index_c);

#ifdef __cplusplus
}
#endif

#endif // SHARDED_IVF_FLAT_C_H
