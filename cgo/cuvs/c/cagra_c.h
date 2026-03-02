#ifndef CAGRA_C_H
#define CAGRA_C_H

#include "brute_force_c.h" // Reuse shared definitions

#ifdef __cplusplus
extern "C" {
#endif

typedef void* GpuCagraIndexC;
typedef void* GpuCagraSearchResultC;

// Constructor for building from dataset
GpuCagraIndexC GpuCagraIndex_New(const float* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                                 CuvsDistanceTypeC metric, size_t intermediate_graph_degree, 
                                 size_t graph_degree, uint32_t nthread, int device_id, void* errmsg);

// Constructor for loading from file
GpuCagraIndexC GpuCagraIndex_NewFromFile(const char* filename, uint32_t dimension, CuvsDistanceTypeC metric, 
                                         uint32_t nthread, int device_id, void* errmsg);

void GpuCagraIndex_Load(GpuCagraIndexC index_c, void* errmsg);

void GpuCagraIndex_Save(GpuCagraIndexC index_c, const char* filename, void* errmsg);

GpuCagraSearchResultC GpuCagraIndex_Search(GpuCagraIndexC index_c, const float* queries_data, 
                                           uint64_t num_queries, uint32_t query_dimension, 
                                           uint32_t limit, size_t itopk_size, void* errmsg);

// Retrieves the results from a search operation (converts uint32_t neighbors to int64_t)
void GpuCagraIndex_GetResults(GpuCagraSearchResultC result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances);

void GpuCagraIndex_FreeSearchResult(GpuCagraSearchResultC result_c);

void GpuCagraIndex_Destroy(GpuCagraIndexC index_c, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // CAGRA_C_H
