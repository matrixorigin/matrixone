#ifndef BRUTE_FORCE_C_H
#define BRUTE_FORCE_C_H

#include "helper.h"

#ifdef __cplusplus
extern "C" {
#endif

// Opaque pointer to the C++ GpuBruteForceIndex object
typedef void* GpuBruteForceIndexC;

// Opaque pointer to the C++ search result object
typedef void* GpuBruteForceSearchResultC;

// Constructor for GpuBruteForceIndex
GpuBruteForceIndexC GpuBruteForceIndex_New(const void* dataset_data, uint64_t count_vectors, uint32_t dimension, CuvsDistanceTypeC metric, uint32_t nthread, int device_id, CuvsQuantizationC qtype, void* errmsg);

// Loads the index to the GPU
void GpuBruteForceIndex_Load(GpuBruteForceIndexC index_c, void* errmsg);

// Performs a search operation
GpuBruteForceSearchResultC GpuBruteForceIndex_Search(GpuBruteForceIndexC index_c, const void* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, void* errmsg);

// Retrieves the results from a search operation
void GpuBruteForceIndex_GetResults(GpuBruteForceSearchResultC result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances);

// Frees the memory for a GpuBruteForceSearchResultC object
void GpuBruteForceIndex_FreeSearchResult(GpuBruteForceSearchResultC result_c);

// Destroys the GpuBruteForceIndex object and frees associated resources
void GpuBruteForceIndex_Destroy(GpuBruteForceIndexC index_c, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // BRUTE_FORCE_C_H
