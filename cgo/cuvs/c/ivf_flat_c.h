#ifndef IVF_FLAT_C_H
#define IVF_FLAT_C_H

#include "brute_force_c.h" // Reuse distance types and other shared definitions

#ifdef __cplusplus
extern "C" {
#endif

// Opaque pointer to the C++ GpuIvfFlatIndex object
typedef void* GpuIvfFlatIndexC;

// Opaque pointer to the C++ IVF search result object
typedef void* GpuIvfFlatSearchResultC;

// Constructor for building from dataset
GpuIvfFlatIndexC GpuIvfFlatIndex_New(const float* dataset_data, uint64_t count_vectors, uint32_t dimension, CuvsDistanceTypeC metric, uint32_t n_list, uint32_t nthread, void* errmsg);

// Constructor for loading from file
GpuIvfFlatIndexC GpuIvfFlatIndex_NewFromFile(const char* filename, uint32_t dimension, CuvsDistanceTypeC metric, uint32_t nthread, void* errmsg);

// Loads the index to the GPU (either builds or loads from file depending on constructor)
void GpuIvfFlatIndex_Load(GpuIvfFlatIndexC index_c, void* errmsg);

// Saves the index to file
void GpuIvfFlatIndex_Save(GpuIvfFlatIndexC index_c, const char* filename, void* errmsg);

// Performs a search operation
GpuIvfFlatSearchResultC GpuIvfFlatIndex_Search(GpuIvfFlatIndexC index_c, const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, uint32_t n_probes, void* errmsg);

// Retrieves the results from a search operation
void GpuIvfFlatIndex_GetResults(GpuIvfFlatSearchResultC result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances);

// Frees the memory for a GpuIvfFlatSearchResultC object
void GpuIvfFlatIndex_FreeSearchResult(GpuIvfFlatSearchResultC result_c);

// Destroys the GpuIvfFlatIndex object
void GpuIvfFlatIndex_Destroy(GpuIvfFlatIndexC index_c, void* errmsg);

// Gets the number of lists (centroids)
uint32_t GpuIvfFlatIndex_GetNList(GpuIvfFlatIndexC index_c);

// Gets the centroids after build
// centers: Pre-allocated array of size n_list * dimension
void GpuIvfFlatIndex_GetCenters(GpuIvfFlatIndexC index_c, float* centers, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // IVF_FLAT_C_H
