#ifndef BRUTE_FORCE_C_H
#define BRUTE_FORCE_C_H

#include <stdint.h> // For uint32_t, uint64_t
#include <stddef.h> // For size_t

#ifdef __cplusplus
extern "C" {
#endif

// Define a C-compatible enum for distance types
typedef enum {
    DistanceType_L2Expanded = 0,
    DistanceType_L1,
    DistanceType_InnerProduct,
    DistanceType_CosineSimilarity,
    DistanceType_Jaccard,
    DistanceType_Hamming,
    DistanceType_Unknown // Should not happen
} CuvsDistanceTypeC;

// Opaque pointer to the C++ GpuBruteForceIndex object
typedef void* GpuBruteForceIndexC;

// Opaque pointer to the C++ SearchResult object
typedef void* GpuBruteForceSearchResultC;

// Constructor for GpuBruteForceIndex
// dataset_data: Flattened array of dataset vectors
// count_vectors: Number of vectors in the dataset
// dimension: Dimension of each vector
// metric: Distance metric to use
// nthread: Number of worker threads
// errmsg: Pointer to a char pointer to store an error message if one occurs. The caller is responsible for freeing the memory.
GpuBruteForceIndexC GpuBruteForceIndex_New(const float* dataset_data, uint64_t count_vectors, uint32_t dimension, CuvsDistanceTypeC metric, uint32_t nthread, void* errmsg);

// Loads the index to the GPU
// index_c: Opaque pointer to the GpuBruteForceIndex object
// errmsg: Pointer to a char pointer to store an error message if one occurs. The caller is responsible for freeing the memory.
void GpuBruteForceIndex_Load(GpuBruteForceIndexC index_c, void* errmsg);

// Performs a search operation
// index_c: Opaque pointer to the GpuBruteForceIndex object
// queries_data: Flattened array of query vectors
// num_queries: Number of query vectors
// query_dimension: Dimension of each query vector (must match index dimension)
// limit: Maximum number of neighbors to return per query
// errmsg: Pointer to a char pointer to store an error message if one occurs. The caller is responsible for freeing the memory.
GpuBruteForceSearchResultC GpuBruteForceIndex_Search(GpuBruteForceIndexC index_c, const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, void* errmsg);

// Retrieves the results from a search operation
// result_c: Opaque pointer to the GpuBruteForceSearchResult object
// neighbors: Pre-allocated flattened array for neighbor indices (size: num_queries * limit)
// distances: Pre-allocated flattened array for distances (size: num_queries * limit)
void GpuBruteForceIndex_GetResults(GpuBruteForceSearchResultC result_c, uint64_t num_queries, uint32_t limit, int64_t* neighbors, float* distances);


// Frees the memory for a GpuBruteForceSearchResultC object
void GpuBruteForceIndex_FreeSearchResult(GpuBruteForceSearchResultC result_c);


// Destroys the GpuBruteForceIndex object and frees associated resources
// index_c: Opaque pointer to the GpuBruteForceIndex object
// errmsg: Pointer to a char pointer to store an error message if one occurs. The caller is responsible for freeing the memory.
void GpuBruteForceIndex_Destroy(GpuBruteForceIndexC index_c, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // BRUTE_FORCE_C_H
