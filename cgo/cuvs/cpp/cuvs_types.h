#ifndef MO_CUVS_TYPES_H
#define MO_CUVS_TYPES_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    DistanceType_L2Expanded,
    DistanceType_L1,
    DistanceType_InnerProduct,
    DistanceType_CosineSimilarity,
    DistanceType_Jaccard,
    DistanceType_Hamming,
    DistanceType_Unknown
} distance_type_t;

typedef enum {
    Quantization_F32,
    Quantization_F16,
    Quantization_INT8,
    Quantization_UINT8
} quantization_t;

typedef enum {
    DistributionMode_SINGLE_GPU,
    DistributionMode_SHARDED,
    DistributionMode_REPLICATED
} distribution_mode_t;

// CAGRA build parameters
typedef struct {
    size_t intermediate_graph_degree; // default 128
    size_t graph_degree;              // default 64
} cagra_build_params_t;

// CAGRA search parameters
typedef struct {
    size_t itopk_size;   // default 64
    size_t search_width; // default 1
} cagra_search_params_t;

// IVF-Flat build parameters
typedef struct {
    uint32_t n_lists; // default 1024
} ivf_flat_build_params_t;

// IVF-Flat search parameters
typedef struct {
    uint32_t n_probes; // default 20
} ivf_flat_search_params_t;

#ifdef __cplusplus
}
#endif

#endif // MO_CUVS_TYPES_H
