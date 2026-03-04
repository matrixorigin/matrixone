#ifndef MO_CUVS_TYPES_H
#define MO_CUVS_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    DistanceType_L2Expanded = 0,
    DistanceType_L2SqrtExpanded = 1,
    DistanceType_CosineExpanded = 2,
    DistanceType_L1 = 3,
    DistanceType_L2Unexpanded = 4,
    DistanceType_L2SqrtUnexpanded = 5,
    DistanceType_InnerProduct = 6,
    DistanceType_Linf = 7,
    DistanceType_Canberra = 8,
    DistanceType_LpUnexpanded = 9,
    DistanceType_CorrelationExpanded = 10,
    DistanceType_JaccardExpanded = 11,
    DistanceType_HellingerExpanded = 12,
    DistanceType_Haversine = 13,
    DistanceType_BrayCurtis = 14,
    DistanceType_JensenShannon = 15,
    DistanceType_HammingUnexpanded = 16,
    DistanceType_KLDivergence = 17,
    DistanceType_RusselRaoExpanded = 18,
    DistanceType_DiceExpanded = 19,
    DistanceType_BitwiseHamming = 20,
    DistanceType_Precomputed = 100,
    // Aliases
    DistanceType_CosineSimilarity = 2,
    DistanceType_Jaccard = 11,
    DistanceType_Hamming = 16,
    DistanceType_Unknown = 255
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
    bool attach_dataset_on_build;     // default true
} cagra_build_params_t;

// CAGRA search parameters
typedef struct {
    size_t itopk_size;   // default 64
    size_t search_width; // default 1
} cagra_search_params_t;

// IVF-Flat build parameters
typedef struct {
    uint32_t n_lists;             // default 1024
    bool add_data_on_build;       // default true
    double kmeans_trainset_fraction; // default 0.5
} ivf_flat_build_params_t;

// IVF-Flat search parameters
typedef struct {
    uint32_t n_probes; // default 20
} ivf_flat_search_params_t;

#ifdef __cplusplus
static inline cagra_build_params_t cagra_build_params_default() {
    return {128, 64, true};
}

static inline cagra_search_params_t cagra_search_params_default() {
    return {64, 1};
}

static inline ivf_flat_build_params_t ivf_flat_build_params_default() {
    return {1024, true, 0.5};
}

static inline ivf_flat_search_params_t ivf_flat_search_params_default() {
    return {20};
}
#endif

#ifdef __cplusplus
}
#endif

#endif // MO_CUVS_TYPES_H
