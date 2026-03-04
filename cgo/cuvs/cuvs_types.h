/* 
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef MO_CUVS_TYPES_H
#define MO_CUVS_TYPES_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief Distance metrics supported by cuVS.
 */
typedef enum {
    DistanceType_L2Expanded = 0,        // Squared L2 distance: sum((x-y)^2)
    DistanceType_L2SqrtExpanded = 1,    // L2 distance: sqrt(sum((x-y)^2))
    DistanceType_CosineExpanded = 2,    // Cosine distance: 1 - (x.y)/(|x||y|)
    DistanceType_L1 = 3,                // L1 (Manhattan) distance: sum(|x-y|)
    DistanceType_L2Unexpanded = 4,      // L2 distance without expansion
    DistanceType_L2SqrtUnexpanded = 5,  // L2 distance with sqrt without expansion
    DistanceType_InnerProduct = 6,      // Inner product: x.y
    DistanceType_Linf = 7,              // Chebyshev distance: max(|x-y|)
    DistanceType_Canberra = 8,          // Canberra distance
    DistanceType_LpUnexpanded = 9,      // Lp distance
    DistanceType_CorrelationExpanded = 10, // Correlation distance
    DistanceType_JaccardExpanded = 11,  // Jaccard distance
    DistanceType_HellingerExpanded = 12, // Hellinger distance
    DistanceType_Haversine = 13,        // Haversine distance
    DistanceType_BrayCurtis = 14,       // Bray-Curtis distance
    DistanceType_JensenShannon = 15,    // Jensen-Shannon distance
    DistanceType_HammingUnexpanded = 16, // Hamming distance
    DistanceType_KLDivergence = 17,     // Kullback-Leibler divergence
    DistanceType_RusselRaoExpanded = 18, // Russel-Rao distance
    DistanceType_DiceExpanded = 19,     // Dice distance
    DistanceType_BitwiseHamming = 20,   // Bitwise Hamming distance
    DistanceType_Precomputed = 100,     // Precomputed distance
    // Aliases
    DistanceType_CosineSimilarity = 2,  // Alias for Cosine distance
    DistanceType_Jaccard = 11,           // Alias for Jaccard distance
    DistanceType_Hamming = 16,           // Alias for Hamming distance
    DistanceType_Unknown = 255          // Unknown distance type
} distance_type_t;

/**
 * @brief Data quantization types.
 */
typedef enum {
    Quantization_F32,   // 32-bit floating point
    Quantization_F16,   // 16-bit floating point (half)
    Quantization_INT8,  // 8-bit signed integer
    Quantization_UINT8  // 8-bit unsigned integer
} quantization_t;

/**
 * @brief GPU distribution modes.
 */
typedef enum {
    DistributionMode_SINGLE_GPU, // Single GPU mode
    DistributionMode_SHARDED,    // Sharded across multiple GPUs
    DistributionMode_REPLICATED  // Replicated across multiple GPUs
} distribution_mode_t;

/**
 * @brief CAGRA index build parameters.
 */
typedef struct {
    size_t intermediate_graph_degree; // Degree of the intermediate graph (default 128)
    size_t graph_degree;              // Degree of the final graph (default 64)
    bool attach_dataset_on_build;     // Whether to attach the dataset to the index (default true)
} cagra_build_params_t;

/**
 * @brief CAGRA search parameters.
 */
typedef struct {
    size_t itopk_size;   // Internal top-k size (default 64)
    size_t search_width; // Number of search paths (default 1)
} cagra_search_params_t;

/**
 * @brief IVF-Flat index build parameters.
 */
typedef struct {
    uint32_t n_lists;             // Number of inverted lists (clusters) (default 1024)
    bool add_data_on_build;       // Whether to add data to the index during build (default true)
    double kmeans_trainset_fraction; // Fraction of data to use for k-means training (default 0.5)
} ivf_flat_build_params_t;

/**
 * @brief IVF-Flat search parameters.
 */
typedef struct {
    uint32_t n_probes; // Number of lists to probe during search (default 20)
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
