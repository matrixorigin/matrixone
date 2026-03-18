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

#ifndef KMEANS_C_H
#define KMEANS_C_H

#include "helper.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// Opaque pointer to the C++ gpu_kmeans_t object
typedef void* gpu_kmeans_c;

// Opaque pointer to the C++ KMeans result object
typedef void* gpu_kmeans_result_c;

// Constructor
gpu_kmeans_c gpu_kmeans_new(uint32_t n_clusters, uint32_t dimension, distance_type_t metric,
                            int max_iter, int device_id, uint32_t nthread, 
                            quantization_t qtype, void* errmsg);

// Destructor
void gpu_kmeans_destroy(gpu_kmeans_c kmeans_c, void* errmsg);

// Starts the worker and initializes resources
void gpu_kmeans_start(gpu_kmeans_c kmeans_c, void* errmsg);

// Trains the scalar quantizer (if T is 1-byte)
void gpu_kmeans_train_quantizer(gpu_kmeans_c kmeans_c, const float* train_data, uint64_t n_samples, void* errmsg);

void gpu_kmeans_set_quantizer(gpu_kmeans_c kmeans_c, float min, float max, void* errmsg);
void gpu_kmeans_get_quantizer(gpu_kmeans_c kmeans_c, float* min, float* max, void* errmsg);

// Fit function
typedef struct {
    float inertia;
    int64_t n_iter;
} gpu_kmeans_fit_res_t;

gpu_kmeans_fit_res_t gpu_kmeans_fit(gpu_kmeans_c kmeans_c, const void* X_data, uint64_t n_samples, void* errmsg);

// Predict function
typedef struct {
    gpu_kmeans_result_c result_ptr;
    float inertia;
} gpu_kmeans_predict_res_t;

gpu_kmeans_predict_res_t gpu_kmeans_predict(gpu_kmeans_c kmeans_c, const void* X_data, uint64_t n_samples, void* errmsg);

gpu_kmeans_predict_res_t gpu_kmeans_predict_float(gpu_kmeans_c kmeans_c, const float* X_data, uint64_t n_samples, void* errmsg);

// FitPredict function
typedef struct {
    gpu_kmeans_result_c result_ptr;
    float inertia;
    int64_t n_iter;
} gpu_kmeans_fit_predict_res_t;

gpu_kmeans_fit_predict_res_t gpu_kmeans_fit_predict(gpu_kmeans_c kmeans_c, const void* X_data, uint64_t n_samples, void* errmsg);

gpu_kmeans_fit_predict_res_t gpu_kmeans_fit_predict_float(gpu_kmeans_c kmeans_c, const float* X_data, uint64_t n_samples, void* errmsg);

// Get results from result object
void gpu_kmeans_get_labels(gpu_kmeans_result_c result_c, uint64_t n_samples, int64_t* labels);

// Free result object
void gpu_kmeans_free_result(gpu_kmeans_result_c result_c);

// Get centroids
void gpu_kmeans_get_centroids(gpu_kmeans_c kmeans_c, void* centroids, void* errmsg);

// Prints info about the kmeans
void gpu_kmeans_info(gpu_kmeans_c kmeans_c, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // KMEANS_C_H
