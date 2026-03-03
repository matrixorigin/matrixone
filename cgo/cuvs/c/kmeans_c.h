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
                            int max_iter, float tol, int n_init, int device_id, uint32_t nthread, 
                            quantization_t qtype, void* errmsg);

// Destructor
void gpu_kmeans_destroy(gpu_kmeans_c kmeans_c, void* errmsg);

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

// FitPredict function
typedef struct {
    gpu_kmeans_result_c result_ptr;
    float inertia;
    int64_t n_iter;
} gpu_kmeans_fit_predict_res_t;

gpu_kmeans_fit_predict_res_t gpu_kmeans_fit_predict(gpu_kmeans_c kmeans_c, const void* X_data, uint64_t n_samples, void* errmsg);

// Get results from result object
void gpu_kmeans_get_labels(gpu_kmeans_result_c result_c, uint64_t n_samples, int64_t* labels);

// Free result object
void gpu_kmeans_free_result(gpu_kmeans_result_c result_c);

// Get centroids
void gpu_kmeans_get_centroids(gpu_kmeans_c kmeans_c, void* centroids, void* errmsg);

#ifdef __cplusplus
}
#endif

#endif // KMEANS_C_H
