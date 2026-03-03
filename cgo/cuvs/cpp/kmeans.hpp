#pragma once

#include "cuvs_worker.hpp" // For cuvs_worker_t and raft_handle_wrapper_t
#include "../c/helper.h"   // For distance_type_t and quantization_t
#include <raft/util/cudart_utils.hpp> // For RAFT_CUDA_TRY
#include <cuda_fp16.h> // For half

// Standard library includes
#include <algorithm>   
#include <memory>
#include <vector>
#include <future>      
#include <shared_mutex> 
#include <optional>
#include <type_traits>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
// RAFT includes
#include <raft/core/device_mdarray.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/host_mdspan.hpp>
#include <raft/core/resources.hpp>
#include <raft/linalg/map.cuh>

// cuVS includes
#include <cuvs/distance/distance.hpp>
#include <cuvs/cluster/kmeans.hpp>
#pragma GCC diagnostic pop

namespace matrixone {

/**
 * @brief gpu_kmeans_t implements K-Means clustering on GPU using cuVS.
 */
template <typename T>
class gpu_kmeans_t {
public:
    uint32_t n_clusters;
    uint32_t dimension;
    
    cuvs::cluster::kmeans::balanced_params params;

    // Type of centroids and inertia. cuVS uses float for these even if input is half, int8, or uint8.
    using CentroidT = float; 

    // Internal storage for centroids on device
    std::unique_ptr<raft::device_matrix<CentroidT, int64_t>> centroids_;
    std::unique_ptr<cuvs_worker_t> worker;
    std::shared_mutex mutex_;

    gpu_kmeans_t(uint32_t n_clusters, uint32_t dimension, cuvs::distance::DistanceType metric,
                 int max_iter = 20, int device_id = 0, uint32_t nthread = 1)
        : n_clusters(n_clusters), dimension(dimension) {
        
        params.n_iters = static_cast<uint32_t>(max_iter);
        params.metric = metric;

        // K-Means in cuVS is currently single-GPU focused in the main cluster API
        worker = std::make_unique<cuvs_worker_t>(nthread, device_id);
        worker->start();
    }

    ~gpu_kmeans_t() {
        destroy();
    }

    struct fit_result_t {
        float inertia;
        int64_t n_iter;
    };

    /**
     * @brief Computes the cluster centroids.
     */
    fit_result_t fit(const T* X_data, uint64_t n_samples) {
        if (!X_data || n_samples == 0) return {0, 0};

        uint64_t job_id = worker->submit(
            [&, X_data, n_samples](raft_handle_wrapper_t& handle) -> std::any {
                std::unique_lock<std::shared_mutex> lock(mutex_);
                auto res = handle.get_raft_resources();
                
                auto X_device = raft::make_device_matrix<T, int64_t>(
                    *res, static_cast<int64_t>(n_samples), static_cast<int64_t>(dimension));
                
                RAFT_CUDA_TRY(cudaMemcpyAsync(X_device.data_handle(), X_data,
                                         n_samples * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                         raft::resource::get_cuda_stream(*res)));

                if (!centroids_) {
                    centroids_ = std::make_unique<raft::device_matrix<CentroidT, int64_t>>(
                        raft::make_device_matrix<CentroidT, int64_t>(*res, static_cast<int64_t>(n_clusters), static_cast<int64_t>(dimension)));
                }

                cuvs::cluster::kmeans::fit(*res, params, 
                                           raft::make_const_mdspan(X_device.view()), 
                                           centroids_->view());

                raft::resource::sync_stream(*res);
                return fit_result_t{0.0f, static_cast<int64_t>(params.n_iters)};
            }
        );
        auto result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<fit_result_t>(result.result);
    }

    struct predict_result_t {
        std::vector<int64_t> labels;
        float inertia;
    };

    /**
     * @brief Assigns labels to new data based on existing centroids.
     */
    predict_result_t predict(const T* X_data, uint64_t n_samples) {
        if (!X_data || n_samples == 0) return {{}, 0};

        uint64_t job_id = worker->submit(
            [&, X_data, n_samples](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                if (!centroids_) throw std::runtime_error("KMeans centroids not trained. Call fit() first.");

                auto res = handle.get_raft_resources();
                
                auto X_device = raft::make_device_matrix<T, int64_t>(
                    *res, static_cast<int64_t>(n_samples), static_cast<int64_t>(dimension));
                
                RAFT_CUDA_TRY(cudaMemcpyAsync(X_device.data_handle(), X_data,
                                         n_samples * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                         raft::resource::get_cuda_stream(*res)));

                predict_result_t res_out;
                res_out.labels.resize(n_samples);
                auto labels_device = raft::make_device_vector<uint32_t, int64_t>(*res, static_cast<int64_t>(n_samples));
                
                cuvs::cluster::kmeans::predict(*res, params,
                                               raft::make_const_mdspan(X_device.view()),
                                               raft::make_const_mdspan(centroids_->view()),
                                               labels_device.view());

                std::vector<uint32_t> host_labels(n_samples);
                RAFT_CUDA_TRY(cudaMemcpyAsync(host_labels.data(), labels_device.data_handle(),
                                         n_samples * sizeof(uint32_t), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*res)));
                
                raft::resource::sync_stream(*res);
                for(uint64_t i=0; i<n_samples; ++i) res_out.labels[i] = (int64_t)host_labels[i];
                res_out.inertia = 0.0f;
                return res_out;
            }
        );
        auto result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<predict_result_t>(result.result);
    }

    struct fit_predict_result_t {
        std::vector<int64_t> labels;
        float inertia;
        int64_t n_iter;
    };

    /**
     * @brief Performs both fitting and labeling in one step.
     */
    fit_predict_result_t fit_predict(const T* X_data, uint64_t n_samples) {
        if (!X_data || n_samples == 0) return {{}, 0, 0};

        uint64_t job_id = worker->submit(
            [&, X_data, n_samples](raft_handle_wrapper_t& handle) -> std::any {
                std::unique_lock<std::shared_mutex> lock(mutex_);
                auto res = handle.get_raft_resources();
                
                auto X_device = raft::make_device_matrix<T, int64_t>(
                    *res, static_cast<int64_t>(n_samples), static_cast<int64_t>(dimension));
                
                RAFT_CUDA_TRY(cudaMemcpyAsync(X_device.data_handle(), X_data,
                                         n_samples * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                         raft::resource::get_cuda_stream(*res)));

                if (!centroids_) {
                    centroids_ = std::make_unique<raft::device_matrix<CentroidT, int64_t>>(
                        raft::make_device_matrix<CentroidT, int64_t>(*res, static_cast<int64_t>(n_clusters), static_cast<int64_t>(dimension)));
                }

                fit_predict_result_t res_out;
                res_out.labels.resize(n_samples);
                auto labels_device = raft::make_device_vector<uint32_t, int64_t>(*res, static_cast<int64_t>(n_samples));

                if constexpr (std::is_same_v<T, float> || std::is_same_v<T, int8_t>) {
                    cuvs::cluster::kmeans::fit_predict(*res, params,
                                                       raft::make_const_mdspan(X_device.view()),
                                                       centroids_->view(),
                                                       labels_device.view());
                } else {
                    // Fallback for half and uint8_t which might missing fit_predict overload in some cuVS versions
                    cuvs::cluster::kmeans::fit(*res, params,
                                               raft::make_const_mdspan(X_device.view()),
                                               centroids_->view());
                    cuvs::cluster::kmeans::predict(*res, params,
                                                   raft::make_const_mdspan(X_device.view()),
                                                   raft::make_const_mdspan(centroids_->view()),
                                                   labels_device.view());
                }

                std::vector<uint32_t> host_labels(n_samples);
                RAFT_CUDA_TRY(cudaMemcpyAsync(host_labels.data(), labels_device.data_handle(),
                                         n_samples * sizeof(uint32_t), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*res)));
                
                raft::resource::sync_stream(*res);
                for(uint64_t i=0; i<n_samples; ++i) res_out.labels[i] = (int64_t)host_labels[i];
                res_out.inertia = 0.0f;
                res_out.n_iter = static_cast<int64_t>(params.n_iters);
                return res_out;
            }
        );
        auto result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<fit_predict_result_t>(result.result);
    }

    /**
     * @brief Returns the trained centroids.
     */
    std::vector<CentroidT> get_centroids() {
        uint64_t job_id = worker->submit(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                if (!centroids_) return std::vector<CentroidT>{};

                auto res = handle.get_raft_resources();
                std::vector<CentroidT> host_centroids(n_clusters * dimension);

                RAFT_CUDA_TRY(cudaMemcpyAsync(host_centroids.data(), centroids_->data_handle(),
                                         host_centroids.size() * sizeof(CentroidT), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*res)));

                raft::resource::sync_stream(*res);
                return host_centroids;
            }
        );
        auto result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<std::vector<CentroidT>>(result.result);
    }

    void destroy() {
        if (worker) worker->stop();
    }
};

} // namespace matrixone
