#pragma once

#include "cuvs_worker.hpp" // For cuvs_worker_t and raft_handle_wrapper_t
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
    
    cuvs::cluster::kmeans::params params;

    // Type of centroids and inertia. cuVS uses float for these even if input is half.
    // Also input data X must be float/double.
    using DataT = typename std::conditional<std::is_same<T, half>::value, float, T>::type;

    // Internal storage for centroids on device
    std::unique_ptr<raft::device_matrix<DataT, int64_t>> centroids_;
    std::unique_ptr<cuvs_worker_t> worker;
    std::shared_mutex mutex_;

    gpu_kmeans_t(uint32_t n_clusters, uint32_t dimension, cuvs::distance::DistanceType metric,
                 int max_iter, float tol, int n_init, int device_id, uint32_t nthread)
        : n_clusters(n_clusters), dimension(dimension) {
        
        params.n_clusters = static_cast<int>(n_clusters);
        params.max_iter = max_iter;
        params.tol = tol;
        params.n_init = n_init;
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
                
                auto X_device = raft::make_device_matrix<DataT, int64_t>(
                    *res, static_cast<int64_t>(n_samples), static_cast<int64_t>(dimension));
                
                if constexpr (std::is_same_v<T, DataT>) {
                    RAFT_CUDA_TRY(cudaMemcpyAsync(X_device.data_handle(), X_data,
                                             n_samples * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                             raft::resource::get_cuda_stream(*res)));
                } else {
                    // Convert half to float on GPU
                    auto X_half_device = raft::make_device_matrix<T, int64_t>(
                        *res, static_cast<int64_t>(n_samples), static_cast<int64_t>(dimension));
                    RAFT_CUDA_TRY(cudaMemcpyAsync(X_half_device.data_handle(), X_data,
                                             n_samples * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                             raft::resource::get_cuda_stream(*res)));
                    
                    raft::linalg::map(*res, X_device.view(), [] __device__(T x) { return (float)x; }, 
                                      raft::make_const_mdspan(X_half_device.view()));
                }

                if (!centroids_) {
                    centroids_ = std::make_unique<raft::device_matrix<DataT, int64_t>>(
                        raft::make_device_matrix<DataT, int64_t>(*res, static_cast<int64_t>(n_clusters), static_cast<int64_t>(dimension)));
                }

                float inertia = 0;
                int64_t n_iter = 0;

                cuvs::cluster::kmeans::fit(*res, params, 
                                           raft::make_const_mdspan(X_device.view()), 
                                           std::nullopt,
                                           centroids_->view(),
                                           raft::make_host_scalar_view(&inertia),
                                           raft::make_host_scalar_view(&n_iter));

                raft::resource::sync_stream(*res);
                return fit_result_t{inertia, n_iter};
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
                
                auto X_device = raft::make_device_matrix<DataT, int64_t>(
                    *res, static_cast<int64_t>(n_samples), static_cast<int64_t>(dimension));
                
                if constexpr (std::is_same_v<T, DataT>) {
                    RAFT_CUDA_TRY(cudaMemcpyAsync(X_device.data_handle(), X_data,
                                             n_samples * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                             raft::resource::get_cuda_stream(*res)));
                } else {
                    auto X_half_device = raft::make_device_matrix<T, int64_t>(
                        *res, static_cast<int64_t>(n_samples), static_cast<int64_t>(dimension));
                    RAFT_CUDA_TRY(cudaMemcpyAsync(X_half_device.data_handle(), X_data,
                                             n_samples * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                             raft::resource::get_cuda_stream(*res)));
                    
                    raft::linalg::map(*res, X_device.view(), [] __device__(T x) { return (float)x; }, 
                                      raft::make_const_mdspan(X_half_device.view()));
                }

                predict_result_t res_out;
                res_out.labels.resize(n_samples);
                auto labels_device = raft::make_device_vector<int64_t, int64_t>(*res, static_cast<int64_t>(n_samples));
                
                float inertia = 0;

                cuvs::cluster::kmeans::predict(*res, params,
                                               raft::make_const_mdspan(X_device.view()),
                                               std::nullopt,
                                               raft::make_const_mdspan(centroids_->view()),
                                               labels_device.view(),
                                               false,
                                               raft::make_host_scalar_view(&inertia));

                RAFT_CUDA_TRY(cudaMemcpyAsync(res_out.labels.data(), labels_device.data_handle(),
                                         n_samples * sizeof(int64_t), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*res)));
                
                raft::resource::sync_stream(*res);
                res_out.inertia = inertia;
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
                
                auto X_device = raft::make_device_matrix<DataT, int64_t>(
                    *res, static_cast<int64_t>(n_samples), static_cast<int64_t>(dimension));
                
                if constexpr (std::is_same_v<T, DataT>) {
                    RAFT_CUDA_TRY(cudaMemcpyAsync(X_device.data_handle(), X_data,
                                             n_samples * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                             raft::resource::get_cuda_stream(*res)));
                } else {
                    auto X_half_device = raft::make_device_matrix<T, int64_t>(
                        *res, static_cast<int64_t>(n_samples), static_cast<int64_t>(dimension));
                    RAFT_CUDA_TRY(cudaMemcpyAsync(X_half_device.data_handle(), X_data,
                                             n_samples * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                             raft::resource::get_cuda_stream(*res)));
                    
                    raft::linalg::map(*res, X_device.view(), [] __device__(T x) { return (float)x; }, 
                                      raft::make_const_mdspan(X_half_device.view()));
                }

                if (!centroids_) {
                    centroids_ = std::make_unique<raft::device_matrix<DataT, int64_t>>(
                        raft::make_device_matrix<DataT, int64_t>(*res, static_cast<int64_t>(n_clusters), static_cast<int64_t>(dimension)));
                }

                fit_predict_result_t res_out;
                res_out.labels.resize(n_samples);
                auto labels_device = raft::make_device_vector<int64_t, int64_t>(*res, static_cast<int64_t>(n_samples));
                float inertia = 0;
                int64_t n_iter = 0;

                cuvs::cluster::kmeans::fit_predict(*res, params,
                                                   raft::make_const_mdspan(X_device.view()),
                                                   std::nullopt,
                                                   std::make_optional(centroids_->view()),
                                                   labels_device.view(),
                                                   raft::make_host_scalar_view(&inertia),
                                                   raft::make_host_scalar_view(&n_iter));

                RAFT_CUDA_TRY(cudaMemcpyAsync(res_out.labels.data(), labels_device.data_handle(),
                                         n_samples * sizeof(int64_t), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*res)));
                
                raft::resource::sync_stream(*res);
                res_out.inertia = inertia;
                res_out.n_iter = n_iter;
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
    std::vector<DataT> get_centroids() {
        uint64_t job_id = worker->submit(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                if (!centroids_) return std::vector<DataT>{};

                auto res = handle.get_raft_resources();
                std::vector<DataT> host_centroids(n_clusters * dimension);

                RAFT_CUDA_TRY(cudaMemcpyAsync(host_centroids.data(), centroids_->data_handle(),
                                         host_centroids.size() * sizeof(DataT), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*res)));

                raft::resource::sync_stream(*res);
                return host_centroids;
            }
        );
        auto result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<std::vector<DataT>>(result.result);
    }

    void destroy() {
        if (worker) worker->stop();
    }
};

} // namespace matrixone
