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

#pragma once

#include "index_base.hpp"
#include "cuvs_worker.hpp" // For cuvs_worker_t and raft_handle_wrapper_t
#include "cuvs_types.h"    // For distance_type_t and quantization_t
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
#include "quantize.hpp"
#pragma GCC diagnostic pop

namespace matrixone {

/**
 * @brief Search/Predict result for K-Means.
 * Common for all KMeans instantiations.
 */
struct kmeans_result_t {
    std::vector<int64_t> labels;
    float inertia;
    int64_t n_iter;
};

/**
 * @brief gpu_kmeans_t implements K-Means clustering on GPU using cuVS.
 */
template <typename T>
class gpu_kmeans_t : public gpu_index_base_t<T, kmeans_build_params_t> {
public:
    using predict_result_t = kmeans_result_t;
    using fit_predict_result_t = kmeans_result_t;

    uint32_t n_clusters;
    
    cuvs::cluster::kmeans::balanced_params params;

    // Type of centroids and inertia. cuVS uses float for these even if input is half, int8, or uint8.
    using CentroidT = float; 

    // Internal storage for centroids on device
    std::unique_ptr<raft::device_matrix<CentroidT, int64_t>> centroids_;

    gpu_kmeans_t(uint32_t n_clusters, uint32_t dimension, cuvs::distance::DistanceType metric,
                 int max_iter = 20, int device_id = 0, uint32_t nthread = 1)
        : n_clusters(n_clusters) {
        
        this->dimension = dimension;
        params.n_iters = static_cast<uint32_t>(max_iter);
        params.metric = metric;
        this->devices_ = {device_id};

        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_);
    }

    ~gpu_kmeans_t() override {
        this->destroy();
    }

    /**
     * @brief Starts the worker and initializes resources.
     */
    void start() {
        auto init_fn = [](raft_handle_wrapper_t&) -> std::any {
            return std::any();
        };

        auto stop_fn = [&](raft_handle_wrapper_t&) -> std::any {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            centroids_.reset();
            this->quantizer_.reset();
            return std::any();
        };

        this->worker->start(init_fn, stop_fn);
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

        uint64_t job_id = this->worker->submit_main(
            [&, X_data, n_samples](raft_handle_wrapper_t& handle) -> std::any {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                auto res = handle.get_raft_resources();
                
                auto X_device = raft::make_device_matrix<T, int64_t>(
                    *res, static_cast<int64_t>(n_samples), static_cast<int64_t>(this->dimension));
                
                RAFT_CUDA_TRY(cudaMemcpyAsync(X_device.data_handle(), X_data,
                                         n_samples * this->dimension * sizeof(T), cudaMemcpyHostToDevice,
                                         raft::resource::get_cuda_stream(*res)));

                if (!centroids_) {
                    centroids_ = std::make_unique<raft::device_matrix<CentroidT, int64_t>>(
                        raft::make_device_matrix<CentroidT, int64_t>(*res, static_cast<int64_t>(n_clusters), static_cast<int64_t>(this->dimension)));
                }

                cuvs::cluster::kmeans::fit(*res, params, 
                                           raft::make_const_mdspan(X_device.view()), 
                                           centroids_->view());

                raft::resource::sync_stream(*res);
                return fit_result_t{0.0f, static_cast<int64_t>(params.n_iters)};
            }
        );
        auto result = this->worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<fit_result_t>(result.result);
    }

    /**
     * @brief Assigns labels to new data based on existing centroids.
     */
    predict_result_t predict(const T* X_data, uint64_t n_samples) {
        if (!X_data || n_samples == 0) return {{}, 0, 0};

        uint64_t job_id = this->worker->submit_main(
            [&, X_data, n_samples](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                if (!centroids_) throw std::runtime_error("KMeans centroids not trained. Call fit() first.");

                auto res = handle.get_raft_resources();
                
                auto X_device = raft::make_device_matrix<T, int64_t>(
                    *res, static_cast<int64_t>(n_samples), static_cast<int64_t>(this->dimension));
                
                RAFT_CUDA_TRY(cudaMemcpyAsync(X_device.data_handle(), X_data,
                                         n_samples * this->dimension * sizeof(T), cudaMemcpyHostToDevice,
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
                res_out.n_iter = 0;
                return res_out;
            }
        );
        auto result = this->worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<predict_result_t>(result.result);
    }

    /**
     * @brief Assigns labels to new float32 data, performing on-the-fly quantization if needed.
     */
    predict_result_t predict_float(const float* X_data, uint64_t n_samples) {
        if constexpr (std::is_same_v<T, float>) {
            return predict(X_data, n_samples);
        }

        if (!X_data || n_samples == 0) return {{}, 0, 0};

        uint64_t job_id = this->worker->submit_main(
            [&, X_data, n_samples](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                if (!centroids_) throw std::runtime_error("KMeans centroids not trained. Call fit() first.");

                auto res = handle.get_raft_resources();
                
                // 1. Quantize/Convert float data to T on device
                auto X_device_float = raft::make_device_matrix<float, int64_t>(*res, n_samples, this->dimension);
                raft::copy(*res, X_device_float.view(), raft::make_host_matrix_view<const float, int64_t>(X_data, n_samples, this->dimension));
                
                auto X_device_target = raft::make_device_matrix<T, int64_t>(*res, n_samples, this->dimension);
                if constexpr (sizeof(T) == 1) {
                    if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
                    this->quantizer_.template transform<T>(*res, X_device_float.view(), X_device_target.data_handle(), true);
                    raft::resource::sync_stream(*res);
                } else {
                    raft::copy(*res, X_device_target.view(), X_device_float.view());
                }

                // 2. Perform prediction
                predict_result_t res_out;
                res_out.labels.resize(n_samples);
                auto labels_device = raft::make_device_vector<uint32_t, int64_t>(*res, static_cast<int64_t>(n_samples));
                
                cuvs::cluster::kmeans::predict(*res, params,
                                               raft::make_const_mdspan(X_device_target.view()),
                                               raft::make_const_mdspan(centroids_->view()),
                                               labels_device.view());

                std::vector<uint32_t> host_labels(n_samples);
                RAFT_CUDA_TRY(cudaMemcpyAsync(host_labels.data(), labels_device.data_handle(),
                                         n_samples * sizeof(uint32_t), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*res)));
                
                raft::resource::sync_stream(*res);
                for(uint64_t i=0; i<n_samples; ++i) res_out.labels[i] = (int64_t)host_labels[i];
                res_out.inertia = 0.0f;
                res_out.n_iter = 0;
                return res_out;
            }
        );
        auto result = this->worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<predict_result_t>(result.result);
    }

    /**
     * @brief Performs both fitting and labeling in one step.
     */
    fit_predict_result_t fit_predict(const T* X_data, uint64_t n_samples) {
        if (!X_data || n_samples == 0) return {{}, 0, 0};

        uint64_t job_id = this->worker->submit_main(
            [&, X_data, n_samples](raft_handle_wrapper_t& handle) -> std::any {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                auto res = handle.get_raft_resources();
                
                auto X_device = raft::make_device_matrix<T, int64_t>(
                    *res, static_cast<int64_t>(n_samples), static_cast<int64_t>(this->dimension));
                
                RAFT_CUDA_TRY(cudaMemcpyAsync(X_device.data_handle(), X_data,
                                         n_samples * this->dimension * sizeof(T), cudaMemcpyHostToDevice,
                                         raft::resource::get_cuda_stream(*res)));

                if (!centroids_) {
                    centroids_ = std::make_unique<raft::device_matrix<CentroidT, int64_t>>(
                        raft::make_device_matrix<CentroidT, int64_t>(*res, static_cast<int64_t>(n_clusters), static_cast<int64_t>(this->dimension)));
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
                    // Fallback for half and uint8_t
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
        auto result = this->worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<fit_predict_result_t>(result.result);
    }

    /**
     * @brief Performs fitting and prediction for float32 data, with on-the-fly quantization if needed.
     */
    fit_predict_result_t fit_predict_float(const float* X_data, uint64_t n_samples) {
        if constexpr (std::is_same_v<T, float>) {
            return fit_predict(X_data, n_samples);
        }

        if (!X_data || n_samples == 0) return {{}, 0, 0};

        uint64_t job_id = this->worker->submit_main(
            [&, X_data, n_samples](raft_handle_wrapper_t& handle) -> std::any {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                auto res = handle.get_raft_resources();
                
                // 1. Quantize/Convert float data to T on device
                auto X_device_float = raft::make_device_matrix<float, int64_t>(*res, n_samples, this->dimension);
                raft::copy(*res, X_device_float.view(), raft::make_host_matrix_view<const float, int64_t>(X_data, n_samples, this->dimension));
                
                auto X_device_target = raft::make_device_matrix<T, int64_t>(*res, n_samples, this->dimension);
                if constexpr (sizeof(T) == 1) {
                    if (!this->quantizer_.is_trained()) {
                        int64_t n_train = std::min(static_cast<int64_t>(n_samples), static_cast<int64_t>(500));
                        auto train_view = raft::make_device_matrix_view<const float, int64_t>(X_device_float.data_handle(), n_train, this->dimension);
                        this->quantizer_.train(*res, train_view);
                    }
                    this->quantizer_.template transform<T>(*res, X_device_float.view(), X_device_target.data_handle(), true);
                    raft::resource::sync_stream(*res);
                } else {
                    raft::copy(*res, X_device_target.view(), X_device_float.view());
                }

                // 2. Perform fit_predict
                if (!centroids_) {
                    centroids_ = std::make_unique<raft::device_matrix<CentroidT, int64_t>>(
                        raft::make_device_matrix<CentroidT, int64_t>(*res, static_cast<int64_t>(n_clusters), static_cast<int64_t>(this->dimension)));
                }

                fit_predict_result_t res_out;
                res_out.labels.resize(n_samples);
                auto labels_device = raft::make_device_vector<uint32_t, int64_t>(*res, static_cast<int64_t>(n_samples));

                if constexpr (std::is_same_v<T, int8_t>) {
                    cuvs::cluster::kmeans::fit_predict(*res, params,
                                                       raft::make_const_mdspan(X_device_target.view()),
                                                       centroids_->view(),
                                                       labels_device.view());
                } else {
                    // Fallback for half and uint8_t
                    cuvs::cluster::kmeans::fit(*res, params,
                                               raft::make_const_mdspan(X_device_target.view()),
                                               centroids_->view());
                    cuvs::cluster::kmeans::predict(*res, params,
                                                   raft::make_const_mdspan(X_device_target.view()),
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
        auto result = this->worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<fit_predict_result_t>(result.result);
    }

    /**
     * @brief Returns the trained centroids.
     */
    std::vector<CentroidT> get_centroids() {
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                if (!centroids_) return std::vector<CentroidT>{};

                auto res = handle.get_raft_resources();
                std::vector<CentroidT> host_centroids(n_clusters * this->dimension);

                RAFT_CUDA_TRY(cudaMemcpyAsync(host_centroids.data(), centroids_->data_handle(),
                                         host_centroids.size() * sizeof(CentroidT), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*res)));

                raft::resource::sync_stream(*res);
                return host_centroids;
            }
        );
        auto result = this->worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<std::vector<CentroidT>>(result.result);
    }
};

} // namespace matrixone
