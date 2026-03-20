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

/*
 * K-Means Index Implementation
 * Supported data types (T): float, half, int8_t, uint8_t (Internally uses float centroids)
 * Result Label type: int64_t
 */

#pragma once

#include "index_base.hpp"
#include "cuvs_worker.hpp"
#include "cuvs_types.h"
#include "quantize.hpp"

#include <cuda_fp16.h>
#include <raft/util/cudart_utils.hpp>

#include <algorithm>
#include <future>
#include <iostream>
#include <limits>
#include <memory>
#include <numeric>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#include <raft/core/copy.cuh>
#include <raft/core/device_mdarray.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/device_resources_snmg.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/resources.hpp>

#include <cuvs/distance/distance.hpp>
#include <cuvs/cluster/kmeans.hpp>
#pragma GCC diagnostic pop


namespace matrixone {

/**
 * @brief Search result for KMeans clustering.
 */
struct kmeans_result_t {
    std::vector<int64_t> labels;
    float inertia;
    int64_t n_iter;
};

/**
 * @brief gpu_kmeans_t implements a KMeans clustering index.
 * Note: cuVS KMeans fits and predicts always use float centroids internally.
 */
template <typename T>
class gpu_kmeans_t : public gpu_index_base_t<T, kmeans_build_params_t> {
public:
    // Internal centroids storage - ALWAYS float for cuVS KMeans
    std::unique_ptr<raft::device_matrix<float, int64_t>> centroids_;

    ~gpu_kmeans_t() override {
        this->destroy();
    }

    // Unified Constructor for building from dataset
    gpu_kmeans_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension,
                    distance_type_t m, const kmeans_build_params_t& bp,
                    uint32_t nthread, int device_id) {

        this->dimension = dimension;
        this->count = static_cast<uint32_t>(count_vectors);
        this->metric = m;
        this->build_params = bp;
        this->devices_ = {device_id};
        this->current_offset_ = static_cast<uint32_t>(count_vectors);

        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_);

        this->flattened_host_dataset.resize(this->count * this->dimension);
        if (dataset_data) {
            std::copy(dataset_data, dataset_data + (this->count * this->dimension), this->flattened_host_dataset.begin());
        }
    }

    // Constructor for chunked input (pre-allocates)
    gpu_kmeans_t(uint64_t total_count, uint32_t dimension, distance_type_t m,
                    const kmeans_build_params_t& bp, uint32_t nthread, int device_id) {

        this->dimension = dimension;
        this->count = static_cast<uint32_t>(total_count);
        this->metric = m;
        this->build_params = bp;
        this->devices_ = {device_id};
        this->current_offset_ = 0;

        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_);

        this->flattened_host_dataset.resize(this->count * this->dimension);
    }

    // Constructor for kmeans_c.cpp compatibility
    gpu_kmeans_t(uint32_t n_clusters, uint32_t dimension, distance_type_t m,
                    int max_iter, int device_id, uint32_t nthread) {
        this->dimension = dimension;
        this->metric = m;
        this->build_params.k = n_clusters;
        this->build_params.max_iter = max_iter;
        this->build_params.tol = 1e-4f;
        this->devices_ = {device_id};
        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_);
    }

    void start() override {
        auto init_fn = [](raft_handle_wrapper_t&) -> std::any { return std::any(); };
        auto stop_fn = [&](raft_handle_wrapper_t&) -> std::any {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            centroids_.reset();
            this->quantizer_.reset();
            return std::any();
        };
        this->worker->start(init_fn, stop_fn);
    }

    void build() override {
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                this->build_internal(handle);
                return std::any();
            }
        );
        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        this->is_loaded_ = true;
        this->flattened_host_dataset.clear();
        this->flattened_host_dataset.shrink_to_fit();
    }

    void build_internal(raft_handle_wrapper_t& handle) {
        std::unique_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        auto dataset_device_t = raft::make_device_matrix<T, int64_t>(
            *res, static_cast<int64_t>(this->count), static_cast<int64_t>(this->dimension));
        RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device_t.data_handle(), this->flattened_host_dataset.data(),
                                    this->flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                    raft::resource::get_cuda_stream(*res)));

        auto dataset_device_f = raft::make_device_matrix<float, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
        raft::copy(*res, dataset_device_f.view(), dataset_device_t.view());

        cuvs::cluster::kmeans::params kmeans_params;
        kmeans_params.n_clusters = this->build_params.k;
        kmeans_params.metric = static_cast<cuvs::distance::DistanceType>(this->metric);
        kmeans_params.max_iter = this->build_params.max_iter;
        kmeans_params.tol = this->build_params.tol;

        centroids_ = std::make_unique<raft::device_matrix<float, int64_t>>(
            raft::make_device_matrix<float, int64_t>(*res, (int64_t)kmeans_params.n_clusters, (int64_t)this->dimension));
        
        float inertia;
        int64_t n_iter;
        cuvs::cluster::kmeans::fit(*res, kmeans_params, dataset_device_f.view(), std::nullopt, centroids_->view(), 
                                    raft::make_host_scalar_view<float>(&inertia), raft::make_host_scalar_view<int64_t>(&n_iter));
        
        raft::resource::sync_stream(*res);
    }

    kmeans_result_t fit(const T* dataset_data, uint64_t count_vectors) {
        this->count = static_cast<uint32_t>(count_vectors);
        this->flattened_host_dataset.resize(this->count * this->dimension);
        std::copy(dataset_data, dataset_data + (this->count * this->dimension), this->flattened_host_dataset.begin());
        
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                auto res = handle.get_raft_resources();

                auto dataset_device_t = raft::make_device_matrix<T, int64_t>(
                    *res, static_cast<int64_t>(this->count), static_cast<int64_t>(this->dimension));
                raft::copy(*res, dataset_device_t.view(), raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data(), this->count, this->dimension));

                auto dataset_device_f = raft::make_device_matrix<float, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
                raft::copy(*res, dataset_device_f.view(), dataset_device_t.view());

                cuvs::cluster::kmeans::params kmeans_params;
                kmeans_params.n_clusters = this->build_params.k;
                kmeans_params.metric = static_cast<cuvs::distance::DistanceType>(this->metric);
                kmeans_params.max_iter = this->build_params.max_iter;
                kmeans_params.tol = this->build_params.tol;

                centroids_ = std::make_unique<raft::device_matrix<float, int64_t>>(
                    raft::make_device_matrix<float, int64_t>(*res, (int64_t)kmeans_params.n_clusters, (int64_t)this->dimension));
                
                float inertia;
                int64_t n_iter;
                cuvs::cluster::kmeans::fit(*res, kmeans_params, dataset_device_f.view(), std::nullopt, centroids_->view(), 
                                            raft::make_host_scalar_view<float>(&inertia), raft::make_host_scalar_view<int64_t>(&n_iter));
                
                raft::resource::sync_stream(*res);
                return std::make_pair(inertia, n_iter);
            }
        );
        auto res_wait = this->worker->wait(job_id).get();
        if (res_wait.error) std::rethrow_exception(res_wait.error);
        auto p = std::any_cast<std::pair<float, int64_t>>(res_wait.result);
        this->is_loaded_ = true;
        return {std::vector<int64_t>{}, p.first, p.second};
    }

    kmeans_result_t predict(const T* queries_data, uint64_t num_queries) {
        if (!queries_data || num_queries == 0 || !centroids_) return {};

        auto task = [this, num_queries, queries_data](raft_handle_wrapper_t& handle) -> std::any {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            auto res = handle.get_raft_resources();

            auto queries_device_t = raft::make_device_matrix<T, int64_t>(
                *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));
            raft::copy(*res, queries_device_t.view(), raft::make_host_matrix_view<const T, int64_t>(queries_data, num_queries, this->dimension));

            auto queries_device_f = raft::make_device_matrix<float, int64_t>(*res, (int64_t)num_queries, (int64_t)this->dimension);
            raft::copy(*res, queries_device_f.view(), queries_device_t.view());

            auto labels_device = raft::make_device_vector<int64_t, int64_t>(*res, (int64_t)num_queries);
            
            float inertia;
            cuvs::cluster::kmeans::params kmeans_params;
            kmeans_params.n_clusters = this->build_params.k;
            kmeans_params.metric = static_cast<cuvs::distance::DistanceType>(this->metric);

            cuvs::cluster::kmeans::predict(*res, kmeans_params, queries_device_f.view(), std::nullopt, centroids_->view(), 
                                            labels_device.view(), true, raft::make_host_scalar_view<float>(&inertia));
            
            std::vector<int64_t> labels_host(num_queries);
            raft::copy(*res, raft::make_host_vector_view<int64_t, int64_t>(labels_host.data(), (int64_t)num_queries), labels_device.view());
            raft::resource::sync_stream(*res);

            return kmeans_result_t{labels_host, inertia, 0};
        };

        uint64_t job_id = this->worker->submit(task);
        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        return std::any_cast<kmeans_result_t>(result_wait.result);
    }

    kmeans_result_t predict_float(const float* queries_data, uint64_t num_queries) {
        if constexpr (std::is_same_v<T, float>) return predict(queries_data, num_queries);
        if (!queries_data || num_queries == 0 || !centroids_) return {};

        auto task = [this, num_queries, queries_data](raft_handle_wrapper_t& handle) -> std::any {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            auto res = handle.get_raft_resources();

            auto queries_device_f = raft::make_device_matrix<float, int64_t>(*res, (int64_t)num_queries, (int64_t)this->dimension);
            raft::copy(*res, queries_device_f.view(), raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, this->dimension));
            
            auto labels_device = raft::make_device_vector<int64_t, int64_t>(*res, (int64_t)num_queries);
            float inertia;
            cuvs::cluster::kmeans::params kmeans_params;
            kmeans_params.n_clusters = this->build_params.k;
            kmeans_params.metric = static_cast<cuvs::distance::DistanceType>(this->metric);

            cuvs::cluster::kmeans::predict(*res, kmeans_params, queries_device_f.view(), std::nullopt, centroids_->view(), 
                                            labels_device.view(), true, raft::make_host_scalar_view<float>(&inertia));
            
            std::vector<int64_t> labels_host(num_queries);
            raft::copy(*res, raft::make_host_vector_view<int64_t, int64_t>(labels_host.data(), (int64_t)num_queries), labels_device.view());
            raft::resource::sync_stream(*res);

            return kmeans_result_t{labels_host, inertia, 0};
        };

        uint64_t job_id = this->worker->submit(task);
        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        return std::any_cast<kmeans_result_t>(result_wait.result);
    }

    kmeans_result_t fit_predict(const T* dataset_data, uint64_t count_vectors) {
        auto res_fit = fit(dataset_data, count_vectors);
        auto res_predict = predict(dataset_data, count_vectors);
        res_predict.inertia = res_fit.inertia;
        res_predict.n_iter = res_fit.n_iter;
        return res_predict;
    }

    kmeans_result_t fit_predict_float(const float* dataset_data, uint64_t count_vectors) {
        this->count = static_cast<uint32_t>(count_vectors);
        
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                auto res = handle.get_raft_resources();

                auto dataset_device_f = raft::make_device_matrix<float, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
                raft::copy(*res, dataset_device_f.view(), raft::make_host_matrix_view<const float, int64_t>(dataset_data, this->count, this->dimension));

                cuvs::cluster::kmeans::params kmeans_params;
                kmeans_params.n_clusters = this->build_params.k;
                kmeans_params.metric = static_cast<cuvs::distance::DistanceType>(this->metric);
                kmeans_params.max_iter = this->build_params.max_iter;
                kmeans_params.tol = this->build_params.tol;

                centroids_ = std::make_unique<raft::device_matrix<float, int64_t>>(
                    raft::make_device_matrix<float, int64_t>(*res, (int64_t)kmeans_params.n_clusters, (int64_t)this->dimension));
                
                float inertia;
                int64_t n_iter;
                cuvs::cluster::kmeans::fit(*res, kmeans_params, dataset_device_f.view(), std::nullopt, centroids_->view(), 
                                            raft::make_host_scalar_view<float>(&inertia), raft::make_host_scalar_view<int64_t>(&n_iter));
                
                auto labels_device = raft::make_device_vector<int64_t, int64_t>(*res, (int64_t)this->count);
                cuvs::cluster::kmeans::predict(*res, kmeans_params, dataset_device_f.view(), std::nullopt, centroids_->view(), 
                                                labels_device.view(), true, raft::make_host_scalar_view<float>(&inertia));

                std::vector<int64_t> labels_host(this->count);
                raft::copy(*res, raft::make_host_vector_view<int64_t, int64_t>(labels_host.data(), (int64_t)this->count), labels_device.view());
                raft::resource::sync_stream(*res);

                return kmeans_result_t{labels_host, inertia, n_iter};
            }
        );
        auto res_wait = this->worker->wait(job_id).get();
        if (res_wait.error) std::rethrow_exception(res_wait.error);
        this->is_loaded_ = true;
        return std::any_cast<kmeans_result_t>(res_wait.result);
    }

    std::vector<T> get_centroids() {
        if (!centroids_) return {};
        
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto res = handle.get_raft_resources();
                
                size_t n_clusters = centroids_->extent(0);
                size_t dim = centroids_->extent(1);

                auto centroids_device_target = raft::make_device_matrix<T, int64_t>(*res, n_clusters, dim);
                if constexpr (sizeof(T) == 1) {
                    if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
                    this->quantizer_.template transform<T>(*res, centroids_->view(), centroids_device_target.data_handle(), true);
                } else {
                    raft::copy(*res, centroids_device_target.view(), centroids_->view());
                }

                std::vector<T> centroids_host(n_clusters * dim);
                raft::copy(*res, raft::make_host_matrix_view<T, int64_t>(centroids_host.data(), n_clusters, dim), centroids_device_target.view());
                raft::resource::sync_stream(*res);
                return centroids_host;
            }
        );
        auto result = this->worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<std::vector<T>>(result.result);
    }

    std::string info() const override {
        std::string json = gpu_index_base_t<T, kmeans_build_params_t>::info();
        json += ", \"type\": \"KMeans\", \"kmeans\": {";
        if (centroids_) json += "\"clusters\": " + std::to_string(centroids_->extent(0));
        else json += "\"built\": false";
        json += "}}";
        return json;
    }

    void destroy() override {
        if (this->worker) this->worker->stop();
        std::unique_lock<std::shared_mutex> lock(this->mutex_);
        centroids_.reset();
        this->quantizer_.reset();
        this->dataset_device_ptr_.reset();
    }
};

} // namespace matrixone
