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
 * IVF-PQ Index Implementation
 * Supported data types (T): float, half, int8_t, uint8_t
 * Neighbor ID type: int64_t
 */

#pragma once

#include "index_base.hpp"
#include "cuvs_worker.hpp"
#include "cuvs_types.h"
#include "quantize.hpp"
#include "helper.h"

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
#include <fstream>

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
#include <cuvs/neighbors/ivf_pq.hpp>
#pragma GCC diagnostic pop


namespace matrixone {

/**
 * @brief Search result containing neighbor IDs and distances.
 * Common for all IVF-PQ instantiations.
 */
struct ivf_pq_search_result_t {
    std::vector<int64_t> neighbors; // Indices of nearest neighbors
    std::vector<float> distances;  // Distances to nearest neighbors
};

/**
 * @brief gpu_ivf_pq_t implements an IVF-PQ index that can run on a single GPU or sharded/replicated across multiple GPUs.
 */
template <typename T>
class gpu_ivf_pq_t : public gpu_index_base_t<T, ivf_pq_build_params_t> {
public:
    using ivf_pq_index = cuvs::neighbors::ivf_pq::index<int64_t>;
    using mg_index = cuvs::neighbors::mg_index<ivf_pq_index, T, int64_t>;
    using search_result_t = ivf_pq_search_result_t;

    // Internal index storage
    std::unique_ptr<ivf_pq_index> index_;
    std::unique_ptr<mg_index> mg_index_;
    std::string data_filename_;

    ~gpu_ivf_pq_t() override {
        this->destroy();
    }

    // Unified Constructor for building from dataset
    gpu_ivf_pq_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension,
                    distance_type_t m, const ivf_pq_build_params_t& bp,
                    const std::vector<int>& devices, uint32_t nthread, distribution_mode_t mode) {

        this->dimension = dimension;
        this->count = static_cast<uint32_t>(count_vectors);
        this->metric = m;
        this->build_params = bp;
        this->dist_mode = mode;
        this->devices_ = devices;
        this->current_offset_ = static_cast<uint32_t>(count_vectors);

        std::vector<int> worker_devices = this->devices_;
        if (mode == DistributionMode_SINGLE_GPU && !worker_devices.empty()) {
            worker_devices = {worker_devices[0]};
        }
        this->worker = std::make_unique<cuvs_worker_t>(nthread, worker_devices, mode);

        this->flattened_host_dataset.resize(this->count * this->dimension);
        if (dataset_data) {
            std::copy(dataset_data, dataset_data + (this->count * this->dimension), this->flattened_host_dataset.begin());
        }
    }

    // Constructor for chunked input (pre-allocates)
    gpu_ivf_pq_t(uint64_t total_count, uint32_t dimension, distance_type_t m,
                    const ivf_pq_build_params_t& bp, const std::vector<int>& devices,
                    uint32_t nthread, distribution_mode_t mode) {

        this->dimension = dimension;
        this->count = static_cast<uint32_t>(total_count);
        this->metric = m;
        this->build_params = bp;
        this->dist_mode = mode;
        this->devices_ = devices;
        this->current_offset_ = 0;

        std::vector<int> worker_devices = this->devices_;
        if (mode == DistributionMode_SINGLE_GPU && !worker_devices.empty()) {
            worker_devices = {worker_devices[0]};
        }
        this->worker = std::make_unique<cuvs_worker_t>(nthread, worker_devices, mode);

        this->flattened_host_dataset.resize(this->count * this->dimension);
    }

    // Constructor for loading metadata from file (used for tests and data-file builds)
    gpu_ivf_pq_t(const std::string& filename, distance_type_t m,
                    const ivf_pq_build_params_t& bp, const std::vector<int>& devices,
                    uint32_t nthread, distribution_mode_t mode) {

        this->metric = m;
        this->build_params = bp;
        this->dist_mode = mode;
        this->devices_ = devices;
        this->data_filename_ = filename;

        std::vector<int> worker_devices = this->devices_;
        if (mode == DistributionMode_SINGLE_GPU && !worker_devices.empty()) {
            worker_devices = {worker_devices[0]};
        }
        this->worker = std::make_unique<cuvs_worker_t>(nthread, worker_devices, mode);

        this->current_offset_ = 0;
    }

    // Existing constructor from file with dimension
    gpu_ivf_pq_t(const std::string& filename, uint32_t dimension, distance_type_t m,
                    const ivf_pq_build_params_t& bp, const std::vector<int>& devices,
                    uint32_t nthread, distribution_mode_t mode) {

        this->dimension = dimension;
        this->metric = m;
        this->build_params = bp;
        this->dist_mode = mode;
        this->devices_ = devices;

        std::vector<int> worker_devices = this->devices_;
        if (mode == DistributionMode_SINGLE_GPU && !worker_devices.empty()) {
            worker_devices = {worker_devices[0]};
        }
        this->worker = std::make_unique<cuvs_worker_t>(nthread, worker_devices, mode);

        this->current_offset_ = 0;
    }

    void start() override {
        auto init_fn = [](raft_handle_wrapper_t&) -> std::any { return std::any(); };
        auto stop_fn = [&](raft_handle_wrapper_t&) -> std::any {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            index_.reset();
            mg_index_.reset();
            this->quantizer_.reset();
            this->dataset_device_ptr_.reset();
            return std::any();
        };
        this->worker->start(init_fn, stop_fn);
    }

    void build() override {
        if (!this->data_filename_.empty() && this->flattened_host_dataset.empty()) {
            uint64_t rows, cols;
            load_host_matrix<T>(this->data_filename_, this->flattened_host_dataset, rows, cols);
            this->count = static_cast<uint32_t>(rows);
            this->dimension = static_cast<uint32_t>(cols);
        }

        std::cout << "[DEBUG] IVF-PQ build: Starting build count=" << this->count << " dim=" << this->dimension << " metric=" << (int)this->metric << std::endl;

        if (this->count == 0) {
            this->is_loaded_ = true;
            std::cout << "[DEBUG] IVF-PQ build: Empty dataset, build skipped" << std::endl;
            return;
        }
        this->train_quantizer_if_needed();
        if (!this->worker) throw std::runtime_error("Worker not initialized");
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                this->build_internal(handle);
                return std::any();
            }
        );
        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) {
            std::cout << "[DEBUG] IVF-PQ build: Build failed" << std::endl;
            std::rethrow_exception(result_wait.error);
        }
        this->is_loaded_ = true;
        this->flattened_host_dataset.clear();
        this->flattened_host_dataset.shrink_to_fit();
        std::cout << "[DEBUG] IVF-PQ build: Build completed successfully" << std::endl;
    }

    void build_internal(raft_handle_wrapper_t& handle) {
        std::unique_lock<std::shared_mutex> lock(this->mutex_);
        
        std::cout << "[DEBUG] IVF-PQ build_internal: Starting internal build on device=" << handle.get_device_id() << std::endl;
        
        cuvs::neighbors::ivf_pq::index_params index_params;
        index_params.metric = static_cast<cuvs::distance::DistanceType>(this->metric);
        index_params.n_lists = this->build_params.n_lists;
        index_params.pq_dim = this->build_params.m;
        index_params.pq_bits = this->build_params.bits_per_code;

        if (this->dist_mode == DistributionMode_SHARDED || this->dist_mode == DistributionMode_REPLICATED) {
            std::cout << "[DEBUG] IVF-PQ build_internal: Multi-GPU build mode=" << (this->dist_mode == DistributionMode_REPLICATED ? "REPLICATED" : "SHARDED") << std::endl;
            auto mg_res = this->worker->get_mg_resources();
            if (!mg_res) throw std::runtime_error("MG resources not initialized for sharded mode");

            auto dataset_pinned = raft::make_host_matrix<T, int64_t, raft::row_major>(*mg_res, (int64_t)this->count, (int64_t)this->dimension);
            std::copy(this->flattened_host_dataset.begin(), this->flattened_host_dataset.end(), dataset_pinned.data_handle());

            cuvs::neighbors::mg_index_params<cuvs::neighbors::ivf_pq::index_params> mg_params(index_params);
            mg_params.mode = (this->dist_mode == DistributionMode_REPLICATED) ? 
                                cuvs::neighbors::distribution_mode::REPLICATED : 
                                cuvs::neighbors::distribution_mode::SHARDED;

            mg_index_.reset(new mg_index(cuvs::neighbors::ivf_pq::build(
                *mg_res, mg_params, raft::make_const_mdspan(dataset_pinned.view()))));
            
            using dataset_t = raft::host_matrix<T, int64_t, raft::row_major>;
            this->dataset_device_ptr_ = std::make_shared<dataset_t>(std::move(dataset_pinned));
            handle.sync(true);
        } else {
            std::cout << "[DEBUG] IVF-PQ build_internal: Single-GPU build" << std::endl;
            auto res = handle.get_raft_resources();
            auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
            RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device.data_handle(), this->flattened_host_dataset.data(),
                                        this->flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                        raft::resource::get_cuda_stream(*res)));

            index_.reset(new ivf_pq_index(cuvs::neighbors::ivf_pq::build(
                *res, index_params, raft::make_const_mdspan(dataset_device.view()))));
            
            using dataset_t = raft::device_matrix<T, int64_t>;
            this->dataset_device_ptr_ = std::make_shared<dataset_t>(std::move(dataset_device));
            handle.sync();
        }
        std::cout << "[DEBUG] IVF-PQ build_internal: Completed internal build" << std::endl;
    }

    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const ivf_pq_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (!this->is_loaded_ || (!index_ && !mg_index_)) return search_result_t{};

        std::cout << "[DEBUG] IVF-PQ search: num_queries=" << num_queries << " limit=" << limit << " n_probes=" << sp.n_probes << std::endl;

        if (num_queries > 16 || !this->worker->use_batching()) {
            auto task = [this, num_queries, limit, sp, queries_data](raft_handle_wrapper_t& handle) -> std::any {
                return this->search_internal(handle, queries_data, num_queries, limit, sp);
            };
            if (!this->worker) throw std::runtime_error("Worker not initialized");
            uint64_t job_id = this->worker->submit(task);
            auto result_wait = this->worker->wait(job_id).get();
            if (result_wait.error) std::rethrow_exception(result_wait.error);
            return std::any_cast<search_result_t>(result_wait.result);
        }

        return this->search_batch_internal(queries_data, num_queries, limit, sp);
    }

    search_result_t search_batch_internal(const T* queries_data, uint64_t num_queries, uint32_t limit, const ivf_pq_search_params_t& sp) {
        struct search_req_t { const T* data; uint64_t n; };
        std::string batch_key = "ivf_pq_s_" + std::to_string((uintptr_t)this) + "_" + std::to_string(limit);

        auto exec_fn = [this, limit, sp](raft_handle_wrapper_t& handle, const std::vector<std::any>& reqs, const std::vector<std::function<void(std::any)>>& setters) {
            uint64_t total_queries = 0;
            for (const auto& r_any : reqs) total_queries += std::any_cast<search_req_t>(r_any).n;

            std::vector<T> aggregated_queries(total_queries * this->dimension);
            uint64_t offset = 0;
            for (const auto& r_any : reqs) {
                auto req = std::any_cast<search_req_t>(r_any);
                std::copy(req.data, req.data + (req.n * this->dimension), aggregated_queries.begin() + (offset * this->dimension));
                offset += req.n;
            }

            auto results = this->search_internal(handle, aggregated_queries.data(), total_queries, limit, sp);

            offset = 0;
            for (size_t i = 0; i < reqs.size(); ++i) {
                auto req = std::any_cast<search_req_t>(reqs[i]);
                search_result_t individual_res;
                individual_res.neighbors.resize(req.n * limit);
                individual_res.distances.resize(req.n * limit);
                std::copy(results.neighbors.begin() + (offset * limit), results.neighbors.begin() + ((offset + req.n) * limit), individual_res.neighbors.begin());
                std::copy(results.distances.begin() + (offset * limit), results.distances.begin() + ((offset + req.n) * limit), individual_res.distances.begin());
                setters[i](individual_res);
                offset += req.n;
            }
        };

        if (!this->worker) throw std::runtime_error("Worker not initialized");
        auto future = this->worker->template submit_batched<search_result_t>(batch_key, search_req_t{queries_data, num_queries}, exec_fn);
        return future.get();
    }

    search_result_t search_internal(raft_handle_wrapper_t& handle, const T* queries_data, uint64_t num_queries, uint32_t limit, const ivf_pq_search_params_t& sp) {
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
        
        std::cout << "[DEBUG] IVF-PQ search_internal: num_queries=" << num_queries << " limit=" << limit << " device=" << handle.get_device_id() << std::endl;

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::ivf_pq::search_params search_params;
        search_params.n_probes = sp.n_probes;

        auto res = handle.get_raft_resources();

        if (this->dist_mode == DistributionMode_SHARDED && is_snmg_handle(*res) && mg_index_) {
            auto q_host = raft::make_host_matrix<T, int64_t, raft::row_major>(*res, (int64_t)num_queries, (int64_t)this->dimension);
            raft::copy(*res, q_host.view(), raft::make_host_matrix_view<const T, int64_t, raft::row_major>(queries_data, num_queries, this->dimension));
            auto n_host = raft::make_host_matrix<int64_t, int64_t, raft::row_major>(*res, (int64_t)num_queries, (int64_t)limit);
            auto d_host = raft::make_host_matrix<float, int64_t, raft::row_major>(*res, (int64_t)num_queries, (int64_t)limit);

            cuvs::neighbors::mg_search_params<cuvs::neighbors::ivf_pq::search_params> mg_search_params(search_params);
            
            auto mg_res = this->worker->get_mg_resources();
            cuvs::neighbors::ivf_pq::search(*mg_res, *mg_index_, mg_search_params, q_host.view(), n_host.view(), d_host.view());
            
            handle.sync(true);

            std::copy(n_host.data_handle(), n_host.data_handle() + (num_queries * limit), search_res.neighbors.begin());
            std::copy(d_host.data_handle(), d_host.data_handle() + (num_queries * limit), search_res.distances.begin());
        } else {
            const ivf_pq_index* local_index = index_.get();
            if (!local_index && mg_index_) {
                int current_device;
                RAFT_CUDA_TRY(cudaGetDevice(&current_device));
                for (size_t i = 0; i < this->devices_.size(); ++i) {
                    if (this->devices_[i] == current_device && i < mg_index_->ann_interfaces_.size()) {
                        if (mg_index_->ann_interfaces_[i].index_.has_value()) {
                            local_index = &mg_index_->ann_interfaces_[i].index_.value();
                            break;
                        }
                    }
                }
            }

            if (local_index) {
                auto queries_device = raft::make_device_matrix<T, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));
                raft::copy(*res, queries_device.view(), raft::make_host_matrix_view<const T, int64_t>(queries_data, num_queries, this->dimension));

                auto neighbors_device_internal = raft::make_device_matrix<int64_t, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
                auto distances_device_internal = raft::make_device_matrix<float, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

                cuvs::neighbors::ivf_pq::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(queries_device.view()), 
                                                    neighbors_device_internal.view(), distances_device_internal.view());

                raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device_internal.view());
                raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device_internal.view());
            } else {
                throw std::runtime_error("Index not loaded or failed to find local index shard for current device.");
            }
            handle.sync();
        }

        return search_res;
    }

    search_result_t search_float(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const ivf_pq_search_params_t& sp) {
        if constexpr (std::is_same_v<T, float>) return search(queries_data, num_queries, query_dimension, limit, sp);
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (!this->is_loaded_ || (!index_ && !mg_index_)) return search_result_t{};

        std::cout << "[DEBUG] IVF-PQ search_float: num_queries=" << num_queries << " limit=" << limit << std::endl;

        if (num_queries > 16 || !this->worker->use_batching()) {
            auto task = [this, num_queries, query_dimension, limit, sp, queries_data](raft_handle_wrapper_t& handle) -> std::any {
                return this->search_float_internal(handle, queries_data, num_queries, query_dimension, limit, sp);
            };
            if (!this->worker) throw std::runtime_error("Worker not initialized");
            uint64_t job_id = this->worker->submit(task);
            auto result_wait = this->worker->wait(job_id).get();
            if (result_wait.error) std::rethrow_exception(result_wait.error);
            return std::any_cast<search_result_t>(result_wait.result);
        }

        return this->search_float_batch_internal(queries_data, num_queries, limit, sp);
    }

    search_result_t search_float_batch_internal(const float* queries_data, uint64_t num_queries, uint32_t limit, const ivf_pq_search_params_t& sp) {
        struct search_req_t { const float* data; uint64_t n; };
        std::string batch_key = "ivf_pq_sf_" + std::to_string((uintptr_t)this) + "_" + std::to_string(limit);

        auto exec_fn = [this, limit, sp](raft_handle_wrapper_t& handle, const std::vector<std::any>& reqs, const std::vector<std::function<void(std::any)>>& setters) {
            uint64_t total_queries = 0;
            for (const auto& r_any : reqs) total_queries += std::any_cast<search_req_t>(r_any).n;

            std::vector<float> aggregated_queries(total_queries * this->dimension);
            uint64_t offset = 0;
            for (const auto& r_any : reqs) {
                auto req = std::any_cast<search_req_t>(r_any);
                std::copy(req.data, req.data + (req.n * this->dimension), aggregated_queries.begin() + (offset * this->dimension));
                offset += req.n;
            }

            auto results = this->search_float_internal(handle, aggregated_queries.data(), total_queries, this->dimension, limit, sp);

            offset = 0;
            for (size_t i = 0; i < reqs.size(); ++i) {
                auto req = std::any_cast<search_req_t>(reqs[i]);
                search_result_t individual_res;
                individual_res.neighbors.resize(req.n * limit);
                individual_res.distances.resize(req.n * limit);
                std::copy(results.neighbors.begin() + (offset * limit), results.neighbors.begin() + ((offset + req.n) * limit), individual_res.neighbors.begin());
                std::copy(results.distances.begin() + (offset * limit), results.distances.begin() + ((offset + req.n) * limit), individual_res.distances.begin());
                setters[i](individual_res);
                offset += req.n;
            }
        };

        if (!this->worker) throw std::runtime_error("Worker not initialized");
        auto future = this->worker->template submit_batched<search_result_t>(batch_key, search_req_t{queries_data, num_queries}, exec_fn);
        return future.get();
    }

    search_result_t search_float_internal(raft_handle_wrapper_t& handle, const float* queries_data, uint64_t num_queries, uint32_t query_dimension, 
                        uint32_t limit, const ivf_pq_search_params_t& sp) {
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        std::cout << "[DEBUG] IVF-PQ search_float_internal: num_queries=" << num_queries << " limit=" << limit << " device=" << handle.get_device_id() << std::endl;

        auto q_dev_t = raft::make_device_matrix<T, int64_t>(*res, num_queries, this->dimension);
        
        if constexpr (sizeof(T) == 1) {
            auto q_dev_f = raft::make_device_matrix<float, int64_t>(*res, num_queries, this->dimension);
            raft::copy(*res, q_dev_f.view(), raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, this->dimension));
            
            if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
            this->quantizer_.template transform<T>(*res, q_dev_f.view(), q_dev_t.data_handle(), true);
        } else {
            raft::copy(*res, q_dev_t.view(), raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, this->dimension));
        }
        raft::resource::sync_stream(*res);

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::ivf_pq::search_params search_params;
        search_params.n_probes = sp.n_probes;

        if (this->dist_mode == DistributionMode_SHARDED && is_snmg_handle(*res) && mg_index_) {
            auto q_host = raft::make_host_matrix<T, int64_t, raft::row_major>(*res, (int64_t)num_queries, (int64_t)this->dimension);
            raft::copy(*res, q_host.view(), q_dev_t.view());
            auto n_host = raft::make_host_matrix<int64_t, int64_t, raft::row_major>(*res, (int64_t)num_queries, (int64_t)limit);
            auto d_host = raft::make_host_matrix<float, int64_t, raft::row_major>(*res, (int64_t)num_queries, (int64_t)limit);

            cuvs::neighbors::mg_search_params<cuvs::neighbors::ivf_pq::search_params> mg_search_params(search_params);
            
            auto mg_res = this->worker->get_mg_resources();
            cuvs::neighbors::ivf_pq::search(*mg_res, *mg_index_, mg_search_params, q_host.view(), n_host.view(), d_host.view());
            
            handle.sync(true);

            std::copy(n_host.data_handle(), n_host.data_handle() + (num_queries * limit), search_res.neighbors.begin());
            std::copy(d_host.data_handle(), d_host.data_handle() + (num_queries * limit), search_res.distances.begin());
        } else {
            const ivf_pq_index* local_index = index_.get();
            if (!local_index && mg_index_) {
                int current_device;
                RAFT_CUDA_TRY(cudaGetDevice(&current_device));
                for (size_t i = 0; i < this->devices_.size(); ++i) {
                    if (this->devices_[i] == current_device && i < mg_index_->ann_interfaces_.size()) {
                        if (mg_index_->ann_interfaces_[i].index_.has_value()) {
                            local_index = &mg_index_->ann_interfaces_[i].index_.value();
                            break;
                        }
                    }
                }
            }

            if (local_index) {
                auto neighbors_device = raft::make_device_matrix<int64_t, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
                auto distances_device = raft::make_device_matrix<float, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

                cuvs::neighbors::ivf_pq::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(q_dev_t.view()), 
                                                    neighbors_device.view(), distances_device.view());

                raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device.view());
                raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device.view());
            } else {
                throw std::runtime_error("Index not loaded or failed to find local index shard for current device.");
            }
            handle.sync();
        }

        return search_res;
    }

    std::vector<T> get_centers() {
        if (!this->is_loaded_ || (!index_ && !mg_index_)) return {};

        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto res = handle.get_raft_resources();

                const ivf_pq_index* local_index = nullptr;
                if (index_) {
                    local_index = index_.get();
                } else if (mg_index_) {
                    for (const auto& iface : mg_index_->ann_interfaces_) {
                        if (iface.index_.has_value()) {
                            local_index = &iface.index_.value();
                            break;
                        }
                    }
                }

                if (!local_index) return std::vector<T>{};

                auto centers_view = local_index->centers();
                size_t n_centers = centers_view.extent(0);
                size_t dim = centers_view.extent(1);

                auto centers_device_target = raft::make_device_matrix<T, int64_t>(*res, n_centers, dim);
                if constexpr (sizeof(T) == 1) {
                    if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
                    auto centers_float_view = raft::make_device_matrix_view<const float, int64_t>(centers_view.data_handle(), n_centers, dim);
                    this->quantizer_.template transform<T>(*res, centers_float_view, centers_device_target.data_handle(), true);
                } else {
                    raft::copy(*res, centers_device_target.view(), centers_view);
                }

                std::vector<T> host_centers(n_centers * dim);
                raft::copy(*res, raft::make_host_matrix_view<T, int64_t>(host_centers.data(), n_centers, dim), centers_device_target.view());
                raft::resource::sync_stream(*res);
                return host_centers;
            }
        );
        auto result = this->worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<std::vector<T>>(result.result);
    }

    std::string info() const override {
        std::string json = gpu_index_base_t<T, ivf_pq_build_params_t>::info();
        json += ", \"type\": \"IVF-PQ\", \"ivf_pq\": {";
        if (index_) json += "\"mode\": \"Single-GPU\", \"size\": " + std::to_string(index_->size());
        else if (mg_index_) json += "\"mode\": \"Multi-GPU\", \"ranks\": " + std::to_string(mg_index_->ann_interfaces_.size());
        else json += "\"built\": false";
        json += "}}";
        return json;
    }

    void save(const std::string& filename) const {
        if (!this->is_loaded_ || (!index_ && !mg_index_)) throw std::runtime_error("Index not built");
        if (mg_index_) throw std::runtime_error("Saving multi-GPU index not supported yet");
        
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                cuvs::neighbors::ivf_pq::serialize(*(handle.get_raft_resources()), filename, *index_);
                return std::any();
            }
        );
        this->worker->wait(job_id).get();
    }

    void load(const std::string& filename) {
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                index_.reset(new ivf_pq_index(*res));
                cuvs::neighbors::ivf_pq::deserialize(*res, filename, index_.get());
                this->count = static_cast<uint32_t>(index_->size());
                this->dimension = static_cast<uint32_t>(index_->dim());
                return std::any();
            }
        );
        auto res = this->worker->wait(job_id).get();
        if (res.error) std::rethrow_exception(res.error);
        this->is_loaded_ = true;
        this->train_quantizer_if_needed();
    }

    void destroy() override {
        if (this->worker) this->worker->stop();
        std::unique_lock<std::shared_mutex> lock(this->mutex_);
        index_.reset();
        mg_index_.reset();
        this->quantizer_.reset();
        this->dataset_device_ptr_.reset();
    }

    uint32_t get_dim() const { return this->dimension; }
    uint32_t get_rot_dim() const { return this->dimension; } 
    uint32_t get_dim_ext() const { return this->dimension; } 
    uint32_t get_n_list() const { return this->build_params.n_lists; }
};

} // namespace matrixone
