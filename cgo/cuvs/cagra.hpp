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
 * CAGRA Index Implementation
 * Supported data types (T): float, half, int8_t, uint8_t
 * Neighbor ID type: uint32_t
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
#include <cuvs/neighbors/cagra.hpp>
#pragma GCC diagnostic pop


namespace matrixone {

/**
 * @brief Search result containing neighbor IDs and distances.
 * Common for all CAGRA instantiations.
 */
struct cagra_search_result_t {
    std::vector<uint32_t> neighbors; // Indices of nearest neighbors
    std::vector<float> distances;  // Distances to nearest neighbors
};

/**
 * @brief gpu_cagra_t implements a CAGRA index that can run on a single GPU or sharded across multiple GPUs.
 */
template <typename T>
class gpu_cagra_t : public gpu_index_base_t<T, cagra_build_params_t> {
public:
    using cagra_index = cuvs::neighbors::cagra::index<T, uint32_t>;
    using mg_index = cuvs::neighbors::mg_index<cagra_index, T, uint32_t>;
    using search_result_t = cagra_search_result_t;

    // Internal index storage
    std::unique_ptr<cagra_index> index_;
    std::unique_ptr<mg_index> mg_index_;

    ~gpu_cagra_t() override {
        this->destroy();
    }

    // Unified Constructor for building from dataset
    gpu_cagra_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension,
                    distance_type_t m, const cagra_build_params_t& bp,
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
    gpu_cagra_t(uint64_t total_count, uint32_t dimension, distance_type_t m,
                    const cagra_build_params_t& bp, const std::vector<int>& devices,
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

    // Constructor for loading from file
    gpu_cagra_t(const std::string& filename, uint32_t dimension, distance_type_t m,
                    const cagra_build_params_t& bp, const std::vector<int>& devices,
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

    // Private constructor for creating from an existing cuVS index (used by merge)
    gpu_cagra_t(std::unique_ptr<cagra_index> idx, uint32_t dimension, distance_type_t m, uint32_t nthread, const std::vector<int>& devs)
        : index_(std::move(idx)) {
        this->dimension = dimension;
        this->metric = m;
        this->dist_mode = DistributionMode_SINGLE_GPU;
        this->devices_ = devs;
        
        std::vector<int> worker_devices = this->devices_;
        if (!worker_devices.empty()) {
            worker_devices = {worker_devices[0]};
        }
        this->worker = std::make_unique<cuvs_worker_t>(nthread, worker_devices, DistributionMode_SINGLE_GPU);
        
        this->count = static_cast<uint32_t>(index_->size());
        this->build_params.graph_degree = static_cast<size_t>(index_->graph_degree());
        this->is_loaded_ = true;
    }

    void start() override {
        auto init_fn = [&](raft_handle_wrapper_t& handle) -> std::any {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (index_) {
                handle.set_index_ptr(static_cast<const cagra_index*>(index_.get()));
            } else if (mg_index_) {
                int rank = handle.get_rank();
                if (rank < (int)mg_index_->ann_interfaces_.size() && mg_index_->ann_interfaces_[rank].index_.has_value()) {
                    handle.set_index_ptr(static_cast<const cagra_index*>(&mg_index_->ann_interfaces_[rank].index_.value()));
                }
            }
            return std::any();
        };
        auto stop_fn = [&](raft_handle_wrapper_t&) -> std::any {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            index_.reset();
            mg_index_.reset();
            this->replicated_indices_.clear();
            this->replicated_datasets_.clear();
            this->quantizer_.reset();
            this->dataset_device_ptr_.reset();
            return std::any();
        };
        this->worker->start(init_fn, stop_fn);
    }

    /**
     * @brief Merges multiple CAGRA indices into a single index.
     * Only works for SINGLE_GPU indices.
     */
    static std::unique_ptr<gpu_cagra_t<T>> merge(const std::vector<gpu_index_base_t<T, cagra_build_params_t>*>& base_indices, uint32_t nthread, const std::vector<int>& devs) {
        if (base_indices.empty()) throw std::invalid_argument("base_indices empty");
        
        uint32_t dim = base_indices[0]->dimension;
        distance_type_t m = base_indices[0]->metric;
        
        cuvs_worker_t transient_worker(nthread, devs, DistributionMode_SINGLE_GPU);
        transient_worker.start();
        
        uint64_t job_id = transient_worker.submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                
                std::vector<cagra_index*> cagra_indices;
                for (auto* bi : base_indices) {
                    auto* idx = static_cast<gpu_cagra_t<T>*>(bi);
                    if (!idx->is_loaded_ || !idx->index_) {
                        throw std::runtime_error("One of the indices to merge is not loaded or is a multi-GPU index.");
                    }
                    cagra_indices.push_back(idx->index_.get());
                }
                
                cuvs::neighbors::cagra::index_params index_params;
                auto merged = cuvs::neighbors::cagra::merge(*res, index_params, cagra_indices);
                raft::resource::sync_stream(*res);
                return new cagra_index(std::move(merged));
            }
        );

        auto result = transient_worker.wait(job_id).get();
        if (result.error) {
            transient_worker.stop();
            std::rethrow_exception(result.error);
        }
        
        auto* merged_idx_ptr = std::any_cast<cagra_index*>(result.result);
        std::unique_ptr<cagra_index> merged_idx(merged_idx_ptr);
        transient_worker.stop();
        
        auto new_idx = std::make_unique<gpu_cagra_t<T>>(
            std::move(merged_idx),
            dim, m, nthread, devs
        );
        return new_idx;
    }

    void build() override {
        if (this->count == 0) {
            this->is_loaded_ = true;
            return;
        }

        std::cout << "[DEBUG] CAGRA build: Starting build count=" << this->count << " dim=" << this->dimension << " metric=" << (int)this->metric << std::endl;

        this->train_quantizer_if_needed();
        if (!this->worker) throw std::runtime_error("Worker not initialized");

        if (this->dist_mode == DistributionMode_SINGLE_GPU) {
            uint64_t job_id = this->worker->submit_main(
                [&](raft_handle_wrapper_t& handle) -> std::any {
                    this->build_internal(handle);
                    return std::any();
                }
            );
            auto result_wait = this->worker->wait(job_id).get();
            if (result_wait.error) std::rethrow_exception(result_wait.error);
        } else {
            // Collective build requires participation from all GPUs
            this->worker->run_on_all_devices([&](raft_handle_wrapper_t& handle) -> std::any {
                this->build_internal(handle);
                return std::any();
            });
        }

        this->is_loaded_ = true;
        this->flattened_host_dataset.clear();
        this->flattened_host_dataset.shrink_to_fit();
        std::cout << "[DEBUG] CAGRA build: Build completed successfully" << std::endl;
    }

    void build_internal(raft_handle_wrapper_t& handle) {
        cuvs::neighbors::cagra::index_params index_params;
        index_params.metric = static_cast<cuvs::distance::DistanceType>(this->metric);
        index_params.intermediate_graph_degree = this->build_params.intermediate_graph_degree;
        index_params.graph_degree = this->build_params.graph_degree;

        if (this->dist_mode == DistributionMode_REPLICATED) {
            // For REPLICATED mode, we build a complete index on each GPU independently.
            // This is much faster and more robust than SNMG replication for search-heavy workloads.
            auto res = handle.get_raft_resources();
            auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
            RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device.data_handle(), this->flattened_host_dataset.data(),
                                        this->flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                        raft::resource::get_cuda_stream(*res)));

            auto local_idx = std::make_unique<cagra_index>(cuvs::neighbors::cagra::build(
                *res, index_params, raft::make_const_mdspan(dataset_device.view())));
            
            // Immediately cache for this thread
            handle.set_index_ptr(static_cast<const cagra_index*>(local_idx.get()));

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_indices_[handle.get_device_id()] = std::shared_ptr<cagra_index>(std::move(local_idx));
                this->replicated_datasets_[handle.get_device_id()] = std::make_shared<raft::device_matrix<T, int64_t>>(std::move(dataset_device));
            }
            handle.sync();
        } else if (this->dist_mode == DistributionMode_SHARDED) {
            auto mg_res = this->worker->get_mg_resources();
            if (!mg_res) throw std::runtime_error("MG resources not initialized for multi-GPU mode");

            auto dataset_pinned = raft::make_host_matrix<T, int64_t, raft::row_major>(*mg_res, (int64_t)this->count, (int64_t)this->dimension);
            std::copy(this->flattened_host_dataset.begin(), this->flattened_host_dataset.end(), dataset_pinned.data_handle());

            cuvs::neighbors::mg_index_params<cuvs::neighbors::cagra::index_params> mg_params(index_params);
            mg_params.mode = cuvs::neighbors::distribution_mode::SHARDED;

            auto built_mg_index = cuvs::neighbors::cagra::build(*mg_res, mg_params, raft::make_const_mdspan(dataset_pinned.view()));
            
            // Every rank must keep its part of the dataset and index alive
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                using dataset_t = raft::host_matrix<T, int64_t, raft::row_major>;
                this->replicated_datasets_[handle.get_rank()] = std::make_shared<dataset_t>(std::move(dataset_pinned));
                if (handle.get_rank() == 0) {
                    mg_index_.reset(new mg_index(std::move(built_mg_index)));
                } else {
                    this->replicated_indices_[handle.get_rank()] = std::make_shared<mg_index>(std::move(built_mg_index));
                }
            }
            handle.sync(true);
        } else {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            auto res = handle.get_raft_resources();
            auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
            RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device.data_handle(), this->flattened_host_dataset.data(),
                                        this->flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                        raft::resource::get_cuda_stream(*res)));

            index_.reset(new cagra_index(cuvs::neighbors::cagra::build(
                *res, index_params, raft::make_const_mdspan(dataset_device.view()))));
            
            using dataset_t = raft::device_matrix<T, int64_t>;
            this->dataset_device_ptr_ = std::make_shared<dataset_t>(std::move(dataset_device));
            handle.sync();
        }
    }

    void extend(const T* additional_data, uint64_t num_vectors) {
        if (!this->is_loaded_ || !index_) {
            uint64_t old_size = this->flattened_host_dataset.size();
            this->flattened_host_dataset.resize(old_size + num_vectors * this->dimension);
            std::copy(additional_data, additional_data + num_vectors * this->dimension, this->flattened_host_dataset.begin() + old_size);
            this->count += static_cast<uint32_t>(num_vectors);
            this->current_offset_ += static_cast<uint32_t>(num_vectors);
            return;
        }

        if (this->dist_mode != DistributionMode_SINGLE_GPU) {
            throw std::runtime_error("CAGRA extend is not supported for multi-GPU indices in cuVS.");
        }

        if constexpr (std::is_same_v<T, half>) {
             throw std::runtime_error("CAGRA single-GPU extend is not supported for float16 (half) by cuVS.");
        } else {
            if (num_vectors == 0) return;

            std::unique_lock<std::shared_mutex> lock(this->mutex_);

            uint64_t job_id = this->worker->submit_main(
                [&, additional_data, num_vectors](raft_handle_wrapper_t& handle) -> std::any {
                    auto res = handle.get_raft_resources();
                    
                    auto additional_dataset_device = raft::make_device_matrix<T, int64_t>(
                        *res, static_cast<int64_t>(num_vectors), static_cast<int64_t>(this->dimension));
                    
                    RAFT_CUDA_TRY(cudaMemcpyAsync(additional_dataset_device.data_handle(), additional_data,
                                            num_vectors * this->dimension * sizeof(T), cudaMemcpyHostToDevice,
                                            raft::resource::get_cuda_stream(*res)));

                    cuvs::neighbors::cagra::extend_params params;
                    cuvs::neighbors::cagra::extend(*res, params, raft::make_const_mdspan(additional_dataset_device.view()), *index_);

                    handle.sync();
                    return std::any();
                }
            );

            auto result = this->worker->wait(job_id).get();
            if (result.error) std::rethrow_exception(result.error);

            this->count = static_cast<uint32_t>(index_->size());
            this->current_offset_ = this->count;
        }
    }

    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const cagra_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (!this->is_loaded_ || (!index_ && !mg_index_ && this->replicated_indices_.empty())) return search_result_t{};

        std::cout << "[DEBUG] CAGRA search: num_queries=" << num_queries << " limit=" << limit << " itopk=" << sp.itopk_size << std::endl;

        if (!this->worker->use_batching()) {
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

    search_result_t search_batch_internal(const T* queries_data, uint64_t num_queries, uint32_t limit, const cagra_search_params_t& sp) {
        struct search_req_t { const T* data; uint64_t n; };
        std::string batch_key = "cagra_s_" + std::to_string((uintptr_t)this) + "_" + std::to_string(limit);

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

    search_result_t search_internal(raft_handle_wrapper_t& handle, const T* queries_data, uint64_t num_queries, uint32_t limit, const cagra_search_params_t& sp) {
        // std::shared_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        // std::cout << "[DEBUG " << get_timestamp() << "] CAGRA search_internal: num_queries=" << num_queries << " limit=" << limit << " device=" << handle.get_device_id() << std::endl;

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::cagra::search_params search_params;
        search_params.itopk_size = sp.itopk_size;

        if (this->dist_mode == DistributionMode_SHARDED && is_snmg_handle(*res) && mg_index_) {
            auto q_host = raft::make_host_matrix<T, int64_t, raft::row_major>(*res, (int64_t)num_queries, (int64_t)this->dimension);
            raft::copy(*res, q_host.view(), raft::make_host_matrix_view<const T, int64_t, raft::row_major>(queries_data, num_queries, this->dimension));
            auto n_host = raft::make_host_matrix<uint32_t, int64_t, raft::row_major>(*res, (int64_t)num_queries, (int64_t)limit);
            auto d_host = raft::make_host_matrix<float, int64_t, raft::row_major>(*res, (int64_t)num_queries, (int64_t)limit);

            cuvs::neighbors::mg_search_params<cuvs::neighbors::cagra::search_params> mg_search_params(search_params);
            
            auto mg_res = this->worker->get_mg_resources();
            cuvs::neighbors::cagra::search(*mg_res, *mg_index_, mg_search_params, q_host.view(), n_host.view(), d_host.view());
            
            handle.sync(true); // Collective sync for SHARDED mode

            std::copy(n_host.data_handle(), n_host.data_handle() + (num_queries * limit), search_res.neighbors.begin());
            std::copy(d_host.data_handle(), d_host.data_handle() + (num_queries * limit), search_res.distances.begin());
        } else {
            const cagra_index* local_index = nullptr;
            std::any cached_ptr = handle.get_index_ptr();
            if (cached_ptr.has_value()) {
                if (cached_ptr.type() == typeid(const cagra_index*)) {
                    local_index = std::any_cast<const cagra_index*>(cached_ptr);
                } else if (cached_ptr.type() == typeid(cagra_index*)) {
                    local_index = std::any_cast<cagra_index*>(cached_ptr);
                } else {
                    handle.set_index_ptr(std::any()); // Clear invalid cache
                }
            }
            
            if (!local_index) {
                // Tiered fallback: Replicated -> Single -> Multi (Sharded)
                if (!this->replicated_indices_.empty()) {
                    std::shared_lock<std::shared_mutex> lock(this->mutex_);
                    auto it = this->replicated_indices_.find(handle.get_device_id());
                    if (it != this->replicated_indices_.end()) {
                        auto shared_idx = std::static_pointer_cast<cagra_index>(it->second);
                        local_index = shared_idx.get();
                    }
                }
                
                if (!local_index) {
                    local_index = index_.get();
                }

                if (!local_index && mg_index_) {
                    int rank = handle.get_rank();
                    if (rank < (int)mg_index_->ann_interfaces_.size() && mg_index_->ann_interfaces_[rank].index_.has_value()) {
                        local_index = &mg_index_->ann_interfaces_[rank].index_.value();
                    }
                }
                if (local_index) handle.set_index_ptr(static_cast<const cagra_index*>(local_index));
            }

            if (local_index) {
                auto queries_device = raft::make_device_matrix<T, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));
                raft::copy(*res, queries_device.view(), raft::make_host_matrix_view<const T, int64_t>(queries_data, num_queries, this->dimension));

                auto neighbors_device = raft::make_device_matrix<uint32_t, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
                auto distances_device = raft::make_device_matrix<float, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

                cuvs::neighbors::cagra::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(queries_device.view()), 
                                                    neighbors_device.view(), distances_device.view());

                raft::copy(*res, raft::make_host_matrix_view<uint32_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device.view());
                raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device.view());
            } else {
                std::string msg = "CAGRA search error: No valid index found for device " + std::to_string(handle.get_device_id()) + 
                                 " (Mode: " + mode_name(this->dist_mode) + ")";
                throw std::runtime_error(msg);
            }
            handle.sync(); // Local sync
        }

        // std::cout << "[DEBUG " << get_timestamp() << "] CAGRA search_internal finished working" << std::endl;
        return search_res;
    }

    search_result_t search_float(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const cagra_search_params_t& sp) {
        if constexpr (std::is_same_v<T, float>) return search(queries_data, num_queries, query_dimension, limit, sp);
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (!this->is_loaded_ || (!index_ && !mg_index_ && this->replicated_indices_.empty())) return search_result_t{};

        std::cout << "[DEBUG] CAGRA search_float: num_queries=" << num_queries << " limit=" << limit << std::endl;

        if (!this->worker->use_batching()) {
            auto task = [this, num_queries, limit, sp, queries_data, query_dimension](raft_handle_wrapper_t& handle) -> std::any {
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

    search_result_t search_float_batch_internal(const float* queries_data, uint64_t num_queries, uint32_t limit, const cagra_search_params_t& sp) {
        struct search_req_t { const float* data; uint64_t n; };
        std::string batch_key = "cagra_sf_" + std::to_string((uintptr_t)this) + "_" + std::to_string(limit);

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
                        uint32_t limit, const cagra_search_params_t& sp) {
        // std::shared_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        // std::cout << "[DEBUG " << get_timestamp() << "] CAGRA search_float_internal: num_queries=" << num_queries << " limit=" << limit << " device=" << handle.get_device_id() << std::endl;

        auto q_dev_t = raft::make_device_matrix<T, int64_t>(*res, num_queries, this->dimension);
        
        if constexpr (sizeof(T) == 1) {
            // For INT8/UINT8, we usually need quantization. 
            // Copy float to device first, then transform.
            auto q_dev_f = raft::make_device_matrix<float, int64_t>(*res, num_queries, this->dimension);
            raft::copy(*res, q_dev_f.view(), raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, this->dimension));
            
            if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
            this->quantizer_.template transform<T>(*res, q_dev_f.view(), q_dev_t.data_handle(), true);
        } else {
            // For float and half, RAFT can copy and convert directly from host float*
            raft::copy(*res, q_dev_t.view(), raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, this->dimension));
        }
        raft::resource::sync_stream(*res);

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::cagra::search_params search_params;
        search_params.itopk_size = sp.itopk_size;

        if (this->dist_mode == DistributionMode_SHARDED && is_snmg_handle(*res) && mg_index_) {
            auto q_host = raft::make_host_matrix<T, int64_t, raft::row_major>(*res, (int64_t)num_queries, (int64_t)this->dimension);
            raft::copy(*res, q_host.view(), q_dev_t.view());
            auto n_host = raft::make_host_matrix<uint32_t, int64_t, raft::row_major>(*res, (int64_t)num_queries, (int64_t)limit);
            auto d_host = raft::make_host_matrix<float, int64_t, raft::row_major>(*res, (int64_t)num_queries, (int64_t)limit);

            cuvs::neighbors::mg_search_params<cuvs::neighbors::cagra::search_params> mg_search_params(search_params);
            
            auto mg_res = this->worker->get_mg_resources();
            cuvs::neighbors::cagra::search(*mg_res, *mg_index_, mg_search_params, q_host.view(), n_host.view(), d_host.view());
            
            handle.sync(true); // Collective sync for SHARDED mode

            std::copy(n_host.data_handle(), n_host.data_handle() + (num_queries * limit), search_res.neighbors.begin());
            std::copy(d_host.data_handle(), d_host.data_handle() + (num_queries * limit), search_res.distances.begin());
        } else {
            const cagra_index* local_index = nullptr;
            std::any cached_ptr = handle.get_index_ptr();
            if (cached_ptr.has_value()) {
                if (cached_ptr.type() == typeid(const cagra_index*)) {
                    local_index = std::any_cast<const cagra_index*>(cached_ptr);
                } else if (cached_ptr.type() == typeid(cagra_index*)) {
                    local_index = std::any_cast<cagra_index*>(cached_ptr);
                } else {
                    handle.set_index_ptr(std::any()); // Clear invalid cache
                }
            }
            
            if (!local_index) {
                // Tiered fallback: Replicated -> Single -> Multi (Sharded)
                if (!this->replicated_indices_.empty()) {
                    std::shared_lock<std::shared_mutex> lock(this->mutex_);
                    auto it = this->replicated_indices_.find(handle.get_device_id());
                    if (it != this->replicated_indices_.end()) {
                        auto shared_idx = std::static_pointer_cast<cagra_index>(it->second);
                        local_index = shared_idx.get();
                    }
                }
                
                if (!local_index) {
                    local_index = index_.get();
                }

                if (!local_index && mg_index_) {
                    int rank = handle.get_rank();
                    if (rank < (int)mg_index_->ann_interfaces_.size() && mg_index_->ann_interfaces_[rank].index_.has_value()) {
                        local_index = &mg_index_->ann_interfaces_[rank].index_.value();
                    }
                }
                if (local_index) handle.set_index_ptr(static_cast<const cagra_index*>(local_index));
            }

            if (local_index) {
                auto neighbors_device = raft::make_device_matrix<uint32_t, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
                auto distances_device = raft::make_device_matrix<float, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

                cuvs::neighbors::cagra::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(q_dev_t.view()), 
                                                    neighbors_device.view(), distances_device.view());

                raft::copy(*res, raft::make_host_matrix_view<uint32_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device.view());
                raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device.view());
            } else {
                std::string msg = "CAGRA search_float error: No valid index found for device " + std::to_string(handle.get_device_id()) + 
                                 " (Mode: " + mode_name(this->dist_mode) + ")";
                throw std::runtime_error(msg);
            }
            handle.sync(); // Local sync
        }

        // std::cout << "[DEBUG " << get_timestamp() << "] CAGRA search_internal finished working" << std::endl;
        return search_res;
    }

    std::string info() const override {
        std::string json = gpu_index_base_t<T, cagra_build_params_t>::info();
        json += ", \"type\": \"CAGRA\", \"cagra\": {";
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
                cuvs::neighbors::cagra::serialize(*(handle.get_raft_resources()), filename, *index_);
                return std::any();
            }
        );
        this->worker->wait(job_id).get();
    }

    void load(const std::string& filename) {
        auto task = [&](raft_handle_wrapper_t& handle) -> std::any {
            auto res = handle.get_raft_resources();
            auto local_idx = std::make_unique<cagra_index>(*res);
            cuvs::neighbors::cagra::deserialize(*res, filename, local_idx.get());
            
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->count = static_cast<uint32_t>(local_idx->size());
                this->dimension = static_cast<uint32_t>(local_idx->dim());
                
                if (this->dist_mode == DistributionMode_SINGLE_GPU) {
                    index_ = std::move(local_idx);
                } else if (this->dist_mode == DistributionMode_REPLICATED) {
                    this->replicated_indices_[handle.get_device_id()] = std::shared_ptr<cagra_index>(std::move(local_idx));
                } else if (this->dist_mode == DistributionMode_SHARDED) {
                    // For SHARDED, each rank would normally load its part.
                    // But MatrixOne's save doesn't support SHARDED yet.
                    throw std::runtime_error("SHARDED mode load is not yet supported in cuVS-MatrixOne");
                }
            }
            return std::any();
        };

        if (this->dist_mode == DistributionMode_SINGLE_GPU) {
            uint64_t job_id = this->worker->submit_main(task);
            auto res = this->worker->wait(job_id).get();
            if (res.error) std::rethrow_exception(res.error);
        } else if (this->dist_mode == DistributionMode_REPLICATED) {
            this->worker->run_on_all_devices(task);
        } else {
            // SHARDED
            this->worker->run_on_all_devices(task);
        }

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
    uint32_t get_n_list() const { return 0; } // CAGRA doesn't have n_lists
};

} // namespace matrixone
