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
class gpu_ivf_pq_t : public gpu_index_base_t<T, ivf_pq_build_params_t, int64_t> {
public:
    using ivf_pq_index = cuvs::neighbors::ivf_pq::index<int64_t>;
    using search_result_t = ivf_pq_search_result_t;

    // Internal index storage
    std::unique_ptr<ivf_pq_index> index_;
    std::string data_filename_;

    ~gpu_ivf_pq_t() override {
        this->destroy();
    }

    // Unified Constructor for building from dataset
    gpu_ivf_pq_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension,
                    distance_type_t m, const ivf_pq_build_params_t& bp,
                    const std::vector<int>& devices, uint32_t nthread, distribution_mode_t mode,
                    const int64_t* ids = nullptr) {

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

        if (ids) {
            this->host_ids.resize(this->count);
            std::copy(ids, ids + this->count, this->host_ids.begin());
        }
    }

    // Constructor for chunked input (pre-allocates)
    gpu_ivf_pq_t(uint64_t total_count, uint32_t dimension, distance_type_t m,
                    const ivf_pq_build_params_t& bp, const std::vector<int>& devices,
                    uint32_t nthread, distribution_mode_t mode,
                    const int64_t* ids = nullptr) {

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
        if (ids) {
            this->host_ids.resize(this->count);
            std::copy(ids, ids + this->count, this->host_ids.begin());
        }
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
        auto init_fn = [&](raft_handle_wrapper_t& handle) -> std::any {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (index_) {
                handle.set_index_ptr(static_cast<const ivf_pq_index*>(index_.get()));
            }
            return std::any();
        };
        auto stop_fn = [&](raft_handle_wrapper_t&) -> std::any {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            index_.reset();
            this->replicated_indices_.clear();
            this->replicated_datasets_.clear();
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
            this->current_offset_ = this->count;
        } else {
            this->count = static_cast<uint32_t>(this->current_offset_);
        }

        if (this->count == 0) {
            this->is_loaded_ = true;
            // std::cout << "[DEBUG] IVF-PQ build: Empty dataset, build skipped" << std::endl;
            return;
        }
        if (this->flattened_host_dataset.size() > (size_t)this->count * this->dimension) {
            this->flattened_host_dataset.resize((size_t)this->count * this->dimension);
        }
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
            // Collective build requires participation from all GPUs (REPLICATED or SHARDED)
            this->worker->submit_all_devices([&](raft_handle_wrapper_t& handle) -> std::any {
                this->build_internal(handle);
                return std::any();
            });
        }

        this->is_loaded_ = true;
        this->flattened_host_dataset.clear();
        this->flattened_host_dataset.shrink_to_fit();
        // std::cout << "[DEBUG] IVF-PQ build: Build completed successfully" << std::endl;
    }

    void build_internal(raft_handle_wrapper_t& handle) {
        cuvs::neighbors::ivf_pq::index_params index_params;
        index_params.metric = static_cast<cuvs::distance::DistanceType>(this->metric);
        index_params.n_lists = this->build_params.n_lists;
        index_params.pq_dim = this->build_params.m;
        index_params.pq_bits = this->build_params.bits_per_code;

        if (this->dist_mode == DistributionMode_REPLICATED) {
            auto res = handle.get_raft_resources();
            auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
            raft::copy(*res, dataset_device.view(), raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data(), this->count, this->dimension));
            raft::resource::sync_stream(*res);

            auto local_idx = std::make_unique<ivf_pq_index>(cuvs::neighbors::ivf_pq::build(
                *res, index_params, raft::make_const_mdspan(dataset_device.view())));
            
            handle.set_index_ptr(static_cast<const ivf_pq_index*>(local_idx.get()));

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_indices_[handle.get_device_id()] = std::shared_ptr<ivf_pq_index>(std::move(local_idx));
                this->replicated_datasets_[handle.get_device_id()] = std::make_shared<raft::device_matrix<T, int64_t>>(std::move(dataset_device));
            }
            handle.sync();
        } else if (this->dist_mode == DistributionMode_SHARDED) {
            auto res = handle.get_raft_resources();
            int num_shards = this->devices_.size();
            int rank = handle.get_rank();
            
            uint64_t rows_per_shard = this->count / num_shards;
            uint64_t start_row = rank * rows_per_shard;
            uint64_t num_rows = (rank == num_shards - 1) ? (this->count - start_row) : rows_per_shard;

            // std::cout << "[DEBUG] IVF-PQ build SHARDED: rank=" << rank << " start_row=" << start_row << " num_rows=" << num_rows << std::endl;

            auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)num_rows, (int64_t)this->dimension);
            raft::copy(*res, dataset_device.view(), 
                       raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data() + (start_row * this->dimension), num_rows, this->dimension));
            raft::resource::sync_stream(*res);

            auto local_idx = std::make_unique<ivf_pq_index>(cuvs::neighbors::ivf_pq::build(
                *res, index_params, raft::make_const_mdspan(dataset_device.view())));
            
            handle.set_index_ptr(static_cast<const ivf_pq_index*>(local_idx.get()));

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_indices_[handle.get_device_id()] = std::shared_ptr<ivf_pq_index>(std::move(local_idx));
                this->replicated_datasets_[handle.get_device_id()] = std::make_shared<raft::device_matrix<T, int64_t>>(std::move(dataset_device));
            }
            handle.sync();
        } else {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            auto res = handle.get_raft_resources();
            auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
            raft::copy(*res, dataset_device.view(), raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data(), this->count, this->dimension));
            raft::resource::sync_stream(*res);

            index_.reset(new ivf_pq_index(cuvs::neighbors::ivf_pq::build(
                *res, index_params, raft::make_const_mdspan(dataset_device.view()))));
            
            using dataset_t = raft::device_matrix<T, int64_t>;
            this->dataset_device_ptr_ = std::make_shared<dataset_t>(std::move(dataset_device));
            handle.sync();
        }
    }

    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const ivf_pq_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) return search_result_t{};

        if (this->dist_mode == DistributionMode_SHARDED) {
            int num_shards = this->devices_.size();
            std::vector<search_result_t> shard_results(num_shards);

            auto shard_search_task = [this, num_queries, limit, sp, queries_data](raft_handle_wrapper_t& gpu_handle) -> std::any {
                return this->search_internal(gpu_handle, queries_data, num_queries, limit, sp);
            };

            auto job_ids = this->worker->submit_all_devices_no_wait(shard_search_task);

            for (int i = 0; i < num_shards; ++i) {
                auto res = this->worker->wait(job_ids[i]).get();
                if (res.error) std::rethrow_exception(res.error);
                shard_results[i] = std::any_cast<search_result_t>(res.result);
            }

            return this->merge_sharded_results(shard_results, num_queries, limit);
        }

        // std::cout << "[DEBUG] IVF-PQ search: num_queries=" << num_queries << " limit=" << limit << " n_probes=" << sp.n_probes << std::endl;

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
        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::ivf_pq::search_params search_params;
        search_params.n_probes = sp.n_probes;

        auto res = handle.get_raft_resources();

        const ivf_pq_index* local_index = nullptr;
        std::any cached_ptr = handle.get_index_ptr();
        if (cached_ptr.has_value()) {
            if (cached_ptr.type() == typeid(const ivf_pq_index*)) {
                local_index = std::any_cast<const ivf_pq_index*>(cached_ptr);
            } else if (cached_ptr.type() == typeid(ivf_pq_index*)) {
                local_index = std::any_cast<ivf_pq_index*>(cached_ptr);
            } else {
                handle.set_index_ptr(std::any());
            }
        }
        
        if (!local_index) {
            if (!this->replicated_indices_.empty()) {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_device_id());
                if (it != this->replicated_indices_.end()) {
                    auto shared_idx = std::static_pointer_cast<ivf_pq_index>(it->second);
                    local_index = shared_idx.get();
                }
            }
            if (!local_index) {
                local_index = index_.get();
            }
            if (local_index) handle.set_index_ptr(static_cast<const ivf_pq_index*>(local_index));
        }

        if (local_index) {
            auto queries_device = raft::make_device_matrix<T, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));
            raft::copy(*res, queries_device.view(), raft::make_host_matrix_view<const T, int64_t>(queries_data, num_queries, this->dimension));
            raft::resource::sync_stream(*res);

            auto neighbors_device_internal = raft::make_device_matrix<int64_t, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
            auto distances_device_internal = raft::make_device_matrix<float, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

            cuvs::neighbors::ivf_pq::search(*res, search_params, *local_index,
                                                raft::make_const_mdspan(queries_device.view()), 
                                                neighbors_device_internal.view(), distances_device_internal.view());

            raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device_internal.view());
            raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device_internal.view());
        } else {
            std::string msg = "IVF-PQ search error: No valid index found for device " + std::to_string(handle.get_device_id()) + 
                             " (Mode: " + mode_name(this->dist_mode) + ")";
            throw std::runtime_error(msg);
        }
        handle.sync();

        if (!this->host_ids.empty()) {
            for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
                if (search_res.neighbors[i] != -1) {
                    search_res.neighbors[i] = (int64_t)this->host_ids[search_res.neighbors[i]];
                }
            }
        } else if (this->dist_mode == DistributionMode_SHARDED) {
            int num_shards = this->devices_.size();
            uint64_t rows_per_shard = this->count / num_shards;
            int64_t offset = (int64_t)(handle.get_rank() * rows_per_shard);
            for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
                if (search_res.neighbors[i] != -1) {
                    search_res.neighbors[i] += offset;
                }
            }
        }

        return search_res;
    }

    search_result_t search_float(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const ivf_pq_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) return search_result_t{};

        if (this->dist_mode == DistributionMode_SHARDED) {
            int num_shards = this->devices_.size();
            std::vector<search_result_t> shard_results(num_shards);

            auto shard_search_task = [this, num_queries, limit, sp, queries_data](raft_handle_wrapper_t& gpu_handle) -> std::any {
                return this->search_float_internal(gpu_handle, queries_data, num_queries, this->dimension, limit, sp);
            };

            auto job_ids = this->worker->submit_all_devices_no_wait(shard_search_task);

            for (int i = 0; i < num_shards; ++i) {
                auto res = this->worker->wait(job_ids[i]).get();
                if (res.error) std::rethrow_exception(res.error);
                shard_results[i] = std::any_cast<search_result_t>(res.result);
            }

            return this->merge_sharded_results(shard_results, num_queries, limit);
        }

        // std::cout << "[DEBUG] IVF-PQ search_float: num_queries=" << num_queries << " limit=" << limit << std::endl;

        if (!this->worker->use_batching()) {
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
        auto res = handle.get_raft_resources();
        auto q_dev_t = raft::make_device_matrix<T, int64_t>(*res, num_queries, this->dimension);
        
        if constexpr (std::is_same_v<T, float>) {
            raft::copy(*res, q_dev_t.view(), raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, this->dimension));
        } else {
            auto q_dev_f = raft::make_device_matrix<float, int64_t>(*res, num_queries, this->dimension);
            raft::copy(*res, q_dev_f.view(), raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, this->dimension));
            
            if constexpr (sizeof(T) == 1) {
                if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
                this->quantizer_.template transform<T>(*res, q_dev_f.view(), q_dev_t.data_handle(), true);
            } else {
                // T is half
                raft::copy(*res, q_dev_t.view(), q_dev_f.view());
            }
        }
        raft::resource::sync_stream(*res);

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::ivf_pq::search_params search_params;
        search_params.n_probes = sp.n_probes;

        const ivf_pq_index* local_index = nullptr;
        std::any cached_ptr = handle.get_index_ptr();
        if (cached_ptr.has_value()) {
            if (cached_ptr.type() == typeid(const ivf_pq_index*)) {
                local_index = std::any_cast<const ivf_pq_index*>(cached_ptr);
            } else if (cached_ptr.type() == typeid(ivf_pq_index*)) {
                local_index = std::any_cast<ivf_pq_index*>(cached_ptr);
            } else {
                handle.set_index_ptr(std::any());
            }
        }
        
        if (!local_index) {
            if (!this->replicated_indices_.empty()) {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_device_id());
                if (it != this->replicated_indices_.end()) {
                    auto shared_idx = std::static_pointer_cast<ivf_pq_index>(it->second);
                    local_index = shared_idx.get();
                }
            }
            if (!local_index) {
                local_index = index_.get();
            }
            if (local_index) handle.set_index_ptr(static_cast<const ivf_pq_index*>(local_index));
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
            std::string msg = "IVF-PQ search error: No valid index found for device " + std::to_string(handle.get_device_id()) + 
                             " (Mode: " + mode_name(this->dist_mode) + ")";
            throw std::runtime_error(msg);
        }
        handle.sync();

        if (!this->host_ids.empty()) {
            for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
                if (search_res.neighbors[i] != -1) {
                    search_res.neighbors[i] = (int64_t)this->host_ids[search_res.neighbors[i]];
                }
            }
        } else if (this->dist_mode == DistributionMode_SHARDED) {
            int num_shards = this->devices_.size();
            uint64_t rows_per_shard = this->count / num_shards;
            int64_t offset = (int64_t)(handle.get_rank() * rows_per_shard);
            for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
                if (search_res.neighbors[i] != -1) {
                    search_res.neighbors[i] += offset;
                }
            }
        }

        return search_res;
    }

    std::vector<T> get_centers() {
        if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) return {};

        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                const ivf_pq_index* local_index = nullptr;
                if (!this->replicated_indices_.empty()) {
                    auto it = this->replicated_indices_.find(handle.get_device_id());
                    if (it != this->replicated_indices_.end()) {
                        local_index = std::static_pointer_cast<ivf_pq_index>(it->second).get();
                    }
                } else if (index_) {
                    local_index = index_.get();
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
        std::string json = gpu_index_base_t<T, ivf_pq_build_params_t, int64_t>::info();
        json += ", \"type\": \"IVF-PQ\", \"ivf_pq\": {";
        if (index_) json += "\"mode\": \"Single-GPU\", \"size\": " + std::to_string(index_->size());
        else if (!this->replicated_indices_.empty()) json += "\"mode\": \"Local-Indices\", \"ranks\": " + std::to_string(this->replicated_indices_.size());
        else json += "\"built\": false";
        json += "}}";
        return json;
    }

    void save(const std::string& filename) const {
        if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) throw std::runtime_error("Index not built");
        
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                cuvs::neighbors::ivf_pq::serialize(*(handle.get_raft_resources()), filename, *index_);
                return std::any();
            }
        );
        this->worker->wait(job_id).get();
        if (!this->host_ids.empty()) {
            this->save_ids(filename + ".ids");
        }
    }

    void load(const std::string& filename) {
        auto task = [&](raft_handle_wrapper_t& handle) -> std::any {
            auto res = handle.get_raft_resources();
            auto local_idx = std::make_unique<ivf_pq_index>(*res);
            cuvs::neighbors::ivf_pq::deserialize(*res, filename, local_idx.get());
            
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->count = static_cast<uint32_t>(local_idx->size());
                this->dimension = static_cast<uint32_t>(local_idx->dim());
                
                if (this->dist_mode == DistributionMode_SINGLE_GPU) {
                    index_ = std::move(local_idx);
                } else {
                    this->replicated_indices_[handle.get_device_id()] = std::shared_ptr<ivf_pq_index>(std::move(local_idx));
                }
            }
            return std::any();
        };

        if (this->dist_mode == DistributionMode_SINGLE_GPU) {
            uint64_t job_id = this->worker->submit_main(task);
            auto res = this->worker->wait(job_id).get();
            if (res.error) std::rethrow_exception(res.error);
        } else {
            this->worker->submit_all_devices(task);
        }

        try {
            this->load_ids(filename + ".ids");
        } catch (...) {}

        this->is_loaded_ = true;
        this->train_quantizer_if_needed();
    }

    search_result_t merge_sharded_results(const std::vector<search_result_t>& shard_results, uint64_t num_queries, uint32_t limit) {
        search_result_t global_res;
        global_res.neighbors.resize(num_queries * limit);
        global_res.distances.resize(num_queries * limit);

        for (uint64_t q = 0; q < num_queries; ++q) {
            std::vector<std::pair<float, int64_t>> candidates;
            for (const auto& sr : shard_results) {
                for (uint32_t k = 0; k < limit; ++k) {
                    int64_t id = sr.neighbors[q * limit + k];
                    if (id != -1) {
                        candidates.push_back({sr.distances[q * limit + k], id});
                    }
                }
            }

            uint32_t to_sort = std::min((uint32_t)limit, (uint32_t)candidates.size());
            std::partial_sort(candidates.begin(), candidates.begin() + to_sort, candidates.end());

            for (uint32_t k = 0; k < limit; ++k) {
                if (k < to_sort) {
                    global_res.neighbors[q * limit + k] = candidates[k].second;
                    global_res.distances[q * limit + k] = candidates[k].first;
                } else {
                    global_res.neighbors[q * limit + k] = -1;
                    global_res.distances[q * limit + k] = std::numeric_limits<float>::max();
                }
            }
        }
        return global_res;
    }

    void destroy() override {
        if (this->worker) this->worker->stop();
        std::unique_lock<std::shared_mutex> lock(this->mutex_);
        index_.reset();
        this->replicated_indices_.clear();
        this->replicated_datasets_.clear();
        this->quantizer_.reset();
        this->dataset_device_ptr_.reset();
    }

    uint32_t get_dim() const { return this->dimension; }
    uint32_t get_rot_dim() const { return this->dimension; } 
    uint32_t get_dim_ext() const { return this->dimension; } 
    uint32_t get_n_list() const { return this->build_params.n_lists; }
};

} // namespace matrixone
