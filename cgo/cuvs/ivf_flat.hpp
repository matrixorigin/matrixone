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
 * IVF-Flat Index Implementation
 * Supported data types (T): float, half, int8_t, uint8_t
 * Neighbor ID type: int64_t
 */

#pragma once

#include "index_base.hpp"
#include <iostream>
#include <numeric>
#include <shared_mutex>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#include <raft/core/bitset.cuh>
#include <raft/core/copy.cuh>
#include <raft/core/device_mdarray.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/device_resources_snmg.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/resources.hpp>

#include <cuvs/distance/distance.hpp>
#include <cuvs/neighbors/ivf_flat.hpp>
#pragma GCC diagnostic pop

namespace matrixone {

/**
 * @brief Search result containing neighbor IDs and distances.
 */
struct ivf_flat_search_result_t {
    std::vector<int64_t> neighbors;
    std::vector<float> distances;
};

/**
 * @brief gpu_ivf_flat_t implements an IVF-Flat index that can run on a single GPU or sharded across multiple GPUs.
 */
template <typename T>
class gpu_ivf_flat_t : public gpu_index_base_t<T, ivf_flat_build_params_t, int64_t> {
public:
    using ivf_flat_index = cuvs::neighbors::ivf_flat::index<T, int64_t>;
    using mg_index = cuvs::neighbors::mg_index<ivf_flat_index, T, int64_t>;
    using search_result_t = ivf_flat_search_result_t;

    std::unique_ptr<ivf_flat_index> index_;
    std::unique_ptr<mg_index> mg_index_;
    std::string data_filename_;

    ~gpu_ivf_flat_t() override {
        destroy();
    }

    // Unified Constructor for building from dataset
    gpu_ivf_flat_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension,
                    distance_type_t m, const ivf_flat_build_params_t& bp,
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
            this->set_ids(ids, this->count);
        }
    }

    // Constructor for chunked input (pre-allocates)
    gpu_ivf_flat_t(uint64_t total_count, uint32_t dimension, distance_type_t m,
                    const ivf_flat_build_params_t& bp, const std::vector<int>& devices,
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
            this->set_ids(ids, this->count);
        }
    }

    // Constructor for loading from file
    gpu_ivf_flat_t(const std::string& filename, uint32_t dimension, distance_type_t m,
                    const ivf_flat_build_params_t& bp, const std::vector<int>& devices,
                    uint32_t nthread, distribution_mode_t mode) {

        this->dimension = dimension;
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

    void start() override {
        auto init_fn = [](raft_handle_wrapper_t&) -> std::any { return std::any(); };
        auto stop_fn = [&](raft_handle_wrapper_t& /*handle*/) -> std::any {
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

    void build() override {
        if (this->is_loaded_) return;
        if (!this->data_filename_.empty()) {
            load(this->data_filename_);
            return;
        }
        this->count = static_cast<uint32_t>(this->current_offset_);
        if (this->count == 0) {
            this->is_loaded_ = true;
            return;
        }
        if (this->flattened_host_dataset.size() > (size_t)this->count * this->dimension) {
            this->flattened_host_dataset.resize((size_t)this->count * this->dimension);
        }

        // std::cout << "[DEBUG] IVF-Flat build: Starting build count=" << this->count << " dim=" << this->dimension << " metric=" << (int)this->metric << std::endl;

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
            // Collective build (SHARDED or REPLICATED)
            this->worker->submit_all_devices([&](raft_handle_wrapper_t& handle) -> std::any {
                this->build_internal(handle);
                return std::any();
            });
        }

        this->is_loaded_ = true;
        this->init_deleted_bitset();
        this->flattened_host_dataset.clear();
        this->flattened_host_dataset.shrink_to_fit();
        // std::cout << "[DEBUG] IVF-Flat build: Build completed successfully" << std::endl;
    }

    void build_internal(raft_handle_wrapper_t& handle) {
        cuvs::neighbors::ivf_flat::index_params index_params;
        index_params.metric = static_cast<cuvs::distance::DistanceType>(this->metric);
        index_params.n_lists = this->build_params.n_lists;

        if (this->dist_mode == DistributionMode_REPLICATED) {
            auto res = handle.get_raft_resources();
            auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
            raft::copy(*res, dataset_device.view(), raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data(), this->count, this->dimension));
            raft::resource::sync_stream(*res);

            auto local_idx = std::make_unique<ivf_flat_index>(cuvs::neighbors::ivf_flat::build(
                *res, index_params, raft::make_const_mdspan(dataset_device.view())));
            
            handle.set_index_ptr(static_cast<const ivf_flat_index*>(local_idx.get()));

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_indices_[handle.get_device_id()] = std::shared_ptr<ivf_flat_index>(std::move(local_idx));
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

            // std::cout << "[DEBUG] IVF-Flat build SHARDED: rank=" << rank << " start_row=" << start_row << " num_rows=" << num_rows << std::endl;

            auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)num_rows, (int64_t)this->dimension);
            raft::copy(*res, dataset_device.view(), 
                       raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data() + (start_row * this->dimension), num_rows, this->dimension));
            raft::resource::sync_stream(*res);

            auto local_idx = std::make_unique<ivf_flat_index>(cuvs::neighbors::ivf_flat::build(
                *res, index_params, raft::make_const_mdspan(dataset_device.view())));
            
            handle.set_index_ptr(static_cast<const ivf_flat_index*>(local_idx.get()));

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_indices_[handle.get_device_id()] = std::shared_ptr<ivf_flat_index>(std::move(local_idx));
                this->replicated_datasets_[handle.get_device_id()] = std::make_shared<raft::device_matrix<T, int64_t>>(std::move(dataset_device));
            }
            handle.sync();
        } else {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            auto res = handle.get_raft_resources();
            auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
            raft::copy(*res, dataset_device.view(), raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data(), this->count, this->dimension));
            raft::resource::sync_stream(*res);

            index_.reset(new ivf_flat_index(cuvs::neighbors::ivf_flat::build(
                *res, index_params, raft::make_const_mdspan(dataset_device.view()))));
            
            using dataset_t = raft::device_matrix<T, int64_t>;
            this->dataset_device_ptr_ = std::make_shared<dataset_t>(std::move(dataset_device));
            handle.sync();
        }
    }

    void extend_internal(raft_handle_wrapper_t& handle, const T* new_data, uint64_t n_rows,
                         const int64_t* seq_ids) {
        auto res = handle.get_raft_resources();

        auto new_vecs_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)n_rows, (int64_t)this->dimension);
        raft::copy(*res, new_vecs_device.view(),
                   raft::make_host_matrix_view<const T, int64_t>(new_data, n_rows, this->dimension));

        auto ids_device = raft::make_device_vector<int64_t, int64_t>(*res, (int64_t)n_rows);
        raft::copy(*res, ids_device.view(),
                   raft::make_host_vector_view<const int64_t, int64_t>(seq_ids, (int64_t)n_rows));
        raft::resource::sync_stream(*res);
        auto indices_opt = std::make_optional(raft::make_const_mdspan(ids_device.view()));

        if (this->dist_mode == DistributionMode_REPLICATED) {
            ivf_flat_index* idx_ptr;
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_device_id());
                if (it == this->replicated_indices_.end())
                    throw std::runtime_error("extend_internal: no index for device");
                idx_ptr = static_cast<ivf_flat_index*>(it->second.get());
            }
            cuvs::neighbors::ivf_flat::extend(*res,
                raft::make_const_mdspan(new_vecs_device.view()), indices_opt, idx_ptr);
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_datasets_.erase(handle.get_device_id());
            }
        } else {
            if (!index_) throw std::runtime_error("extend_internal: index not built");
            cuvs::neighbors::ivf_flat::extend(*res,
                raft::make_const_mdspan(new_vecs_device.view()), indices_opt, index_.get());
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->dataset_device_ptr_.reset();
            }
        }
        handle.sync();
    }

    void extend_internal_float(raft_handle_wrapper_t& handle, const float* new_data, uint64_t n_rows,
                               const int64_t* seq_ids) {
        auto res = handle.get_raft_resources();

        auto new_vecs_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)n_rows, (int64_t)this->dimension);
        if constexpr (std::is_same_v<T, float>) {
            raft::copy(*res, new_vecs_device.view(),
                       raft::make_host_matrix_view<const float, int64_t>(new_data, n_rows, this->dimension));
        } else {
            auto new_vecs_float = raft::make_device_matrix<float, int64_t>(*res, (int64_t)n_rows, (int64_t)this->dimension);
            raft::copy(*res, new_vecs_float.view(),
                       raft::make_host_matrix_view<const float, int64_t>(new_data, n_rows, this->dimension));
            if constexpr (sizeof(T) == 1) {
                if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained for extend_float");
                this->quantizer_.template transform<T>(*res, new_vecs_float.view(), new_vecs_device.data_handle(), true);
            } else {
                // T is half
                raft::copy(*res, new_vecs_device.view(), new_vecs_float.view());
            }
        }

        auto ids_device = raft::make_device_vector<int64_t, int64_t>(*res, (int64_t)n_rows);
        raft::copy(*res, ids_device.view(),
                   raft::make_host_vector_view<const int64_t, int64_t>(seq_ids, (int64_t)n_rows));
        raft::resource::sync_stream(*res);
        auto indices_opt = std::make_optional(raft::make_const_mdspan(ids_device.view()));

        if (this->dist_mode == DistributionMode_REPLICATED) {
            ivf_flat_index* idx_ptr;
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_device_id());
                if (it == this->replicated_indices_.end())
                    throw std::runtime_error("extend_internal_float: no index for device");
                idx_ptr = static_cast<ivf_flat_index*>(it->second.get());
            }
            cuvs::neighbors::ivf_flat::extend(*res,
                raft::make_const_mdspan(new_vecs_device.view()), indices_opt, idx_ptr);
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_datasets_.erase(handle.get_device_id());
            }
        } else {
            if (!index_) throw std::runtime_error("extend_internal_float: index not built");
            cuvs::neighbors::ivf_flat::extend(*res,
                raft::make_const_mdspan(new_vecs_device.view()), indices_opt, index_.get());
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->dataset_device_ptr_.reset();
            }
        }
        handle.sync();
    }

    void extend(const T* new_data, uint64_t n_rows, const int64_t* new_ids) {
        if (!this->is_loaded_) throw std::runtime_error("extend: index not built");
        if (!new_data || n_rows == 0) return;
        if (this->dist_mode == DistributionMode_SHARDED)
            throw std::runtime_error("extend: SHARDED mode not supported");

        std::vector<int64_t> seq_ids(n_rows);
        std::iota(seq_ids.begin(), seq_ids.end(), (int64_t)this->count);

        if (this->dist_mode == DistributionMode_REPLICATED) {
            this->worker->submit_all_devices([&](raft_handle_wrapper_t& handle) -> std::any {
                this->extend_internal(handle, new_data, n_rows, seq_ids.data());
                return std::any();
            });
        } else {
            uint64_t job_id = this->worker->submit_main(
                [&](raft_handle_wrapper_t& handle) -> std::any {
                    this->extend_internal(handle, new_data, n_rows, seq_ids.data());
                    return std::any();
                });
            auto result_wait = this->worker->wait(job_id).get();
            if (result_wait.error) std::rethrow_exception(result_wait.error);
        }
        {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            if (new_ids) this->set_ids(new_ids, n_rows, this->count);
            this->count += static_cast<uint32_t>(n_rows);
            this->current_offset_ += n_rows;
        }
    }

    void extend_float(const float* new_data, uint64_t n_rows, const int64_t* new_ids) {
        if (!this->is_loaded_) throw std::runtime_error("extend_float: index not built");
        if (!new_data || n_rows == 0) return;
        if (this->dist_mode == DistributionMode_SHARDED)
            throw std::runtime_error("extend_float: SHARDED mode not supported");

        std::vector<int64_t> seq_ids(n_rows);
        std::iota(seq_ids.begin(), seq_ids.end(), (int64_t)this->count);

        if (this->dist_mode == DistributionMode_REPLICATED) {
            this->worker->submit_all_devices([&](raft_handle_wrapper_t& handle) -> std::any {
                this->extend_internal_float(handle, new_data, n_rows, seq_ids.data());
                return std::any();
            });
        } else {
            uint64_t job_id = this->worker->submit_main(
                [&](raft_handle_wrapper_t& handle) -> std::any {
                    this->extend_internal_float(handle, new_data, n_rows, seq_ids.data());
                    return std::any();
                });
            auto result_wait = this->worker->wait(job_id).get();
            if (result_wait.error) std::rethrow_exception(result_wait.error);
        }
        {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            if (new_ids) this->set_ids(new_ids, n_rows, this->count);
            this->count += static_cast<uint32_t>(n_rows);
            this->current_offset_ += n_rows;
        }
    }

    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/, uint32_t limit, const ivf_flat_search_params_t& sp) {
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

        // std::cout << "[DEBUG] IVF-Flat search: num_queries=" << num_queries << " limit=" << limit << " n_probes=" << sp.n_probes << std::endl;

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

    search_result_t search_batch_internal(const T* queries_data, uint64_t num_queries, uint32_t limit, const ivf_flat_search_params_t& sp) {
        struct search_req_t { const T* data; uint64_t n; };
        std::string batch_key = "ivf_flat_s_" + std::to_string((uintptr_t)this) + "_" + std::to_string(limit);

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

    search_result_t search_float(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const ivf_flat_search_params_t& sp) {
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

        // std::cout << "[DEBUG] IVF-Flat search_float: num_queries=" << num_queries << " limit=" << limit << std::endl;

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

    search_result_t search_float_batch_internal(const float* queries_data, uint64_t num_queries, uint32_t limit, const ivf_flat_search_params_t& sp) {
        struct search_req_t { const float* data; uint64_t n; };
        std::string batch_key = "ivf_flat_sf_" + std::to_string((uintptr_t)this) + "_" + std::to_string(limit);

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

    search_result_t search_internal(raft_handle_wrapper_t& handle, const T* queries_data, uint64_t num_queries, uint32_t limit, const ivf_flat_search_params_t& sp) {
        // std::shared_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        // std::cout << "[DEBUG " << get_timestamp() << "] IVF-Flat search_internal: num_queries=" << num_queries << " limit=" << limit << " device=" << handle.get_device_id() << std::endl;

        auto queries_device = raft::make_device_matrix<T, int64_t>(*res, num_queries, this->dimension);
        raft::copy(*res, queries_device.view(), raft::make_host_matrix_view<const T, int64_t>(queries_data, num_queries, this->dimension));
        raft::resource::sync_stream(*res);

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::ivf_flat::search_params search_params;
        search_params.n_probes = sp.n_probes;

        const ivf_flat_index* local_index = nullptr;
        std::any cached_ptr = handle.get_index_ptr();
        if (cached_ptr.has_value()) {
            if (cached_ptr.type() == typeid(const ivf_flat_index*)) {
                local_index = std::any_cast<const ivf_flat_index*>(cached_ptr);
            } else if (cached_ptr.type() == typeid(ivf_flat_index*)) {
                local_index = std::any_cast<ivf_flat_index*>(cached_ptr);
            } else {
                handle.set_index_ptr(std::any()); // Clear invalid cache
            }
        }
        
        if (!local_index) {
            // Tiered fallback: Replicated -> Single
            if (!this->replicated_indices_.empty()) {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_device_id());
                if (it != this->replicated_indices_.end()) {
                    auto shared_idx = std::static_pointer_cast<ivf_flat_index>(it->second);
                    local_index = shared_idx.get();
                }
            } else if (index_) {
                local_index = index_.get();
            }
            if (local_index) handle.set_index_ptr(static_cast<const ivf_flat_index*>(local_index));
        }

        if (local_index) {
            auto neighbors_device = raft::make_device_matrix<int64_t, int64_t>(*res, num_queries, limit);
            auto distances_device = raft::make_device_matrix<float, int64_t>(*res, num_queries, limit);

            if (this->deleted_count_ > 0) {
                this->sync_device_bitset(handle.get_device_id(), *res);
                auto info = this->get_device_bitset_info(handle.get_device_id());
                using bs_t = raft::core::bitset<uint32_t, int64_t>;
                auto* bs = static_cast<bs_t*>(info->ptr.get());
                auto filter = cuvs::neighbors::filtering::bitset_filter(bs->view());
                cuvs::neighbors::ivf_flat::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(queries_device.view()),
                                                    neighbors_device.view(), distances_device.view(), filter);
            } else {
                cuvs::neighbors::ivf_flat::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(queries_device.view()),
                                                    neighbors_device.view(), distances_device.view());
            }

            raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device.view());
            raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device.view());
        } else {
            std::string msg = "IVF-Flat search error: No valid index found for device " + std::to_string(handle.get_device_id()) +
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
            // Manual sharding offset if no custom IDs
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

    search_result_t search_float_internal(raft_handle_wrapper_t& handle, const float* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/, uint32_t limit, const ivf_flat_search_params_t& sp) {
        // std::shared_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        // std::cout << "[DEBUG " << get_timestamp() << "] IVF-Flat search_float_internal: num_queries=" << num_queries << " limit=" << limit << " device=" << handle.get_device_id() << std::endl;

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

        cuvs::neighbors::ivf_flat::search_params search_params;
        search_params.n_probes = sp.n_probes;

        const ivf_flat_index* local_index = nullptr;
        std::any cached_ptr = handle.get_index_ptr();
        if (cached_ptr.has_value()) {
            if (cached_ptr.type() == typeid(const ivf_flat_index*)) {
                local_index = std::any_cast<const ivf_flat_index*>(cached_ptr);
            } else if (cached_ptr.type() == typeid(ivf_flat_index*)) {
                local_index = std::any_cast<ivf_flat_index*>(cached_ptr);
            } else {
                handle.set_index_ptr(std::any()); // Clear invalid cache
            }
        }
        
        if (!local_index) {
            // Tiered fallback: Replicated -> Single
            if (!this->replicated_indices_.empty()) {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_device_id());
                if (it != this->replicated_indices_.end()) {
                    auto shared_idx = std::static_pointer_cast<ivf_flat_index>(it->second);
                    local_index = shared_idx.get();
                }
            } else if (index_) {
                local_index = index_.get();
            }
            if (local_index) handle.set_index_ptr(static_cast<const ivf_flat_index*>(local_index));
        }

        if (local_index) {
            auto neighbors_device = raft::make_device_matrix<int64_t, int64_t>(*res, num_queries, limit);
            auto distances_device = raft::make_device_matrix<float, int64_t>(*res, num_queries, limit);

            if (this->deleted_count_ > 0) {
                this->sync_device_bitset(handle.get_device_id(), *res);
                auto info = this->get_device_bitset_info(handle.get_device_id());
                using bs_t = raft::core::bitset<uint32_t, int64_t>;
                auto* bs = static_cast<bs_t*>(info->ptr.get());
                auto filter = cuvs::neighbors::filtering::bitset_filter(bs->view());
                cuvs::neighbors::ivf_flat::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(q_dev_t.view()),
                                                    neighbors_device.view(), distances_device.view(), filter);
            } else {
                cuvs::neighbors::ivf_flat::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(q_dev_t.view()),
                                                    neighbors_device.view(), distances_device.view());
            }

            raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device.view());
            raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device.view());
        } else {
            std::string msg = "IVF-Flat search error: No valid index found for device " + std::to_string(handle.get_device_id()) +
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
            // Manual sharding offset if no custom IDs
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

    void save(const std::string& filename) const {
        if (!this->is_loaded_ || (!index_ && !mg_index_)) throw std::runtime_error("Index not built");
        if (mg_index_) throw std::runtime_error("Saving multi-GPU index not supported yet");
        
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                cuvs::neighbors::ivf_flat::serialize(*(handle.get_raft_resources()), filename, *index_);
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
            auto local_idx = std::make_unique<ivf_flat_index>(*res);
            cuvs::neighbors::ivf_flat::deserialize(*res, filename, local_idx.get());

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->count = static_cast<uint32_t>(local_idx->size());
                this->dimension = static_cast<uint32_t>(local_idx->dim());

                if (this->dist_mode == DistributionMode_SINGLE_GPU) {
                    index_ = std::move(local_idx);
                } else if (this->dist_mode == DistributionMode_REPLICATED) {
                    this->replicated_indices_[handle.get_device_id()] =
                        std::shared_ptr<ivf_flat_index>(std::move(local_idx));
                } else if (this->dist_mode == DistributionMode_SHARDED) {
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
            this->worker->submit_all_devices(task);
        } else {
            this->worker->submit_all_devices(task);
        }

        try {
            this->load_ids(filename + ".ids");
        } catch (...) {}

        this->is_loaded_ = true;
        this->train_quantizer_if_needed();
    }

    // Save all index components to a directory with manifest.json.
    void save_dir(const std::string& dir) const {
        if (!this->is_loaded_ || (!index_ && !mg_index_ && this->replicated_indices_.empty()))
            throw std::runtime_error("IVF-Flat index not built; cannot save_dir");
        if (mg_index_) throw std::runtime_error("Saving multi-GPU index not supported yet");

        this->ensure_dir(dir);
        auto comp_entries = this->save_common_components(dir);

        if (this->dist_mode == DistributionMode_SINGLE_GPU) {
            uint64_t job_id = this->worker->submit_main(
                [&](raft_handle_wrapper_t& handle) -> std::any {
                    cuvs::neighbors::ivf_flat::serialize(
                        *(handle.get_raft_resources()), dir + "/index.bin", *index_);
                    return std::any();
                }
            );
            auto wait_res = this->worker->wait(job_id).get();
            if (wait_res.error) std::rethrow_exception(wait_res.error);
            comp_entries.push_back("    \"index\": \"index.bin\"");

        } else if (this->dist_mode == DistributionMode_REPLICATED) {
            uint64_t job_id = this->worker->submit_main(
                [&](raft_handle_wrapper_t& handle) -> std::any {
                    int dev_id = handle.get_device_id();
                    auto it = this->replicated_indices_.find(dev_id);
                    if (it == this->replicated_indices_.end())
                        it = this->replicated_indices_.begin();
                    if (it == this->replicated_indices_.end())
                        throw std::runtime_error("No replicated IVF-Flat index found to serialize");
                    cuvs::neighbors::ivf_flat::serialize(
                        *(handle.get_raft_resources()), dir + "/index.bin",
                        *std::static_pointer_cast<ivf_flat_index>(it->second));
                    return std::any();
                }
            );
            auto wait_res = this->worker->wait(job_id).get();
            if (wait_res.error) std::rethrow_exception(wait_res.error);
            comp_entries.push_back("    \"index\": \"index.bin\"");

        } else { // SHARDED
            this->worker->submit_all_devices(
                [&](raft_handle_wrapper_t& handle) -> std::any {
                    int rank = handle.get_rank();
                    std::string shard_file = dir + "/shard_" + std::to_string(rank) + ".bin";
                    auto it = this->replicated_indices_.find(handle.get_device_id());
                    if (it != this->replicated_indices_.end()) {
                        cuvs::neighbors::ivf_flat::serialize(
                            *(handle.get_raft_resources()), shard_file,
                            *std::static_pointer_cast<ivf_flat_index>(it->second));
                    }
                    return std::any();
                }
            );
            comp_entries.push_back(this->shards_comp_entry());
        }

        std::string bp_json =
            "    \"n_lists\": " + std::to_string(this->build_params.n_lists) + ",\n" +
            "    \"kmeans_trainset_fraction\": " +
                std::to_string(this->build_params.kmeans_trainset_fraction);
        this->write_manifest(dir, "ivf_flat", bp_json, comp_entries);
    }

    // Restore all index state from a directory previously written by save_dir().
    void load_dir(const std::string& dir) {
        auto m = this->read_manifest(dir, "ivf_flat");

        std::string bp_json = json_object(m.raw, "build_params");
        this->build_params.n_lists =
            static_cast<uint32_t>(json_int(bp_json, "n_lists", 1024));
        this->build_params.kmeans_trainset_fraction =
            std::stod(json_value(bp_json, "kmeans_trainset_fraction").empty()
                      ? "0.5" : json_value(bp_json, "kmeans_trainset_fraction"));

        std::string idx_file   = json_value(m.comp_json, "index");
        std::vector<std::string> shard_files = json_string_array(m.comp_json, "shards");

        if (!idx_file.empty() && this->dist_mode == DistributionMode_SINGLE_GPU) {
            std::string full_path = dir + "/" + idx_file;
            auto task = [&, full_path](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                auto local_idx = std::make_unique<ivf_flat_index>(*res);
                cuvs::neighbors::ivf_flat::deserialize(*res, full_path, local_idx.get());
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                index_ = std::move(local_idx);
                return std::any();
            };
            uint64_t job_id = this->worker->submit_main(task);
            auto wait_res = this->worker->wait(job_id).get();
            if (wait_res.error) std::rethrow_exception(wait_res.error);

        } else if (!idx_file.empty() && this->dist_mode == DistributionMode_REPLICATED) {
            std::string full_path = dir + "/" + idx_file;
            this->worker->submit_all_devices(
                [&, full_path](raft_handle_wrapper_t& handle) -> std::any {
                    auto res = handle.get_raft_resources();
                    auto local_idx = std::make_unique<ivf_flat_index>(*res);
                    cuvs::neighbors::ivf_flat::deserialize(*res, full_path, local_idx.get());
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_indices_[handle.get_device_id()] =
                        std::shared_ptr<ivf_flat_index>(std::move(local_idx));
                    return std::any();
                }
            );

        } else if (!shard_files.empty()) {
            this->worker->submit_all_devices(
                [&, shard_files, dir](raft_handle_wrapper_t& handle) -> std::any {
                    int rank = handle.get_rank();
                    if (rank >= static_cast<int>(shard_files.size()))
                        return std::any();
                    std::string shard_path = dir + "/" + shard_files[rank];
                    auto res = handle.get_raft_resources();
                    auto local_idx = std::make_unique<ivf_flat_index>(*res);
                    cuvs::neighbors::ivf_flat::deserialize(*res, shard_path, local_idx.get());
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_indices_[handle.get_device_id()] =
                        std::shared_ptr<ivf_flat_index>(std::move(local_idx));
                    return std::any();
                }
            );
            this->count           = static_cast<uint32_t>(json_int(m.raw, "capacity"));
            this->current_offset_ = static_cast<uint64_t>(json_int(m.raw, "length"));

        } else {
            throw std::runtime_error("manifest has neither 'index' nor 'shards' in components");
        }

        this->load_common_components(dir, m);
        this->is_loaded_ = true;
    }

    uint32_t get_n_list() const { return this->build_params.n_lists; }

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

            uint32_t num_candidates = candidates.size();
            uint32_t to_sort = std::min(limit, num_candidates);
            
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
        mg_index_.reset();
        this->replicated_indices_.clear();
        this->replicated_datasets_.clear();
        this->quantizer_.reset();
        this->dataset_device_ptr_.reset();
    }

    std::vector<T> get_centers() {
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
        
        auto task = [&](raft_handle_wrapper_t& handle) -> std::any {
            auto res = handle.get_raft_resources();
            const ivf_flat_index* local_index = nullptr;
            if (!this->replicated_indices_.empty()) {
                auto it = this->replicated_indices_.find(handle.get_device_id());
                if (it != this->replicated_indices_.end()) {
                    local_index = std::static_pointer_cast<ivf_flat_index>(it->second).get();
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
                auto centers_float_view = raft::make_device_matrix_view<const float, int64_t>(centers_view.data_handle(), n_centers, dim);
                this->quantizer_.template transform<T>(*res, centers_float_view, centers_device_target.data_handle(), true);
            } else {
                raft::copy(*res, centers_device_target.view(), centers_view);
            }

            std::vector<T> host_centers(n_centers * dim);
            raft::copy(*res, raft::make_host_matrix_view<T, int64_t>(host_centers.data(), n_centers, dim), centers_device_target.view());
            handle.sync();
            return host_centers;
        };

        if (!this->worker) throw std::runtime_error("Worker not initialized");
        uint64_t job_id = this->worker->submit(task);
        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        return std::any_cast<std::vector<T>>(result_wait.result);
    }

    std::string info() const override {
        std::string json = gpu_index_base_t<T, ivf_flat_build_params_t, int64_t>::info();
        json += ", \"type\": \"IVF-Flat\", \"ivf_flat\": {";
        if (index_) json += "\"mode\": \"Single-GPU\", \"size\": " + std::to_string(index_->size());
        else if (!this->replicated_indices_.empty()) json += "\"mode\": \"Local-Indices\", \"ranks\": " + std::to_string(this->replicated_indices_.size());
        else json += "\"built\": false";
        json += "}}";
        return json;
    }
};

} // namespace matrixone
