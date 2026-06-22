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
#include "dynamic_batching.hpp"
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

// =============================================================================
// gpu_ivf_flat_t — Developer Guide
// =============================================================================
//
// OVERVIEW
// --------
// gpu_ivf_flat_t<T> implements an IVF-Flat (Inverted File with Flat storage)
// approximate nearest-neighbor index backed by cuVS.
//
// cuVS type: cuvs::neighbors::ivf_flat::index<T, int64_t>
// IdT      : int64_t
// Supported T: float, half (__half), int8_t, uint8_t
//
// IVF-Flat partitions the dataset into n_lists clusters (Voronoi cells).
// Each vector is stored uncompressed in exactly one cluster's list.
// Search probes the n_probes nearest centroids and scans their lists exactly.
// No approximation from compression — only from limiting the number of probes.
//
//
// DISTRIBUTION MODES
// ------------------
// SINGLE_GPU:
//   index_  holds the single cuVS IVF-Flat index.
//   dataset_device_ptr_ holds the build dataset on device (reset after extend).
//
// REPLICATED:
//   replicated_indices_[rank] holds a full copy per rank (cast to ivf_flat_index*).
//   replicated_datasets_[rank] holds the build dataset per rank (erased after extend).
//   Searches can run on any GPU concurrently.
//   Extends must replicate to all GPUs via submit_all_devices(); set_ids() is
//   called once in extend() after all GPU work completes.
//
// SHARDED:
//   Each GPU holds a disjoint shard. build_internal assigns shard k the rows
//   [k*rows_per_shard .. (k+1)*rows_per_shard) (last shard gets remainder).
//   rows_per_shard = (count / num_shards) & ~31  (rounded down to multiple of 32).
//   Search submits to all shards in parallel; results merged by matrixone::cpu_topk_merge_sharded.
//   Extend routes new rows to the last shard via submit_to_rank(last_rank).
//   shard-local seq_ids = [old_last_shard_size .. old_last_shard_size+n_rows).
//   replicated_datasets_ for other shards is NOT touched.
//
//
// EXTEND RULES
// ------------
// - Can only be called after build() (is_loaded_ must be true).
// - extend_mutex_ serializes concurrent extend() calls.
// - Sequence IDs for cuVS are [count .. count+n_rows) for SINGLE_GPU/REPLICATED,
//   or shard-local [old_shard_size .. old_shard_size+n_rows) for SHARDED.
// - After GPU extend, call set_ids() and update count + current_offset_ under unique_lock.
// - SINGLE_GPU: dataset_device_ptr_ reset after extend.
// - REPLICATED: replicated_datasets_[rank] erased after extend (all ranks).
// - SHARDED: replicated_datasets_ NOT touched (other shards' entries remain valid).
//
//
// SEARCH PATH
// -----------
// search() dispatches to search_internal() via the worker:
//   - Non-SHARDED: submit() (round-robin GPU assignment)
//   - SHARDED: submit_all_devices_no_wait() → matrixone::cpu_topk_merge_sharded()
//
// search_quantize() is the same but accepts base-typed (B) queries and converts on the fly
// (via quantizer for 1-byte T, via half conversion for T=half, direct for T=float).
//
// search_batchable_typed() / search_batchable_quantize() just submit the search to
// the worker; request-level batching, when enabled (batch_window() > 0),
// happens inside search_internal via cuVS dynamic_batching (see dynamic_batching.hpp).
//
// Soft-delete filtering (if deleted_count_ > 0):
//   - Non-SHARDED: sync_device_bitset() → bitset_filter over full index
//   - SHARDED: sync_shard_bitset() → bitset_filter over shard-local slice
//     (see index_base.hpp Developer Guide for bitset details)
//
//
// ID OFFSET IN SHARDED SEARCH
// ----------------------------
// After cuVS returns shard-local IDs (0-based within the shard), they are
// adjusted to global IDs by adding (rank * rows_per_shard) — the same rounded
// rows_per_shard used at build time.  If host_ids is non-empty this adjustment
// is skipped and the ID is looked up in host_ids[] directly.
//
// =============================================================================

/**
 * @brief Search result for IVF-Flat queries.
 */
struct ivf_flat_search_result_t {
    std::vector<int64_t> neighbors;
    std::vector<float> distances;
};

/**
 * @brief gpu_ivf_flat_t implements an IVF-Flat index that can run on a single GPU or sharded across multiple GPUs.
 */
template <typename B, typename T>
class gpu_ivf_flat_t : public gpu_index_base_t<B, T, ivf_flat_build_params_t, int64_t> {
public:
    using base_type    = B;
    using storage_type = T;
    using ivf_flat_index = cuvs::neighbors::ivf_flat::index<T, int64_t>;
    using mg_index = cuvs::neighbors::mg_index<ivf_flat_index, T, int64_t>;
    using search_result_t = ivf_flat_search_result_t;
    // Inherited dependent type — bring into scope so search_internal can take a
    // const host_mask_bundle_t* parameter without `typename Base::...` everywhere.
    using host_mask_bundle_t = typename gpu_index_base_t<B, T, ivf_flat_build_params_t, int64_t>::host_mask_bundle_t;

    std::unique_ptr<ivf_flat_index> index_;
    std::string data_filename_;

    // cuVS dynamic_batching wrappers, keyed by (device_id, k=limit, n_probes).
    // Only touched when batch_window() > 0.
    // See dynamic_batching.hpp.
    dynb_cache_t<T, int64_t> dynb_cache_;

    ~gpu_ivf_flat_t() override {
        destroy();
    }

    // Unified Constructor for building from dataset
    gpu_ivf_flat_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension,
                    distance_type_t m, const ivf_flat_build_params_t& bp,
                    const std::vector<int>& devices, uint32_t nthread, distribution_mode_t mode,
                    const int64_t* ids = nullptr) {

        this->dimension = dimension;
        this->count = count_vectors;
        this->metric = m;
        this->build_params = bp;
        this->dist_mode = mode;
        this->devices_ = devices;
        this->current_offset_ = count_vectors;

        std::vector<int> worker_devices = this->devices_;
        if (mode == DistributionMode_SINGLE_GPU && !worker_devices.empty()) {
            worker_devices = {worker_devices[0]};
        }
        this->worker = std::make_unique<cuvs_worker_t>(nthread, worker_devices, mode);

        this->flattened_host_dataset.resize(this->count * this->dimension);
        if (dataset_data) {
            std::copy(dataset_data, dataset_data + (this->count * this->dimension), this->flattened_host_dataset.begin());
        }

        this->host_ids.reserve(this->count);
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
        this->count = total_count;
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

        this->host_ids.reserve(this->count);
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
        {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            this->count = this->current_offset_;
            if (this->flattened_host_dataset.size() > (size_t)this->current_offset_ * this->dimension)
                this->flattened_host_dataset.resize((size_t)this->current_offset_ * this->dimension);
        }
        if (this->count == 0) {
            if (this->pending_total_count_ == 0) {
                this->is_loaded_ = true;
                return;
            }
        }

        // std::cout << "[DEBUG] IVF-Flat build: Starting build count=" << this->count << " dim=" << this->dimension << " metric=" << (int)this->metric << std::endl;

        this->train_quantizer_if_needed();
        if (!this->worker) throw std::runtime_error("Worker not initialized");

        if (this->dist_mode == DistributionMode_SHARDED) {
            int num_shards = static_cast<int>(this->devices_.size());
            uint64_t rows_per_shard = (this->count / num_shards) & ~static_cast<uint64_t>(31);
            uint64_t last_shard_rows = this->count - rows_per_shard * (num_shards - 1);
            uint64_t min_shard_rows = std::min(rows_per_shard, last_shard_rows);
            validate_build_params(this->build_params, min_shard_rows);
            this->shard_sizes_.assign(num_shards, 0);
        } else {
            validate_build_params(this->build_params, this->count);
        }

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
            if (this->dist_mode == DistributionMode_SHARDED)
                this->shard_sizes_.assign(this->devices_.size(), 0);
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

    static void validate_build_params(const ivf_flat_build_params_t& bp, uint64_t num_rows) {
        if (num_rows < bp.n_lists) {
            throw std::invalid_argument(
                "IVF-Flat build requires at least n_lists vectors (got " + std::to_string(num_rows) +
                " vectors, n_lists=" + std::to_string(bp.n_lists) + ")");
        }
    }

    void build_internal(raft_handle_wrapper_t& handle) {
        cuvs::neighbors::ivf_flat::index_params index_params;
        index_params.metric = static_cast<cuvs::distance::DistanceType>(this->metric);
        index_params.n_lists = this->build_params.n_lists;

        if (this->dist_mode == DistributionMode_REPLICATED) {
            auto res = handle.get_raft_resources();
            // Pool-bypass training matrix — see matrixone::raw_device_mr() in cuvs_worker.hpp.
            auto dataset_storage = std::make_shared<rmm::device_uvector<T>>(
                static_cast<size_t>(this->count) * this->dimension,
                raft::resource::get_cuda_stream(*res),
                matrixone::raw_device_mr());
            auto dataset_device = raft::make_device_matrix_view<T, int64_t>(
                dataset_storage->data(), (int64_t)this->count, (int64_t)this->dimension);
            raft::copy(*res, dataset_device, raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data(), this->count, this->dimension));
            raft::resource::sync_stream(*res);

            // Serialize concurrent builds on the same physical device (cuVS kmeans
            // is not safe to run twice at once on one GPU — see device_build_mutex).
            std::unique_ptr<ivf_flat_index> local_idx;
            {
                std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                local_idx = std::make_unique<ivf_flat_index>(cuvs::neighbors::ivf_flat::build(
                    *res, index_params, raft::make_const_mdspan(dataset_device)));
            }

            handle.set_index_ptr(static_cast<const ivf_flat_index*>(local_idx.get()));

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_indices_[handle.get_rank()] = std::shared_ptr<ivf_flat_index>(std::move(local_idx));
                this->replicated_datasets_[handle.get_rank()] = std::move(dataset_storage);
            }
            handle.sync();
        } else if (this->dist_mode == DistributionMode_SHARDED) {
            auto res = handle.get_raft_resources();
            int num_shards = this->devices_.size();
            int rank = handle.get_rank();
            
            // Round down to a multiple of 32 so every shard offset is word-aligned in
            // the deleted bitset, making shard-slice sync cheap (no bit-shifting needed).
            // The last shard absorbs the remainder and may be slightly larger.
            uint64_t rows_per_shard = (this->count / num_shards) & ~static_cast<uint64_t>(31);
            uint64_t start_row = rank * rows_per_shard;
            uint64_t num_rows = (rank == num_shards - 1) ? (this->count - start_row) : rows_per_shard;
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->shard_sizes_[rank] = num_rows;
            }

            // std::cout << "[DEBUG] IVF-Flat build SHARDED: rank=" << rank << " start_row=" << start_row << " num_rows=" << num_rows << std::endl;

            // Pool-bypass training shard — see REPLICATED branch.
            auto dataset_storage = std::make_shared<rmm::device_uvector<T>>(
                static_cast<size_t>(num_rows) * this->dimension,
                raft::resource::get_cuda_stream(*res),
                matrixone::raw_device_mr());
            auto dataset_device = raft::make_device_matrix_view<T, int64_t>(
                dataset_storage->data(), (int64_t)num_rows, (int64_t)this->dimension);
            raft::copy(*res, dataset_device,
                       raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data() + (start_row * this->dimension), num_rows, this->dimension));
            raft::resource::sync_stream(*res);

            // Serialize concurrent builds on the same physical device (cuVS kmeans
            // is not safe to run twice at once on one GPU — see device_build_mutex).
            std::unique_ptr<ivf_flat_index> local_idx;
            {
                std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                local_idx = std::make_unique<ivf_flat_index>(cuvs::neighbors::ivf_flat::build(
                    *res, index_params, raft::make_const_mdspan(dataset_device)));
            }

            handle.set_index_ptr(static_cast<const ivf_flat_index*>(local_idx.get()));

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_indices_[handle.get_rank()] = std::shared_ptr<ivf_flat_index>(std::move(local_idx));
                this->replicated_datasets_[handle.get_rank()] = std::move(dataset_storage);
            }
            handle.sync();
        } else {
            // Do all GPU work outside the lock — holding shared_mutex across GPU calls
            // would block concurrent readers for the entire build duration.
            auto res = handle.get_raft_resources();
            // Pool-bypass training matrix — see REPLICATED branch.
            auto dataset_storage = std::make_shared<rmm::device_uvector<T>>(
                static_cast<size_t>(this->count) * this->dimension,
                raft::resource::get_cuda_stream(*res),
                matrixone::raw_device_mr());
            auto dataset_device = raft::make_device_matrix_view<T, int64_t>(
                dataset_storage->data(), (int64_t)this->count, (int64_t)this->dimension);
            raft::copy(*res, dataset_device, raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data(), this->count, this->dimension));
            raft::resource::sync_stream(*res);

            std::unique_ptr<ivf_flat_index> new_idx;
            {
                std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                new_idx = std::make_unique<ivf_flat_index>(cuvs::neighbors::ivf_flat::build(
                    *res, index_params, raft::make_const_mdspan(dataset_device)));
            }

            handle.sync();

            // Assign results under lock
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                index_ = std::move(new_idx);
                this->dataset_device_ptr_ = std::move(dataset_storage);
            }
        }
    }

    void extend_internal(raft_handle_wrapper_t& handle, const T* new_data, uint64_t n_rows,
                         const int64_t* seq_ids) {
        auto res = handle.get_raft_resources();

        auto new_vecs_storage = this->upload_T_matrix(handle, new_data, n_rows);
        auto new_vecs_device  = raft::make_device_matrix_view<T, int64_t>(
            new_vecs_storage.data(), (int64_t)n_rows, (int64_t)this->dimension);

        auto ids_device = raft::make_device_vector<int64_t, int64_t>(*res, (int64_t)n_rows);
        raft::copy(*res, ids_device.view(),
                   raft::make_host_vector_view<const int64_t, int64_t>(seq_ids, (int64_t)n_rows));
        raft::resource::sync_stream(*res);
        auto indices_opt = std::make_optional(raft::make_const_mdspan(ids_device.view()));

        if (this->dist_mode == DistributionMode_REPLICATED) {
            ivf_flat_index* idx_ptr;
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_rank());
                if (it == this->replicated_indices_.end())
                    throw std::runtime_error("extend_internal: no index for device");
                idx_ptr = static_cast<ivf_flat_index*>(it->second.get());
            }
            {
                // Serialize index-mutating cuVS calls on the same physical device.
                std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                cuvs::neighbors::ivf_flat::extend(*res,
                    raft::make_const_mdspan(new_vecs_device), indices_opt, idx_ptr);
            }
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_datasets_.erase(handle.get_rank());
            }
        } else if (this->dist_mode == DistributionMode_SHARDED) {
            // Only the last shard's device calls this; seq_ids are already shard-local.
            ivf_flat_index* idx_ptr;
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_rank());
                if (it == this->replicated_indices_.end())
                    throw std::runtime_error("extend_internal: no SHARDED index for device");
                idx_ptr = static_cast<ivf_flat_index*>(it->second.get());
            }
            {
                // Serialize index-mutating cuVS calls on the same physical device.
                std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                cuvs::neighbors::ivf_flat::extend(*res,
                    raft::make_const_mdspan(new_vecs_device), indices_opt, idx_ptr);
            }
            {
                // Erase only the last shard's stale build dataset; other shards' entries remain valid.
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_datasets_.erase(handle.get_rank());
            }
        } else {
            if (!index_) throw std::runtime_error("extend_internal: index not built");
            {
                // Serialize index-mutating cuVS calls on the same physical device.
                std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                cuvs::neighbors::ivf_flat::extend(*res,
                    raft::make_const_mdspan(new_vecs_device), indices_opt, index_.get());
            }
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

        auto new_vecs_storage = this->upload_float_matrix_as_T(handle, new_data, n_rows);
        auto new_vecs_device  = raft::make_device_matrix_view<T, int64_t>(
            new_vecs_storage.data(), (int64_t)n_rows, (int64_t)this->dimension);

        auto ids_device = raft::make_device_vector<int64_t, int64_t>(*res, (int64_t)n_rows);
        raft::copy(*res, ids_device.view(),
                   raft::make_host_vector_view<const int64_t, int64_t>(seq_ids, (int64_t)n_rows));
        raft::resource::sync_stream(*res);
        auto indices_opt = std::make_optional(raft::make_const_mdspan(ids_device.view()));

        if (this->dist_mode == DistributionMode_REPLICATED) {
            ivf_flat_index* idx_ptr;
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_rank());
                if (it == this->replicated_indices_.end())
                    throw std::runtime_error("extend_internal_float: no index for device");
                idx_ptr = static_cast<ivf_flat_index*>(it->second.get());
            }
            {
                // Serialize index-mutating cuVS calls on the same physical device.
                std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                cuvs::neighbors::ivf_flat::extend(*res,
                    raft::make_const_mdspan(new_vecs_device), indices_opt, idx_ptr);
            }
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_datasets_.erase(handle.get_rank());
            }
        } else if (this->dist_mode == DistributionMode_SHARDED) {
            // Only the last shard's device calls this; seq_ids are already shard-local.
            ivf_flat_index* idx_ptr;
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_rank());
                if (it == this->replicated_indices_.end())
                    throw std::runtime_error("extend_internal_float: no SHARDED index for device");
                idx_ptr = static_cast<ivf_flat_index*>(it->second.get());
            }
            {
                // Serialize index-mutating cuVS calls on the same physical device.
                std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                cuvs::neighbors::ivf_flat::extend(*res,
                    raft::make_const_mdspan(new_vecs_device), indices_opt, idx_ptr);
            }
            {
                // Erase only the last shard's stale build dataset; other shards' entries remain valid.
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_datasets_.erase(handle.get_rank());
            }
        } else {
            if (!index_) throw std::runtime_error("extend_internal_float: index not built");
            {
                // Serialize index-mutating cuVS calls on the same physical device.
                std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                cuvs::neighbors::ivf_flat::extend(*res,
                    raft::make_const_mdspan(new_vecs_device), indices_opt, index_.get());
            }
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->dataset_device_ptr_.reset();
            }
        }
        handle.sync();
    }

    void extend(const T* new_data, uint64_t n_rows, const int64_t* new_ids) {
        if (!new_data) return;
        this->run_extend("extend", n_rows, new_ids, /*support_sharded=*/true,
            [&](raft_handle_wrapper_t& handle, const int64_t* seq_ids, uint64_t n) {
                this->extend_internal(handle, new_data, n, seq_ids);
            });
        // Invalidate dynamic-batching cache: IVF-Flat extend mutates the cuVS
        // index in place, so dynb_cache_t entries keyed on the raw upstream
        // pointer survive with pre-extend state and serve stale search
        // results. dynb_cache_ has its own mutex; in-flight searches keep
        // their wrapper alive via shared_ptr, so clearing is race-safe.
        this->dynb_cache_.clear();
    }

    void extend_float(const float* new_data, uint64_t n_rows, const int64_t* new_ids) {
        if (!new_data) return;
        this->run_extend("extend_float", n_rows, new_ids, /*support_sharded=*/true,
            [&](raft_handle_wrapper_t& handle, const int64_t* seq_ids, uint64_t n) {
                this->extend_internal_float(handle, new_data, n, seq_ids);
            });
        // See extend() above — same staleness, same fix.
        this->dynb_cache_.clear();
    }

    // Sync T-typed entry — wraps search_async + search_wait. SHARDED inline
    // fan-out / merge that used to live here is handled by the async path's
    // composite_search_pending_t sentinel inside search_wait().
    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const ivf_flat_search_params_t& sp) {
        uint64_t job_id = this->search_async(queries_data, num_queries, query_dimension, limit, sp);
        return this->search_wait(job_id);
    }

    // Sync T-typed filtered entry — wraps search_with_filter_async + search_wait.
    search_result_t search_with_filter(const T* queries_data, uint64_t num_queries,
                                       uint32_t query_dimension, uint32_t limit,
                                       const ivf_flat_search_params_t& sp,
                                       const std::string& preds_json) {
        uint64_t job_id = this->search_with_filter_async(queries_data, num_queries, query_dimension, limit, sp, preds_json);
        return this->search_wait(job_id);
    }

    // Async T-typed filtered search. Mirrors search_quantize_with_filter_async
    // but uses search_internal (T) instead of search_quantize_internal (B).
    uint64_t search_with_filter_async(const T* queries_data, uint64_t num_queries,
                                      uint32_t query_dimension, uint32_t limit,
                                      const ivf_flat_search_params_t& sp,
                                      const std::string& preds_json) {
        if (!queries_data) throw std::invalid_argument("search_async: queries_data is null");
        if (num_queries == 0) throw std::invalid_argument("search_async: num_queries is 0");
        if (this->dimension == 0) throw std::runtime_error("search_async: index dimension is 0");
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) throw std::runtime_error("search_async: index not loaded");
        }
        if (!this->worker) throw std::runtime_error("Worker not initialized");
        // search_internal uses this->dimension internally; reject any
        // mismatched caller dim instead of silently coercing.
        if (query_dimension != this->dimension) {
            throw std::invalid_argument(
                "search_with_filter_async: query_dimension (" + std::to_string(query_dimension) +
                ") does not match index dimension (" + std::to_string(this->dimension) + ")");
        }

        auto queries_copy = std::make_shared<std::vector<T>>(queries_data, queries_data + num_queries * this->dimension);

        if (this->dist_mode == DistributionMode_SHARDED) {
            auto shard_masks = this->build_filter_shard_masks(preds_json);
            auto shard_search_task = [this, num_queries, limit, sp, queries_copy, shard_masks](raft_handle_wrapper_t& gpu_handle) -> std::any {
                int rank = gpu_handle.get_rank();
                return this->search_internal(gpu_handle, queries_copy->data(), num_queries, limit, sp, /*preds_json=*/"", shard_masks[rank].get());
            };
            auto job_ids = this->worker->submit_all_devices_no_wait(shard_search_task);
            return this->worker->submit_composite_pending(std::move(job_ids), num_queries, limit);
        }

        auto mask = this->build_filter_single_mask(preds_json);
        auto task = [this, num_queries, limit, sp, queries_copy, mask](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_internal(handle, queries_copy->data(), num_queries, limit, sp, /*preds_json=*/"", mask.get());
        };
        return this->worker->submit(task);
    }

    uint64_t search_async(const T* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/, uint32_t limit, const ivf_flat_search_params_t& sp) {
        if (!queries_data) throw std::invalid_argument("search_async: queries_data is null");
        if (num_queries == 0) throw std::invalid_argument("search_async: num_queries is 0");
        if (this->dimension == 0) throw std::runtime_error("search_async: index dimension is 0");
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) throw std::runtime_error("search_async: index not loaded");
        }

        auto queries_copy = std::make_shared<std::vector<T>>(queries_data, queries_data + num_queries * this->dimension);

        if (this->dist_mode == DistributionMode_SHARDED) {
            // See plan .claude/plans/effervescent-hatching-dewdrop.md — fan out
            // per-shard via device queues, hand back a composite job_id; the
            // merge runs in search_wait() on the caller's thread, never on
            // main_thread_.
            auto shard_search_task = [this, num_queries, limit, sp, queries_copy](raft_handle_wrapper_t& gpu_handle) -> std::any {
                return this->search_internal(gpu_handle, queries_copy->data(), num_queries, limit, sp);
            };
            auto job_ids = this->worker->submit_all_devices_no_wait(shard_search_task);
            return this->worker->submit_composite_pending(std::move(job_ids), num_queries, limit);
        }

        // Single-GPU / replicated: the helper decides standalone vs fused; the
        // shared_ptr keeps the copied queries alive until the search runs.
        return this->search_batchable_typed(queries_copy, queries_copy->data(), num_queries, limit, sp);
    }

    search_result_t search_wait(uint64_t job_id) {
        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);

        // SHARDED composite path: the result holds the shard job_ids. Wait on
        // each shard here (caller's thread) and run the k-way merge.
        if (result_wait.result.type() == typeid(matrixone::composite_search_pending_t)) {
            auto pending = std::any_cast<matrixone::composite_search_pending_t>(std::move(result_wait.result));
            std::vector<search_result_t> shard_results(pending.shard_ids.size());
            for (size_t i = 0; i < pending.shard_ids.size(); ++i) {
                auto r = this->worker->wait(pending.shard_ids[i]).get();
                if (r.error) std::rethrow_exception(r.error);
                shard_results[i] = std::any_cast<search_result_t>(std::move(r.result));
            }
            return matrixone::cpu_topk_merge_sharded(shard_results, pending.num_queries, pending.limit);
        }
        return std::any_cast<search_result_t>(result_wait.result);
    }

    // Submit a T-typed search to the worker (round-robin across device threads).
    // Request-level batching, when enabled (batch_window() > 0), happens
    // inside search_internal via cuVS dynamic_batching — see dynamic_batching.hpp.
    // `owner` is null on the sync path (the caller blocks in wait().get(), so the
    // query buffer stays alive) and on the async path is the shared_ptr that
    // keeps the copied queries alive until the search runs. Returns a job id
    // resolvable via worker->wait().
    uint64_t search_batchable_typed(std::shared_ptr<std::vector<T>> owner, const T* queries_data,
                                    uint64_t num_queries, uint32_t limit, const ivf_flat_search_params_t& sp) {
        if (!this->worker) throw std::runtime_error("Worker not initialized");
        auto task = [this, owner, queries_data, num_queries, limit, sp](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_internal(handle, queries_data, num_queries, limit, sp);
        };
        return this->worker->submit(task);
    }

    // Sync quantize entry — wraps search_quantize_async + search_wait.
    search_result_t search_quantize(const B* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const ivf_flat_search_params_t& sp) {
        uint64_t job_id = this->search_quantize_async(queries_data, num_queries, query_dimension, limit, sp);
        return this->search_wait(job_id);
    }

    // Sync quantize filtered entry — wraps search_quantize_with_filter_async + search_wait.
    search_result_t search_quantize_with_filter(const B* queries_data, uint64_t num_queries,
                                             uint32_t query_dimension, uint32_t limit,
                                             const ivf_flat_search_params_t& sp,
                                             const std::string& preds_json) {
        uint64_t job_id = this->search_quantize_with_filter_async(queries_data, num_queries, query_dimension, limit, sp, preds_json);
        return this->search_wait(job_id);
    }

    // Async variant of search_quantize_with_filter. Builds the host mask bundle on
    // the calling thread (off-worker), copies queries into a shared_ptr so they
    // outlive the Go caller, captures both in the worker lambda, and returns a
    // job_id that search_wait() can collect. Used by the multi-index filter
    // path so per-shard searches run in parallel. The query is the BASE type B
    // (float or half); search_quantize_internal converts it to storage T.
    uint64_t search_quantize_with_filter_async(const B* queries_data, uint64_t num_queries,
                                            uint32_t query_dimension, uint32_t limit,
                                            const ivf_flat_search_params_t& sp,
                                            const std::string& preds_json) {
        if (!queries_data) throw std::invalid_argument("search_async: queries_data is null");
        if (num_queries == 0) throw std::invalid_argument("search_async: num_queries is 0");
        if (this->dimension == 0) throw std::runtime_error("search_async: index dimension is 0");
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) throw std::runtime_error("search_async: index not loaded");
        }
        if (!this->worker) throw std::runtime_error("Worker not initialized");

        auto queries_copy = std::make_shared<std::vector<B>>(queries_data, queries_data + num_queries * query_dimension);

        if (this->dist_mode == DistributionMode_SHARDED) {
            // Bitmap eval runs on the caller's (Go) thread; per-shard searches
            // hit their device queues directly; merge runs in search_wait().
            // See plan .claude/plans/effervescent-hatching-dewdrop.md.
            auto shard_masks = this->build_filter_shard_masks(preds_json);
            auto shard_search_task = [this, num_queries, query_dimension, limit, sp, queries_copy, shard_masks](raft_handle_wrapper_t& gpu_handle) -> std::any {
                int rank = gpu_handle.get_rank();
                return this->search_quantize_internal(gpu_handle, queries_copy->data(), num_queries, query_dimension, limit, sp, /*preds_json=*/"", shard_masks[rank].get());
            };
            auto job_ids = this->worker->submit_all_devices_no_wait(shard_search_task);
            return this->worker->submit_composite_pending(std::move(job_ids), num_queries, limit);
        }

        // Single-GPU / replicated: bitmap eval stays on the calling thread
        // and the GPU search goes through worker->submit so concurrent calls
        // can be auto-batched in the device queue. Wrapping in submit_main
        // would force serialization through main_thread_ and lose batching.
        auto mask = this->build_filter_single_mask(preds_json);
        auto task = [this, num_queries, query_dimension, limit, sp, queries_copy, mask](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_quantize_internal(handle, queries_copy->data(), num_queries, query_dimension, limit, sp, /*preds_json=*/"", mask.get());
        };
        return this->worker->submit(task);
    }

    uint64_t search_quantize_async(const B* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const ivf_flat_search_params_t& sp) {
        if (!queries_data) throw std::invalid_argument("search_async: queries_data is null");
        if (num_queries == 0) throw std::invalid_argument("search_async: num_queries is 0");
        if (this->dimension == 0) throw std::runtime_error("search_async: index dimension is 0");
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) throw std::runtime_error("search_async: index not loaded");
        }

        auto queries_copy = std::make_shared<std::vector<B>>(queries_data, queries_data + num_queries * query_dimension);

        if (this->dist_mode == DistributionMode_SHARDED) {
            // Same shape as search_async — fan out, hand back a composite id,
            // let search_wait() do the merge on the caller's thread.
            auto shard_search_task = [this, num_queries, query_dimension, limit, sp, queries_copy](raft_handle_wrapper_t& gpu_handle) -> std::any {
                return this->search_quantize_internal(gpu_handle, queries_copy->data(), num_queries, query_dimension, limit, sp);
            };
            auto job_ids = this->worker->submit_all_devices_no_wait(shard_search_task);
            return this->worker->submit_composite_pending(std::move(job_ids), num_queries, limit);
        }

        // Single-GPU / replicated: the helper decides standalone vs fused; the
        // shared_ptr keeps the copied queries alive until the search runs.
        return this->search_batchable_quantize(queries_copy, queries_copy->data(), num_queries, limit, sp);
    }

    // Base-typed (B) quantize search. Mirrors search_batchable_typed but calls
    // search_quantize_internal; request-level batching (if enabled) happens inside it.
    uint64_t search_batchable_quantize(std::shared_ptr<std::vector<B>> owner, const B* queries_data,
                                    uint64_t num_queries, uint32_t limit, const ivf_flat_search_params_t& sp) {
        if (!this->worker) throw std::runtime_error("Worker not initialized");
        auto task = [this, owner, queries_data, num_queries, limit, sp](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_quantize_internal(handle, queries_data, num_queries, this->dimension, limit, sp);
        };
        return this->worker->submit(task);
    }

    // `prebuilt`, when non-null, supplies a host_mask_bundle_t computed off the
    // worker thread (see search_with_filter below). On that path we skip the
    // queries-H2D sync_stream — the search kernel queues on the same stream
    // immediately after the queries / bitset H2D copies and is naturally
    // ordered, so the only barrier we need is the terminal handle.sync().
    // The bundle's host_mask is kept alive by a shared_ptr captured in the
    // worker lambda. When prebuilt is null we use the legacy build_search_bitset
    // entry point which keeps its own internal sync (host_mask is local there).
    search_result_t search_internal(raft_handle_wrapper_t& handle, const T* queries_data, uint64_t num_queries, uint32_t limit, const ivf_flat_search_params_t& sp, const std::string& preds_json = "", const host_mask_bundle_t* prebuilt = nullptr) {
        // No top-level lock: the index pointer is taken from the per-handle
        // cache or, on a miss, a narrow inner shared_lock below (see the
        // replicated_indices_ lookup). GPU work must run unlocked.
        auto res = handle.get_raft_resources();

        // Step C: reuse per-thread grow-only query workspace buffer.
        auto& q_buf = handle.template q_dev_buf<T>(static_cast<size_t>(num_queries) * this->dimension);
        auto queries_device = raft::make_device_matrix_view<T, int64_t>(
            q_buf.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));
        raft::copy(*res, queries_device, raft::make_host_matrix_view<const T, int64_t>(queries_data, num_queries, this->dimension));
        // Legacy path syncs so build_search_bitset's stack-local host bitmap can
        // drain on the same stream. Prebuilt path skips: bitset H2D queues
        // naturally behind queries H2D and the kernel.
        if (!prebuilt) {
            raft::resource::sync_stream(*res);
        }

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
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                if (!this->replicated_indices_.empty()) {
                    auto it = this->replicated_indices_.find(handle.get_rank());
                    if (it != this->replicated_indices_.end()) {
                        auto shared_idx = std::static_pointer_cast<ivf_flat_index>(it->second);
                        local_index = shared_idx.get();
                    }
                }
                if (!local_index) {
                    local_index = index_.get();
                }
            }
            if (local_index) handle.set_index_ptr(static_cast<const ivf_flat_index*>(local_index));
        }

        if (local_index) {
            uint64_t start_row = 0, shard_sz = this->count;
            if (this->dist_mode == DistributionMode_SHARDED) {
                int rank = handle.get_rank();
                shard_sz = this->shard_sizes_[rank];
                start_row = 0;
                for (int r = 0; r < rank; ++r) start_row += this->shard_sizes_[r];
            }

            // Clamp cuVS top-k to shard_sz (cuVS rejects k > index_size).
            // See index_base.hpp for clamp_k_to_index_size / scatter_with_padding;
            // rationale documented in ivf_pq.hpp search_internal.
            const uint32_t effective_k = matrixone::clamp_k_to_index_size(
                static_cast<uint32_t>(limit), shard_sz);

            if (effective_k == 0) {
                matrixone::fill_all_sentinel<int64_t>(
                    search_res.neighbors.data(), search_res.distances.data(),
                    static_cast<size_t>(num_queries) * limit, /*neighbor_sentinel=*/-1LL);
            } else {
                // Step C: reuse per-thread neighbor / distance workspaces.
                auto& n_buf = handle.neighbors_buf(static_cast<size_t>(num_queries) * limit);
                auto& d_buf = handle.distances_buf(static_cast<size_t>(num_queries) * limit);
                auto neighbors_device = raft::make_device_matrix_view<int64_t, int64_t>(
                    n_buf.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(effective_k));
                auto distances_device = raft::make_device_matrix_view<float, int64_t>(
                    d_buf.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(effective_k));

                std::shared_ptr<raft::core::bitset<uint32_t, int64_t>> bs_ptr;
                if (prebuilt) {
                    if (prebuilt->has_filter) {
                        bs_ptr = this->upload_host_mask(handle, prebuilt->mask, shard_sz);
                    } else if (prebuilt->deletes_only) {
                        bs_ptr = this->acquire_delete_bitset_device(handle, start_row, shard_sz);
                    }
                } else {
                    bs_ptr = this->build_search_bitset(handle, preds_json, start_row, shard_sz);
                }

                if (bs_ptr) {
                    auto filter = cuvs::neighbors::filtering::bitset_filter(bs_ptr->view());
                    cuvs::neighbors::ivf_flat::search(*res, search_params, *local_index,
                                                        raft::make_const_mdspan(queries_device),
                                                        neighbors_device, distances_device, filter);
                } else if constexpr (!std::is_same_v<T, __half>) {
                    // cuVS ships no dynamic_batching wrapper for ivf_flat::index<__half>,
                    // so half-typed IVF-Flat stays unbatched (this branch is discarded for T=__half).
                    if (this->batch_window() != 0) {
                        this->dynb_cache_.search(*res, handle.get_device_id(), local_index, search_params,
                                                 static_cast<int64_t>(effective_k),
                                                 static_cast<uint32_t>(search_params.n_probes), 0u,
                                                 this->dynb_concurrency_hint(),
                                                 this->dynb_conservative_dispatch(),
                                                 static_cast<double>(this->batch_window()) / 1000.0,
                                                 raft::make_const_mdspan(queries_device),
                                                 neighbors_device, distances_device);
                    } else {
                        cuvs::neighbors::ivf_flat::search(*res, search_params, *local_index,
                                                            raft::make_const_mdspan(queries_device),
                                                            neighbors_device, distances_device);
                    }
                } else {
                    cuvs::neighbors::ivf_flat::search(*res, search_params, *local_index,
                                                        raft::make_const_mdspan(queries_device),
                                                        neighbors_device, distances_device);
                }

                if (effective_k == static_cast<uint32_t>(limit)) {
                    raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device);
                    raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device);
                } else {
                    std::vector<int64_t> tmp_n(static_cast<size_t>(num_queries) * effective_k);
                    std::vector<float>   tmp_d(static_cast<size_t>(num_queries) * effective_k);
                    raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(tmp_n.data(), num_queries, effective_k), neighbors_device);
                    raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(tmp_d.data(), num_queries, effective_k), distances_device);
                    handle.sync();
                    matrixone::scatter_with_padding<int64_t>(
                        search_res.neighbors.data(), search_res.distances.data(),
                        tmp_n.data(), tmp_d.data(),
                        num_queries, static_cast<uint32_t>(limit), effective_k,
                        /*neighbor_sentinel=*/-1LL);
                }
            }
        } else {
            std::string msg = "IVF-Flat search error: No valid index found for device " + std::to_string(handle.get_device_id()) +
                             " (Mode: " + mode_name(this->dist_mode) + ")";
            throw std::runtime_error(msg);
        }
        handle.sync();

        // GPU search is done; take shared_lock only for the CPU-side ID translation
        // so that concurrent extend() calls (which write host_ids under unique_lock) are safe.
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            int64_t offset = 0;
            int64_t data_size = static_cast<int64_t>(this->count);
            if (this->dist_mode == DistributionMode_SHARDED) {
                for (int r = 0; r < handle.get_rank(); ++r) offset += (int64_t)this->shard_sizes_[r];
                data_size = static_cast<int64_t>(this->shard_sizes_[handle.get_rank()]);
            }
            // Always run map_neighbor_id — the helper bounds-checks raw
            // against data_size regardless of host_ids being empty, so
            // junk cuvs sentinels (UINT32_MAX, INT32_MAX, etc.) get
            // normalized to -1 even on the implicit-id path.
            for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
                search_res.neighbors[i] = map_neighbor_id(search_res.neighbors[i], offset, data_size, this->host_ids);
            }
        }

        transform_distance(this->metric, search_res.distances);
        return search_res;
    }

    // See search_internal() above for the prebuilt-bundle contract; identical
    // semantics here (off-worker CPU mask eval, skip queries-H2D sync_stream
    // when prebuilt is non-null).
    // Takes the query in the BASE element type B (float or half) and converts
    // it to the storage type T on-device — see the cagra search_quantize_internal
    // comment. B==T copies straight, sizeof(T)==1 quantizes B -> int8/uint8, and
    // the (B=float, T=half) instantiation casts f32 -> f16 on the host.
    search_result_t search_quantize_internal(raft_handle_wrapper_t& handle, const B* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/, uint32_t limit, const ivf_flat_search_params_t& sp, const std::string& preds_json = "", const host_mask_bundle_t* prebuilt = nullptr) {
        // No top-level lock: see search_internal() above — pointer fetched
        // via per-handle cache / narrow inner shared_lock, GPU work runs
        // unlocked.
        auto res = handle.get_raft_resources();

        // Step C: reuse the per-thread T-typed query workspace buffer.
        const size_t n_q_elems = static_cast<size_t>(num_queries) * this->dimension;
        auto& q_buf_t = handle.template q_dev_buf<T>(n_q_elems);
        auto q_dev_t = raft::make_device_matrix_view<T, int64_t>(
            q_buf_t.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));

        if constexpr (std::is_same_v<T, B>) {
            // B == T (float->float or half->half): no conversion.
            raft::copy(*res, q_dev_t, raft::make_host_matrix_view<const T, int64_t>(queries_data, num_queries, this->dimension));
        } else if constexpr (sizeof(T) == 1) {
            // sizeof(T) == 1: quantize the base-typed query B -> int8/uint8.
            // Stage the B query on its own per-thread device workspace (distinct
            // from q_buf_t — see q_dev_buf<U>), then transform B -> T on-device.
            if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
            auto& q_buf_b = handle.template q_dev_buf<B>(n_q_elems);
            auto q_dev_b = raft::make_device_matrix_view<B, int64_t>(
                q_buf_b.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));
            raft::copy(*res, q_dev_b, raft::make_host_matrix_view<const B, int64_t>(queries_data, num_queries, this->dimension));
            this->quantizer_.template transform<T>(*res, q_dev_b, q_buf_t.data(), true);
        } else {
            // B != T and sizeof(T) != 1: only (B=float, T=half). Host fp32 -> fp16
            // cast (F16C / AVX, IEEE round-to-nearest-even — bit-identical to
            // mdspan_copy_kernel<__half>) into a pinned staging buffer, then a
            // single half-sized H2D copy.
            __half* host_h = handle.ensure_host_half_buf(n_q_elems);
            matrixone::cast_float_to_half_host(queries_data, host_h, n_q_elems);
            raft::copy(*res, q_dev_t,
                raft::make_host_matrix_view<const __half, int64_t>(host_h, num_queries, this->dimension));
        }
        // Legacy path syncs so build_search_bitset's stack-local host bitmap can
        // drain on the same stream. Prebuilt path skips: bitset H2D queues
        // naturally behind queries H2D and the kernel.
        if (!prebuilt) {
            raft::resource::sync_stream(*res);
        }


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
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                if (!this->replicated_indices_.empty()) {
                    auto it = this->replicated_indices_.find(handle.get_rank());
                    if (it != this->replicated_indices_.end()) {
                        auto shared_idx = std::static_pointer_cast<ivf_flat_index>(it->second);
                        local_index = shared_idx.get();
                    }
                }
                if (!local_index) {
                    local_index = index_.get();
                }
            }
            if (local_index) handle.set_index_ptr(static_cast<const ivf_flat_index*>(local_index));
        }

        if (local_index) {
            uint64_t start_row = 0, shard_sz = this->count;
            if (this->dist_mode == DistributionMode_SHARDED) {
                int rank = handle.get_rank();
                shard_sz = this->shard_sizes_[rank];
                start_row = 0;
                for (int r = 0; r < rank; ++r) start_row += this->shard_sizes_[r];
            }

            // See search_internal above (and ivf_pq.hpp) for the rationale.
            const uint32_t effective_k = matrixone::clamp_k_to_index_size(
                static_cast<uint32_t>(limit), shard_sz);

            if (effective_k == 0) {
                matrixone::fill_all_sentinel<int64_t>(
                    search_res.neighbors.data(), search_res.distances.data(),
                    static_cast<size_t>(num_queries) * limit, /*neighbor_sentinel=*/-1LL);
            } else {
                // Step C: reuse per-thread neighbor / distance workspaces.
                auto& n_buf = handle.neighbors_buf(static_cast<size_t>(num_queries) * limit);
                auto& d_buf = handle.distances_buf(static_cast<size_t>(num_queries) * limit);
                auto neighbors_device = raft::make_device_matrix_view<int64_t, int64_t>(
                    n_buf.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(effective_k));
                auto distances_device = raft::make_device_matrix_view<float, int64_t>(
                    d_buf.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(effective_k));

                std::shared_ptr<raft::core::bitset<uint32_t, int64_t>> bs_ptr;
                if (prebuilt) {
                    if (prebuilt->has_filter) {
                        bs_ptr = this->upload_host_mask(handle, prebuilt->mask, shard_sz);
                    } else if (prebuilt->deletes_only) {
                        bs_ptr = this->acquire_delete_bitset_device(handle, start_row, shard_sz);
                    }
                } else {
                    bs_ptr = this->build_search_bitset(handle, preds_json, start_row, shard_sz);
                }

                if (bs_ptr) {
                    auto filter = cuvs::neighbors::filtering::bitset_filter(bs_ptr->view());
                    cuvs::neighbors::ivf_flat::search(*res, search_params, *local_index,
                                                        raft::make_const_mdspan(q_dev_t),
                                                        neighbors_device, distances_device, filter);
                } else if constexpr (!std::is_same_v<T, __half>) {
                    // cuVS ships no dynamic_batching wrapper for ivf_flat::index<__half>,
                    // so half-typed IVF-Flat stays unbatched (this branch is discarded for T=__half).
                    if (this->batch_window() != 0) {
                        this->dynb_cache_.search(*res, handle.get_device_id(), local_index, search_params,
                                                 static_cast<int64_t>(effective_k),
                                                 static_cast<uint32_t>(search_params.n_probes), 0u,
                                                 this->dynb_concurrency_hint(),
                                                 this->dynb_conservative_dispatch(),
                                                 static_cast<double>(this->batch_window()) / 1000.0,
                                                 raft::make_const_mdspan(q_dev_t),
                                                 neighbors_device, distances_device);
                    } else {
                        cuvs::neighbors::ivf_flat::search(*res, search_params, *local_index,
                                                            raft::make_const_mdspan(q_dev_t),
                                                            neighbors_device, distances_device);
                    }
                } else {
                    cuvs::neighbors::ivf_flat::search(*res, search_params, *local_index,
                                                        raft::make_const_mdspan(q_dev_t),
                                                        neighbors_device, distances_device);
                }

                if (effective_k == static_cast<uint32_t>(limit)) {
                    raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device);
                    raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device);
                } else {
                    std::vector<int64_t> tmp_n(static_cast<size_t>(num_queries) * effective_k);
                    std::vector<float>   tmp_d(static_cast<size_t>(num_queries) * effective_k);
                    raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(tmp_n.data(), num_queries, effective_k), neighbors_device);
                    raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(tmp_d.data(), num_queries, effective_k), distances_device);
                    handle.sync();
                    matrixone::scatter_with_padding<int64_t>(
                        search_res.neighbors.data(), search_res.distances.data(),
                        tmp_n.data(), tmp_d.data(),
                        num_queries, static_cast<uint32_t>(limit), effective_k,
                        /*neighbor_sentinel=*/-1LL);
                }
            }
        } else {
            std::string msg = "IVF-Flat search error: No valid index found for device " + std::to_string(handle.get_device_id()) +
                             " (Mode: " + mode_name(this->dist_mode) + ")";
            throw std::runtime_error(msg);
        }
        handle.sync();

        // GPU search is done; take shared_lock only for the CPU-side ID translation
        // so that concurrent extend() calls (which write host_ids under unique_lock) are safe.
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            int64_t offset = 0;
            int64_t data_size = static_cast<int64_t>(this->count);
            if (this->dist_mode == DistributionMode_SHARDED) {
                for (int r = 0; r < handle.get_rank(); ++r) offset += (int64_t)this->shard_sizes_[r];
                data_size = static_cast<int64_t>(this->shard_sizes_[handle.get_rank()]);
            }
            // Always run map_neighbor_id — the helper bounds-checks raw
            // against data_size regardless of host_ids being empty, so
            // junk cuvs sentinels (UINT32_MAX, INT32_MAX, etc.) get
            // normalized to -1 even on the implicit-id path.
            for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
                search_res.neighbors[i] = map_neighbor_id(search_res.neighbors[i], offset, data_size, this->host_ids);
            }
        }

        transform_distance(this->metric, search_res.distances);
        return search_res;
    }

    void save(const std::string& filename) const {
        if (!this->is_loaded_ || (!index_)) throw std::runtime_error("Index not built");
        
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                cuvs::neighbors::ivf_flat::serialize(*(handle.get_raft_resources()), filename, *index_);
                return std::any();
            }
        );
        auto res = this->worker->wait(job_id).get();
        if (res.error) std::rethrow_exception(res.error);
        if (!this->host_ids.empty()) {
            this->save_ids(filename + ".ids");
        }
    }

    void load(const std::string& filename) {
        auto task = [&](raft_handle_wrapper_t& handle) -> std::any {
            auto res = handle.get_raft_resources();
            auto local_idx = std::make_unique<ivf_flat_index>(*res);
            cuvs::neighbors::ivf_flat::deserialize(*res, filename, local_idx.get());
            // Drain `res`'s stream so any H2D copy committed by deserialize is
            // visible before any search thread reads the loaded index. Without
            // this, a worker on a different stream can race the H2D and see
            // pre-copy garbage (manifests as 0 / NaN distances → bogus
            // neighbors). Same race fixed in cagra.hpp.
            raft::resource::sync_stream(*res);

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->count = local_idx->size();
                this->dimension = static_cast<uint32_t>(local_idx->dim());
                this->current_offset_ = this->count;

                if (this->dist_mode == DistributionMode_SINGLE_GPU) {
                    index_ = std::move(local_idx);
                } else if (this->dist_mode == DistributionMode_REPLICATED) {
                    this->replicated_indices_[handle.get_rank()] =
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

        // .ids sidecar is optional — see cagra.hpp's load() for the rationale.
        {
            std::ifstream probe(filename + ".ids", std::ios::binary);
            if (probe.good()) {
                probe.close();
                this->load_ids(filename + ".ids");
            }
        }

        this->is_loaded_ = true;
        this->train_quantizer_if_needed();
    }

    // Save all index components to a directory with manifest.json.
    void save_dir(const std::string& dir) const {
        if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty()))
            throw std::runtime_error("IVF-Flat index not built; cannot save_dir");

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
                    int key = handle.get_rank();
                    auto it = this->replicated_indices_.find(key);
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
                    auto it = this->replicated_indices_.find(handle.get_rank());
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
        if (this->dist_mode == DistributionMode_SHARDED && !this->shard_sizes_.empty()) {
            bp_json += ",\n    \"shard_sizes\": [";
            for (size_t i = 0; i < this->shard_sizes_.size(); ++i) {
                if (i) bp_json += ", ";
                bp_json += std::to_string(this->shard_sizes_[i]);
            }
            bp_json += "]";
        }
        this->write_manifest(dir, "ivf_flat", bp_json, comp_entries);
    }

    // Restore all index state from a directory previously written by save_dir().
    void load_dir(const std::string& dir, distribution_mode_t target_mode) {
        auto m = this->read_manifest(dir, "ivf_flat");
        if (this->dist_mode == DistributionMode_SHARDED && target_mode != DistributionMode_SHARDED)
            throw std::invalid_argument("cannot change dist_mode: index was built as SHARDED");
        this->dist_mode = target_mode;

        std::string bp_json = json_object(m.raw, "build_params");
        this->build_params.n_lists =
            static_cast<uint32_t>(json_int(bp_json, "n_lists", 1024));
        this->build_params.kmeans_trainset_fraction =
            std::stod(json_value(bp_json, "kmeans_trainset_fraction").empty()
                      ? "0.5" : json_value(bp_json, "kmeans_trainset_fraction"));
        {
            std::vector<int> ss = json_int_array(bp_json, "shard_sizes");
            if (!ss.empty()) {
                this->shard_sizes_.resize(ss.size());
                for (size_t i = 0; i < ss.size(); ++i)
                    this->shard_sizes_[i] = static_cast<uint64_t>(ss[i]);
            }
        }

        std::string idx_file   = json_value(m.comp_json, "index");
        std::vector<std::string> shard_files = json_string_array(m.comp_json, "shards");

        if (!idx_file.empty() && this->dist_mode == DistributionMode_SINGLE_GPU) {
            std::string full_path = dir + "/" + idx_file;
            auto task = [&, full_path](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                auto local_idx = std::make_unique<ivf_flat_index>(*res);
                cuvs::neighbors::ivf_flat::deserialize(*res, full_path, local_idx.get());
                // Drain `res`'s stream so deserialize's H2D copy is committed
                // before any search thread reads the loaded index. See the
                // longer comment in cagra.hpp load_dir() for the failure mode.
                raft::resource::sync_stream(*res);
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
                    // See SINGLE_GPU branch above for the rationale.
                    raft::resource::sync_stream(*res);
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_indices_[handle.get_rank()] =
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
                    // See SINGLE_GPU branch above for the rationale.
                    raft::resource::sync_stream(*res);
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_indices_[handle.get_rank()] =
                        std::shared_ptr<ivf_flat_index>(std::move(local_idx));
                    return std::any();
                }
            );
            this->count           = static_cast<uint64_t>(json_int(m.raw, "capacity"));
            this->current_offset_ = static_cast<uint64_t>(json_int(m.raw, "length"));

        } else {
            throw std::runtime_error("manifest has neither 'index' nor 'shards' in components");
        }

        this->load_common_components(dir, m);
        this->is_loaded_ = true;
    }

    uint32_t get_n_list() const { return this->build_params.n_lists; }

    void destroy() override {
        // Drop dynamic_batching wrappers *before* worker->stop() — they hold CUDA
        // streams/buffers tied to the worker threads' resources (see ivf_pq.hpp).
        this->dynb_cache_.clear();
        // ALL GPU-memory holders must also be freed *before* worker->stop() — they
        // free device memory back into the per-device RMM pool, which must happen
        // while the worker's CUDA streams are still alive (see ivf_pq.hpp::destroy).
        {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            index_.reset();
            this->replicated_indices_.clear();
            this->replicated_datasets_.clear();
            this->quantizer_.reset();
            this->dataset_device_ptr_.reset();
        }
        if (this->worker) this->worker->stop();
    }

    std::vector<T> get_centers() {
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
        
        auto task = [&](raft_handle_wrapper_t& handle) -> std::any {
            auto res = handle.get_raft_resources();
            const ivf_flat_index* local_index = nullptr;
            if (!this->replicated_indices_.empty()) {
                auto it = this->replicated_indices_.find(handle.get_rank());
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
                if constexpr (std::is_same_v<B, float>) {
                    this->quantizer_.template transform<T>(*res, centers_float_view, centers_device_target.data_handle(), true);
                } else {
                    // B == half: cast the float centers to B on-device, then quantize B -> T.
                    auto centers_b = raft::make_device_matrix<B, int64_t>(*res, n_centers, dim);
                    raft::copy(*res, centers_b.view(), centers_float_view);
                    this->quantizer_.template transform<T>(*res, centers_b.view(), centers_device_target.data_handle(), true);
                }
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
        std::string json = gpu_index_base_t<B, T, ivf_flat_build_params_t, int64_t>::info();
        json += ", \"type\": \"IVF-Flat\", \"ivf_flat\": {";
        if (index_) json += "\"mode\": \"Single-GPU\", \"size\": " + std::to_string(index_->size());
        else if (!this->replicated_indices_.empty()) json += "\"mode\": \"Local-Indices\", \"ranks\": " + std::to_string(this->replicated_indices_.size());
        else json += "\"built\": false";
        json += "}}";
        return json;
    }
};

} // namespace matrixone
