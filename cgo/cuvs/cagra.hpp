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
#include <raft/core/bitset.cuh>
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

// =============================================================================
// gpu_cagra_t — Developer Guide
// =============================================================================
//
// ALGORITHM
// ---------
// CAGRA (Concurrent And Graph-based RAG Algorithm) is a GPU-native graph-based
// ANN index. Unlike IVF types, CAGRA builds a proximity graph (k-NN graph) and
// navigates it during search. This gives faster query throughput than IVF on GPU
// for many workloads, at the cost of higher build time and memory.
//
// DATA TYPE (T)
// -------------
// Supported T: float, half (fp16), int8_t, uint8_t.
// IMPORTANT: `half` (fp16) is NOT supported for extend() — guarded by
//   `if constexpr (!std::is_same_v<T, half>)` in extend()/extend_float().
//   Attempting extend on a half-precision CAGRA index will throw at runtime.
//
// NEIGHBOR ID TYPE
// ----------------
// IdT = uint32_t (unlike IVF types which use int64_t).
// All host_ids / id_to_index_ structures use int64_t.
// The C wrapper and Go layer also use uint32_t for CAGRA neighbor IDs.
//
// LIFECYCLE
// ---------
//   1. Construct via one of:
//      - gpu_cagra_t(dataset, count, dim, ...)  — from in-memory dataset
//      - gpu_cagra_t(total_count, dim, ...)     — chunked: pre-allocate, fill via add_chunk()
//      - gpu_cagra_t(filename, dim, ...)        — load from serialized file
//      - gpu_cagra_t(unique_ptr<index>, ...)    — private; used only by merge()
//   2. Call start() — initializes the worker thread pool and CUDA context.
//   3. Call build() — triggers CAGRA graph construction (or file load).
//      If is_loaded_ is already true (private constructor path), build() is a no-op.
//   4. Call search() / search_float() to query.
//   5. Call extend() / extend_float() to add new vectors (SINGLE_GPU or REPLICATED).
//   6. Destructor calls destroy() which calls stop() on the worker.
//
// DISTRIBUTION MODES
// ------------------
// SINGLE_GPU:
//   One index on devices_[0]. Build, extend, search all dispatch to primary GPU.
//
// REPLICATED:
//   Full index replicated to all GPUs. build_internal() dispatches to all devices
//   via submit_all_devices(). extend_internal() also dispatches to all devices
//   concurrently. search() round-robins across all GPU replicas for load balancing.
//
// SHARDED:
//   Each GPU holds a disjoint slice of the dataset (a separate CAGRA sub-graph).
//   Build dispatches each shard to its GPU. Search dispatches to all shards
//   concurrently via submit_all_devices_no_wait(), collects results, and merges
//   them (merge_sharded_results). Each shard returns local IDs (0..shard_sz-1);
//   search_internal adds the shard offset before returning so callers see global IDs.
//   Extend is NOT supported for SHARDED mode — throws std::runtime_error.
//
// EXTEND
// ------
// cuVS exposes `cuvs::neighbors::cagra::extend()` which re-runs the graph
// construction over the existing index + new vectors. This is expensive.
// Rules:
//   - half (fp16) T: extend is NOT supported (compile-time guard).
//   - extend() takes raw T* vectors; extend_float() takes float* and handles
//     on-the-fly quantization for int8/uint8 T.
//   - REPLICATED: extend_internal() is submitted to all devices concurrently
//     via submit_all_devices(). set_ids() is called ONCE in extend() after
//     the worker wait, not inside extend_internal().
//   - count and current_offset_ are both updated together under unique_lock
//     after all device jobs complete.
//   - dataset_device_ptr_ / replicated_datasets_ are reset after extend.
//
// MERGE
// -----
// cuVS provides `cuvs::neighbors::cagra::merge()` which merges multiple CAGRA
// indices at the graph level (the only ANN type cuVS can merge natively).
// Rules:
//   - Only SINGLE_GPU (is_loaded_ == true, index_ != nullptr) sources accepted.
//   - A transient worker is created for the merge GPU operation.
//   - The merged index is always SINGLE_GPU on devs[0].
//   - host_ids merging: vectors are laid out as source[0] .. source[N-1].
//     If any source has custom IDs, all sources are merged (synthesizing
//     sequential IDs for sources that have none).
//   - init_deleted_bitset() is called on the resulting index.
//   - The private constructor (line ~169) sets is_loaded_=true and
//     current_offset_=count, making the merged index immediately queryable.
//
// SOFT-DELETE BITSET
// ------------------
// Inherited from gpu_index_base_t. Bit j = 1 means vector j is alive.
// SINGLE_GPU / REPLICATED: sync_device_bitset() uploads the full bitset to the
//   device; passed as a bitset_filter to cuvs::neighbors::cagra::search().
// SHARDED: sync_shard_bitset() uploads only the shard-local slice of the bitset
//   so that local IDs (0..shard_sz-1) index correctly into the filter.
//   shard_offset must be a multiple of 32 (enforced by the build-time rounding
//   rows_per_shard = (count/num_shards) & ~31) so no bit-shifting is needed.
//
// SEARCH
// ------
// search_internal() is dispatched via submit() (round-robin) for SINGLE_GPU /
//   REPLICATED, or submit_all_devices_no_wait() for SHARDED.
// If deleted_count_ > 0, the appropriate bitset is synced and passed to
//   cuvs::neighbors::cagra::search() as a bitset_filter (full bitset for
//   SINGLE_GPU/REPLICATED; shard-local slice for SHARDED).
// Search always returns `limit` neighbors; invalid/deleted results have
//   neighbor ID = 0xFFFFFFFF and distance = +inf.
//
// SERIALIZATION
// -------------
// save(filename): serializes via cuvs::neighbors::cagra::serialize().
// load(filename): deserializes into a new cagra_index; sets is_loaded_=true.
// save_dir() / load_dir(): directory format (manifest.json + index + ids + bitset).
//   Used by the database storage layer for persistence.
//
// LOCKING
// -------
// - shared_lock for reading index_/replicated_indices_ pointer and host_ids (ID translation)
// - unique_lock for writing count, current_offset_, host_ids, replicated_indices_,
//   and for snapshotting count/dataset in build()
// - NO lock during GPU calls themselves (build, extend, search kernels)
// - shared_lock IS held during post-GPU CPU-side ID translation in search_internal /
//   search_float_internal (protects host_ids and shard_sizes_ against concurrent extend)
// - extend_mutex_ (std::mutex in base) serializes concurrent extend() callers
// - Per-device bitset cache uses its own std::mutex (not the shared_mutex)
//
// =============================================================================

/**
 * @brief Search result containing neighbor IDs and distances.
 * Common for all CAGRA instantiations.
 */
struct cagra_search_result_t {
    std::vector<int64_t> neighbors; // External neighbor IDs (int64 user PKs after host_ids translation)
    std::vector<float> distances;  // Distances to nearest neighbors
};

/**
 * @brief gpu_cagra_t implements a CAGRA index that can run on a single GPU or sharded across multiple GPUs.
 */
template <typename T>
class gpu_cagra_t : public gpu_index_base_t<T, cagra_build_params_t, int64_t> {
public:
    using cagra_index = cuvs::neighbors::cagra::index<T, uint32_t>;
    using search_result_t = cagra_search_result_t;

    // Internal index storage
    std::unique_ptr<cagra_index> index_;
    std::string index_filename_;

    ~gpu_cagra_t() override {
        this->destroy();
    }

    // Unified Constructor for building from dataset
    gpu_cagra_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension,
                    distance_type_t m, const cagra_build_params_t& bp,
                    const std::vector<int>& devices, uint32_t nthread, distribution_mode_t mode,
                    const int64_t* ids = nullptr) {

        this->dimension = dimension;
        this->count = static_cast<uint64_t>(count_vectors);
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
    gpu_cagra_t(uint64_t total_count, uint32_t dimension, distance_type_t m,
                    const cagra_build_params_t& bp, const std::vector<int>& devices,
                    uint32_t nthread, distribution_mode_t mode,
                    const int64_t* ids = nullptr) {

        this->dimension = dimension;
        this->count = static_cast<uint64_t>(total_count);
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
    gpu_cagra_t(const std::string& filename, uint32_t dimension, distance_type_t m,
                    const cagra_build_params_t& bp, const std::vector<int>& devices,
                    uint32_t nthread, distribution_mode_t mode) {

        this->index_filename_ = filename;
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
        
        this->count = static_cast<uint64_t>(index_->size());
        this->current_offset_ = this->count;
        this->build_params.graph_degree = static_cast<size_t>(index_->graph_degree());
        this->is_loaded_ = true;
    }

    void start() override {
        auto init_fn = [&](raft_handle_wrapper_t&) -> std::any {
            return std::any();
        };
        auto stop_fn = [&](raft_handle_wrapper_t&) -> std::any {
            return std::any();
        };
        this->worker->start(init_fn, stop_fn);
    }

    /**
     * @brief Merges multiple CAGRA indices into a single index.
     * Only works for SINGLE_GPU indices.
     */
    static std::unique_ptr<gpu_cagra_t<T>> merge(const std::vector<gpu_index_base_t<T, cagra_build_params_t, int64_t>*>& base_indices, uint32_t nthread, const std::vector<int>& devs) {
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

        // Merge host_ids: the cuVS merge lays vectors as source[0]..source[N-1] in order.
        // If any source has custom IDs, concatenate them all (synthesising sequential IDs
        // for sources that have none) so the merged index can map back to external IDs.
        bool any_has_ids = false;
        for (auto* bi : base_indices) {
            if (!bi->host_ids.empty()) { any_has_ids = true; break; }
        }
        if (any_has_ids) {
            std::vector<int64_t> merged_ids;
            merged_ids.reserve(new_idx->count);
            int64_t offset = 0;
            for (auto* bi : base_indices) {
                uint32_t n = bi->count;
                if (!bi->host_ids.empty()) {
                    merged_ids.insert(merged_ids.end(), bi->host_ids.begin(), bi->host_ids.end());
                } else {
                    for (uint32_t i = 0; i < n; ++i) merged_ids.push_back(offset + i);
                }
                offset += n;
            }
            new_idx->set_ids(merged_ids.data(), new_idx->count, 0);
        }

        new_idx->init_deleted_bitset();
        return new_idx;
    }

    void build() override {
        if (this->is_loaded_) return;
        if (!this->index_filename_.empty()) {
            load(this->index_filename_);
            return;
        }
        {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            this->count = static_cast<uint64_t>(this->current_offset_);
            if (this->flattened_host_dataset.size() > (size_t)this->current_offset_ * this->dimension)
                this->flattened_host_dataset.resize((size_t)this->current_offset_ * this->dimension);
        }
        if (this->count == 0) {
            if (this->pending_total_count_ == 0) {
                this->is_loaded_ = true;
                return;
            }
        }

        // std::cout << "[DEBUG] CAGRA build: Starting build count=" << this->count << " dim=" << this->dimension << " metric=" << (int)this->metric << std::endl;

        this->train_quantizer_if_needed();
        if (!this->worker) throw std::runtime_error("Worker not initialized");

        // Validate build params against effective per-shard row count before
        // submitting to worker threads. submit_all_devices() does not propagate
        // exceptions, so validation must happen here in the calling thread.
        if (this->dist_mode == DistributionMode_SHARDED) {
            int num_shards = static_cast<int>(this->devices_.size());
            uint64_t rows_per_shard = (this->count / num_shards) & ~static_cast<uint64_t>(31);
            uint64_t last_shard_rows = this->count - rows_per_shard * (num_shards - 1);
            // The smallest shard (worst case) is the minimum of uniform and last shard.
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
            // Collective build requires participation from all GPUs
            this->worker->submit_all_devices([&](raft_handle_wrapper_t& handle) -> std::any {
                this->build_internal(handle);
                return std::any();
            });
        }

        this->is_loaded_ = true;
        this->init_deleted_bitset();
        this->flattened_host_dataset.clear();
        this->flattened_host_dataset.shrink_to_fit();
        // std::cout << "[DEBUG] CAGRA build: Build completed successfully" << std::endl;
    }

    static void validate_build_params(const cagra_build_params_t& bp, uint64_t num_rows) {
        if (num_rows < 2) {
            throw std::invalid_argument(
                "CAGRA build requires at least 2 vectors (got " + std::to_string(num_rows) + ")");
        }
        if (bp.graph_degree >= num_rows) {
            throw std::invalid_argument(
                "number of vectors per shard (" + std::to_string(num_rows) +
                ") must be larger than build_params.graph_degree (" +
                std::to_string(bp.graph_degree) + ")");
        }
        if (bp.intermediate_graph_degree >= num_rows) {
            throw std::invalid_argument(
                "number of vectors per shard (" + std::to_string(num_rows) +
                ") must be larger than build_params.intermediate_graph_degree (" +
                std::to_string(bp.intermediate_graph_degree) + ")");
        }
    }

    void build_internal(raft_handle_wrapper_t& handle) {
        cuvs::neighbors::cagra::index_params index_params;
        index_params.metric = static_cast<cuvs::distance::DistanceType>(this->metric);
        index_params.intermediate_graph_degree = this->build_params.intermediate_graph_degree;
        index_params.graph_degree = this->build_params.graph_degree;
        index_params.attach_dataset_on_build = this->build_params.attach_dataset_on_build;

        if constexpr (std::is_same_v<T, half> || sizeof(T) == 1) {
            // When T=half, NN-Descent with DIST_COMP_DTYPE::AUTO picks fp16 arithmetic
            // for distance computations during graph construction.  fp16's limited
            // precision creates many distance ties, making NN-Descent's stochastic
            // neighbor selection highly variable across builds and causing the
            // occasional sudden recall drop that is observed on repeat builds of the
            // same dataset.  Forcing FP32 distance computation during build eliminates
            // the ties and gives stable, consistent graph quality.  The stored dataset
            // remains fp16; only the build-time kNN distances are promoted to fp32.
            cuvs::neighbors::graph_build_params::nn_descent_params nd_params(
                this->build_params.intermediate_graph_degree,
                static_cast<cuvs::distance::DistanceType>(this->metric));
            nd_params.dist_comp_dtype = cuvs::neighbors::nn_descent::DIST_COMP_DTYPE::FP32;
            nd_params.max_iterations = 30; // Increase from default (usually 20)
            index_params.graph_build_params = nd_params;
        }

        if (this->dist_mode == DistributionMode_REPLICATED) {
            auto res = handle.get_raft_resources();
            auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
            raft::copy(*res, dataset_device.view(), raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data(), this->count, this->dimension));
            raft::resource::sync_stream(*res);

            auto local_idx = std::make_unique<cagra_index>(cuvs::neighbors::cagra::build(
                *res, index_params, raft::make_const_mdspan(dataset_device.view())));
            
            handle.set_index_ptr(static_cast<const cagra_index*>(local_idx.get()));

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_indices_[handle.get_device_id()] = std::shared_ptr<cagra_index>(std::move(local_idx));
                this->replicated_datasets_[handle.get_device_id()] = std::make_shared<raft::device_matrix<T, int64_t>>(std::move(dataset_device));
            }
            handle.sync();
        } else if (this->dist_mode == DistributionMode_SHARDED) {
            auto res = handle.get_raft_resources();
            int num_shards = this->devices_.size();
            int rank = handle.get_rank();
            
            // Round down to a multiple of 32 so every shard offset is word-aligned in
            // the deleted bitset, making shard-slice sync cheap (no bit-shifting needed).
            uint64_t rows_per_shard = (this->count / num_shards) & ~static_cast<uint64_t>(31);
            uint64_t start_row = rank * rows_per_shard;
            uint64_t num_rows = (rank == num_shards - 1) ? (this->count - start_row) : rows_per_shard;
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->shard_sizes_[rank] = num_rows;
            }

            // std::cout << "[DEBUG] CAGRA build SHARDED: rank=" << rank << " start_row=" << start_row << " num_rows=" << num_rows << " rows_per_shard=" << rows_per_shard << std::endl;
            // if (!this->host_ids.empty()) {
            //     std::cout << "[DEBUG] Shard " << rank << " first 3 host_ids: " 
            //               << this->host_ids[start_row] << ", " 
            //               << this->host_ids[start_row+1] << ", " 
            //               << this->host_ids[start_row+2] << std::endl;
            // }

            auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)num_rows, (int64_t)this->dimension);
            raft::copy(*res, dataset_device.view(), 
                       raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data() + (start_row * this->dimension), num_rows, this->dimension));
            raft::resource::sync_stream(*res);

            auto local_idx = std::make_unique<cagra_index>(cuvs::neighbors::cagra::build(
                *res, index_params, raft::make_const_mdspan(dataset_device.view())));
            
            handle.set_index_ptr(static_cast<const cagra_index*>(local_idx.get()));

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_indices_[handle.get_device_id()] = std::shared_ptr<cagra_index>(std::move(local_idx));
                this->replicated_datasets_[handle.get_device_id()] = std::make_shared<raft::device_matrix<T, int64_t>>(std::move(dataset_device));
            }
            handle.sync();
        } else {
            // Do all GPU work outside the lock — holding shared_mutex across GPU calls
            // would block concurrent readers for the entire build duration.
            auto res = handle.get_raft_resources();
            auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
            raft::copy(*res, dataset_device.view(), raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data(), this->count, this->dimension));
            raft::resource::sync_stream(*res);

            auto new_idx = std::make_unique<cagra_index>(cuvs::neighbors::cagra::build(
                *res, index_params, raft::make_const_mdspan(dataset_device.view())));
            using dataset_t = raft::device_matrix<T, int64_t>;
            auto new_dataset = std::make_shared<dataset_t>(std::move(dataset_device));
            handle.sync();

            // Assign results under lock
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                index_ = std::move(new_idx);
                this->dataset_device_ptr_ = std::move(new_dataset);
            }
        }
    }

    void extend_internal(raft_handle_wrapper_t& handle, const T* additional_data, uint64_t num_vectors) {
        if constexpr (std::is_same_v<T, half>) {
            // cuVS cagra::extend does not support float16 — guarded in extend() but
            // extend_internal must be constexpr-safe for all T.
            throw std::runtime_error("CAGRA extend is not supported for float16 (half) by cuVS.");
        } else {
            auto res = handle.get_raft_resources();

            auto additional_dataset_device = raft::make_device_matrix<T, int64_t>(
                *res, static_cast<int64_t>(num_vectors), static_cast<int64_t>(this->dimension));
            raft::copy(*res, additional_dataset_device.view(),
                       raft::make_host_matrix_view<const T, int64_t>(additional_data, num_vectors, this->dimension));
            raft::resource::sync_stream(*res);

            cuvs::neighbors::cagra::extend_params params;

            if (this->dist_mode == DistributionMode_REPLICATED) {
                cagra_index* idx;
                {
                    std::shared_lock<std::shared_mutex> lock(this->mutex_);
                    idx = static_cast<cagra_index*>(this->replicated_indices_.at(handle.get_device_id()).get());
                }
                cuvs::neighbors::cagra::extend(*res, params, raft::make_const_mdspan(additional_dataset_device.view()), *idx);
                handle.sync();
                {
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_datasets_.erase(handle.get_device_id());
                }
            } else {
                // index_ is accessed without mutex here. This is safe because:
                // (a) the GPU worker serializes all tasks on the main device, so no
                //     concurrent GPU search can be running while extend is in-flight;
                // (b) extend_mutex_ in extend() ensures only one extend at a time.
                cuvs::neighbors::cagra::extend(*res, params, raft::make_const_mdspan(additional_dataset_device.view()), *index_);
                handle.sync();
                {
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->dataset_device_ptr_.reset();
                }
            }
        }
    }

    void extend(const T* additional_data, uint64_t num_vectors, const int64_t* new_ids = nullptr) {
        {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_) {
                // Pre-build: buffer data for later build()
                uint64_t old_size = this->flattened_host_dataset.size();
                this->flattened_host_dataset.resize(old_size + num_vectors * this->dimension);
                std::copy(additional_data, additional_data + num_vectors * this->dimension,
                          this->flattened_host_dataset.begin() + old_size);
                if (new_ids) this->set_ids_internal(new_ids, num_vectors, this->count);
                this->count += num_vectors;
                this->current_offset_ += num_vectors;
                return;
            }
        }

        if (this->dist_mode == DistributionMode_SHARDED)
            throw std::runtime_error("extend: SHARDED mode not supported for CAGRA");

        if constexpr (std::is_same_v<T, half>) {
            throw std::runtime_error("CAGRA extend is not supported for float16 (half) by cuVS.");
        } else {
            if (num_vectors == 0) return;

            // Serialize concurrent extends — callers queue here rather than race
            std::lock_guard<std::mutex> extend_lock(this->extend_mutex_);

            if (this->dist_mode == DistributionMode_REPLICATED) {
                this->worker->submit_all_devices([&](raft_handle_wrapper_t& handle) -> std::any {
                    this->extend_internal(handle, additional_data, num_vectors);
                    return std::any();
                });
            } else {
                uint64_t job_id = this->worker->submit_main(
                    [&](raft_handle_wrapper_t& handle) -> std::any {
                        this->extend_internal(handle, additional_data, num_vectors);
                        return std::any();
                    });
                auto result = this->worker->wait(job_id).get();
                if (result.error) std::rethrow_exception(result.error);
            }

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                if (new_ids) this->set_ids_internal(new_ids, num_vectors, static_cast<uint64_t>(this->count));
                this->count += num_vectors;
                this->current_offset_ += num_vectors;
            }
        }
    }

    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/, uint32_t limit, const cagra_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) return search_result_t{};
        }

        if (this->dist_mode == DistributionMode_SHARDED) {
            int num_shards = this->devices_.size();
            // std::cout << "[DEBUG] CAGRA search SHARDED: num_shards=" << num_shards << " num_queries=" << num_queries << " limit=" << limit << std::endl;
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

        // std::cout << "[DEBUG] CAGRA search: num_queries=" << num_queries << " limit=" << limit << " itopk=" << sp.itopk_size << std::endl;

        if (this->worker->batch_window() == 0) {
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

    uint64_t search_async(const T* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/, uint32_t limit, const cagra_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || this->dimension == 0) return 0;
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) return 0;
        }

        auto queries_copy = std::make_shared<std::vector<T>>(queries_data, queries_data + num_queries * this->dimension);

        if (this->dist_mode == DistributionMode_SHARDED) {
            auto task = [this, num_queries, limit, sp, queries_copy](raft_handle_wrapper_t& /*handle*/) -> std::any {
                int num_shards = this->devices_.size();
                std::vector<search_result_t> shard_results(num_shards);
                auto shard_search_task = [this, num_queries, limit, sp, queries_copy](raft_handle_wrapper_t& gpu_handle) -> std::any {
                    return this->search_internal(gpu_handle, queries_copy->data(), num_queries, limit, sp);
                };
                auto job_ids = this->worker->submit_all_devices_no_wait(shard_search_task);
                for (int i = 0; i < num_shards; ++i) {
                    auto res = this->worker->wait(job_ids[i]).get();
                    if (res.error) std::rethrow_exception(res.error);
                    shard_results[i] = std::any_cast<search_result_t>(res.result);
                }
                return this->merge_sharded_results(shard_results, num_queries, limit);
            };
            return this->worker->submit_main(task);
        }

        auto task = [this, num_queries, limit, sp, queries_copy](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_internal(handle, queries_copy->data(), num_queries, limit, sp);
        };
        return this->worker->submit(task);
    }

    search_result_t search_wait(uint64_t job_id) {
        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        return std::any_cast<search_result_t>(result_wait.result);
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
        search_res.distances.resize(num_queries * limit);
        // search_res.neighbors is intentionally NOT pre-sized here;
        // the resize(n, -1LL) below initializes all slots to -1LL correctly.

        cuvs::neighbors::cagra::search_params search_params{};
        search_params.itopk_size = std::max((size_t)limit, (size_t)sp.itopk_size);
        search_params.search_width = sp.search_width;

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
            // Tiered fallback: Replicated -> Single (lock covers both the map read and index_ read)
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_device_id());
                if (it != this->replicated_indices_.end()) {
                    auto shared_idx = std::static_pointer_cast<cagra_index>(it->second);
                    local_index = shared_idx.get();
                }
                if (!local_index) {
                    local_index = index_.get();
                }
            }
            if (local_index) handle.set_index_ptr(static_cast<const cagra_index*>(local_index));
        }

        // Temporary buffer for uint32_t raw GPU neighbor positions.
        std::vector<uint32_t> raw_neighbors(num_queries * limit, (uint32_t)-1);

        if (local_index) {
            auto queries_device = raft::make_device_matrix<T, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));
            raft::copy(*res, queries_device.view(), raft::make_host_matrix_view<const T, int64_t>(queries_data, num_queries, this->dimension));
            raft::resource::sync_stream(*res);

            auto neighbors_device = raft::make_device_matrix<uint32_t, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
            auto distances_device = raft::make_device_matrix<float, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

            if (this->deleted_count_ > 0) {
                using bs_t = raft::core::bitset<uint32_t, int64_t>;
                bs_t* bs;
                if (this->dist_mode == DistributionMode_SHARDED) {
                    int rank = handle.get_rank();
                    uint64_t shard_sz = this->shard_sizes_[rank];
                    uint64_t shard_offset = 0;
                    for (int r = 0; r < rank; ++r) shard_offset += this->shard_sizes_[r];
                    this->sync_shard_bitset(handle.get_device_id(), shard_offset, shard_sz, *res);
                    bs = static_cast<bs_t*>(this->get_device_shard_bitset_info(handle.get_device_id())->ptr.get());
                } else {
                    this->sync_device_bitset(handle.get_device_id(), *res);
                    bs = static_cast<bs_t*>(this->get_device_bitset_info(handle.get_device_id())->ptr.get());
                }
                auto filter = cuvs::neighbors::filtering::bitset_filter(bs->view());
                cuvs::neighbors::cagra::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(queries_device.view()),
                                                    neighbors_device.view(), distances_device.view(), filter);
            } else {
                cuvs::neighbors::cagra::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(queries_device.view()),
                                                    neighbors_device.view(), distances_device.view());
            }

            raft::copy(*res, raft::make_host_matrix_view<uint32_t, int64_t>(raw_neighbors.data(), num_queries, limit), neighbors_device.view());
            raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device.view());
        } else {
            std::string msg = "CAGRA search error: No valid index found for device " + std::to_string(handle.get_device_id()) +
                             " (Mode: " + mode_name(this->dist_mode) + ")";
            throw std::runtime_error(msg);
        }
        handle.sync(); // Local sync

        // GPU search is done; translate raw uint32 positions → int64 external IDs.
        // Take shared_lock only for the CPU-side host_ids read so that concurrent
        // extend() calls (which write host_ids under unique_lock) are safe.
        search_res.neighbors.resize(num_queries * limit, -1LL);
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (this->dist_mode == DistributionMode_SHARDED) {
                uint64_t offset = 0;
                for (int r = 0; r < handle.get_rank(); ++r) offset += this->shard_sizes_[r];
                
                // std::cout << "[DEBUG] CAGRA search_internal SHARDED: rank=" << handle.get_rank() 
                //           << " offset=" << offset << " host_ids.size=" << this->host_ids.size() 
                //           << " count=" << this->count << " num_queries=" << num_queries << std::endl;

                for (size_t i = 0; i < raw_neighbors.size(); ++i) {
                    if (raw_neighbors[i] != (uint32_t)-1) {
                        uint64_t global_pos = (uint64_t)raw_neighbors[i] + offset;
                        if (this->host_ids.empty()) {
                            search_res.neighbors[i] = (int64_t)global_pos;
                        } else if (global_pos < this->host_ids.size()) {
                            search_res.neighbors[i] = (int64_t)this->host_ids[global_pos];
                        } else {
                            std::cout << "[ERROR] CAGRA sharded: global_pos " << global_pos 
                                      << " out of range (raw=" << raw_neighbors[i] 
                                      << " offset=" << offset 
                                      << " host_ids.size=" << this->host_ids.size() << ")" << std::endl;
                        }
                    }
                }
                
                // if (num_queries > 0) {
                //     std::cout << "[DEBUG] Shard " << handle.get_rank() << " query 0 first 5 results: ";
                //     for (uint32_t k = 0; k < std::min(limit, 5U); ++k) {
                //         std::cout << "raw=" << raw_neighbors[k] << "->id=" << search_res.neighbors[k] << " ";
                //     }
                //     std::cout << std::endl;
                // }
            } else {
                for (size_t i = 0; i < raw_neighbors.size(); ++i) {
                    if (raw_neighbors[i] != (uint32_t)-1) {
                        search_res.neighbors[i] = this->host_ids.empty()
                            ? (int64_t)raw_neighbors[i]
                            : (int64_t)this->host_ids[raw_neighbors[i]];
                    }
                }
            }
        }

        this->transform_distance(this->metric, search_res.distances);
        return search_res;
    }

    search_result_t search_float(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const cagra_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) return search_result_t{};
        }

        if (this->dist_mode == DistributionMode_SHARDED) {
            int num_shards = this->devices_.size();
            // std::cout << "[DEBUG] CAGRA search SHARDED: num_shards=" << num_shards << " num_queries=" << num_queries << " limit=" << limit << std::endl;
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

        // std::cout << "[DEBUG] CAGRA search_float: num_queries=" << num_queries << " limit=" << limit << std::endl;

        if (this->worker->batch_window() == 0) {
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

    uint64_t search_float_async(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const cagra_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || this->dimension == 0) return 0;
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) return 0;
        }

        auto queries_copy = std::make_shared<std::vector<float>>(queries_data, queries_data + num_queries * query_dimension);

        if (this->dist_mode == DistributionMode_SHARDED) {
            auto task = [this, num_queries, query_dimension, limit, sp, queries_copy](raft_handle_wrapper_t& /*handle*/) -> std::any {
                int num_shards = this->devices_.size();
                std::vector<search_result_t> shard_results(num_shards);
                auto shard_search_task = [this, num_queries, query_dimension, limit, sp, queries_copy](raft_handle_wrapper_t& gpu_handle) -> std::any {
                    return this->search_float_internal(gpu_handle, queries_copy->data(), num_queries, query_dimension, limit, sp);
                };
                auto job_ids = this->worker->submit_all_devices_no_wait(shard_search_task);
                for (int i = 0; i < num_shards; ++i) {
                    auto res = this->worker->wait(job_ids[i]).get();
                    if (res.error) std::rethrow_exception(res.error);
                    shard_results[i] = std::any_cast<search_result_t>(res.result);
                }
                return this->merge_sharded_results(shard_results, num_queries, limit);
            };
            return this->worker->submit_main(task);
        }

        auto task = [this, num_queries, query_dimension, limit, sp, queries_copy](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_float_internal(handle, queries_copy->data(), num_queries, query_dimension, limit, sp);
        };
        return this->worker->submit(task);
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

    search_result_t search_float_internal(raft_handle_wrapper_t& handle, const float* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/,
                        uint32_t limit, const cagra_search_params_t& sp) {
        // std::shared_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        // std::cout << "[DEBUG " << get_timestamp() << "] CAGRA search_float_internal: num_queries=" << num_queries << " limit=" << limit << " device=" << handle.get_device_id() << std::endl;

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

        // Temporary buffer for uint32_t raw GPU neighbor positions.
        std::vector<uint32_t> raw_neighbors_f(num_queries * limit, (uint32_t)-1);

        search_result_t search_res;
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::cagra::search_params search_params{};
        search_params.itopk_size = std::max((size_t)limit, (size_t)sp.itopk_size);
        search_params.search_width = sp.search_width;

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
            // Tiered fallback: Replicated -> Single (lock covers both the map read and index_ read)
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_device_id());
                if (it != this->replicated_indices_.end()) {
                    auto shared_idx = std::static_pointer_cast<cagra_index>(it->second);
                    local_index = shared_idx.get();
                }
                if (!local_index) {
                    local_index = index_.get();
                }
            }
            if (local_index) handle.set_index_ptr(static_cast<const cagra_index*>(local_index));
        }

        if (local_index) {
            auto neighbors_device = raft::make_device_matrix<uint32_t, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
            auto distances_device = raft::make_device_matrix<float, int64_t>(*res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

            if (this->deleted_count_ > 0) {
                using bs_t = raft::core::bitset<uint32_t, int64_t>;
                bs_t* bs;
                if (this->dist_mode == DistributionMode_SHARDED) {
                    int rank = handle.get_rank();
                    uint64_t shard_sz = this->shard_sizes_[rank];
                    uint64_t shard_offset = 0;
                    for (int r = 0; r < rank; ++r) shard_offset += this->shard_sizes_[r];
                    this->sync_shard_bitset(handle.get_device_id(), shard_offset, shard_sz, *res);
                    bs = static_cast<bs_t*>(this->get_device_shard_bitset_info(handle.get_device_id())->ptr.get());
                } else {
                    this->sync_device_bitset(handle.get_device_id(), *res);
                    bs = static_cast<bs_t*>(this->get_device_bitset_info(handle.get_device_id())->ptr.get());
                }
                auto filter = cuvs::neighbors::filtering::bitset_filter(bs->view());
                cuvs::neighbors::cagra::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(q_dev_t.view()),
                                                    neighbors_device.view(), distances_device.view(), filter);
            } else {
                cuvs::neighbors::cagra::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(q_dev_t.view()),
                                                    neighbors_device.view(), distances_device.view());
            }

            raft::copy(*res, raft::make_host_matrix_view<uint32_t, int64_t>(raw_neighbors_f.data(), num_queries, limit), neighbors_device.view());
            raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device.view());
        } else {
            std::string msg = "CAGRA search error: No valid index found for device " + std::to_string(handle.get_device_id()) +
                             " (Mode: " + mode_name(this->dist_mode) + ")";
            throw std::runtime_error(msg);
        }
        handle.sync();

        // Translate raw uint32 positions → int64 external IDs.
        search_res.neighbors.resize(num_queries * limit, -1LL);
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (this->dist_mode == DistributionMode_SHARDED) {
                uint64_t offset = 0;
                for (int r = 0; r < handle.get_rank(); ++r) offset += this->shard_sizes_[r];
                
                // std::cout << "[DEBUG] CAGRA search_float_internal SHARDED: rank=" << handle.get_rank() 
                //           << " offset=" << offset << " host_ids.size=" << this->host_ids.size() 
                //           << " count=" << this->count << " num_queries=" << num_queries << std::endl;

                for (size_t i = 0; i < raw_neighbors_f.size(); ++i) {
                    if (raw_neighbors_f[i] != (uint32_t)-1) {
                        uint64_t global_pos = (uint64_t)raw_neighbors_f[i] + offset;
                        if (this->host_ids.empty()) {
                            search_res.neighbors[i] = (int64_t)global_pos;
                        } else if (global_pos < this->host_ids.size()) {
                            search_res.neighbors[i] = (int64_t)this->host_ids[global_pos];
                        } else {
                            std::cout << "[ERROR] CAGRA sharded: global_pos " << global_pos 
                                      << " out of range (raw=" << raw_neighbors_f[i] 
                                      << " offset=" << offset 
                                      << " host_ids.size=" << this->host_ids.size() << ")" << std::endl;
                        }
                    }
                }
                
                // if (num_queries > 0) {
                //     std::cout << "[DEBUG] Shard " << handle.get_rank() << " query 0 first 5 results: ";
                //     for (uint32_t k = 0; k < std::min(limit, 5U); ++k) {
                //         std::cout << "raw=" << raw_neighbors_f[k] << "->id=" << search_res.neighbors[k] << " ";
                //     }
                //     std::cout << std::endl;
                // }
            } else {
                for (size_t i = 0; i < raw_neighbors_f.size(); ++i) {
                    if (raw_neighbors_f[i] != (uint32_t)-1) {
                        search_res.neighbors[i] = this->host_ids.empty()
                            ? (int64_t)raw_neighbors_f[i]
                            : (int64_t)this->host_ids[raw_neighbors_f[i]];
                    }
                }
            }
        }

        this->transform_distance(this->metric, search_res.distances);
        return search_res;
    }

    std::string info() const override {
        std::string json = gpu_index_base_t<T, cagra_build_params_t, int64_t>::info();
        json += ", \"type\": \"CAGRA\", \"cagra\": {";
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
        if (index_) json += "\"mode\": \"Single-GPU\", \"size\": " + std::to_string(index_->size());
        else if (!this->replicated_indices_.empty()) json += "\"mode\": \"Local-Indices\", \"ranks\": " + std::to_string(this->replicated_indices_.size());
        else json += "\"built\": false";
        json += "}}";
        return json;
    }

    void save(const std::string& filename) const {
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) throw std::runtime_error("Index not built");
        }
        
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                cuvs::neighbors::cagra::serialize(*(handle.get_raft_resources()), filename, *index_);
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
            auto local_idx = std::make_unique<cagra_index>(*res);
            cuvs::neighbors::cagra::deserialize(*res, filename, local_idx.get());

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->count = static_cast<uint64_t>(local_idx->size());
                this->dimension = static_cast<uint32_t>(local_idx->dim());
                this->current_offset_ = this->count;

                if (this->dist_mode == DistributionMode_SINGLE_GPU) {
                    index_ = std::move(local_idx);
                } else {
                    this->replicated_indices_[handle.get_device_id()] = std::shared_ptr<cagra_index>(std::move(local_idx));
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

    // Save all index components (index data, IDs, quantizer, bitset) to a directory.
    // Also writes manifest.json describing all components.
    // Supports SingleGPU, REPLICATED, and SHARDED distribution modes.
    void save_dir(const std::string& dir) const {
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty()))
                throw std::runtime_error("CAGRA index not built; cannot save_dir");
        }

        this->ensure_dir(dir);
        auto comp_entries = this->save_common_components(dir);

        if (this->dist_mode == DistributionMode_SINGLE_GPU) {
            uint64_t job_id = this->worker->submit_main(
                [&](raft_handle_wrapper_t& handle) -> std::any {
                    cuvs::neighbors::cagra::serialize(
                        *(handle.get_raft_resources()), dir + "/index.bin", *index_);
                    return std::any();
                }
            );
            auto wait_res = this->worker->wait(job_id).get();
            if (wait_res.error) std::rethrow_exception(wait_res.error);
            comp_entries.push_back("    \"index\": \"index.bin\"");

        } else if (this->dist_mode == DistributionMode_REPLICATED) {
            // All replicas are identical — serialize just one (main device's replica)
            uint64_t job_id = this->worker->submit_main(
                [&](raft_handle_wrapper_t& handle) -> std::any {
                    int dev_id = handle.get_device_id();
                    auto it = this->replicated_indices_.find(dev_id);
                    if (it == this->replicated_indices_.end())
                        it = this->replicated_indices_.begin();
                    if (it == this->replicated_indices_.end())
                        throw std::runtime_error("No replicated index found to serialize");
                    cuvs::neighbors::cagra::serialize(
                        *(handle.get_raft_resources()), dir + "/index.bin",
                        *std::static_pointer_cast<cagra_index>(it->second));
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
                        cuvs::neighbors::cagra::serialize(
                            *(handle.get_raft_resources()), shard_file,
                            *std::static_pointer_cast<cagra_index>(it->second));
                    }
                    return std::any();
                }
            );
            comp_entries.push_back(this->shards_comp_entry());
        }

        std::string bp_json =
            "    \"intermediate_graph_degree\": " +
                std::to_string(this->build_params.intermediate_graph_degree) + ",\n" +
            "    \"graph_degree\": " +
                std::to_string(this->build_params.graph_degree);
        if (this->dist_mode == DistributionMode_SHARDED && !this->shard_sizes_.empty()) {
            bp_json += ",\n    \"shard_sizes\": [";
            for (size_t i = 0; i < this->shard_sizes_.size(); ++i) {
                if (i) bp_json += ", ";
                bp_json += std::to_string(this->shard_sizes_[i]);
            }
            bp_json += "]";
        }
        this->write_manifest(dir, "cagra", bp_json, comp_entries);
    }

    // Restore all index state from a directory previously written by save_dir().
    // The index object must have been constructed with the appropriate device list
    // and worker already initialized.
    void load_dir(const std::string& dir, distribution_mode_t target_mode) {
        auto m = this->read_manifest(dir, "cagra");
        if (this->dist_mode == DistributionMode_SHARDED && target_mode != DistributionMode_SHARDED)
            throw std::invalid_argument("cannot change dist_mode: index was built as SHARDED");
        this->dist_mode = target_mode;

        std::string bp_json = json_object(m.raw, "build_params");
        this->build_params.intermediate_graph_degree =
            static_cast<size_t>(json_int(bp_json, "intermediate_graph_degree", 128));
        this->build_params.graph_degree =
            static_cast<size_t>(json_int(bp_json, "graph_degree", 64));
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
                auto local_idx = std::make_unique<cagra_index>(*res);
                cuvs::neighbors::cagra::deserialize(*res, full_path, local_idx.get());
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
                    auto local_idx = std::make_unique<cagra_index>(*res);
                    cuvs::neighbors::cagra::deserialize(*res, full_path, local_idx.get());
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_indices_[handle.get_device_id()] =
                        std::shared_ptr<cagra_index>(std::move(local_idx));
                    return std::any();
                }
            );

        } else if (!shard_files.empty()) { // SHARDED
            this->worker->submit_all_devices(
                [&, shard_files, dir](raft_handle_wrapper_t& handle) -> std::any {
                    int rank = handle.get_rank();
                    if (rank >= static_cast<int>(shard_files.size()))
                        return std::any();
                    std::string shard_path = dir + "/" + shard_files[rank];
                    auto res = handle.get_raft_resources();
                    auto local_idx = std::make_unique<cagra_index>(*res);
                    cuvs::neighbors::cagra::deserialize(*res, shard_path, local_idx.get());
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_indices_[handle.get_device_id()] =
                        std::shared_ptr<cagra_index>(std::move(local_idx));
                    return std::any();
                }
            );
            // Restore total count from manifest (per-shard size() would be smaller)
            this->count           = static_cast<uint32_t>(json_int(m.raw, "capacity"));
            this->current_offset_ = static_cast<uint64_t>(json_int(m.raw, "length"));

        } else {
            throw std::runtime_error("manifest has neither 'index' nor 'shards' in components");
        }

        this->load_common_components(dir, m);
        this->is_loaded_ = true;
    }

    search_result_t merge_sharded_results(const std::vector<search_result_t>& shard_results, uint64_t num_queries, uint32_t limit) {
        // std::cout << "[DEBUG] merge_sharded_results: num_shards=" << shard_results.size() << " num_queries=" << num_queries << " limit=" << limit << std::endl;
        search_result_t global_res;
        global_res.neighbors.resize(num_queries * limit);
        global_res.distances.resize(num_queries * limit);

        for (uint64_t q = 0; q < num_queries; ++q) {
            std::vector<std::pair<float, int64_t>> candidates;
            for (size_t s = 0; s < shard_results.size(); ++s) {
                const auto& sr = shard_results[s];
                /*
                if (q == 0) {
                    std::cout << "  Shard " << s << " query 0 results: ";
                    for (uint32_t k = 0; k < std::min(limit, 5U); ++k) {
                        std::cout << "id=" << sr.neighbors[q * limit + k] << "(d=" << sr.distances[q * limit + k] << ") ";
                    }
                    std::cout << std::endl;
                }
                */
                for (uint32_t k = 0; k < limit; ++k) {
                    int64_t id = sr.neighbors[q * limit + k];
                    if (id != -1LL) {
                        candidates.push_back({sr.distances[q * limit + k], id});
                    }
                }
            }

            uint32_t to_sort = std::min((uint32_t)limit, (uint32_t)candidates.size());
            std::partial_sort(candidates.begin(), candidates.begin() + to_sort, candidates.end());

            /*
            if (q == 0) {
                std::cout << "  Query 0 total candidates: " << candidates.size() << " best candidate id=" << (candidates.empty() ? -1 : candidates[0].second) << " dist=" << (candidates.empty() ? -1 : candidates[0].first) << std::endl;
            }
            */

            for (uint32_t k = 0; k < limit; ++k) {
                if (k < to_sort) {
                    global_res.neighbors[q * limit + k] = candidates[k].second;
                    global_res.distances[q * limit + k] = candidates[k].first;
                } else {
                    global_res.neighbors[q * limit + k] = -1LL;
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
    uint32_t get_n_list() const { return 0; } // CAGRA doesn't have n_lists
};

} // namespace matrixone
