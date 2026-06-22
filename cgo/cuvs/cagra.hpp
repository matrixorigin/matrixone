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
#include "dynamic_batching.hpp"

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
//   4. Call search() / search_quantize() to query.
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
//   them (matrixone::cpu_topk_merge_sharded). Each shard returns local IDs (0..shard_sz-1);
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
//   search_quantize_internal (protects host_ids and shard_sizes_ against concurrent extend)
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
template <typename B, typename T>
class gpu_cagra_t : public gpu_index_base_t<B, T, cagra_build_params_t, int64_t> {
public:
    using base_type    = B;
    using storage_type = T;
    using cagra_index = cuvs::neighbors::cagra::index<T, uint32_t>;
    using search_result_t = cagra_search_result_t;
    // Inherited dependent type — bring into scope so search_internal can take a
    // const host_mask_bundle_t* parameter without `typename Base::...` everywhere.
    using host_mask_bundle_t = typename gpu_index_base_t<B, T, cagra_build_params_t, int64_t>::host_mask_bundle_t;

    // Internal index storage
    std::unique_ptr<cagra_index> index_;
    std::string index_filename_;

    // cuVS dynamic_batching wrappers, keyed by (device_id, k=limit, itopk_size,
    // search_width). Only touched when batch_window() > 0.
    // See dynamic_batching.hpp. (CAGRA neighbor ids are uint32_t.)
    dynb_cache_t<T, uint32_t> dynb_cache_;

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
    static std::unique_ptr<gpu_cagra_t<B, T>> merge(const std::vector<gpu_index_base_t<B, T, cagra_build_params_t, int64_t>*>& base_indices, uint32_t nthread, const std::vector<int>& devs) {
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
                    auto* idx = static_cast<gpu_cagra_t<B, T>*>(bi);
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
        
        auto new_idx = std::make_unique<gpu_cagra_t<B, T>>(
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
                uint64_t n = bi->count;
                if (!bi->host_ids.empty()) {
                    merged_ids.insert(merged_ids.end(), bi->host_ids.begin(), bi->host_ids.end());
                } else {
                    for (uint64_t i = 0; i < n; ++i) merged_ids.push_back(offset + i);
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

        // 1-byte storage T: train the B-source quantizer on the buffered B
        // sample, transform B->T, and store as T. For float/half storage this
        // is a no-op.
        this->train_quantizer_if_needed();
        if (!this->worker) throw std::runtime_error("Worker not initialized");

        // Validate build params against effective per-shard row count before
        // submitting to worker threads. submit_all_devices() propagates
        // exceptions, but validation here provides faster feedback and better error messages.
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
            // Pool-bypass training matrix — see matrixone::raw_device_mr() in cuvs_worker.hpp.
            auto dataset_storage = std::make_shared<rmm::device_uvector<T>>(
                static_cast<size_t>(this->count) * this->dimension,
                raft::resource::get_cuda_stream(*res),
                matrixone::raw_device_mr());
            auto dataset_device = raft::make_device_matrix_view<T, int64_t>(
                dataset_storage->data(), (int64_t)this->count, (int64_t)this->dimension);
            raft::copy(*res, dataset_device, raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data(), this->count, this->dimension));
            raft::resource::sync_stream(*res);

            // Serialize concurrent builds on the same physical device (see
            // device_build_mutex). No-op across distinct real GPUs.
            std::unique_ptr<cagra_index> local_idx;
            {
                std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                local_idx = std::make_unique<cagra_index>(cuvs::neighbors::cagra::build(
                    *res, index_params, raft::make_const_mdspan(dataset_device)));
            }

            handle.set_index_ptr(static_cast<const cagra_index*>(local_idx.get()));

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_indices_[handle.get_rank()] = std::shared_ptr<cagra_index>(std::move(local_idx));
                this->replicated_datasets_[handle.get_rank()] = std::move(dataset_storage);
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

            // Serialize concurrent builds on the same physical device (see
            // device_build_mutex). No-op across distinct real GPUs.
            std::unique_ptr<cagra_index> local_idx;
            {
                std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                local_idx = std::make_unique<cagra_index>(cuvs::neighbors::cagra::build(
                    *res, index_params, raft::make_const_mdspan(dataset_device)));
            }

            handle.set_index_ptr(static_cast<const cagra_index*>(local_idx.get()));

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_indices_[handle.get_rank()] = std::shared_ptr<cagra_index>(std::move(local_idx));
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

            std::unique_ptr<cagra_index> new_idx;
            {
                std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                new_idx = std::make_unique<cagra_index>(cuvs::neighbors::cagra::build(
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

    void extend_internal(raft_handle_wrapper_t& handle, const T* additional_data, uint64_t num_vectors) {
        if constexpr (std::is_same_v<T, half>) {
            // cuVS cagra::extend does not support float16 — guarded in extend() but
            // extend_internal must be constexpr-safe for all T.
            throw std::runtime_error("CAGRA extend is not supported for float16 (half) by cuVS.");
        } else {
            auto res = handle.get_raft_resources();
            auto stream = raft::resource::get_cuda_stream(*res);

            // Pool-bypass: extend's upload buffer is one-shot, freed on return.
            rmm::device_uvector<T> additional_storage(
                static_cast<size_t>(num_vectors) * this->dimension, stream, matrixone::raw_device_mr());
            auto additional_dataset_device = raft::make_device_matrix_view<T, int64_t>(
                additional_storage.data(), static_cast<int64_t>(num_vectors), static_cast<int64_t>(this->dimension));
            raft::copy(*res, additional_dataset_device,
                       raft::make_host_matrix_view<const T, int64_t>(additional_data, num_vectors, this->dimension));
            raft::resource::sync_stream(*res);

            cuvs::neighbors::cagra::extend_params params;

            if (this->dist_mode == DistributionMode_REPLICATED) {
                cagra_index* idx;
                {
                    std::shared_lock<std::shared_mutex> lock(this->mutex_);
                    idx = static_cast<cagra_index*>(this->replicated_indices_.at(handle.get_rank()).get());
                }
                {
                    // Serialize index-mutating cuVS calls on the same physical device.
                    std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                    cuvs::neighbors::cagra::extend(*res, params, raft::make_const_mdspan(additional_dataset_device), *idx);
                }
                handle.sync();
                {
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_datasets_.erase(handle.get_rank());
                }
            } else {
                // index_ is mutated in place without holding mutex_ during the
                // GPU op (per the "no lock during GPU operations" rule). Safe
                // because: (a) extend_mutex_ in extend() serializes concurrent
                // extends; (b) extend and search run in separate processes
                // (build vs. serve), so no concurrent search reads index_ here.
                // NOTE: this is process-level separation, not worker-level — the
                // worker runs main-thread extend tasks and device-thread search
                // tasks on the same GPU concurrently, so within one process they
                // would NOT be serialized.
                {
                    // Serialize index-mutating cuVS calls on the same physical device.
                    std::lock_guard<std::mutex> build_lk(matrixone::device_build_mutex(handle.get_device_id()));
                    cuvs::neighbors::cagra::extend(*res, params, raft::make_const_mdspan(additional_dataset_device), *index_);
                }
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

        if constexpr (std::is_same_v<T, half>) {
            throw std::runtime_error("CAGRA extend is not supported for float16 (half) by cuVS.");
        } else {
            if (!additional_data) return;
            // CAGRA does not pass seq_ids to extend_internal — cuVS CAGRA
            // auto-indexes the new rows. The helper still generates seq_ids
            // for set_ids_internal bookkeeping; the lambda discards them.
            this->run_extend("extend", num_vectors, new_ids, /*support_sharded=*/false,
                [&](raft_handle_wrapper_t& handle, const int64_t* /*seq_ids*/, uint64_t n) {
                    this->extend_internal(handle, additional_data, n);
                });
            // Invalidate dynamic-batching cache: CAGRA extend mutates the
            // cuVS graph in place, so dynb_cache_t entries keyed on the raw
            // upstream pointer survive with pre-extend state and serve stale
            // search results. dynb_cache_ has its own mutex; in-flight
            // searches keep their wrapper alive via shared_ptr, so clearing
            // is race-safe.
            this->dynb_cache_.clear();
        }
    }

    // Sync API surface preserved for the C-shim and direct callers. Routes
    // through search_async + search_wait so SHARDED async/wait paths and
    // sync share one execution path.
    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const cagra_search_params_t& sp) {
        uint64_t job_id = this->search_async(queries_data, num_queries, query_dimension, limit, sp);
        return this->search_wait(job_id);
    }

    // Filtered T-typed sync. Single execution path: routes through
    // search_with_filter_async + search_wait. Mask build still runs on the
    // caller's thread (inside search_with_filter_async, see
    // feedback_ivfpq_offworker_bitmap_qps memory).
    search_result_t search_with_filter(const T* queries_data, uint64_t num_queries,
                                       uint32_t query_dimension, uint32_t limit,
                                       const cagra_search_params_t& sp,
                                       const std::string& preds_json) {
        uint64_t job_id = this->search_with_filter_async(queries_data, num_queries, query_dimension, limit, sp, preds_json);
        return this->search_wait(job_id);
    }

    // Quantize a B-source query to the 1-byte storage type T via the B-source
    // quantizer, writing num_queries*dimension T values into `out`. The caller
    // then runs the normal native search(const T*) path. No f32 detour.
    void quantize_query(const B* queries_data, uint64_t num_queries, T* out) {
        if constexpr (sizeof(T) != 1) {
            throw std::runtime_error("quantize_query requires a 1-byte storage type (int8/uint8)");
        } else {
            uint64_t job = this->worker->submit_main(
                [this, queries_data, num_queries, out](raft_handle_wrapper_t& handle) -> std::any {
                    auto res = handle.get_raft_resources();
                    auto q_b_host = raft::make_host_matrix_view<const B, int64_t>(queries_data, num_queries, this->dimension);
                    auto q_b_dev = raft::make_device_matrix<B, int64_t>(*res, num_queries, this->dimension);
                    raft::copy(*res, q_b_dev.view(), q_b_host);
                    if (!this->quantizer_.is_trained()) throw std::runtime_error("quantizer not trained");
                    auto q_t_dev = raft::make_device_matrix<T, int64_t>(*res, num_queries, this->dimension);
                    this->quantizer_.template transform<T>(*res, q_b_dev.view(), q_t_dev.data_handle(), true);
                    raft::copy(*res, raft::make_host_matrix_view<T, int64_t>(out, num_queries, this->dimension), q_t_dev.view());
                    handle.sync();
                    return std::any();
                });
            auto r = this->worker->wait(job).get();
            if (r.error) std::rethrow_exception(r.error);
        }
    }

    // Async T-typed filtered search. Mirrors search_quantize_with_filter_async
    // but for the T-typed query path (T may be float / half / int8 / uint8).
    // Build masks on the caller's thread, copy queries into a shared_ptr so
    // they outlive the Go caller, capture both in the worker lambda.
    uint64_t search_with_filter_async(const T* queries_data, uint64_t num_queries,
                                      uint32_t query_dimension, uint32_t limit,
                                      const cagra_search_params_t& sp,
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

    uint64_t search_async(const T* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/, uint32_t limit, const cagra_search_params_t& sp) {
        if (!queries_data) throw std::invalid_argument("search_async: queries_data is null");
        if (num_queries == 0) throw std::invalid_argument("search_async: num_queries is 0");
        if (this->dimension == 0) throw std::runtime_error("search_async: index dimension is 0");
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) throw std::runtime_error("search_async: index not loaded");
        }

        auto queries_copy = std::make_shared<std::vector<T>>(queries_data, queries_data + num_queries * this->dimension);

        if (this->dist_mode == DistributionMode_SHARDED) {
            // Per-shard searches go to their own device queues — auto-batching
            // is preserved. The merge is deferred to search_wait() via a
            // composite_search_pending_t sentinel so we never sit on
            // main_thread_ in the search hot path. See plan
            // .claude/plans/effervescent-hatching-dewdrop.md.
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

        return std::any_cast<search_result_t>(std::move(result_wait.result));
    }

    // Submit a T-typed search to the worker (round-robin across device threads).
    // Request-level batching, when enabled (batch_window() > 0), happens
    // inside search_internal via cuVS dynamic_batching — see dynamic_batching.hpp.
    // `owner` is null on the sync path (the caller blocks in wait().get(), so the
    // query buffer stays alive) and on the async path is the shared_ptr that
    // keeps the copied queries alive until the search runs. Returns a job id
    // resolvable via worker->wait().
    uint64_t search_batchable_typed(std::shared_ptr<std::vector<T>> owner, const T* queries_data,
                                    uint64_t num_queries, uint32_t limit, const cagra_search_params_t& sp) {
        if (!this->worker) throw std::runtime_error("Worker not initialized");
        auto task = [this, owner, queries_data, num_queries, limit, sp](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_internal(handle, queries_data, num_queries, limit, sp);
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
    search_result_t search_internal(raft_handle_wrapper_t& handle, const T* queries_data, uint64_t num_queries, uint32_t limit, const cagra_search_params_t& sp, const std::string& preds_json = "", const host_mask_bundle_t* prebuilt = nullptr) {
        // No top-level lock: the index pointer is taken from the per-handle
        // cache or, on a miss, a narrow inner shared_lock below (see the
        // replicated_indices_ lookup). GPU work must run unlocked.
        auto res = handle.get_raft_resources();

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
                auto it = this->replicated_indices_.find(handle.get_rank());
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
            // Step C: reuse per-thread grow-only query workspace buffer.
            auto& q_buf = handle.template q_dev_buf<T>(static_cast<size_t>(num_queries) * this->dimension);
            auto queries_device = raft::make_device_matrix_view<T, int64_t>(
                q_buf.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));
            raft::copy(*res, queries_device, raft::make_host_matrix_view<const T, int64_t>(queries_data, num_queries, this->dimension));
            // Legacy path syncs here so build_search_bitset's stack-local host
            // bitmap can drain on the same stream. Prebuilt path skips: bitset
            // H2D queues behind queries H2D on the same stream, and host_mask
            // outlives the kernel via the bundle's shared_ptr capture.
            if (!prebuilt) {
                raft::resource::sync_stream(*res);
            }

            // Compute this device's row range for build_search_bitset. Matches the
            // slicing used by sync_shard_bitset (shard_offset is always % 32 == 0).
            uint64_t start_row = 0, shard_sz = this->count;
            if (this->dist_mode == DistributionMode_SHARDED) {
                int rank = handle.get_rank();
                shard_sz = this->shard_sizes_[rank];
                start_row = 0;
                for (int r = 0; r < rank; ++r) start_row += this->shard_sizes_[r];
            }

            // Clamp cuVS top-k to shard_sz (cuVS rejects k > index_size). See
            // ivf_pq.hpp search_internal for the full rationale; the CAGRA
            // variant uses uint32_t raw_neighbors which is already prefilled
            // with (uint32_t)-1 at construction (line 880 above), so empty-shard
            // / pad tails inherit the sentinel for free on the neighbor side.
            // Distances still need an explicit FLT_MAX pad.
            const uint32_t effective_k = matrixone::clamp_k_to_index_size(
                static_cast<uint32_t>(limit), shard_sz);

            if (effective_k == 0) {
                // raw_neighbors is already (uint32_t)-1 from its constructor;
                // just pad distances. map_neighbor_id below converts
                // (uint32_t)-1 → int64(UINT32_MAX), which fails the
                // data_size bound and becomes -1.
                std::fill(search_res.distances.begin(), search_res.distances.end(),
                          std::numeric_limits<float>::max());
            } else {
                // Step C: reuse per-thread neighbor / distance workspaces. CAGRA
                // returns uint32 neighbors so we use cagra_neighbors_buf.
                auto& n_buf = handle.cagra_neighbors_buf(static_cast<size_t>(num_queries) * limit);
                auto& d_buf = handle.distances_buf(static_cast<size_t>(num_queries) * limit);
                auto neighbors_device = raft::make_device_matrix_view<uint32_t, int64_t>(
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
                    cuvs::neighbors::cagra::search(*res, search_params, *local_index,
                                                        raft::make_const_mdspan(queries_device),
                                                        neighbors_device, distances_device, filter);
                } else if (this->batch_window() != 0) {
                    this->dynb_cache_.search(*res, handle.get_device_id(), local_index, search_params,
                                             static_cast<int64_t>(effective_k),
                                             static_cast<uint32_t>(search_params.itopk_size),
                                             static_cast<uint32_t>(search_params.search_width),
                                             this->dynb_concurrency_hint(),
                                             this->dynb_conservative_dispatch(),
                                             static_cast<double>(this->batch_window()) / 1000.0,
                                             raft::make_const_mdspan(queries_device),
                                             neighbors_device, distances_device);
                } else {
                    cuvs::neighbors::cagra::search(*res, search_params, *local_index,
                                                        raft::make_const_mdspan(queries_device),
                                                        neighbors_device, distances_device);
                }

                if (effective_k == static_cast<uint32_t>(limit)) {
                    raft::copy(*res, raft::make_host_matrix_view<uint32_t, int64_t>(raw_neighbors.data(), num_queries, limit), neighbors_device);
                    raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device);
                } else {
                    std::vector<uint32_t> tmp_n(static_cast<size_t>(num_queries) * effective_k);
                    std::vector<float>    tmp_d(static_cast<size_t>(num_queries) * effective_k);
                    raft::copy(*res, raft::make_host_matrix_view<uint32_t, int64_t>(tmp_n.data(), num_queries, effective_k), neighbors_device);
                    raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(tmp_d.data(), num_queries, effective_k), distances_device);
                    handle.sync();
                    matrixone::scatter_with_padding<uint32_t>(
                        raw_neighbors.data(), search_res.distances.data(),
                        tmp_n.data(), tmp_d.data(),
                        num_queries, static_cast<uint32_t>(limit), effective_k,
                        /*neighbor_sentinel=*/static_cast<uint32_t>(-1));
                }
            }
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
            int64_t offset = 0;
            int64_t data_size = static_cast<int64_t>(this->count);
            if (this->dist_mode == DistributionMode_SHARDED) {
                for (int r = 0; r < handle.get_rank(); ++r) offset += (int64_t)this->shard_sizes_[r];
                data_size = static_cast<int64_t>(this->shard_sizes_[handle.get_rank()]);
            }
            for (size_t i = 0; i < raw_neighbors.size(); ++i) {
                // raw_neighbors[i] is uint32_t; cuvs sentinel (uint32_t)-1
                // becomes UINT32_MAX as int64, which the data_size bound
                // catches without an explicit check.
                search_res.neighbors[i] = map_neighbor_id(
                    static_cast<int64_t>(raw_neighbors[i]), offset, data_size, this->host_ids);
            }
        }

        transform_distance(this->metric, search_res.distances, this->quantized_l2_dequant_factor());
        return search_res;
    }

    // Sync quantize entry — wraps search_quantize_async + search_wait.
    search_result_t search_quantize(const B* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const cagra_search_params_t& sp) {
        uint64_t job_id = this->search_quantize_async(queries_data, num_queries, query_dimension, limit, sp);
        return this->search_wait(job_id);
    }

    // Sync quantize filtered entry — wraps search_quantize_with_filter_async + search_wait.
    search_result_t search_quantize_with_filter(const B* queries_data, uint64_t num_queries,
                                             uint32_t query_dimension, uint32_t limit,
                                             const cagra_search_params_t& sp,
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
                                            const cagra_search_params_t& sp,
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
            // Bitmap eval runs on the caller's (Go) thread — off-worker — so
            // the device queues never see CPU mask work. Per-shard searches go
            // straight to their device queues, and the merge is deferred to
            // search_wait() via composite_search_pending_t. See plan.
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

    uint64_t search_quantize_async(const B* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const cagra_search_params_t& sp) {
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
                                    uint64_t num_queries, uint32_t limit, const cagra_search_params_t& sp) {
        if (!this->worker) throw std::runtime_error("Worker not initialized");
        auto task = [this, owner, queries_data, num_queries, limit, sp](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_quantize_internal(handle, queries_data, num_queries, this->dimension, limit, sp);
        };
        return this->worker->submit(task);
    }

    // See search_internal() above for the prebuilt-bundle contract; identical
    // semantics here (off-worker CPU mask eval, skip queries-H2D sync_stream
    // when prebuilt is non-null, kernel queues naturally behind the H2Ds on
    // the same stream).
    //
    // Takes the query in the BASE element type B (float or half) and converts
    // it to the storage type T on-device: B==T is a plain copy, sizeof(T)==1
    // quantizes B -> int8/uint8 via the learned scalar quantizer, and the
    // remaining (B=float, T=half) instantiation casts f32 -> f16. This is the
    // "quantize" entry — see search_internal() for the already-storage-typed T
    // path that performs no conversion.
    search_result_t search_quantize_internal(raft_handle_wrapper_t& handle, const B* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/,
                        uint32_t limit, const cagra_search_params_t& sp, const std::string& preds_json = "", const host_mask_bundle_t* prebuilt = nullptr) {
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
            // B == T (float->float or half->half): no conversion, copy straight
            // into the storage-typed workspace.
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
            // B != T and sizeof(T) != 1: the only such instantiation is
            // B=float, T=half (f32 base -> fp16 storage). Host-side fp32 -> fp16
            // cast (F16C / AVX, IEEE round-to-nearest-even — bit-identical to
            // mdspan_copy_kernel<__half>) into a pinned staging buffer, then a
            // single half-sized H2D copy.
            __half* host_h = handle.ensure_host_half_buf(n_q_elems);
            matrixone::cast_float_to_half_host(queries_data, host_h, n_q_elems);
            raft::copy(*res, q_dev_t,
                raft::make_host_matrix_view<const __half, int64_t>(host_h, num_queries, this->dimension));
        }
        // Legacy path syncs so build_search_bitset's stack-local host bitmap
        // can drain on the same stream. Prebuilt path skips: bitset H2D queues
        // naturally behind queries H2D and the kernel.
        if (!prebuilt) {
            raft::resource::sync_stream(*res);
        }

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
                auto it = this->replicated_indices_.find(handle.get_rank());
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
            uint64_t start_row = 0, shard_sz = this->count;
            if (this->dist_mode == DistributionMode_SHARDED) {
                int rank = handle.get_rank();
                shard_sz = this->shard_sizes_[rank];
                start_row = 0;
                for (int r = 0; r < rank; ++r) start_row += this->shard_sizes_[r];
            }

            // See search_internal above for the rationale. raw_neighbors_f is
            // already (uint32_t)-1-filled; distances need an explicit FLT_MAX pad.
            const uint32_t effective_k = matrixone::clamp_k_to_index_size(
                static_cast<uint32_t>(limit), shard_sz);

            if (effective_k == 0) {
                std::fill(search_res.distances.begin(), search_res.distances.end(),
                          std::numeric_limits<float>::max());
            } else {
                // Step C: reuse per-thread neighbor / distance workspaces (CAGRA
                // returns uint32 neighbors).
                auto& n_buf = handle.cagra_neighbors_buf(static_cast<size_t>(num_queries) * limit);
                auto& d_buf = handle.distances_buf(static_cast<size_t>(num_queries) * limit);
                auto neighbors_device = raft::make_device_matrix_view<uint32_t, int64_t>(
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
                    cuvs::neighbors::cagra::search(*res, search_params, *local_index,
                                                        raft::make_const_mdspan(q_dev_t),
                                                        neighbors_device, distances_device, filter);
                } else if (this->batch_window() != 0) {
                    this->dynb_cache_.search(*res, handle.get_device_id(), local_index, search_params,
                                             static_cast<int64_t>(effective_k),
                                             static_cast<uint32_t>(search_params.itopk_size),
                                             static_cast<uint32_t>(search_params.search_width),
                                             this->dynb_concurrency_hint(),
                                             this->dynb_conservative_dispatch(),
                                             static_cast<double>(this->batch_window()) / 1000.0,
                                             raft::make_const_mdspan(q_dev_t),
                                             neighbors_device, distances_device);
                } else {
                    cuvs::neighbors::cagra::search(*res, search_params, *local_index,
                                                        raft::make_const_mdspan(q_dev_t),
                                                        neighbors_device, distances_device);
                }

                if (effective_k == static_cast<uint32_t>(limit)) {
                    raft::copy(*res, raft::make_host_matrix_view<uint32_t, int64_t>(raw_neighbors_f.data(), num_queries, limit), neighbors_device);
                    raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device);
                } else {
                    std::vector<uint32_t> tmp_n(static_cast<size_t>(num_queries) * effective_k);
                    std::vector<float>    tmp_d(static_cast<size_t>(num_queries) * effective_k);
                    raft::copy(*res, raft::make_host_matrix_view<uint32_t, int64_t>(tmp_n.data(), num_queries, effective_k), neighbors_device);
                    raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(tmp_d.data(), num_queries, effective_k), distances_device);
                    handle.sync();
                    matrixone::scatter_with_padding<uint32_t>(
                        raw_neighbors_f.data(), search_res.distances.data(),
                        tmp_n.data(), tmp_d.data(),
                        num_queries, static_cast<uint32_t>(limit), effective_k,
                        /*neighbor_sentinel=*/static_cast<uint32_t>(-1));
                }
            }
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
            int64_t offset = 0;
            int64_t data_size = static_cast<int64_t>(this->count);
            if (this->dist_mode == DistributionMode_SHARDED) {
                for (int r = 0; r < handle.get_rank(); ++r) offset += (int64_t)this->shard_sizes_[r];
                data_size = static_cast<int64_t>(this->shard_sizes_[handle.get_rank()]);
            }
            for (size_t i = 0; i < raw_neighbors_f.size(); ++i) {
                search_res.neighbors[i] = map_neighbor_id(
                    static_cast<int64_t>(raw_neighbors_f[i]), offset, data_size, this->host_ids);
            }
        }

        transform_distance(this->metric, search_res.distances, this->quantized_l2_dequant_factor());
        return search_res;
    }

    std::string info() const override {
        std::string json = gpu_index_base_t<B, T, cagra_build_params_t, int64_t>::info();
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
        auto res = this->worker->wait(job_id).get();
        if (res.error) std::rethrow_exception(res.error);
        if (!this->host_ids.empty()) {
            this->save_ids(filename + ".ids");
        }
    }

    void load(const std::string& filename) {
        auto task = [&](raft_handle_wrapper_t& handle) -> std::any {
            auto res = handle.get_raft_resources();
            auto local_idx = std::make_unique<cagra_index>(*res);
            cuvs::neighbors::cagra::deserialize(*res, filename, local_idx.get());
            // Drain `res`'s stream so the dataset H2D copy committed by
            // deserialize is visible before any search thread reads it.
            // See the longer comment in load_dir() for the failure mode.
            raft::resource::sync_stream(*res);

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->count = static_cast<uint64_t>(local_idx->size());
                this->dimension = static_cast<uint32_t>(local_idx->dim());
                this->current_offset_ = this->count;

                if (this->dist_mode == DistributionMode_SINGLE_GPU) {
                    index_ = std::move(local_idx);
                } else {
                    this->replicated_indices_[handle.get_rank()] = std::shared_ptr<cagra_index>(std::move(local_idx));
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

        // .ids sidecar is optional — only written by save() when host_ids is
        // non-empty. Probe-first matches the manifest-based load_common_components
        // pattern (index_base.hpp): no exception machinery, and any failure
        // inside load_ids (truncated read, etc.) propagates as a real error.
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
                    int key = handle.get_rank();
                    auto it = this->replicated_indices_.find(key);
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
                    auto it = this->replicated_indices_.find(handle.get_rank());
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
                // cuVS' cagra::deserialize stages the dataset host→device on
                // `res`'s stream and returns BEFORE the H2D copy is committed.
                // If we publish `local_idx` to other worker threads (search
                // tasks run on round-robin workers, each with its own stream)
                // without first draining `res`'s stream, the first search can
                // read pre-H2D garbage from the dataset region and return
                // bogus neighbors at distance² ≈ ‖q‖² (the all-zeros tie). On
                // hosts where the H2D happens to finish before any search
                // runs, the race is benign; on this WSL2 box it fires every
                // time. Drain the stream here to make the dataset visible to
                // any subsequent searcher.
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
                    auto local_idx = std::make_unique<cagra_index>(*res);
                    cuvs::neighbors::cagra::deserialize(*res, full_path, local_idx.get());
                    // See SINGLE_GPU branch above for the rationale.
                    raft::resource::sync_stream(*res);
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_indices_[handle.get_rank()] =
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
                    // See SINGLE_GPU branch above for the rationale.
                    raft::resource::sync_stream(*res);
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_indices_[handle.get_rank()] =
                        std::shared_ptr<cagra_index>(std::move(local_idx));
                    return std::any();
                }
            );
            // Restore total count from manifest (per-shard size() would be smaller).
            // this->count is uint64_t; casting through uint32_t would truncate at 4B rows.
            this->count           = static_cast<uint64_t>(json_int(m.raw, "capacity"));
            this->current_offset_ = static_cast<uint64_t>(json_int(m.raw, "length"));

        } else {
            throw std::runtime_error("manifest has neither 'index' nor 'shards' in components");
        }

        this->load_common_components(dir, m);
        this->is_loaded_ = true;
    }

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

    uint32_t get_dim() const { return this->dimension; }
    uint32_t get_rot_dim() const { return this->dimension; }
    uint32_t get_dim_ext() const { return this->dimension; }
    uint32_t get_n_list() const { return 0; } // CAGRA doesn't have n_lists
};

} // namespace matrixone
