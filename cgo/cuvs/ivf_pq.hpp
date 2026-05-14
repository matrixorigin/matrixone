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
#include <tuple>
#include <type_traits>
#include <vector>
#include <fstream>

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
#include <cuvs/neighbors/ivf_pq.hpp>
#pragma GCC diagnostic pop


namespace matrixone {

// =============================================================================
// gpu_ivf_pq_t — Developer Guide
// =============================================================================
//
// OVERVIEW
// --------
// gpu_ivf_pq_t<T> implements an IVF-PQ (Inverted File with Product Quantization)
// approximate nearest-neighbor index backed by cuVS.
//
// cuVS type: cuvs::neighbors::ivf_pq::index<int64_t>
//   Note: the cuVS IVF-PQ index type is NOT templated on T — it always stores
//   PQ codes internally (uint8).  T is used only for the input/query element type.
// IdT      : int64_t
// Supported T: float, half (__half), int8_t, uint8_t
//
// IVF-PQ partitions the dataset into n_lists clusters (same as IVF-Flat), but
// instead of storing vectors verbatim it encodes each residual vector using
// Product Quantization: the residual is split into M sub-vectors, each encoded
// with pq_bits bits from a trained codebook.  This dramatically reduces storage
// at the cost of search precision.
//
// Key build parameters:
//   n_lists  — number of IVF clusters (centroids)
//   M        — number of PQ sub-vectors (dimension must be divisible by M)
//   pq_bits  — bits per PQ code (typically 4 or 8)
//
//
// QUANTIZER vs PQ CODEBOOK
// ------------------------
// The scalar quantizer (scalar_quantizer_t, from index_base) is separate from
// the PQ codebook and is only relevant for 1-byte input types (int8/uint8):
//   - It maps float32 inputs to the int8/uint8 range [min, max] before building.
//   - Must be trained before add_chunk_float() or extend_float() for 1-byte T.
//   - Extended vectors MUST fall within the trained [min, max] range; values
//     outside this range are clamped and degrade search quality.
//
// The PQ codebook is trained automatically by cuVS during build().
//
//
// DISTRIBUTION MODES
// ------------------
// SINGLE_GPU:
//   index_ holds the single cuVS IVF-PQ index (unique_ptr).
//   dataset_device_ptr_ holds the build dataset on device (reset after extend).
//
// REPLICATED:
//   replicated_indices_[dev_id] holds a full copy per GPU (cast to ivf_pq_index*).
//   The replicated dataset pointers (replicated_datasets_) are used during build
//   and erased after the first extend on each device.
//   search_internal / search_float_internal use per-thread cached index ptr
//   (handle.get_index_ptr()) to avoid repeated map lookups.
//
// SHARDED:
//   Each GPU holds a disjoint shard.
//   rows_per_shard = (count / num_shards) & ~31  (rounded down to multiple of 32).
//   Search results from all shards are merged by matrixone::cpu_topk_merge_sharded.
//   Extend is NOT supported (throws).
//
//
// EXTEND RULES
// ------------
// - extend() / extend_float() may only be called after build() (is_loaded_ must be true).
// - extend_mutex_ (in derived class, defined here) serializes concurrent extends.
// - cuVS requires explicit int64_t indices for non-empty index extend;
//   generate sequential IDs [count .. count+n_rows) in extend() before dispatch.
// - After GPU work, set_ids() + count + current_offset_ update happens under unique_lock.
// - For extend_float() with 1-byte T, the quantizer must be trained and the data
//   must fall within [quantizer.min, quantizer.max].
// - SHARDED mode throws std::runtime_error.
//
//
// SEARCH PATHS
// ------------
// search_internal(handle, T* queries, ...)
//   Converts T queries to device, searches cuVS, applies bitset filter if needed.
//   For REPLICATED: uses per-thread cached index ptr to avoid mutex on hot path.
//   For SHARDED: called once per shard with the shard's local index.
//
// search_float_internal(handle, float* queries, ...)
//   Converts float → T on device (quantize for 1-byte T, half-cast for T=half,
//   direct copy for T=float), then searches the same way as search_internal.
//
// Soft-delete filtering:
//   - Non-SHARDED: sync_device_bitset() → bitset_filter over full index
//   - SHARDED: sync_shard_bitset() → bitset_filter over shard-local bit slice
//     Bit j of the shard bitset = global bit (rank * rows_per_shard + j)
//
// search_batchable_typed() / search_batchable_float() just submit the search to
// the worker; request-level batching, when enabled (batch_window() > 0),
// happens inside search_internal via cuVS dynamic_batching (see dynamic_batching.hpp).
//
//
// ID OFFSET IN SHARDED SEARCH
// ----------------------------
// After search returns shard-local IDs, the offset (rank * rows_per_shard) is
// added to convert to global IDs — unless host_ids is non-empty, in which case
// host_ids[local_id + offset] is used (but host_ids + SHARDED is unusual).
// rows_per_shard must match the value used at build (same rounded formula).
//
// =============================================================================

/**
 * @brief Search result for IVF-PQ queries.
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
    // Inherited dependent type — bring into scope so search_internal can take a
    // const host_mask_bundle_t* parameter without `typename Base::...` everywhere.
    using host_mask_bundle_t = typename gpu_index_base_t<T, ivf_pq_build_params_t, int64_t>::host_mask_bundle_t;

    // Internal index storage
    std::unique_ptr<ivf_pq_index> index_;
    std::string data_filename_;    // raw feature-vector file → load_host_matrix in build()
    std::string index_filename_;   // serialized index file → load() in build()

    // cuVS dynamic_batching wrappers, keyed by (device_id, k=limit, n_probes).
    // Only touched when batch_window() > 0. See
    // dynamic_batching.hpp.
    dynb_cache_t<T, int64_t> dynb_cache_;

    ~gpu_ivf_pq_t() override {
        this->destroy();
    }

    // Unified Constructor for building from dataset
    gpu_ivf_pq_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension,
                    distance_type_t m, const ivf_pq_build_params_t& bp,
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
        this->host_ids.reserve(this->count);
        if (dataset_data) {
            std::copy(dataset_data, dataset_data + (this->count * this->dimension), this->flattened_host_dataset.begin());
        }

        if (ids) {
            this->set_ids(ids, this->count);
        }
    }

    // Constructor for chunked input (pre-allocates)
    gpu_ivf_pq_t(uint64_t total_count, uint32_t dimension, distance_type_t m,
                    const ivf_pq_build_params_t& bp, const std::vector<int>& devices,
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

    // Constructor for loading a serialized IVF-PQ index from file
    gpu_ivf_pq_t(const std::string& filename, uint32_t dimension, distance_type_t m,
                    const ivf_pq_build_params_t& bp, const std::vector<int>& devices,
                    uint32_t nthread, distribution_mode_t mode) {

        this->dimension = dimension;
        this->metric = m;
        this->build_params = bp;
        this->dist_mode = mode;
        this->devices_ = devices;
        this->index_filename_ = filename;

        std::vector<int> worker_devices = this->devices_;
        if (mode == DistributionMode_SINGLE_GPU && !worker_devices.empty()) {
            worker_devices = {worker_devices[0]};
        }
        this->worker = std::make_unique<cuvs_worker_t>(nthread, worker_devices, mode);

        this->current_offset_ = 0;
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

    void build() override {
        const char* mode_str =
            (this->dist_mode == DistributionMode_REPLICATED) ? "REPLICATED" :
            (this->dist_mode == DistributionMode_SHARDED)    ? "SHARDED"    : "SINGLE_GPU";
        std::cerr << "[IVFPQ build] ENTRY mode=" << mode_str
                  << " count=" << this->count
                  << " current_offset_=" << this->current_offset_
                  << " dim=" << this->dimension
                  << " is_loaded_=" << (this->is_loaded_ ? "true" : "false")
                  << " index_filename_=" << (this->index_filename_.empty() ? "(none)" : this->index_filename_)
                  << " data_filename_=" << (this->data_filename_.empty() ? "(none)" : this->data_filename_)
                  << " flattened_host_dataset.size()=" << this->flattened_host_dataset.size()
                  << " devices.size()=" << this->devices_.size()
                  << " n_lists=" << this->build_params.n_lists
                  << " m=" << this->build_params.m
                  << " bits=" << this->build_params.bits_per_code
                  << " kmeans_fraction=" << this->build_params.kmeans_trainset_fraction
                  << std::endl;

        if (this->is_loaded_) {
            std::cerr << "[IVFPQ build] already loaded, skipping" << std::endl;
            return;
        }
        if (!this->index_filename_.empty()) {
            std::cerr << "[IVFPQ build] delegating to load(" << this->index_filename_ << ")" << std::endl;
            load(this->index_filename_);
            return;
        }
        try {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->data_filename_.empty() && this->flattened_host_dataset.empty()) {
                uint64_t rows, cols;
                load_host_matrix<T>(this->data_filename_, this->flattened_host_dataset, rows, cols);
                this->count = rows;
                this->dimension = static_cast<uint32_t>(cols);
                this->current_offset_ = this->count;
            } else {
                this->count = this->current_offset_;
            }
            if (this->flattened_host_dataset.size() > (size_t)this->count * this->dimension)
                this->flattened_host_dataset.resize((size_t)this->count * this->dimension);
        } catch (const std::exception& e) {
            std::cerr << "[IVFPQ build ERROR] during host dataset prep: " << e.what()
                      << " count=" << this->count
                      << " dim=" << this->dimension
                      << " flattened_host_dataset.size()=" << this->flattened_host_dataset.size()
                      << std::endl;
            throw;
        }

        if (this->count == 0) {
            if (this->pending_total_count_ == 0) {
                std::cerr << "[IVFPQ build] EARLY RETURN count=0 && pending_total_count_=0"
                          << " -> is_loaded_=true but NO index populated (save_dir will fail)"
                          << std::endl;
                this->is_loaded_ = true;
                return;
            }
        }
        this->train_quantizer_if_needed();
        if (!this->worker) throw std::runtime_error("Worker not initialized");

        if (this->dist_mode == DistributionMode_SHARDED) {
            int num_shards = static_cast<int>(this->devices_.size());
            uint64_t rows_per_shard = (this->count / num_shards) & ~static_cast<uint64_t>(31);
            uint64_t last_shard_rows = this->count - rows_per_shard * (num_shards - 1);
            uint64_t min_shard_rows = std::min(rows_per_shard, last_shard_rows);
            std::cerr << "[IVFPQ build] SHARDED plan num_shards=" << num_shards
                      << " rows_per_shard=" << rows_per_shard
                      << " last_shard_rows=" << last_shard_rows
                      << " min_shard_rows=" << min_shard_rows
                      << " host_bytes_needed=" << ((size_t)this->count * this->dimension * sizeof(T))
                      << " per_shard_device_bytes=" << ((size_t)rows_per_shard * this->dimension * sizeof(T))
                      << std::endl;
            validate_build_params(this->build_params, min_shard_rows);
            this->shard_sizes_.assign(num_shards, 0);
        } else {
            std::cerr << "[IVFPQ build] " << mode_str
                      << " host_bytes_needed=" << ((size_t)this->count * this->dimension * sizeof(T))
                      << " per_gpu_device_bytes=" << ((size_t)this->count * this->dimension * sizeof(T))
                      << std::endl;
            validate_build_params(this->build_params, this->count);
        }

        try {
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
                if (this->dist_mode == DistributionMode_SHARDED)
                    this->shard_sizes_.assign(this->devices_.size(), 0);
                this->worker->submit_all_devices([&](raft_handle_wrapper_t& handle) -> std::any {
                    this->build_internal(handle);
                    return std::any();
                });
            }
        } catch (const std::exception& e) {
            std::cerr << "[IVFPQ build ERROR] during GPU build dispatch mode=" << mode_str
                      << " count=" << this->count
                      << " dim=" << this->dimension
                      << " replicated_indices_.size()=" << this->replicated_indices_.size()
                      << " index_=" << (index_ ? "set" : "null")
                      << " what=" << e.what() << std::endl;
            throw;
        }

        this->is_loaded_ = true;
        this->init_deleted_bitset();
        this->flattened_host_dataset.clear();
        this->flattened_host_dataset.shrink_to_fit();
        std::cerr << "[IVFPQ build] DONE mode=" << mode_str
                  << " count=" << this->count
                  << " replicated_indices_.size()=" << this->replicated_indices_.size()
                  << " index_=" << (index_ ? "set" : "null")
                  << std::endl;
    }

    static void validate_build_params(const ivf_pq_build_params_t& bp, uint64_t num_rows) {
        if (num_rows < bp.n_lists) {
            throw std::invalid_argument(
                "IVF-PQ build requires at least n_lists vectors (got " + std::to_string(num_rows) +
                " vectors, n_lists=" + std::to_string(bp.n_lists) + ")");
        }
    }

    void build_internal(raft_handle_wrapper_t& handle) {
        int dev_id = handle.get_device_id();
        int rank   = -1;
        try { rank = handle.get_rank(); } catch (...) {}
        const char* mode_str =
            (this->dist_mode == DistributionMode_REPLICATED) ? "REPLICATED" :
            (this->dist_mode == DistributionMode_SHARDED)    ? "SHARDED"    : "SINGLE_GPU";

        auto log_mem = [&](const char* phase) {
            size_t free_b = 0, total_b = 0;
            cudaError_t cerr = cudaMemGetInfo(&free_b, &total_b);
            std::cerr << "[IVFPQ build_internal] rank=" << rank << " dev=" << dev_id
                      << " mode=" << mode_str << " phase=" << phase
                      << " GPU_free_MB=" << (cerr == cudaSuccess ? (free_b >> 20) : 0)
                      << " GPU_total_MB=" << (cerr == cudaSuccess ? (total_b >> 20) : 0)
                      << (cerr == cudaSuccess ? "" : " (cudaMemGetInfo failed)")
                      << std::endl;
        };

        try {
            cuvs::neighbors::ivf_pq::index_params index_params;
            index_params.metric = static_cast<cuvs::distance::DistanceType>(this->metric);
            index_params.n_lists = this->build_params.n_lists;
            index_params.pq_dim = this->build_params.m;
            index_params.pq_bits = this->build_params.bits_per_code;
            index_params.kmeans_trainset_fraction = this->build_params.kmeans_trainset_fraction;

            if (this->dist_mode == DistributionMode_REPLICATED) {
                auto res = handle.get_raft_resources();
                log_mem("REPLICATED:before-alloc");
                // Pool-bypass: training matrix is one huge transient allocation that
                // must not pin the per-device pool's high-water mark. See
                // matrixone::raw_device_mr() rationale in cuvs_worker.hpp.
                auto dataset_storage = std::make_shared<rmm::device_uvector<T>>(
                    static_cast<size_t>(this->count) * this->dimension,
                    raft::resource::get_cuda_stream(*res),
                    matrixone::raw_device_mr());
                auto dataset_device = raft::make_device_matrix_view<T, int64_t>(
                    dataset_storage->data(), (int64_t)this->count, (int64_t)this->dimension);
                log_mem("REPLICATED:after-alloc");
                raft::copy(*res, dataset_device, raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data(), this->count, this->dimension));
                raft::resource::sync_stream(*res);
                log_mem("REPLICATED:before-cuvs-build");

                auto local_idx = std::make_unique<ivf_pq_index>(cuvs::neighbors::ivf_pq::build(
                    *res, index_params, raft::make_const_mdspan(dataset_device)));
                log_mem("REPLICATED:after-cuvs-build");

                handle.set_index_ptr(static_cast<const ivf_pq_index*>(local_idx.get()));

                {
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_indices_[handle.get_device_id()] = std::shared_ptr<ivf_pq_index>(std::move(local_idx));
                    this->replicated_datasets_[handle.get_device_id()] = std::move(dataset_storage);
                }
                handle.sync();
            } else if (this->dist_mode == DistributionMode_SHARDED) {
                auto res = handle.get_raft_resources();
                int num_shards = this->devices_.size();

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

                std::cerr << "[IVFPQ build_internal] SHARDED rank=" << rank << " dev=" << dev_id
                          << " start_row=" << start_row << " num_rows=" << num_rows
                          << " bytes_device=" << ((size_t)num_rows * this->dimension * sizeof(T))
                          << std::endl;
                log_mem("SHARDED:before-alloc");
                // Pool-bypass training shard — see REPLICATED branch.
                auto dataset_storage = std::make_shared<rmm::device_uvector<T>>(
                    static_cast<size_t>(num_rows) * this->dimension,
                    raft::resource::get_cuda_stream(*res),
                    matrixone::raw_device_mr());
                auto dataset_device = raft::make_device_matrix_view<T, int64_t>(
                    dataset_storage->data(), (int64_t)num_rows, (int64_t)this->dimension);
                log_mem("SHARDED:after-alloc");
                raft::copy(*res, dataset_device,
                           raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data() + (start_row * this->dimension), num_rows, this->dimension));
                raft::resource::sync_stream(*res);
                log_mem("SHARDED:before-cuvs-build");

                auto local_idx = std::make_unique<ivf_pq_index>(cuvs::neighbors::ivf_pq::build(
                    *res, index_params, raft::make_const_mdspan(dataset_device)));
                log_mem("SHARDED:after-cuvs-build");

                handle.set_index_ptr(static_cast<const ivf_pq_index*>(local_idx.get()));

                {
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_indices_[handle.get_device_id()] = std::shared_ptr<ivf_pq_index>(std::move(local_idx));
                    this->replicated_datasets_[handle.get_device_id()] = std::move(dataset_storage);
                }
                handle.sync();
            } else {
                // Do all GPU work outside the lock — holding shared_mutex across GPU calls
                // would block concurrent readers for the entire build duration.
                auto res = handle.get_raft_resources();
                log_mem("SINGLE_GPU:before-alloc");
                // Pool-bypass training matrix — see REPLICATED branch.
                auto dataset_storage = std::make_shared<rmm::device_uvector<T>>(
                    static_cast<size_t>(this->count) * this->dimension,
                    raft::resource::get_cuda_stream(*res),
                    matrixone::raw_device_mr());
                auto dataset_device = raft::make_device_matrix_view<T, int64_t>(
                    dataset_storage->data(), (int64_t)this->count, (int64_t)this->dimension);
                log_mem("SINGLE_GPU:after-alloc");
                raft::copy(*res, dataset_device, raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data(), this->count, this->dimension));
                raft::resource::sync_stream(*res);
                log_mem("SINGLE_GPU:before-cuvs-build");

                auto new_idx = std::make_unique<ivf_pq_index>(cuvs::neighbors::ivf_pq::build(
                    *res, index_params, raft::make_const_mdspan(dataset_device)));
                log_mem("SINGLE_GPU:after-cuvs-build");

                handle.sync();

                // Assign results under lock
                {
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    index_ = std::move(new_idx);
                    this->dataset_device_ptr_ = std::move(dataset_storage);
                }
            }
        } catch (const std::exception& e) {
            // submit_all_devices rethrows the first exception it sees; log every
            // shard's failure here so none are lost.
            size_t free_b = 0, total_b = 0;
            cudaMemGetInfo(&free_b, &total_b);
            std::cerr << "[IVFPQ build_internal ERROR] rank=" << rank << " dev=" << dev_id
                      << " mode=" << mode_str
                      << " count=" << this->count << " dim=" << this->dimension
                      << " n_lists=" << this->build_params.n_lists
                      << " m=" << this->build_params.m
                      << " GPU_free_MB=" << (free_b >> 20)
                      << " what=" << e.what() << std::endl;
            throw std::runtime_error(std::string("[IVFPQ build_internal rank=") +
                                     std::to_string(rank) + " dev=" + std::to_string(dev_id) +
                                     " mode=" + mode_str + "] " + e.what());
        } catch (...) {
            std::cerr << "[IVFPQ build_internal ERROR] rank=" << rank << " dev=" << dev_id
                      << " mode=" << mode_str << " unknown non-std::exception" << std::endl;
            throw;
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
            ivf_pq_index* idx_ptr;
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_device_id());
                if (it == this->replicated_indices_.end())
                    throw std::runtime_error("extend_internal: no index for device");
                idx_ptr = static_cast<ivf_pq_index*>(it->second.get());
            }
            cuvs::neighbors::ivf_pq::extend(*res,
                raft::make_const_mdspan(new_vecs_device), indices_opt, idx_ptr);
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_datasets_.erase(handle.get_device_id());
            }
        } else if (this->dist_mode == DistributionMode_SHARDED) {
            // Only the last shard's device calls this; seq_ids are already shard-local.
            ivf_pq_index* idx_ptr;
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_device_id());
                if (it == this->replicated_indices_.end())
                    throw std::runtime_error("extend_internal: no SHARDED index for device");
                idx_ptr = static_cast<ivf_pq_index*>(it->second.get());
            }
            cuvs::neighbors::ivf_pq::extend(*res,
                raft::make_const_mdspan(new_vecs_device), indices_opt, idx_ptr);
            {
                // Erase only the last shard's stale build dataset; other shards' entries remain valid.
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_datasets_.erase(handle.get_device_id());
            }
        } else {
            if (!index_) throw std::runtime_error("extend_internal: index not built");
            cuvs::neighbors::ivf_pq::extend(*res,
                raft::make_const_mdspan(new_vecs_device), indices_opt, index_.get());
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
            ivf_pq_index* idx_ptr;
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_device_id());
                if (it == this->replicated_indices_.end())
                    throw std::runtime_error("extend_internal_float: no index for device");
                idx_ptr = static_cast<ivf_pq_index*>(it->second.get());
            }
            cuvs::neighbors::ivf_pq::extend(*res,
                raft::make_const_mdspan(new_vecs_device), indices_opt, idx_ptr);
            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_datasets_.erase(handle.get_device_id());
            }
        } else if (this->dist_mode == DistributionMode_SHARDED) {
            // Only the last shard's device calls this; seq_ids are already shard-local.
            ivf_pq_index* idx_ptr;
            {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto it = this->replicated_indices_.find(handle.get_device_id());
                if (it == this->replicated_indices_.end())
                    throw std::runtime_error("extend_internal_float: no SHARDED index for device");
                idx_ptr = static_cast<ivf_pq_index*>(it->second.get());
            }
            cuvs::neighbors::ivf_pq::extend(*res,
                raft::make_const_mdspan(new_vecs_device), indices_opt, idx_ptr);
            {
                // Erase only the last shard's stale build dataset; other shards' entries remain valid.
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->replicated_datasets_.erase(handle.get_device_id());
            }
        } else {
            if (!index_) throw std::runtime_error("extend_internal_float: index not built");
            cuvs::neighbors::ivf_pq::extend(*res,
                raft::make_const_mdspan(new_vecs_device), indices_opt, index_.get());
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
    }

    void extend_float(const float* new_data, uint64_t n_rows, const int64_t* new_ids) {
        if (!new_data) return;
        this->run_extend("extend_float", n_rows, new_ids, /*support_sharded=*/true,
            [&](raft_handle_wrapper_t& handle, const int64_t* seq_ids, uint64_t n) {
                this->extend_internal_float(handle, new_data, n, seq_ids);
            });
    }

    // Sync T-typed entry — wraps search_async + search_wait. SHARDED inline
    // fan-out / merge handled by the async path's composite_search_pending_t
    // sentinel inside search_wait().
    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const ivf_pq_search_params_t& sp) {
        uint64_t job_id = this->search_async(queries_data, num_queries, query_dimension, limit, sp);
        return this->search_wait(job_id);
    }

    // Sync T-typed filtered entry — wraps search_with_filter_async + search_wait.
    search_result_t search_with_filter(const T* queries_data, uint64_t num_queries,
                                       uint32_t query_dimension, uint32_t limit,
                                       const ivf_pq_search_params_t& sp,
                                       const std::string& preds_json) {
        uint64_t job_id = this->search_with_filter_async(queries_data, num_queries, query_dimension, limit, sp, preds_json);
        return this->search_wait(job_id);
    }

    // Async T-typed filtered search. Mirrors search_float_with_filter_async
    // but uses search_internal (T) instead of search_float_internal (float).
    uint64_t search_with_filter_async(const T* queries_data, uint64_t num_queries,
                                      uint32_t query_dimension, uint32_t limit,
                                      const ivf_pq_search_params_t& sp,
                                      const std::string& preds_json) {
        if (!queries_data) throw std::invalid_argument("search_async: queries_data is null");
        if (num_queries == 0) throw std::invalid_argument("search_async: num_queries is 0");
        if (this->dimension == 0) throw std::runtime_error("search_async: index dimension is 0");
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) throw std::runtime_error("search_async: index not loaded");
        }
        if (!this->worker) throw std::runtime_error("Worker not initialized");
        (void)query_dimension;  // search_internal uses this->dimension; param kept for signature parity.

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

    uint64_t search_async(const T* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/, uint32_t limit, const ivf_pq_search_params_t& sp) {
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
                                    uint64_t num_queries, uint32_t limit, const ivf_pq_search_params_t& sp) {
        if (!this->worker) throw std::runtime_error("Worker not initialized");
        auto task = [this, owner, queries_data, num_queries, limit, sp](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_internal(handle, queries_data, num_queries, limit, sp);
        };
        return this->worker->submit(task);
    }

    // =====================================================================
    // WARNING: cuVS IVF-PQ bitset_filter quirk — filter-excluded rows leak
    //          into the result when popcount(filter) < limit.
    // ---------------------------------------------------------------------
    // When the number of rows passing (user_filter AND NOT deleted) is less
    // than `limit`, cuVS IVF-PQ pads the remaining result slots with
    // filter-excluded nearest neighbors instead of returning sentinels.
    // So rows explicitly excluded by predicate or by soft-delete can still
    // appear in search_res.neighbors.
    //   Canonical reproducer: FilteredSearchCombinesWithDeleteBitset in
    //   cgo/cuvs/test/ivf_pq_test.cu.
    //
    // Mitigation: re-apply the combined (user_filter AND NOT deleted) mask
    // on the host and replace any failing slot with (-1, float::max). The
    // Go layer already treats -1 as an empty slot (see multi_index.go), so
    // callers see a result array with fewer than `limit` valid neighbors,
    // not a corrupted one. Only IVF-PQ is patched; IVF-Flat and CAGRA paths
    // correctly write -1 for filter-excluded slots natively.
    //
    // Skip-fast: the quirk can only trigger when popcount(combined_mask) is
    // strictly less than num_queries * limit. build_search_bitset returns
    // the popcount via out_popcount; callers gate this function behind
    //   popcount >= num_queries * limit * kPqPostFilterSkipFactor
    // (with kPqPostFilterSkipFactor = 4 for empirical headroom). At moderate
    // selectivity (5–50% pass), this skips the post-filter pass and its
    // shared_lock acquisition entirely. The canonical reproducer above sits
    // well below the threshold (popcount=2, n*limit=5) so it still exercises
    // this function.
    //
    // Mask sources (after build_search_bitset refactor):
    //   * User filter present — user_host_mask already holds (user ∧ ¬deleted)
    //     because build_search_bitset ANDs on the host before upload. Just
    //     bit-test it; no further combine needed.
    //   * Deletes-only path  — user_host_mask is empty (cached device delete
    //     bitset is reused for the search itself). Synthesize a host mask
    //     here by copying the delete-bitset slice over [start_row,
    //     start_row+shard_sz).
    //
    // Caveats:
    //   * Suppresses junk only — cannot recover valid rows that cuVS never
    //     scored (e.g. rows living in non-probed IVF lists).
    //
    // Caller must hold mutex_ as shared_lock; this function reads
    // deleted_bitset_ / deleted_count_.
    // =====================================================================
    // user_host_mask is consumed read-only on the user-filter path (the bundle
    // may be shared across shards on the new prebuilt path, so we can't mutate
    // it). On the deletes-only path the caller passes an empty vector and we
    // synthesize the delete-bitset slice into a local scratch vector — this
    // costs one slice allocation per deletes-only post-filter pass, which is
    // dominated by the per-result raw-position bit test.
    void apply_pq_post_filter_locked(search_result_t& search_res,
                                     uint64_t start_row,
                                     uint64_t shard_sz,
                                     const std::vector<uint32_t>& user_host_mask) const {
        const bool has_user = !user_host_mask.empty();  // non-empty iff build_search_bitset ran the user-filter path
        const bool has_del  = this->deleted_count_ > 0;
        if (!has_user && !has_del) return;

        std::vector<uint32_t> synthesized_del_slice;
        const std::vector<uint32_t>* host_mask = &user_host_mask;
        if (!has_user) {
            // Deletes-only: copy the delete-bitset slice straight into the
            // local scratch. start_row is 0 (non-SHARDED) or a multiple of 32
            // (SHARDED), so start_word is always an integer (see
            // index_base.hpp lifecycle).
            const uint64_t n_mask_words = (shard_sz + 31) / 32;
            const uint64_t start_word   = start_row / 32;
            const uint64_t del_words    = this->deleted_bitset_.size();
            synthesized_del_slice.resize(n_mask_words);
            for (uint64_t w = 0; w < n_mask_words; ++w) {
                synthesized_del_slice[w] = (start_word + w < del_words)
                                             ? this->deleted_bitset_[start_word + w]
                                             : 0xFFFFFFFFu;
            }
            host_mask = &synthesized_del_slice;
            // Tail bits past shard_sz are unreachable: the raw-position check
            // below rejects p >= shard_sz before touching host_mask.
        }

        const float kDistSentinel = std::numeric_limits<float>::max();
        for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
            int64_t raw = search_res.neighbors[i];
            if (raw < 0) continue;
            uint64_t p = static_cast<uint64_t>(raw);
            if (p >= shard_sz
                || !(((*host_mask)[p / 32] >> (p % 32)) & 1U)) {
                search_res.neighbors[i] = -1;
                search_res.distances[i] = kDistSentinel;
            }
        }
    }

    // `prebuilt`, when non-null, supplies a host_mask_bundle_t computed off the
    // worker thread (see search_with_filter below). On that path we skip the
    // queries-H2D sync_stream — the search kernel queues on the same stream
    // immediately after the queries / bitset H2D copies and is naturally
    // ordered, so the only barrier we need is the terminal handle.sync().
    // The bundle's host_mask is kept alive by a shared_ptr captured in the
    // worker lambda. When prebuilt is null we use the legacy build_search_bitset
    // entry point which keeps its own internal sync (host_mask is local there).
    search_result_t search_internal(raft_handle_wrapper_t& handle, const T* queries_data, uint64_t num_queries, uint32_t limit, const ivf_pq_search_params_t& sp, const std::string& preds_json = "", const host_mask_bundle_t* prebuilt = nullptr) {
        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::ivf_pq::search_params search_params;
        search_params.n_probes = sp.n_probes;
        if constexpr (std::is_same_v<T, __half>) {
            search_params.lut_dtype = CUDA_R_16F;
            // Keep accumulation in float32 to prevent overflow when M sub-vector
            // distances are summed: M * max_lut_entry can easily exceed float16 max.
            search_params.internal_distance_dtype = CUDA_R_32F;
        }

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

        // Declared at function scope so the post-filter block after the GPU
        // search can reuse the shard range and the host-side user-filter mask
        // (see WARNING comment below).
        uint64_t start_row = 0, shard_sz = this->count;
        // Pointer (not owned) into either the prebuilt bundle's mask or the
        // local fallback below. apply_pq_post_filter_locked reads it after the
        // GPU finishes; the underlying buffer outlives the GPU work because:
        //  - prebuilt path: caller's lambda capture keeps bundle alive
        //  - legacy path:   `local_user_mask` lives until function return
        const std::vector<uint32_t>* user_host_mask_ptr = nullptr;
        std::vector<uint32_t> local_user_mask;
        uint64_t user_filter_popcount = 0;     // popcount(user_filter ∧ ¬deleted); see WARNING above

        if (local_index) {
            // Reuse per-thread grow-only workspace buffers (Step C). Allocated
            // once per worker thread out of the RMM pool installed by
            // ensure_rmm_pool_for_device, then resized lazily to the largest
            // num_queries seen so far. Eliminates 4-5 cudaMallocs per search.
            auto& q_buf = handle.template q_dev_buf<T>(static_cast<size_t>(num_queries) * this->dimension);
            auto queries_device = raft::make_device_matrix_view<T, int64_t>(
                q_buf.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));
            raft::copy(*res, queries_device, raft::make_host_matrix_view<const T, int64_t>(queries_data, num_queries, this->dimension));
            // Legacy path syncs here so build_search_bitset's stack-local host
            // bitmap can drain on the same stream. Prebuilt path skips: bitset
            // H2D queues behind queries H2D on the same stream, and host_mask
            // outlives the kernel via the bundle's shared_ptr capture. Also skip
            // when no bitmap will be produced at all (no user filter, no soft
            // deletes) — build_search_bitset short-circuits to nullptr with no
            // GPU work, so the sync would be a pure per-query CPU↔GPU round-trip.
            const bool will_build_bitset =
                !prebuilt && (!preds_json.empty() || this->has_soft_deletes());
            if (will_build_bitset) {
                raft::resource::sync_stream(*res);
            }

            auto& n_buf = handle.neighbors_buf(static_cast<size_t>(num_queries) * limit);
            auto& d_buf = handle.distances_buf(static_cast<size_t>(num_queries) * limit);
            auto neighbors_device_internal = raft::make_device_matrix_view<int64_t, int64_t>(
                n_buf.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
            auto distances_device_internal = raft::make_device_matrix_view<float, int64_t>(
                d_buf.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

            if (this->dist_mode == DistributionMode_SHARDED) {
                int rank = handle.get_rank();
                shard_sz = this->shard_sizes_[rank];
                start_row = 0;
                for (int r = 0; r < rank; ++r) start_row += this->shard_sizes_[r];
            }

            std::shared_ptr<raft::core::bitset<uint32_t, int64_t>> bs_ptr;
            if (prebuilt) {
                if (prebuilt->has_filter) {
                    bs_ptr = this->upload_host_mask(handle, prebuilt->mask, shard_sz);
                    user_host_mask_ptr    = &prebuilt->mask;
                    user_filter_popcount  = prebuilt->popcount;
                } else if (prebuilt->deletes_only) {
                    bs_ptr = this->acquire_delete_bitset_device(handle, start_row, shard_sz);
                }
            } else {
                bs_ptr = this->build_search_bitset(handle, preds_json, start_row, shard_sz, &local_user_mask, &user_filter_popcount);
                if (!local_user_mask.empty()) user_host_mask_ptr = &local_user_mask;
            }

            if (bs_ptr) {
                auto filter = cuvs::neighbors::filtering::bitset_filter(bs_ptr->view());
                cuvs::neighbors::ivf_pq::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(queries_device),
                                                    neighbors_device_internal, distances_device_internal, filter);
            } else if (this->batch_window() != 0) {
                this->dynb_cache_.search(*res, handle.get_device_id(), local_index, search_params,
                                         static_cast<int64_t>(limit),
                                         static_cast<uint32_t>(search_params.n_probes), 0u,
                                         this->dynb_concurrency_hint(),
                                         this->dynb_conservative_dispatch(),
                                         static_cast<double>(this->batch_window()) / 1000.0,
                                         raft::make_const_mdspan(queries_device),
                                         neighbors_device_internal, distances_device_internal);
            } else {
                cuvs::neighbors::ivf_pq::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(queries_device),
                                                    neighbors_device_internal, distances_device_internal);
            }
            raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device_internal);
            raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device_internal);
        } else {
            std::string msg = "IVF-PQ search error: No valid index found for device " + std::to_string(handle.get_device_id()) +
                             " (Mode: " + mode_name(this->dist_mode) + ")";
            throw std::runtime_error(msg);
        }
        handle.sync();

        // GPU search is done; take shared_lock only for the CPU-side ID translation
        // so that concurrent extend() calls (which write host_ids under unique_lock) are safe.
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);

            // Skip the post-filter pass when popcount(user ∧ ¬deleted) is
            // comfortably above num_queries * limit — the cuVS bitset_filter
            // leak quirk cannot trigger in that regime (see WARNING above for
            // kPqPostFilterSkipFactor rationale). Deletes-only and unfiltered
            // paths leave user_host_mask_ptr null and fall through to the
            // existing function (which is itself a no-op there).
            constexpr uint64_t kPqPostFilterSkipFactor = 4;
            const bool skip_pq_post_filter =
                user_host_mask_ptr != nullptr &&
                user_filter_popcount >=
                    static_cast<uint64_t>(num_queries) *
                    static_cast<uint64_t>(limit) *
                    kPqPostFilterSkipFactor;
            if (!skip_pq_post_filter) {
                // Empty vec on the deletes-only / unfiltered paths — the
                // function synthesizes the delete-bitset slice locally there.
                static const std::vector<uint32_t> kEmptyMask;
                apply_pq_post_filter_locked(search_res, start_row, shard_sz,
                    user_host_mask_ptr ? *user_host_mask_ptr : kEmptyMask);
            }

            if (this->dist_mode == DistributionMode_SHARDED) {
                int64_t offset = 0;
                for (int r = 0; r < handle.get_rank(); ++r) offset += (int64_t)this->shard_sizes_[r];
                for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
                    if (search_res.neighbors[i] != -1) {
                        int64_t global_pos = search_res.neighbors[i] + offset;
                        if (!this->host_ids.empty()) {
                            search_res.neighbors[i] = (int64_t)this->host_ids[global_pos];
                        } else {
                            search_res.neighbors[i] = global_pos;
                        }
                    }
                }
            } else {
                if (!this->host_ids.empty()) {
                    for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
                        if (search_res.neighbors[i] != -1) {
                            search_res.neighbors[i] = (int64_t)this->host_ids[search_res.neighbors[i]];
                        }
                    }
                }
            }
        }

        this->transform_distance(this->metric, search_res.distances);
        return search_res;
    }

    // Sync float entry — wraps search_float_async + search_wait.
    search_result_t search_float(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const ivf_pq_search_params_t& sp) {
        uint64_t job_id = this->search_float_async(queries_data, num_queries, query_dimension, limit, sp);
        return this->search_wait(job_id);
    }

    // Sync float filtered entry — wraps search_float_with_filter_async + search_wait.
    search_result_t search_float_with_filter(const float* queries_data, uint64_t num_queries,
                                             uint32_t query_dimension, uint32_t limit,
                                             const ivf_pq_search_params_t& sp,
                                             const std::string& preds_json) {
        uint64_t job_id = this->search_float_with_filter_async(queries_data, num_queries, query_dimension, limit, sp, preds_json);
        return this->search_wait(job_id);
    }

    // Async variant of search_float_with_filter. Builds the host mask bundle on
    // the calling thread (same off-worker pattern as the sync filter), copies
    // queries into a shared_ptr so they outlive the Go caller, captures both in
    // the worker lambda, and returns a job_id that search_wait() can collect.
    // Used by the multi-index filter path so per-shard searches run in parallel.
    uint64_t search_float_with_filter_async(const float* queries_data, uint64_t num_queries,
                                            uint32_t query_dimension, uint32_t limit,
                                            const ivf_pq_search_params_t& sp,
                                            const std::string& preds_json) {
        if (!queries_data) throw std::invalid_argument("search_async: queries_data is null");
        if (num_queries == 0) throw std::invalid_argument("search_async: num_queries is 0");
        if (this->dimension == 0) throw std::runtime_error("search_async: index dimension is 0");
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) throw std::runtime_error("search_async: index not loaded");
        }
        if (!this->worker) throw std::runtime_error("Worker not initialized");

        auto queries_copy = std::make_shared<std::vector<float>>(queries_data, queries_data + num_queries * query_dimension);

        if (this->dist_mode == DistributionMode_SHARDED) {
            // Bitmap eval runs on the caller's (Go) thread; per-shard searches
            // hit their device queues directly; merge runs in search_wait().
            // See plan .claude/plans/effervescent-hatching-dewdrop.md.
            auto shard_masks = this->build_filter_shard_masks(preds_json);
            auto shard_search_task = [this, num_queries, query_dimension, limit, sp, queries_copy, shard_masks](raft_handle_wrapper_t& gpu_handle) -> std::any {
                int rank = gpu_handle.get_rank();
                return this->search_float_internal(gpu_handle, queries_copy->data(), num_queries, query_dimension, limit, sp, /*preds_json=*/"", shard_masks[rank].get());
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
            return this->search_float_internal(handle, queries_copy->data(), num_queries, query_dimension, limit, sp, /*preds_json=*/"", mask.get());
        };
        return this->worker->submit(task);
    }

    uint64_t search_float_async(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const ivf_pq_search_params_t& sp) {
        if (!queries_data) throw std::invalid_argument("search_async: queries_data is null");
        if (num_queries == 0) throw std::invalid_argument("search_async: num_queries is 0");
        if (this->dimension == 0) throw std::runtime_error("search_async: index dimension is 0");
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);
            if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) throw std::runtime_error("search_async: index not loaded");
        }

        auto queries_copy = std::make_shared<std::vector<float>>(queries_data, queries_data + num_queries * query_dimension);

        if (this->dist_mode == DistributionMode_SHARDED) {
            // Same shape as search_async — fan out, hand back a composite id,
            // let search_wait() do the merge on the caller's thread.
            auto shard_search_task = [this, num_queries, query_dimension, limit, sp, queries_copy](raft_handle_wrapper_t& gpu_handle) -> std::any {
                return this->search_float_internal(gpu_handle, queries_copy->data(), num_queries, query_dimension, limit, sp);
            };
            auto job_ids = this->worker->submit_all_devices_no_wait(shard_search_task);
            return this->worker->submit_composite_pending(std::move(job_ids), num_queries, limit);
        }

        // Single-GPU / replicated: the helper decides standalone vs fused; the
        // shared_ptr keeps the copied queries alive until the search runs.
        return this->search_batchable_float(queries_copy, queries_copy->data(), num_queries, limit, sp);
    }

    // float32-input search. Mirrors search_batchable_typed but calls
    // search_float_internal; request-level batching (if enabled) happens inside it.
    uint64_t search_batchable_float(std::shared_ptr<std::vector<float>> owner, const float* queries_data,
                                    uint64_t num_queries, uint32_t limit, const ivf_pq_search_params_t& sp) {
        if (!this->worker) throw std::runtime_error("Worker not initialized");
        auto task = [this, owner, queries_data, num_queries, limit, sp](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_float_internal(handle, queries_data, num_queries, this->dimension, limit, sp);
        };
        return this->worker->submit(task);
    }

    // See `search_internal` for the contract on `prebuilt`.
    search_result_t search_float_internal(raft_handle_wrapper_t& handle, const float* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/,
                        uint32_t limit, const ivf_pq_search_params_t& sp, const std::string& preds_json = "", const host_mask_bundle_t* prebuilt = nullptr) {
        auto res = handle.get_raft_resources();
        // Step C: reuse the per-thread T-typed query workspace buffer.
        const size_t n_q_elems = static_cast<size_t>(num_queries) * this->dimension;
        auto& q_buf_t = handle.template q_dev_buf<T>(n_q_elems);
        auto q_dev_t = raft::make_device_matrix_view<T, int64_t>(
            q_buf_t.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));

        if constexpr (std::is_same_v<T, float>) {
            raft::copy(*res, q_dev_t, raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, this->dimension));
        } else if constexpr (std::is_same_v<T, __half>) {
            // Cast fp32 → fp16 on the host (F16C / AVX, IEEE round-to-nearest-even
            // — bit-identical to mdspan_copy_kernel<__half>) into a pinned
            // staging buffer, then a single H2D copy moves half the bytes.
            // This eliminates one device alloc (q_dev_f), one full H2D fp32
            // upload, and the per-search mdspan_copy_kernel<__half> dispatch.
            __half* host_h = handle.ensure_host_half_buf(n_q_elems);
            matrixone::cast_float_to_half_host(queries_data, host_h, n_q_elems);
            raft::copy(*res, q_dev_t,
                raft::make_host_matrix_view<const __half, int64_t>(host_h, num_queries, this->dimension));
        } else {
            // sizeof(T) == 1: int8 quantizer path keeps an fp32 device copy
            // because quantizer_.transform reads it on-device. Reuse the
            // per-thread float workspace too.
            auto& q_buf_f = handle.q_dev_buf_float(n_q_elems);
            auto q_dev_f = raft::make_device_matrix_view<float, int64_t>(
                q_buf_f.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));
            raft::copy(*res, q_dev_f, raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, this->dimension));

            if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
            this->quantizer_.template transform<T>(*res, q_dev_f, q_buf_t.data(), true);
        }
        // Legacy path syncs to drain queries DMA before the stack-local host
        // bitmap inside build_search_bitset goes through its own sync. Prebuilt
        // path skips: bitset H2D and search kernel queue behind queries H2D on
        // the same stream and the terminal handle.sync() drains everything. Also
        // skip when no bitmap will be produced (no user filter, no soft deletes):
        // build_search_bitset returns nullptr with no GPU work, so the sync is
        // a pure per-query CPU↔GPU round-trip.
        const bool will_build_bitset =
            !prebuilt && (!preds_json.empty() || this->has_soft_deletes());
        if (will_build_bitset) {
            raft::resource::sync_stream(*res);
        }

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::ivf_pq::search_params search_params;
        search_params.n_probes = sp.n_probes;
        if constexpr (std::is_same_v<T, __half>) {
            search_params.lut_dtype = CUDA_R_16F;
            search_params.internal_distance_dtype = CUDA_R_32F;
        }

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

        // Declared at function scope so the post-filter block after the GPU
        // search can reuse the shard range and the host-side user-filter mask
        // (see WARNING comment below).
        uint64_t start_row = 0, shard_sz = this->count;
        const std::vector<uint32_t>* user_host_mask_ptr = nullptr;
        std::vector<uint32_t> local_user_mask;  // legacy fallback storage
        uint64_t user_filter_popcount = 0;      // popcount(user_filter ∧ ¬deleted); see WARNING above

        if (local_index) {
            // Reuse per-thread grow-only neighbor / distance workspace buffers (Step C).
            auto& n_buf = handle.neighbors_buf(static_cast<size_t>(num_queries) * limit);
            auto& d_buf = handle.distances_buf(static_cast<size_t>(num_queries) * limit);
            auto neighbors_device = raft::make_device_matrix_view<int64_t, int64_t>(
                n_buf.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
            auto distances_device = raft::make_device_matrix_view<float, int64_t>(
                d_buf.data(), static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

            if (this->dist_mode == DistributionMode_SHARDED) {
                int rank = handle.get_rank();
                shard_sz = this->shard_sizes_[rank];
                start_row = 0;
                for (int r = 0; r < rank; ++r) start_row += this->shard_sizes_[r];
            }

            std::shared_ptr<raft::core::bitset<uint32_t, int64_t>> bs_ptr;
            if (prebuilt) {
                if (prebuilt->has_filter) {
                    bs_ptr = this->upload_host_mask(handle, prebuilt->mask, shard_sz);
                    user_host_mask_ptr   = &prebuilt->mask;
                    user_filter_popcount = prebuilt->popcount;
                } else if (prebuilt->deletes_only) {
                    bs_ptr = this->acquire_delete_bitset_device(handle, start_row, shard_sz);
                }
            } else {
                bs_ptr = this->build_search_bitset(handle, preds_json, start_row, shard_sz, &local_user_mask, &user_filter_popcount);
                if (!local_user_mask.empty()) user_host_mask_ptr = &local_user_mask;
            }

            if (bs_ptr) {
                auto filter = cuvs::neighbors::filtering::bitset_filter(bs_ptr->view());
                cuvs::neighbors::ivf_pq::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(q_dev_t),
                                                    neighbors_device, distances_device, filter);
            } else if (this->batch_window() != 0) {
                this->dynb_cache_.search(*res, handle.get_device_id(), local_index, search_params,
                                         static_cast<int64_t>(limit),
                                         static_cast<uint32_t>(search_params.n_probes), 0u,
                                         this->dynb_concurrency_hint(),
                                         this->dynb_conservative_dispatch(),
                                         static_cast<double>(this->batch_window()) / 1000.0,
                                         raft::make_const_mdspan(q_dev_t),
                                         neighbors_device, distances_device);
            } else {
                cuvs::neighbors::ivf_pq::search(*res, search_params, *local_index,
                                                    raft::make_const_mdspan(q_dev_t),
                                                    neighbors_device, distances_device);
            }

            raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device);
            raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device);
        } else {
            std::string msg = "IVF-PQ search error: No valid index found for device " + std::to_string(handle.get_device_id()) +
                             " (Mode: " + mode_name(this->dist_mode) + ")";
            throw std::runtime_error(msg);
        }
        handle.sync();

        // GPU search is done; take shared_lock only for the CPU-side ID translation
        // so that concurrent extend() calls (which write host_ids under unique_lock) are safe.
        {
            std::shared_lock<std::shared_mutex> lock(this->mutex_);

            // Skip the post-filter pass when popcount(user ∧ ¬deleted) is
            // comfortably above num_queries * limit — the cuVS bitset_filter
            // leak quirk cannot trigger in that regime (see WARNING above for
            // kPqPostFilterSkipFactor rationale). Deletes-only and unfiltered
            // paths leave user_host_mask_ptr null and fall through to the
            // existing function (which is a no-op there).
            constexpr uint64_t kPqPostFilterSkipFactor = 4;
            const bool skip_pq_post_filter =
                user_host_mask_ptr != nullptr &&
                user_filter_popcount >=
                    static_cast<uint64_t>(num_queries) *
                    static_cast<uint64_t>(limit) *
                    kPqPostFilterSkipFactor;
            if (!skip_pq_post_filter) {
                std::vector<uint32_t> empty_mask;
                std::vector<uint32_t>& mask_ref =
                    user_host_mask_ptr ? const_cast<std::vector<uint32_t>&>(*user_host_mask_ptr)
                                       : empty_mask;
                apply_pq_post_filter_locked(search_res, start_row, shard_sz, mask_ref);
            }

            if (this->dist_mode == DistributionMode_SHARDED) {
                int64_t offset = 0;
                for (int r = 0; r < handle.get_rank(); ++r) offset += (int64_t)this->shard_sizes_[r];
                for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
                    if (search_res.neighbors[i] != -1) {
                        int64_t global_pos = search_res.neighbors[i] + offset;
                        if (!this->host_ids.empty()) {
                            search_res.neighbors[i] = (int64_t)this->host_ids[global_pos];
                        } else {
                            search_res.neighbors[i] = global_pos;
                        }
                    }
                }
            } else {
                if (!this->host_ids.empty()) {
                    for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
                        if (search_res.neighbors[i] != -1) {
                            search_res.neighbors[i] = (int64_t)this->host_ids[search_res.neighbors[i]];
                        }
                    }
                }
            }
        }

        this->transform_distance(this->metric, search_res.distances);
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

                // cuVS stores cluster centers as (n_lists, dim_ext): the first
                // rot_dim columns are the rotated center coords, then one column
                // for the center's squared L2 norm, then SIMD padding. Callers
                // (and the Go binding, which sizes its slice as n_lists*rot_dim)
                // want only the rot_dim coords — returning the padded width here
                // overruns the caller's buffer (see gpu_ivf_pq_get_centers).
                auto centers_view = local_index->centers();
                const size_t n_centers = centers_view.extent(0);
                const size_t dim_ext   = centers_view.extent(1);
                const size_t rot_dim   = static_cast<size_t>(local_index->rot_dim());

                auto centers_device_target = raft::make_device_matrix<T, int64_t>(*res, n_centers, dim_ext);
                if constexpr (sizeof(T) == 1) {
                    if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
                    auto centers_float_view = raft::make_device_matrix_view<const float, int64_t>(centers_view.data_handle(), n_centers, dim_ext);
                    this->quantizer_.template transform<T>(*res, centers_float_view, centers_device_target.data_handle(), true);
                } else {
                    raft::copy(*res, centers_device_target.view(), centers_view);
                }

                std::vector<T> padded(n_centers * dim_ext);
                raft::copy(*res, raft::make_host_matrix_view<T, int64_t>(padded.data(), n_centers, dim_ext), centers_device_target.view());
                raft::resource::sync_stream(*res);

                if (rot_dim == dim_ext) return padded;  // no padding to strip
                std::vector<T> host_centers(n_centers * rot_dim);
                for (size_t i = 0; i < n_centers; ++i) {
                    const T* src = padded.data() + i * dim_ext;
                    std::copy(src, src + rot_dim, host_centers.data() + i * rot_dim);
                }
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
        auto res = this->worker->wait(job_id).get();
        if (res.error) std::rethrow_exception(res.error);
        if (!this->host_ids.empty()) {
            this->save_ids(filename + ".ids");
        }
    }

    void load(const std::string& filename) {
        auto task = [&](raft_handle_wrapper_t& handle) -> std::any {
            auto res = handle.get_raft_resources();
            auto local_idx = std::make_unique<ivf_pq_index>(*res);
            cuvs::neighbors::ivf_pq::deserialize(*res, filename, local_idx.get());
            // Drain `res`'s stream so any H2D copy committed by deserialize is
            // visible before any search thread reads the loaded index. Without
            // this, a worker on a different stream can race the H2D and see
            // pre-copy garbage. Same race fixed in cagra.hpp.
            raft::resource::sync_stream(*res);

            {
                std::unique_lock<std::shared_mutex> lock(this->mutex_);
                this->count = static_cast<uint64_t>(local_idx->size());
                this->dimension = static_cast<uint32_t>(local_idx->dim());
                this->current_offset_ = this->count;

                if (this->dist_mode == DistributionMode_SINGLE_GPU) {
                    index_ = std::move(local_idx);
                } else {
                    this->replicated_indices_[handle.get_device_id()] =
                        std::shared_ptr<ivf_pq_index>(std::move(local_idx));
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

    // Save all index components to a directory with manifest.json.
    void save_dir(const std::string& dir) const {
        if (!this->is_loaded_ || (!index_ && this->replicated_indices_.empty())) {
            const char* mode_str =
                (this->dist_mode == DistributionMode_REPLICATED) ? "REPLICATED" :
                (this->dist_mode == DistributionMode_SHARDED)    ? "SHARDED"    : "SINGLE_GPU";
            std::string msg = "IVF-PQ index not built; cannot save_dir"
                " [is_loaded_=" + std::string(this->is_loaded_ ? "true" : "false") +
                ", index_=" + std::string(index_ ? "set" : "null") +
                ", replicated_indices_.size()=" + std::to_string(this->replicated_indices_.size()) +
                ", dist_mode=" + mode_str +
                ", count=" + std::to_string(this->count) +
                ", current_offset_=" + std::to_string(this->current_offset_) +
                ", devices.size()=" + std::to_string(this->devices_.size()) + "]";
            std::cerr << "[IVFPQ save_dir ERROR] " << msg << std::endl;
            throw std::runtime_error(msg);
        }

        this->ensure_dir(dir);
        auto comp_entries = this->save_common_components(dir);

        if (this->dist_mode == DistributionMode_SINGLE_GPU) {
            uint64_t job_id = this->worker->submit_main(
                [&](raft_handle_wrapper_t& handle) -> std::any {
                    cuvs::neighbors::ivf_pq::serialize(
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
                        throw std::runtime_error("No replicated IVF-PQ index found to serialize");
                    cuvs::neighbors::ivf_pq::serialize(
                        *(handle.get_raft_resources()), dir + "/index.bin",
                        *std::static_pointer_cast<ivf_pq_index>(it->second));
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
                        cuvs::neighbors::ivf_pq::serialize(
                            *(handle.get_raft_resources()), shard_file,
                            *std::static_pointer_cast<ivf_pq_index>(it->second));
                    }
                    return std::any();
                }
            );
            comp_entries.push_back(this->shards_comp_entry());
        }

        std::string bp_json =
            "    \"n_lists\": " + std::to_string(this->build_params.n_lists) + ",\n" +
            "    \"m\": " + std::to_string(this->build_params.m) + ",\n" +
            "    \"bits_per_code\": " + std::to_string(this->build_params.bits_per_code) + ",\n" +
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
        this->write_manifest(dir, "ivf_pq", bp_json, comp_entries);
    }

    // Restore all index state from a directory previously written by save_dir().
    // target_mode overrides this->dist_mode, allowing a SINGLE_GPU .tar to be
    // loaded as REPLICATED (broadcasts index.bin to all GPUs) without rebuilding.
    void load_dir(const std::string& dir, distribution_mode_t target_mode) {
        auto m = this->read_manifest(dir, "ivf_pq");
        if (this->dist_mode == DistributionMode_SHARDED && target_mode != DistributionMode_SHARDED)
            throw std::invalid_argument("cannot change dist_mode: index was built as SHARDED");
        this->dist_mode = target_mode;

        std::string bp_json = json_object(m.raw, "build_params");
        this->build_params.n_lists =
            static_cast<uint32_t>(json_int(bp_json, "n_lists", 1024));
        this->build_params.m =
            static_cast<uint32_t>(json_int(bp_json, "m", 16));
        this->build_params.bits_per_code =
            static_cast<uint32_t>(json_int(bp_json, "bits_per_code", 8));
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
                auto local_idx = std::make_unique<ivf_pq_index>(*res);
                cuvs::neighbors::ivf_pq::deserialize(*res, full_path, local_idx.get());
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
                    auto local_idx = std::make_unique<ivf_pq_index>(*res);
                    cuvs::neighbors::ivf_pq::deserialize(*res, full_path, local_idx.get());
                    // See SINGLE_GPU branch above for the rationale.
                    raft::resource::sync_stream(*res);
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_indices_[handle.get_device_id()] =
                        std::shared_ptr<ivf_pq_index>(std::move(local_idx));
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
                    auto local_idx = std::make_unique<ivf_pq_index>(*res);
                    cuvs::neighbors::ivf_pq::deserialize(*res, shard_path, local_idx.get());
                    // See SINGLE_GPU branch above for the rationale.
                    raft::resource::sync_stream(*res);
                    std::unique_lock<std::shared_mutex> lock(this->mutex_);
                    this->replicated_indices_[handle.get_device_id()] =
                        std::shared_ptr<ivf_pq_index>(std::move(local_idx));
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

    void destroy() override {
        // Drop the dynamic_batching wrappers first — *before* worker->stop().
        // They hold CUDA streams / IO buffers created with the worker threads'
        // resources; destroying a wrapper after the worker is stopped would free
        // those against torn-down resources (cudaErrorInvalidResourceHandle).
        // dynb_cache_ has its own mutex, so this is safe vs. an in-flight search
        // (which keeps the wrapper alive via its own shared_ptr).
        this->dynb_cache_.clear();
        if (this->worker) this->worker->stop();
        std::unique_lock<std::shared_mutex> lock(this->mutex_);
        index_.reset();
        this->replicated_indices_.clear();
        this->replicated_datasets_.clear();
        this->quantizer_.reset();
        this->dataset_device_ptr_.reset();
    }

    uint32_t get_dim() const { return this->dimension; }

    // rot_dim / dim_ext come from the built cuVS index, not from `dimension`:
    // cuVS rounds rot_dim up to a multiple of pq_dim (so rot_dim >= dim), and
    // dim_ext = rot_dim + 1 (squared-norm column) rounded up for SIMD. These
    // sizes must match what get_centers() returns / the Go caller allocates.
    // Falls back to `dimension` before the index is built.
    const ivf_pq_index* any_local_index_() const {
        if (index_) return index_.get();
        if (!this->replicated_indices_.empty())
            return std::static_pointer_cast<ivf_pq_index>(this->replicated_indices_.begin()->second).get();
        return nullptr;
    }
    uint32_t get_rot_dim() const {
        const auto* idx = any_local_index_();
        return idx ? static_cast<uint32_t>(idx->rot_dim()) : this->dimension;
    }
    uint32_t get_dim_ext() const {
        const auto* idx = any_local_index_();
        return idx ? static_cast<uint32_t>(idx->dim_ext()) : this->dimension;
    }
    uint32_t get_n_list() const { return this->build_params.n_lists; }
};

} // namespace matrixone
