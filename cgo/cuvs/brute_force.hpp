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
 * Brute-force Index Implementation (Flat)
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
#include <cuvs/neighbors/brute_force.hpp>
#pragma GCC diagnostic pop


namespace matrixone {

// =============================================================================
// gpu_brute_force_t — Developer Guide
// =============================================================================
//
// ALGORITHM
// ---------
// Brute-force (exact) nearest-neighbor search: every query is compared against
// every vector in the dataset.  No approximation — guaranteed to return the true
// k nearest neighbors.  Practical only for small datasets (up to ~1M vectors)
// because search cost is O(n * dim) per query.
// Internally uses cuvs::neighbors::brute_force::build / search.
//
// DATA TYPE (T)
// -------------
// Supported T: float, half (fp16), int8_t, uint8_t.
// DistT is always float — cuVS brute_force::index<T, DistT=float>.
// IdT = int64_t (same as IVF types, unlike CAGRA which uses uint32_t).
//
// DISTRIBUTION MODE
// -----------------
// Only SINGLE_GPU is supported.  There is no REPLICATED or SHARDED path —
// the brute_force index holds the full dataset on one GPU.
// All build, extend, and search operations dispatch to devices_[0].
//
// LIFECYCLE
// ---------
//   1. Construct via one of:
//      - gpu_brute_force_t(dataset, count, dim, metric, bp, devices, nthread, mode, ids)
//        — full dataset constructor; copies data into flattened_host_dataset.
//      - gpu_brute_force_t(dataset, count, dim, metric, nthread, device_id, ids)
//        — compatibility constructor for tests (SINGLE_GPU only).
//      - gpu_brute_force_t(total_count, dim, metric, nthread, device_id, ids)
//        — chunked/empty constructor; fill via add_chunk() then build().
//      - gpu_brute_force_t(total_count, dim, metric, bp, devices, nthread, mode, ids)
//        — chunked constructor with full params.
//   2. start()  — creates the worker thread; no init_fn needed (brute force has
//                 no per-thread index cache).
//   3. build()  — uploads dataset to device, calls brute_force::build, sets
//                 is_loaded_=true, then clears flattened_host_dataset to free memory.
//                 The built index holds a device pointer to the dataset via
//                 dataset_device_ptr_ (shared_ptr kept alive by index_).
//   4. search() / search_float() — dispatched via submit() (round-robin, but with
//                 SINGLE_GPU there is only one device).
//   5. Destructor calls destroy() which stops the worker and resets index_.
//
// BUILD DETAILS
// -------------
// build_internal() holds a unique_lock during the entire build (dataset upload +
// brute_force::build call).  This is acceptable because brute_force is SINGLE_GPU
// only and build is a one-time operation — no concurrent searches can proceed
// before is_loaded_ is set.
// After build(), flattened_host_dataset is cleared to release host memory because
// brute_force keeps its own copy of the data on the GPU.
//
// EXTEND
// ------
// NOT supported — there is no cuVS brute_force::extend() API.
// To add new vectors, rebuild the index from scratch.
//
// SEARCH
// ------
// search_internal() holds a shared_lock during the GPU search call (read-only
// access to index_).  This is fine because brute_force is SINGLE_GPU and has no
// concurrent extend path.
// search_float_internal() converts float queries to T on the device before
// searching (quantize for 1-byte T, half-cast for T=half, direct for T=float).
//
// SOFT-DELETE BITSET
// ------------------
// Inherited from gpu_index_base_t.  Only the full-index (non-sharded) bitset
// path is used: sync_device_bitset() + bitset_filter passed to brute_force::search.
// delete_id() marks a bit as dead; the vector remains in the index but is
// filtered out of search results.
//
// HOST ID MAPPING
// ---------------
// If host_ids is non-empty, after search returns internal int64_t positions,
// each valid neighbor ID is mapped through host_ids[internal_id].
// Invalid neighbors are returned as -1.
//
// SERIALIZATION
// -------------
// Not implemented — brute_force does not have a save/load path.
// The dataset must be re-provided and rebuilt after restart.
//
// =============================================================================

/**
 * @brief Search result containing neighbor IDs and distances.
 * Common for all brute force instantiations.
 */
struct brute_force_search_result_t {
    std::vector<int64_t> neighbors; // Indices of nearest neighbors
    std::vector<float> distances;  // Distances to nearest neighbors
};

/**
 * @brief gpu_brute_force_t implements a Brute Force index that can run on a single GPU.
 */
template <typename T>
class gpu_brute_force_t : public gpu_index_base_t<T, brute_force_build_params_t, int64_t> {
public:
    // We force DistT=float for all our indices to avoid template bloat and satisfy cuVS
    using brute_force_index = cuvs::neighbors::brute_force::index<T, float>;
    using search_result_t = brute_force_search_result_t;

    // Internal index storage
    std::unique_ptr<brute_force_index> index_;

    ~gpu_brute_force_t() override {
        this->destroy();
    }

    // Unified Constructor for building from dataset
    gpu_brute_force_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension,
                    distance_type_t m, const brute_force_build_params_t& bp,
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

    // Compatibility constructor for tests
    gpu_brute_force_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension,
                       distance_type_t m, int nthread, int device_id, const int64_t* ids = nullptr) {
        this->dimension = dimension;
        this->count = static_cast<uint32_t>(count_vectors);
        this->metric = m;
        this->dist_mode = DistributionMode_SINGLE_GPU;
        this->devices_ = {device_id};
        this->current_offset_ = static_cast<uint32_t>(count_vectors);

        this->worker = std::make_unique<cuvs_worker_t>(static_cast<uint32_t>(nthread), this->devices_, this->dist_mode);

        this->flattened_host_dataset.resize(this->count * this->dimension);
        if (dataset_data) {
            std::copy(dataset_data, dataset_data + (this->count * this->dimension), this->flattened_host_dataset.begin());
        }

        if (ids) {
            this->set_ids(ids, this->count);
        }
    }

    // Compatibility constructor for brute_force_c.cpp (empty/chunked build)
    gpu_brute_force_t(uint64_t total_count, uint32_t dimension, distance_type_t m,
                       uint32_t nthread, int device_id, const int64_t* ids = nullptr) {
        this->dimension = dimension;
        this->count = static_cast<uint32_t>(total_count);
        this->metric = m;
        this->dist_mode = DistributionMode_SINGLE_GPU;
        this->devices_ = {device_id};
        this->current_offset_ = 0;

        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_, this->dist_mode);

        this->flattened_host_dataset.resize(this->count * this->dimension);
        if (ids) {
            this->set_ids(ids, this->count);
        }
    }

    // Constructor for chunked input (pre-allocates)
    gpu_brute_force_t(uint64_t total_count, uint32_t dimension, distance_type_t m,
                    const brute_force_build_params_t& bp, const std::vector<int>& devices,
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

    void start() override {
        auto init_fn = [](raft_handle_wrapper_t&) -> std::any { return std::any(); };
        auto stop_fn = [&](raft_handle_wrapper_t&) -> std::any {
            std::unique_lock<std::shared_mutex> lock(this->mutex_);
            index_.reset();
            this->quantizer_.reset();
            this->dataset_device_ptr_.reset();
            return std::any();
        };
        this->worker->start(init_fn, stop_fn);
    }

    void build() override {
        this->count = static_cast<uint32_t>(this->current_offset_);
        if (this->count == 0) {
            this->is_loaded_ = true;
            return;
        }
        if (this->flattened_host_dataset.size() > (size_t)this->count * this->dimension) {
            this->flattened_host_dataset.resize((size_t)this->count * this->dimension);
        }

        // std::cout << "[DEBUG] Brute-Force build: Starting build count=" << this->count << " dim=" << this->dimension << " metric=" << (int)this->metric << std::endl;

        this->train_quantizer_if_needed();
        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                this->build_internal(handle);
                return std::any();
            }
        );
        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        this->is_loaded_ = true;
        this->init_deleted_bitset();
        this->flattened_host_dataset.clear();
        this->flattened_host_dataset.shrink_to_fit();
    }

    void build_internal(raft_handle_wrapper_t& handle) {
        std::unique_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        // Create and own the device memory
        using dataset_t = raft::device_matrix<T, int64_t>;
        auto dataset_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)this->count, (int64_t)this->dimension);
        raft::copy(*res, dataset_device.view(), raft::make_host_matrix_view<const T, int64_t>(this->flattened_host_dataset.data(), this->count, this->dimension));
        raft::resource::sync_stream(*res);

        // Move the matrix into the shared pointer FIRST to ensure it's alive and owned
        auto shared_dataset = std::make_shared<dataset_t>(std::move(dataset_device));
        this->dataset_device_ptr_ = shared_dataset;

        cuvs::neighbors::brute_force::index_params ip;
        ip.metric = matrixone::convert_distance_type(this->metric);
        index_.reset(new brute_force_index(cuvs::neighbors::brute_force::build(
            *res, ip, raft::make_const_mdspan(shared_dataset->view()))));
        
        handle.sync();
    }

    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/, uint32_t limit, const brute_force_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (!this->is_loaded_ || !index_) return search_result_t{};

        // std::cout << "[DEBUG] Brute-Force search: num_queries=" << num_queries << " limit=" << limit << std::endl;

        auto task = [this, num_queries, limit, sp, queries_data](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_internal(handle, queries_data, num_queries, limit, sp);
        };
        uint64_t job_id = this->worker->submit(task);
        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        return std::any_cast<search_result_t>(result_wait.result);
    }

    uint64_t search_async(const T* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/, uint32_t limit, const brute_force_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || this->dimension == 0) return 0;
        if (!this->is_loaded_ || !index_) return 0;

        auto queries_copy = std::make_shared<std::vector<T>>(queries_data, queries_data + num_queries * this->dimension);

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

    search_result_t search_internal(raft_handle_wrapper_t& handle, const T* queries_data, uint64_t num_queries, uint32_t limit, const brute_force_search_params_t& /*sp*/) {
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        auto queries_device = raft::make_device_matrix<T, int64_t>(*res, (int64_t)num_queries, (int64_t)this->dimension);
        raft::copy(*res, queries_device.view(), raft::make_host_matrix_view<const T, int64_t>(queries_data, num_queries, this->dimension));
        raft::resource::sync_stream(*res);

        auto neighbors_device = raft::make_device_matrix<int64_t, int64_t>(*res, (int64_t)num_queries, (int64_t)limit);
        auto distances_device = raft::make_device_matrix<float, int64_t>(*res, (int64_t)num_queries, (int64_t)limit);

        cuvs::neighbors::brute_force::search_params bf_sp;
        if (this->deleted_count_ > 0) {
            this->sync_device_bitset(handle.get_device_id(), *res);
            auto info = this->get_device_bitset_info(handle.get_device_id());
            using bs_t = raft::core::bitset<uint32_t, int64_t>;
            auto* bs = static_cast<bs_t*>(info->ptr.get());
            auto filter = cuvs::neighbors::filtering::bitset_filter(bs->view());
            cuvs::neighbors::brute_force::search(*res, bf_sp, *index_,
                                                raft::make_const_mdspan(queries_device.view()),
                                                neighbors_device.view(), distances_device.view(), filter);
        } else {
            cuvs::neighbors::brute_force::search(*res, bf_sp, *index_,
                                                raft::make_const_mdspan(queries_device.view()),
                                                neighbors_device.view(), distances_device.view());
        }

        raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device.view());
        raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device.view());

        handle.sync();

        if (!this->host_ids.empty()) {
            for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
                if (search_res.neighbors[i] != -1) {
                    search_res.neighbors[i] = (int64_t)this->host_ids[search_res.neighbors[i]];
                }
            }
        }

        this->transform_distance(this->metric, search_res.distances);
        return search_res;
    }

    search_result_t search_float(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const brute_force_search_params_t& sp) {
        if constexpr (std::is_same_v<T, float>) return search(queries_data, num_queries, query_dimension, limit, sp);
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (!this->is_loaded_ || !index_) return search_result_t{};

        // std::cout << "[DEBUG] Brute-Force search_float: num_queries=" << num_queries << " limit=" << limit << " query_dimension=" << query_dimension << std::endl;

        auto task = [this, num_queries, query_dimension, limit, sp, queries_data](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_float_internal(handle, queries_data, num_queries, query_dimension, limit, sp);
        };
        uint64_t job_id = this->worker->submit(task);
        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        return std::any_cast<search_result_t>(result_wait.result);
    }

    uint64_t search_float_async(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, const brute_force_search_params_t& sp) {
        if constexpr (std::is_same_v<T, float>) return search_async(queries_data, num_queries, query_dimension, limit, sp);
        if (!queries_data || num_queries == 0 || this->dimension == 0) return 0;
        if (!this->is_loaded_ || !index_) return 0;

        auto queries_copy = std::make_shared<std::vector<float>>(queries_data, queries_data + num_queries * query_dimension);

        auto task = [this, num_queries, query_dimension, limit, sp, queries_copy](raft_handle_wrapper_t& handle) -> std::any {
            return this->search_float_internal(handle, queries_copy->data(), num_queries, query_dimension, limit, sp);
        };
        return this->worker->submit(task);
    }

    search_result_t search_float_internal(raft_handle_wrapper_t& handle, const float* queries_data, uint64_t num_queries, uint32_t /*query_dimension*/, uint32_t limit, const brute_force_search_params_t& /*sp*/) {
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
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

        auto neighbors_device = raft::make_device_matrix<int64_t, int64_t>(*res, (int64_t)num_queries, (int64_t)limit);
        auto distances_device = raft::make_device_matrix<float, int64_t>(*res, (int64_t)num_queries, (int64_t)limit);

        cuvs::neighbors::brute_force::search_params bf_sp;
        if (this->deleted_count_ > 0) {
            this->sync_device_bitset(handle.get_device_id(), *res);
            auto info = this->get_device_bitset_info(handle.get_device_id());
            using bs_t = raft::core::bitset<uint32_t, int64_t>;
            auto* bs = static_cast<bs_t*>(info->ptr.get());
            auto filter = cuvs::neighbors::filtering::bitset_filter(bs->view());
            cuvs::neighbors::brute_force::search(*res, bf_sp, *index_,
                                                raft::make_const_mdspan(q_dev_t.view()),
                                                neighbors_device.view(), distances_device.view(), filter);
        } else {
            cuvs::neighbors::brute_force::search(*res, bf_sp, *index_,
                                                raft::make_const_mdspan(q_dev_t.view()),
                                                neighbors_device.view(), distances_device.view());
        }

        raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device.view());
        raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device.view());

        handle.sync();

        if (!this->host_ids.empty()) {
            for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
                if (search_res.neighbors[i] != -1) {
                    search_res.neighbors[i] = (int64_t)this->host_ids[search_res.neighbors[i]];
                }
            }
        }

        this->transform_distance(this->metric, search_res.distances);
        return search_res;
    }

    void destroy() override {
        if (this->worker) this->worker->stop();
        std::unique_lock<std::shared_mutex> lock(this->mutex_);
        index_.reset();
        this->quantizer_.reset();
        this->dataset_device_ptr_.reset();
    }

    std::string info() const override {
        std::string json = gpu_index_base_t<T, brute_force_build_params_t, int64_t>::info();
        json += ", \"type\": \"Brute-Force\", \"brute_force\": {";
        if (index_) json += "\"built\": true";
        else json += "\"built\": false";
        json += "}}";
        return json;
    }
};

} // namespace matrixone
