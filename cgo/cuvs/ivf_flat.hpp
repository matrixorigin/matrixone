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

#pragma once

#include "index_base.hpp"
#include "cuvs_worker.hpp" // For cuvs_worker_t and raft_handle_wrapper_t
#include "cuvs_types.h"    // For distance_type_t, ivf_flat_build_params_t, etc.
#include <raft/util/cudart_utils.hpp> // For RAFT_CUDA_TRY
#include <cuda_fp16.h> // For half

// Standard library includes
#include <algorithm>   // For std::copy
#include <iostream>    // For simulation debug logs
#include <memory>
#include <numeric>     // For std::iota
#include <stdexcept>   // For std::runtime_error
#include <string>      
#include <type_traits> 
#include <vector>
#include <future>      // For std::promise and std::future
#include <limits>      // For std::numeric_limits
#include <shared_mutex> // For std::shared_mutex

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
// RAFT includes
#include <raft/core/device_mdarray.hpp> // For raft::device_matrix
#include <raft/core/device_mdspan.hpp>   // Required for device_matrix_view
#include <raft/core/host_mdarray.hpp> // For raft::host_matrix
#include <raft/core/resources.hpp>       // Core resource handle
#include <raft/core/copy.cuh>            // For raft::copy with type conversion
#include <raft/core/device_resources_snmg.hpp> // For checking SNMG type

// cuVS includes
#include <cuvs/distance/distance.hpp>    // cuVS distance API
#include <cuvs/neighbors/ivf_flat.hpp>   // IVF-Flat include
#include "quantize.hpp"
#pragma GCC diagnostic pop


namespace matrixone {

/**
 * @brief Search result containing neighbor IDs and distances.
 * Common for all IVF-Flat instantiations.
 */
struct ivf_flat_search_result_t {
    std::vector<int64_t> neighbors; // Indices of nearest neighbors
    std::vector<float> distances;  // Distances to nearest neighbors
};

/**
 * @brief gpu_ivf_flat_t implements an IVF-Flat index that can run on a single GPU or sharded across multiple GPUs.
 * It automatically chooses between single-GPU and multi-GPU (SNMG) cuVS APIs based on the RAFT handle resources.
 */
template <typename T>
class gpu_ivf_flat_t : public gpu_index_base_t<T, ivf_flat_build_params_t> {
public:
    using ivf_flat_index = cuvs::neighbors::ivf_flat::index<T, int64_t>;
    using mg_index = cuvs::neighbors::mg_index<ivf_flat_index, T, int64_t>;
    using search_result_t = ivf_flat_search_result_t;

    // Internal index storage
    std::unique_ptr<ivf_flat_index> index_;
    std::unique_ptr<mg_index> mg_index_;

    ~gpu_ivf_flat_t() override {
        this->destroy();
    }

    // Unified Constructor for building from dataset
    gpu_ivf_flat_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                    cuvs::distance::DistanceType m, const ivf_flat_build_params_t& bp, 
                    const std::vector<int>& devices, uint32_t nthread, distribution_mode_t mode) {
        
        this->dimension = dimension;
        this->count = static_cast<uint32_t>(count_vectors);
        this->metric = m;
        this->build_params = bp;
        this->dist_mode = mode;
        this->devices_ = devices;
        this->current_offset_ = static_cast<uint32_t>(count_vectors);

        bool force_mg = (mode == DistributionMode_SHARDED || mode == DistributionMode_REPLICATED);
        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_, force_mg || (this->devices_.size() > 1));

        this->flattened_host_dataset.resize(this->count * this->dimension);
        std::copy(dataset_data, dataset_data + (this->count * this->dimension), this->flattened_host_dataset.begin());
    }

    // Constructor for chunked input (pre-allocates)
    gpu_ivf_flat_t(uint64_t total_count, uint32_t dimension, cuvs::distance::DistanceType m, 
                    const ivf_flat_build_params_t& bp, const std::vector<int>& devices, 
                    uint32_t nthread, distribution_mode_t mode) {
        
        this->dimension = dimension;
        this->count = static_cast<uint32_t>(total_count);
        this->metric = m;
        this->build_params = bp;
        this->dist_mode = mode;
        this->devices_ = devices;
        this->current_offset_ = 0;

        bool force_mg = (mode == DistributionMode_SHARDED || mode == DistributionMode_REPLICATED);
        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_, force_mg || (this->devices_.size() > 1));

        this->flattened_host_dataset.resize(this->count * this->dimension);
    }

    // Unified Constructor for loading from file
    gpu_ivf_flat_t(const std::string& filename, uint32_t dimension, cuvs::distance::DistanceType m, 
                    const ivf_flat_build_params_t& bp, const std::vector<int>& devices, uint32_t nthread, distribution_mode_t mode) {
        
        this->filename_ = filename;
        this->dimension = dimension;
        this->metric = m;
        this->count = 0;
        this->build_params = bp;
        this->dist_mode = mode;
        this->devices_ = devices;
        this->current_offset_ = 0;

        bool force_mg = (mode == DistributionMode_SHARDED || mode == DistributionMode_REPLICATED);
        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_, force_mg || (this->devices_.size() > 1));
    }

    /**
     * @brief Starts the worker and initializes resources.
     */
    void start() {
        auto init_fn = [](raft_handle_wrapper_t&) -> std::any {
            return std::any();
        };

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

    /**
     * @brief Loads the index from file or builds it from the dataset.
     */
    void build() {
        std::unique_lock<std::shared_mutex> lock(this->mutex_);
        if (this->is_loaded_) return;

        if (this->filename_.empty() && this->current_offset_ > 0 && this->current_offset_ < this->count) {
            this->count = static_cast<uint32_t>(this->current_offset_);
            this->flattened_host_dataset.resize(this->count * this->dimension);
        }

        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                bool is_mg = is_snmg_handle(res);

                if (!this->filename_.empty()) {
                    if (is_mg) {
                        mg_index_ = std::make_unique<mg_index>(
                            cuvs::neighbors::ivf_flat::deserialize<T, int64_t>(*res, this->filename_));
                        // Update metadata
                        this->count = 0;
                        for (const auto& iface : mg_index_->ann_interfaces_) {
                            if (iface.index_.has_value()) this->count += static_cast<uint32_t>(iface.index_.value().size());
                        }
                        if (!mg_index_->ann_interfaces_.empty() && mg_index_->ann_interfaces_[0].index_.has_value()) {
                            this->build_params.n_lists = static_cast<uint32_t>(mg_index_->ann_interfaces_[0].index_.value().n_lists());
                        }
                    } else {
                        cuvs::neighbors::ivf_flat::index_params index_params;
                        index_params.metric = this->metric;
                        index_ = std::make_unique<ivf_flat_index>(*res, index_params, this->dimension);
                        cuvs::neighbors::ivf_flat::deserialize(*res, this->filename_, index_.get());
                        this->count = static_cast<uint32_t>(index_->size());
                        this->build_params.n_lists = static_cast<uint32_t>(index_->n_lists());
                    }
                    raft::resource::sync_stream(*res);
                } else if (!this->flattened_host_dataset.empty()) {
                    if (this->count < this->build_params.n_lists) {
                        throw std::runtime_error("Dataset too small: count (" + std::to_string(this->count) + 
                                                ") must be >= n_list (" + std::to_string(this->build_params.n_lists) + 
                                                ") to build IVF index.");
                    }

                    if (is_mg) {
                        auto dataset_host_view = raft::make_host_matrix_view<const T, int64_t>(
                            this->flattened_host_dataset.data(), (int64_t)this->count, (int64_t)this->dimension);

                        cuvs::neighbors::ivf_flat::index_params index_params;
                        index_params.metric = this->metric;
                        index_params.n_lists = this->build_params.n_lists;
                        index_params.add_data_on_build = this->build_params.add_data_on_build;
                        index_params.kmeans_trainset_fraction = this->build_params.kmeans_trainset_fraction;

                        cuvs::neighbors::mg_index_params<cuvs::neighbors::ivf_flat::index_params> mg_params(index_params);
                        if (this->dist_mode == DistributionMode_REPLICATED) {
                            mg_params.mode = cuvs::neighbors::distribution_mode::REPLICATED;
                        } else {
                            mg_params.mode = cuvs::neighbors::distribution_mode::SHARDED;
                        }

                        mg_index_ = std::make_unique<mg_index>(
                            cuvs::neighbors::ivf_flat::build(*res, mg_params, dataset_host_view));
                    } else {
                        auto dataset_device = new auto(raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                            *res, static_cast<int64_t>(this->count), static_cast<int64_t>(this->dimension)));
                        
                        this->dataset_device_ptr_ = std::shared_ptr<void>(dataset_device, [](void* ptr) {
                            delete static_cast<raft::device_matrix<T, int64_t, raft::layout_c_contiguous>*>(ptr);
                        });

                        RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device->data_handle(), this->flattened_host_dataset.data(),
                                                 this->flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                                 raft::resource::get_cuda_stream(*res)));

                        cuvs::neighbors::ivf_flat::index_params index_params;
                        index_params.metric = this->metric;
                        index_params.n_lists = this->build_params.n_lists;
                        index_params.add_data_on_build = this->build_params.add_data_on_build;
                        index_params.kmeans_trainset_fraction = this->build_params.kmeans_trainset_fraction;

                        index_ = std::make_unique<ivf_flat_index>(
                            cuvs::neighbors::ivf_flat::build(*res, index_params, raft::make_const_mdspan(dataset_device->view())));
                    }
                    raft::resource::sync_stream(*res);
                }
                return std::any();
            }
        );

        auto result_wait = this->worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        this->is_loaded_ = true;
        // Clear host dataset after building to save memory
        if (this->filename_.empty()) {
            this->flattened_host_dataset.clear();
            this->flattened_host_dataset.shrink_to_fit();
        }
    }

    /**
     * @brief Serializes the index to a file.
     * @param filename Path to the output file.
     */
    void save(const std::string& filename) {
        if (!this->is_loaded_ || (!index_ && !mg_index_)) throw std::runtime_error("index not loaded");

        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto res = handle.get_raft_resources();
                if (is_snmg_handle(res)) {
                    cuvs::neighbors::ivf_flat::serialize(*res, *mg_index_, filename);
                } else {
                    cuvs::neighbors::ivf_flat::serialize(*res, filename, *index_);
                }
                raft::resource::sync_stream(*res);
                return std::any();
            }
        );

        cuvs_task_result_t result = this->worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
    }

    /**
     * @brief Performs IVF-Flat search for given queries.
     * @param queries_data Pointer to flattened query vectors on host.
     * @param num_queries Number of query vectors.
     * @param query_dimension Dimension of query vectors.
     * @param limit Number of nearest neighbors to find.
     * @param sp IVF-Flat search parameters.
     * @return Search results.
     */
    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, 
                        uint32_t limit, const ivf_flat_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (query_dimension != this->dimension) throw std::runtime_error("dimension mismatch");
        if (!this->is_loaded_ || (!index_ && !mg_index_)) return search_result_t{};

        // For large batches or if batching is explicitly disabled, use standard path
        if (num_queries > 16 || !this->worker->use_batching()) {
            uint64_t job_id = this->worker->submit(
                [&, num_queries, limit, sp, queries_data](raft_handle_wrapper_t& handle) -> std::any {
                    return this->search_internal(handle, queries_data, num_queries, limit, sp);
                }
            );
            auto result_wait = this->worker->wait(job_id).get();
            if (result_wait.error) std::rethrow_exception(result_wait.error);
            return std::any_cast<search_result_t>(result_wait.result);
        }

        // Dynamic batching for small query counts
        struct search_req_t {
            const T* data;
            uint64_t n;
        };

        std::string batch_key = "ivf_flat_s_" + std::to_string((uintptr_t)this) + "_" + std::to_string(limit) + "_" + std::to_string(sp.n_probes);
        
        auto exec_fn = [this, limit, sp](cuvs_worker_t::raft_handle& handle, const std::vector<std::any>& reqs, const std::vector<std::function<void(std::any)>>& setters) {
            uint64_t total_queries = 0;
            for (const auto& r : reqs) total_queries += std::any_cast<search_req_t>(r).n;

            std::vector<T> aggregated_queries(total_queries * this->dimension);
            uint64_t offset = 0;
            for (const auto& r : reqs) {
                auto req = std::any_cast<search_req_t>(r);
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

        auto future = this->worker->template submit_batched<search_result_t>(batch_key, search_req_t{queries_data, num_queries}, exec_fn);
        return future.get();
    }

    /**
     * @brief Internal search implementation (no worker submission)
     */
    search_result_t search_internal(raft_handle_wrapper_t& handle, const T* queries_data, uint64_t num_queries, uint32_t limit, const ivf_flat_search_params_t& sp) {
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::ivf_flat::search_params search_params;
        search_params.n_probes = sp.n_probes;

        const ivf_flat_index* local_index = index_.get();
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

        if (is_snmg_handle(res) && mg_index_) {
            auto queries_host_view = raft::make_host_matrix_view<const T, int64_t>(
                queries_data, (int64_t)num_queries, (int64_t)this->dimension);
            auto neighbors_host_view = raft::make_host_matrix_view<int64_t, int64_t>(
                search_res.neighbors.data(), (int64_t)num_queries, (int64_t)limit);
            auto distances_host_view = raft::make_host_matrix_view<float, int64_t>(
                search_res.distances.data(), (int64_t)num_queries, (int64_t)limit);

            cuvs::neighbors::mg_search_params<cuvs::neighbors::ivf_flat::search_params> mg_search_params(search_params);
            cuvs::neighbors::ivf_flat::search(*res, *mg_index_, mg_search_params,
                                                queries_host_view, neighbors_host_view, distances_host_view);
        } else if (local_index) {
            auto queries_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));
            RAFT_CUDA_TRY(cudaMemcpyAsync(queries_device.data_handle(), queries_data,
                                        num_queries * this->dimension * sizeof(T), cudaMemcpyHostToDevice,
                                        raft::resource::get_cuda_stream(*res)));

            auto neighbors_device = raft::make_device_matrix<int64_t, int64_t, raft::layout_c_contiguous>(
                *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
            auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

            cuvs::neighbors::ivf_flat::search(*res, search_params, *local_index,
                                                raft::make_const_mdspan(queries_device.view()), 
                                                neighbors_device.view(), distances_device.view());

            RAFT_CUDA_TRY(cudaMemcpyAsync(search_res.neighbors.data(), neighbors_device.data_handle(),
                                        search_res.neighbors.size() * sizeof(int64_t), cudaMemcpyDeviceToHost,
                                        raft::resource::get_cuda_stream(*res)));
            RAFT_CUDA_TRY(cudaMemcpyAsync(search_res.distances.data(), distances_device.data_handle(),
                                        search_res.distances.size() * sizeof(float), cudaMemcpyDeviceToHost,
                                        raft::resource::get_cuda_stream(*res)));
        } else {
            throw std::runtime_error("Index not loaded or failed to find local index shard for current device.");
        }

        raft::resource::sync_stream(*res);

        for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
            if (search_res.neighbors[i] == std::numeric_limits<int64_t>::max() || 
                search_res.neighbors[i] == 4294967295LL || search_res.neighbors[i] < 0) {
                search_res.neighbors[i] = -1;
            }
        }
        return search_res;
    }

    /**
     * @brief Performs IVF-Flat search for given float32 queries, with on-the-fly quantization if needed.
     */
    search_result_t search_float(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, 
                        uint32_t limit, const ivf_flat_search_params_t& sp) {
        if constexpr (std::is_same_v<T, float>) {
            return search(queries_data, num_queries, query_dimension, limit, sp);
        }

        if (!queries_data || num_queries == 0 || this->dimension == 0) return search_result_t{};
        if (query_dimension != this->dimension) throw std::runtime_error("dimension mismatch");
        if (!this->is_loaded_ || (!index_ && !mg_index_)) return search_result_t{};

        // For large batches or if batching is explicitly disabled, use standard path
        if (num_queries > 16 || !this->worker->use_batching()) {
            uint64_t job_id = this->worker->submit(
                [&, num_queries, limit, sp, queries_data](raft_handle_wrapper_t& handle) -> std::any {
                    return this->search_float_internal(handle, queries_data, num_queries, query_dimension, limit, sp);
                }
            );
            auto result_wait = this->worker->wait(job_id).get();
            if (result_wait.error) std::rethrow_exception(result_wait.error);
            return std::any_cast<search_result_t>(result_wait.result);
        }

        // Dynamic batching for small query counts
        struct search_req_t {
            const float* data;
            uint64_t n;
        };

        std::string batch_key = "ivf_flat_sf_" + std::to_string((uintptr_t)this) + "_" + std::to_string(limit) + "_" + std::to_string(sp.n_probes);
        
        auto exec_fn = [this, limit, sp](cuvs_worker_t::raft_handle& handle, const std::vector<std::any>& reqs, const std::vector<std::function<void(std::any)>>& setters) {
            uint64_t total_queries = 0;
            for (const auto& r : reqs) total_queries += std::any_cast<search_req_t>(r).n;

            std::vector<float> aggregated_queries(total_queries * this->dimension);
            uint64_t offset = 0;
            for (const auto& r : reqs) {
                auto req = std::any_cast<search_req_t>(r);
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

        auto future = this->worker->template submit_batched<search_result_t>(batch_key, search_req_t{queries_data, num_queries}, exec_fn);
        return future.get();
    }

    /**
     * @brief Internal search_float implementation (no worker submission)
     */
    search_result_t search_float_internal(raft_handle_wrapper_t& handle, const float* queries_data, uint64_t num_queries, uint32_t query_dimension, 
                        uint32_t limit, const ivf_flat_search_params_t& sp) {
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        // 1. Quantize/Convert float queries to T on device
        auto queries_device_float = raft::make_device_matrix<float, int64_t>(*res, num_queries, this->dimension);
        raft::copy(*res, queries_device_float.view(), raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, this->dimension));
        
        auto queries_device_target = raft::make_device_matrix<T, int64_t>(*res, num_queries, this->dimension);
        if constexpr (sizeof(T) == 1) {
            if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
            this->quantizer_.template transform<T>(*res, queries_device_float.view(), queries_device_target.data_handle(), true);
            raft::resource::sync_stream(*res);
        } else {
            raft::copy(*res, queries_device_target.view(), queries_device_float.view());
        }

        // 2. Perform search
        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::ivf_flat::search_params search_params;
        search_params.n_probes = sp.n_probes;

        const ivf_flat_index* local_index = index_.get();
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

        if (is_snmg_handle(res) && mg_index_) {
            auto queries_host_target = raft::make_host_matrix<T, int64_t>(num_queries, this->dimension);
            raft::copy(*res, queries_host_target.view(), queries_device_target.view());
            raft::resource::sync_stream(*res);

            auto neighbors_host_view = raft::make_host_matrix_view<int64_t, int64_t>(
                search_res.neighbors.data(), (int64_t)num_queries, (int64_t)limit);
            auto distances_host_view = raft::make_host_matrix_view<float, int64_t>(
                search_res.distances.data(), (int64_t)num_queries, (int64_t)limit);

            cuvs::neighbors::mg_search_params<cuvs::neighbors::ivf_flat::search_params> mg_search_params(search_params);
            cuvs::neighbors::ivf_flat::search(*res, *mg_index_, mg_search_params,
                                                queries_host_target.view(), neighbors_host_view, distances_host_view);
        } else if (local_index) {
            auto neighbors_device = raft::make_device_matrix<int64_t, int64_t, raft::layout_c_contiguous>(
                *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
            auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

            cuvs::neighbors::ivf_flat::search(*res, search_params, *local_index,
                                                raft::make_const_mdspan(queries_device_target.view()), 
                                                neighbors_device.view(), distances_device.view());

            RAFT_CUDA_TRY(cudaMemcpyAsync(search_res.neighbors.data(), neighbors_device.data_handle(),
                                        search_res.neighbors.size() * sizeof(int64_t), cudaMemcpyDeviceToHost,
                                        raft::resource::get_cuda_stream(*res)));
            RAFT_CUDA_TRY(cudaMemcpyAsync(search_res.distances.data(), distances_device.data_handle(),
                                        search_res.distances.size() * sizeof(float), cudaMemcpyDeviceToHost,
                                        raft::resource::get_cuda_stream(*res)));
        } else {
            throw std::runtime_error("Index not loaded or failed to find local index shard for current device.");
        }

        raft::resource::sync_stream(*res);

        for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
            if (search_res.neighbors[i] == std::numeric_limits<int64_t>::max() || 
                search_res.neighbors[i] == 4294967295LL || search_res.neighbors[i] < 0) {
                search_res.neighbors[i] = -1;
            }
        }
        return search_res;
    }

    std::vector<T> get_centers() {
        if (!this->is_loaded_ || (!index_ && !mg_index_)) return {};

        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(this->mutex_);
                auto res = handle.get_raft_resources();
                
                const ivf_flat_index* local_index = nullptr;
                if (is_snmg_handle(res)) {
                    for (const auto& iface : mg_index_->ann_interfaces_) {
                        if (iface.index_.has_value()) { local_index = &iface.index_.value(); break; }
                    }
                } else {
                    local_index = index_.get();
                }

                if (!local_index) return std::vector<T>{};

                auto centers_view = local_index->centers();
                size_t n_centers = centers_view.extent(0);
                size_t dim = centers_view.extent(1);
                std::vector<T> host_centers(n_centers * dim);

                RAFT_CUDA_TRY(cudaMemcpyAsync(host_centers.data(), centers_view.data_handle(),
                                         host_centers.size() * sizeof(T), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*res)));

                raft::resource::sync_stream(*res);
                return host_centers;
            }
        );

        cuvs_task_result_t result = this->worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<std::vector<T>>(result.result);
    }

    uint32_t get_n_list() {
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
        if (!this->is_loaded_) return this->build_params.n_lists;
        
        if (index_) return static_cast<uint32_t>(index_->n_lists());
        if (mg_index_) {
            for (const auto& iface : mg_index_->ann_interfaces_) {
                if (iface.index_.has_value()) return static_cast<uint32_t>(iface.index_.value().n_lists());
            }
        }
        return this->build_params.n_lists;
    }
};

} // namespace matrixone
