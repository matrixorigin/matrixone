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
#include "cuvs_worker.hpp"
#include "cuvs_types.h"
#include "quantize.hpp"

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
    std::vector<int64_t> neighbors; // Indices of nearest neighbors
    std::vector<float> distances;   // Distances to nearest neighbors
};

/**
 * @brief gpu_cagra_t implements a CAGRA index that can run on a single GPU or sharded across multiple GPUs.
 * It automatically chooses between single-GPU and multi-GPU (SNMG) cuVS APIs based on the RAFT handle resources.
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
                  cuvs::distance::DistanceType m, const cagra_build_params_t& bp,
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
        if (dataset_data) {
            std::copy(dataset_data, dataset_data + (this->count * this->dimension), this->flattened_host_dataset.begin());
        }
    }

    // Constructor for chunked input (pre-allocates)
    gpu_cagra_t(uint64_t total_count, uint32_t dimension, cuvs::distance::DistanceType m, 
                    const cagra_build_params_t& bp, const std::vector<int>& devices, 
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
    gpu_cagra_t(const std::string& filename, uint32_t dimension, cuvs::distance::DistanceType m, 
                    const cagra_build_params_t& bp, const std::vector<int>& devices, uint32_t nthread, distribution_mode_t mode) {
        
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

    // Private constructor for creating from an existing cuVS index (used by merge)
    gpu_cagra_t(std::unique_ptr<cagra_index> idx, 
                  uint32_t dim, cuvs::distance::DistanceType m, uint32_t nthread, const std::vector<int>& devices)
        : index_(std::move(idx)) {
        
        this->metric = m;
        this->dimension = dim;
        this->devices_ = devices;

        // Merge result is currently a single-GPU index.
        this->worker = std::make_unique<cuvs_worker_t>(nthread, this->devices_, false);
        
        this->count = static_cast<uint32_t>(index_->size());
        this->build_params.graph_degree = static_cast<size_t>(index_->graph_degree());
        this->build_params.intermediate_graph_degree = this->build_params.graph_degree * 2; // Best guess
        this->dist_mode = DistributionMode_SINGLE_GPU;
        this->current_offset_ = this->count;
        this->is_loaded_ = true;
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

        if (this->filename_.empty() && !index_ && this->current_offset_ > 0 && this->current_offset_ < this->count) {
            this->count = static_cast<uint32_t>(this->current_offset_);
            this->flattened_host_dataset.resize(this->count * this->dimension);
        }

        uint64_t job_id = this->worker->submit_main(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                this->build_internal(handle);
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
     * @brief Internal build implementation (no worker submission)
     */
    void build_internal(raft_handle_wrapper_t& handle) {
        auto res = handle.get_raft_resources();
        bool is_mg = is_snmg_handle(res);

        if (!this->filename_.empty()) {
            if (is_mg) {
                mg_index_ = std::make_unique<mg_index>(
                    cuvs::neighbors::cagra::deserialize<T, uint32_t>(*res, this->filename_));
                this->count = 0;
                for (const auto& iface : mg_index_->ann_interfaces_) {
                    if (iface.index_.has_value()) this->count += static_cast<uint32_t>(iface.index_.value().size());
                }
                if (!mg_index_->ann_interfaces_.empty() && mg_index_->ann_interfaces_[0].index_.has_value()) {
                    this->build_params.graph_degree = static_cast<size_t>(mg_index_->ann_interfaces_[0].index_.value().graph_degree());
                }
            } else {
                index_ = std::make_unique<cagra_index>(*res);
                cuvs::neighbors::cagra::deserialize(*res, this->filename_, index_.get());
                this->count = static_cast<uint32_t>(index_->size());
                this->build_params.graph_degree = static_cast<size_t>(index_->graph_degree());
            }
            raft::resource::sync_stream(*res);
        } else if (!this->flattened_host_dataset.empty()) {
            if (is_mg) {
                auto dataset_host_view = raft::make_host_matrix_view<const T, int64_t>(
                    this->flattened_host_dataset.data(), (int64_t)this->count, (int64_t)this->dimension);

                cuvs::neighbors::cagra::index_params index_params;
                index_params.metric = this->metric;
                index_params.intermediate_graph_degree = this->build_params.intermediate_graph_degree;
                index_params.graph_degree = this->build_params.graph_degree;

                cuvs::neighbors::mg_index_params<cuvs::neighbors::cagra::index_params> mg_params(index_params);
                if (this->dist_mode == DistributionMode_REPLICATED) {
                    mg_params.mode = cuvs::neighbors::distribution_mode::REPLICATED;
                } else {
                    mg_params.mode = cuvs::neighbors::distribution_mode::SHARDED;
                }

                mg_index_ = std::make_unique<mg_index>(
                    cuvs::neighbors::cagra::build(*res, mg_params, dataset_host_view));
            } else {
                auto dataset_device = new auto(raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(this->count), static_cast<int64_t>(this->dimension)));
                
                this->dataset_device_ptr_ = std::shared_ptr<void>(dataset_device, [](void* ptr) {
                    delete static_cast<raft::device_matrix<T, int64_t, raft::layout_c_contiguous>*>(ptr);
                });

                RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device->data_handle(), this->flattened_host_dataset.data(),
                                            this->flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                            raft::resource::get_cuda_stream(*res)));

                cuvs::neighbors::cagra::index_params index_params;
                index_params.metric = this->metric;
                index_params.intermediate_graph_degree = this->build_params.intermediate_graph_degree;
                index_params.graph_degree = this->build_params.graph_degree;
                index_params.attach_dataset_on_build = this->build_params.attach_dataset_on_build;

                index_ = std::make_unique<cagra_index>(
                    cuvs::neighbors::cagra::build(*res, index_params, raft::make_const_mdspan(dataset_device->view())));
            }
            raft::resource::sync_stream(*res);
        }
    }

    /**
     * @brief Extends the existing index with additional vectors.
     * @param additional_data Pointer to additional vectors on host.
     * @param num_vectors Number of vectors to add.
     */
    void extend(const T* additional_data, uint64_t num_vectors) {
        if (!this->is_loaded_ || !index_) {
            uint64_t old_size = this->flattened_host_dataset.size();
            this->flattened_host_dataset.resize(old_size + num_vectors * this->dimension);
            std::copy(additional_data, additional_data + num_vectors * this->dimension, this->flattened_host_dataset.begin() + old_size);
            this->count += static_cast<uint32_t>(num_vectors);
            this->current_offset_ += static_cast<uint32_t>(num_vectors);
            return;
        }

        if constexpr (std::is_same_v<T, half>) {
             throw std::runtime_error("CAGRA single-GPU extend is not supported for float16 (half) by cuVS.");
        } else {
            if (num_vectors == 0) return;

            std::unique_lock<std::shared_mutex> lock(this->mutex_);

            uint64_t job_id = this->worker->submit_main(
                [&, additional_data, num_vectors](raft_handle_wrapper_t& handle) -> std::any {
                    auto res = handle.get_raft_resources();
                    
                    auto additional_dataset_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                        *res, static_cast<int64_t>(num_vectors), static_cast<int64_t>(this->dimension));
                    
                    RAFT_CUDA_TRY(cudaMemcpyAsync(additional_dataset_device.data_handle(), additional_data,
                                            num_vectors * this->dimension * sizeof(T), cudaMemcpyHostToDevice,
                                            raft::resource::get_cuda_stream(*res)));

                    cuvs::neighbors::cagra::extend_params params;
                    cuvs::neighbors::cagra::extend(*res, params, raft::make_const_mdspan(additional_dataset_device.view()), *index_);

                    raft::resource::sync_stream(*res);
                    return std::any();
                }
            );

            cuvs_task_result_t result = this->worker->wait(job_id).get();
            if (result.error) std::rethrow_exception(result.error);

            this->count = static_cast<uint32_t>(index_->size());
            this->current_offset_ = this->count;
        }
    }

    /**
     * @brief Merges multiple single-GPU CAGRA indices into a single index.
     * @param indices Vector of pointers to indices to merge.
     * @param nthread Number of worker threads for the merged index.
     * @param devices GPU devices to use for the merged index.
     * @return A new merged CAGRA index.
     */
    static std::unique_ptr<gpu_cagra_t<T>> merge(const std::vector<gpu_cagra_t<T>*>& indices, uint32_t nthread, const std::vector<int>& devices) {
        if (indices.empty()) throw std::invalid_argument("indices empty");
        uint32_t dim = indices[0]->dimension;
        cuvs::distance::DistanceType m = indices[0]->metric;

        cuvs_worker_t transient_worker(1, devices, false);
        transient_worker.start();

        uint64_t job_id = transient_worker.submit_main(
            [&indices](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                
                std::vector<cagra_index*> cagra_indices;
                for (auto* idx : indices) {
                    if (!idx->is_loaded_ || !idx->index_) {
                        throw std::runtime_error("One of the indices to merge is not loaded or is a multi-GPU index (merge only supports single-GPU indices).");
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
            dim, m, nthread, devices
        );
        new_idx->is_loaded_ = true;
        return new_idx;
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
                    cuvs::neighbors::cagra::serialize(*res, *mg_index_, filename);
                } else {
                    cuvs::neighbors::cagra::serialize(*res, filename, *index_);
                }
                raft::resource::sync_stream(*res);
                return std::any();
            }
        );

        cuvs_task_result_t result = this->worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
    }

    /**
     * @brief Performs CAGRA search for given queries.
     * @param queries_data Pointer to flattened query vectors on host.
     * @param num_queries Number of query vectors.
     * @param query_dimension Dimension of query vectors.
     * @param limit Number of nearest neighbors to find.
     * @param sp CAGRA search parameters.
     * @return Search results.
     */
    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, 
                        uint32_t limit, const cagra_search_params_t& sp) {
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

        return this->search_batch_internal(queries_data, num_queries, limit, sp);
    }

    /**
     * @brief Internal batch search implementation
     */
    search_result_t search_batch_internal(const T* queries_data, uint64_t num_queries, uint32_t limit, const cagra_search_params_t& sp) {
        // Dynamic batching for small query counts
        struct search_req_t {
            const T* data;
            uint64_t n;
        };

        std::string batch_key = "cagra_s_" + std::to_string((uintptr_t)this) + "_" + std::to_string(limit) + "_" + std::to_string(sp.itopk_size);
        
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
    search_result_t search_internal(raft_handle_wrapper_t& handle, const T* queries_data, uint64_t num_queries, uint32_t limit, const cagra_search_params_t& sp) {
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        // Scope for temporary device resources
        {
            cuvs::neighbors::cagra::search_params search_params;
            search_params.itopk_size = sp.itopk_size;
            search_params.search_width = sp.search_width;

            const cagra_index* local_index = index_.get();
            if (!local_index && mg_index_) {
                int current_device;
                cudaGetDevice(&current_device);
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

                cuvs::neighbors::mg_search_params<cuvs::neighbors::cagra::search_params> mg_search_params(search_params);
                cuvs::neighbors::cagra::search(*res, *mg_index_, mg_search_params,
                                                    queries_host_view, neighbors_host_view, distances_host_view);
            } else if (local_index) {
                auto queries_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(this->dimension));
                raft::copy(*res, queries_device.view(), raft::make_host_matrix_view<const T, int64_t>(queries_data, num_queries, this->dimension));

                auto neighbors_device = raft::make_device_matrix<int64_t, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
                auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

                cuvs::neighbors::cagra::search(*res, search_params, *local_index,
                                                raft::make_const_mdspan(queries_device.view()), 
                                                neighbors_device.view(), distances_device.view());

                raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device.view());
                raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device.view());
            } else {
                throw std::runtime_error("Index not loaded or failed to find local index shard for current device.");
            }

            raft::resource::sync_stream(*res);
        } // <- Temporary device resources are destroyed HERE while lock is held

        for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
            if (search_res.neighbors[i] == std::numeric_limits<int64_t>::max() || 
                search_res.neighbors[i] == 4294967295LL || search_res.neighbors[i] < 0) {
                search_res.neighbors[i] = -1; 
            }
        }
        return search_res;
    }

    /**
     * @brief Performs CAGRA search for given float32 queries, with on-the-fly quantization if needed.
     */
    search_result_t search_float(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, 
                        uint32_t limit, const cagra_search_params_t& sp) {
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

        return this->search_float_batch_internal(queries_data, num_queries, limit, sp);
    }

    /**
     * @brief Internal batch search implementation for float32 queries
     */
    search_result_t search_float_batch_internal(const float* queries_data, uint64_t num_queries, uint32_t limit, const cagra_search_params_t& sp) {
        // Dynamic batching for small query counts
        struct search_req_t {
            const float* data;
            uint64_t n;
        };

        std::string batch_key = "cagra_sf_" + std::to_string((uintptr_t)this) + "_" + std::to_string(limit) + "_" + std::to_string(sp.itopk_size);
        
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
                        uint32_t limit, const cagra_search_params_t& sp) {
        std::shared_lock<std::shared_mutex> lock(this->mutex_);
        auto res = handle.get_raft_resources();

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        // Scope for temporary device resources
        {
            // 1. Quantize/Convert float queries to T on device
            auto queries_device_float = raft::make_device_matrix<float, int64_t>(*res, num_queries, this->dimension);
            raft::copy(*res, queries_device_float.view(), raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, this->dimension));
            
            auto queries_device_target = raft::make_device_matrix<T, int64_t>(*res, num_queries, this->dimension);
            if constexpr (sizeof(T) == 1) {
                if (!this->quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
                this->quantizer_.template transform<T>(*res, queries_device_float.view(), queries_device_target.data_handle(), true);
            } else {
                raft::copy(*res, queries_device_target.view(), queries_device_float.view());
            }
            raft::resource::sync_stream(*res);

            // 2. Perform search
            cuvs::neighbors::cagra::search_params search_params;
            search_params.itopk_size = sp.itopk_size;
            search_params.search_width = sp.search_width;

            const cagra_index* local_index = index_.get();
            if (!local_index && mg_index_) {
                int current_device;
                cudaGetDevice(&current_device);
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

                cuvs::neighbors::mg_search_params<cuvs::neighbors::cagra::search_params> mg_search_params(search_params);
                cuvs::neighbors::cagra::search(*res, *mg_index_, mg_search_params,
                                                    queries_host_target.view(), 
                                                    neighbors_host_view, distances_host_view);
            } else if (local_index) {
                auto neighbors_device = raft::make_device_matrix<int64_t, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
                auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                    *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

                cuvs::neighbors::cagra::search(*res, search_params, *local_index,
                                                raft::make_const_mdspan(queries_device_target.view()), 
                                                neighbors_device.view(), distances_device.view());

                raft::copy(*res, raft::make_host_matrix_view<int64_t, int64_t>(search_res.neighbors.data(), num_queries, limit), neighbors_device.view());
                raft::copy(*res, raft::make_host_matrix_view<float, int64_t>(search_res.distances.data(), num_queries, limit), distances_device.view());
            } else {
                throw std::runtime_error("Index not loaded or failed to find local index shard for current device.");
            }

            raft::resource::sync_stream(*res);
        } // <- ALL temporary device matrices are destroyed HERE while lock is held

        for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
            if (search_res.neighbors[i] == std::numeric_limits<int64_t>::max() || 
                search_res.neighbors[i] == 4294967295LL || search_res.neighbors[i] < 0) {
                search_res.neighbors[i] = -1; 
            }
        }
        return search_res;
    }

    std::string info() const override {
        std::string json = gpu_index_base_t<T, cagra_build_params_t>::info();
        json += ", \"type\": \"CAGRA\", \"cagra\": {";
        if (index_) {
            json += "\"mode\": \"Single-GPU\", \"size\": " + std::to_string(index_->size()) + 
                    ", \"graph_degree\": " + std::to_string(index_->graph_degree());
        } else if (mg_index_) {
            json += "\"mode\": \"Multi-GPU\", \"shards\": [";
            for (size_t i = 0; i < mg_index_->ann_interfaces_.size(); ++i) {
                const auto& iface = mg_index_->ann_interfaces_[i];
                json += "{\"device\": " + std::to_string(this->devices_[i]);
                if (iface.index_.has_value()) {
                    json += ", \"size\": " + std::to_string(iface.index_.value().size()) + 
                            ", \"graph_degree\": " + std::to_string(iface.index_.value().graph_degree());
                } else {
                    json += ", \"status\": \"Not loaded\"";
                }
                json += "}" + std::string(i == mg_index_->ann_interfaces_.size() - 1 ? "" : ", ");
            }
            json += "]";
        } else {
            json += "\"built\": false";
        }
        json += "}}";
        return json;
    }

    void destroy() override {
        if (this->worker) {
            this->worker->stop();
        }
        std::unique_lock<std::shared_mutex> lock(this->mutex_);
        index_.reset();
        mg_index_.reset();
        this->quantizer_.reset();
        this->dataset_device_ptr_.reset();
    }
};

} // namespace matrixone
