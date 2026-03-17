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

#include "cuvs_worker.hpp" // For cuvs_worker_t and raft_handle_wrapper_t
#include "cuvs_types.h"    // For distance_type_t, ivf_pq_build_params_t, etc.
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
#include <cuvs/neighbors/ivf_pq.hpp>     // IVF-PQ include
#include "quantize.hpp"
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
 * @brief gpu_ivf_pq_t implements an IVF-PQ index that can run on a single GPU or sharded across multiple GPUs.
 * It automatically chooses between single-GPU and multi-GPU (SNMG) cuVS APIs based on the RAFT handle resources.
 */
template <typename T>
class gpu_ivf_pq_t {
public:
    using ivf_pq_index = cuvs::neighbors::ivf_pq::index<int64_t>;
    using mg_index = cuvs::neighbors::mg_index<ivf_pq_index, T, int64_t>;
    using search_result_t = ivf_pq_search_result_t;

    std::vector<T> flattened_host_dataset;
    std::vector<int> devices_;
    std::string filename_;
    
    // Internal index storage
    std::unique_ptr<ivf_pq_index> index_;
    std::unique_ptr<mg_index> mg_index_;

    cuvs::distance::DistanceType metric;
    uint32_t dimension;
    uint32_t count;
    ivf_pq_build_params_t build_params;
    distribution_mode_t dist_mode;

    std::unique_ptr<cuvs_worker_t> worker;
    std::shared_mutex mutex_;
    bool is_loaded_ = false;

    ~gpu_ivf_pq_t() {
        destroy();
    }

    // Unified Constructor for building from dataset
    gpu_ivf_pq_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                    cuvs::distance::DistanceType m, const ivf_pq_build_params_t& bp, 
                    const std::vector<int>& devices, uint32_t nthread, distribution_mode_t mode)
        : dimension(dimension), count(static_cast<uint32_t>(count_vectors)), metric(m), 
          build_params(bp), dist_mode(mode), devices_(devices), current_offset_(static_cast<uint32_t>(count_vectors)) {
        
        bool force_mg = (mode == DistributionMode_SHARDED || mode == DistributionMode_REPLICATED);
        worker = std::make_unique<cuvs_worker_t>(nthread, devices_, force_mg || (devices_.size() > 1));

        flattened_host_dataset.resize(count * dimension);
        std::copy(dataset_data, dataset_data + (count * dimension), flattened_host_dataset.begin());
    }

    // Constructor for chunked input (pre-allocates)
    gpu_ivf_pq_t(uint64_t total_count, uint32_t dimension, cuvs::distance::DistanceType m, 
                    const ivf_pq_build_params_t& bp, const std::vector<int>& devices, 
                    uint32_t nthread, distribution_mode_t mode)
        : dimension(dimension), count(static_cast<uint32_t>(total_count)), metric(m), 
          build_params(bp), dist_mode(mode), devices_(devices), current_offset_(0) {
        
        bool force_mg = (mode == DistributionMode_SHARDED || mode == DistributionMode_REPLICATED);
        worker = std::make_unique<cuvs_worker_t>(nthread, devices_, force_mg || (devices_.size() > 1));

        flattened_host_dataset.resize(count * dimension);
    }

    // Constructor for building from MODF datafile
    gpu_ivf_pq_t(const std::string& data_filename, cuvs::distance::DistanceType m, 
                    const ivf_pq_build_params_t& bp, const std::vector<int>& devices, 
                    uint32_t nthread, distribution_mode_t mode)
        : metric(m), build_params(bp), dist_mode(mode), devices_(devices) {
        
        bool force_mg = (mode == DistributionMode_SHARDED || mode == DistributionMode_REPLICATED);
        worker = std::make_unique<cuvs_worker_t>(nthread, devices_, force_mg || (devices_.size() > 1));

        uint64_t file_count = 0;
        uint64_t file_dim = 0;
        load_host_matrix<T>(data_filename, flattened_host_dataset, file_count, file_dim);
        
        count = static_cast<uint32_t>(file_count);
        dimension = static_cast<uint32_t>(file_dim);
        current_offset_ = count;
    }

    // Unified Constructor for loading from file
    gpu_ivf_pq_t(const std::string& filename, uint32_t dimension, cuvs::distance::DistanceType m, 
                    const ivf_pq_build_params_t& bp, const std::vector<int>& devices, uint32_t nthread, distribution_mode_t mode)
        : filename_(filename), dimension(dimension), metric(m), count(0), 
          build_params(bp), dist_mode(mode), devices_(devices), current_offset_(0) {
        
        bool force_mg = (mode == DistributionMode_SHARDED || mode == DistributionMode_REPLICATED);
        worker = std::make_unique<cuvs_worker_t>(nthread, devices_, force_mg || (devices_.size() > 1));
    }

    /**
     * @brief Starts the worker and initializes resources.
     */
    void start() {
        auto init_fn = [](raft_handle_wrapper_t&) -> std::any {
            return std::any();
        };

        auto stop_fn = [&](raft_handle_wrapper_t&) -> std::any {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            index_.reset();
            mg_index_.reset();
            quantizer_.reset();
            return std::any();
        };

        worker->start(init_fn, stop_fn);
    }

    /**
     * @brief Loads the index from file or builds it from the dataset.
     */
    void build() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (is_loaded_) return;

        if (filename_.empty() && current_offset_ > 0 && current_offset_ < count) {
            count = static_cast<uint32_t>(current_offset_);
            flattened_host_dataset.resize(count * dimension);
        }

        uint64_t job_id = worker->submit(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                bool is_mg = is_snmg_handle(res);

                if (!filename_.empty()) {
                    if (is_mg) {
                        mg_index_ = std::make_unique<mg_index>(
                            cuvs::neighbors::ivf_pq::deserialize<T, int64_t>(*res, filename_));
                        // Update metadata
                        count = 0;
                        for (const auto& iface : mg_index_->ann_interfaces_) {
                            if (iface.index_.has_value()) count += static_cast<uint32_t>(iface.index_.value().size());
                        }
                        if (!mg_index_->ann_interfaces_.empty() && mg_index_->ann_interfaces_[0].index_.has_value()) {
                            build_params.n_lists = static_cast<uint32_t>(mg_index_->ann_interfaces_[0].index_.value().n_lists());
                            build_params.m = static_cast<uint32_t>(mg_index_->ann_interfaces_[0].index_.value().pq_dim());
                            build_params.bits_per_code = static_cast<uint32_t>(mg_index_->ann_interfaces_[0].index_.value().pq_bits());
                        }
                    } else {
                        index_ = std::make_unique<ivf_pq_index>(*res);
                        cuvs::neighbors::ivf_pq::deserialize(*res, filename_, index_.get());
                        count = static_cast<uint32_t>(index_->size());
                        build_params.n_lists = static_cast<uint32_t>(index_->n_lists());
                        build_params.m = static_cast<uint32_t>(index_->pq_dim());
                        build_params.bits_per_code = static_cast<uint32_t>(index_->pq_bits());
                    }
                    raft::resource::sync_stream(*res);
                } else if (!flattened_host_dataset.empty()) {
                    if (count < build_params.n_lists) {
                        throw std::runtime_error("Dataset too small: count (" + std::to_string(count) + 
                                                ") must be >= n_list (" + std::to_string(build_params.n_lists) + 
                                                ") to build IVF index.");
                    }

                    cuvs::neighbors::ivf_pq::index_params index_params;
                    index_params.metric = metric;
                    index_params.n_lists = build_params.n_lists;
                    index_params.pq_dim = build_params.m;
                    index_params.pq_bits = build_params.bits_per_code;
                    index_params.add_data_on_build = build_params.add_data_on_build;
                    index_params.kmeans_trainset_fraction = build_params.kmeans_trainset_fraction;

                    if (is_mg) {
                        auto dataset_host_view = raft::make_host_matrix_view<const T, int64_t>(
                            flattened_host_dataset.data(), (int64_t)count, (int64_t)dimension);

                        cuvs::neighbors::mg_index_params<cuvs::neighbors::ivf_pq::index_params> mg_params(index_params);
                        if (dist_mode == DistributionMode_REPLICATED) {
                            mg_params.mode = cuvs::neighbors::distribution_mode::REPLICATED;
                        } else {
                            mg_params.mode = cuvs::neighbors::distribution_mode::SHARDED;
                        }

                        mg_index_ = std::make_unique<mg_index>(
                            cuvs::neighbors::ivf_pq::build(*res, mg_params, dataset_host_view));
                    } else {
                        auto dataset_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                            *res, static_cast<int64_t>(count), static_cast<int64_t>(dimension));
                        
                        RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device.data_handle(), flattened_host_dataset.data(),
                                                 flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                                 raft::resource::get_cuda_stream(*res)));

                        index_ = std::make_unique<ivf_pq_index>(
                            cuvs::neighbors::ivf_pq::build(*res, index_params, raft::make_const_mdspan(dataset_device.view())));
                    }
                    raft::resource::sync_stream(*res);
                }
                return std::any();
            }
        );

        auto result_wait = worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        is_loaded_ = true;
        // Clear host dataset after building to save memory (IVF-PQ stores its own copy on device)
        if (filename_.empty()) {
            flattened_host_dataset.clear();
            flattened_host_dataset.shrink_to_fit();
        }
    }

    /**
     * @brief Serializes the index to a file.
     * @param filename Path to the output file.
     */
    void save(const std::string& filename) {
        if (!is_loaded_ || (!index_ && !mg_index_)) throw std::runtime_error("index not loaded");

        uint64_t job_id = worker->submit(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                auto res = handle.get_raft_resources();
                if (is_snmg_handle(res)) {
                    cuvs::neighbors::ivf_pq::serialize(*res, *mg_index_, filename);
                } else {
                    cuvs::neighbors::ivf_pq::serialize(*res, filename, *index_);
                }
                raft::resource::sync_stream(*res);
                return std::any();
            }
        );

        cuvs_task_result_t result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
    }

    /**
     * @brief Performs IVF-PQ search for given queries.
     * @param queries_data Pointer to flattened query vectors on host.
     * @param num_queries Number of query vectors.
     * @param query_dimension Dimension of query vectors.
     * @param limit Number of nearest neighbors to find.
     * @param sp IVF-PQ search parameters.
     * @return Search results.
     */
    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, 
                        uint32_t limit, const ivf_pq_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || dimension == 0) return search_result_t{};
        if (query_dimension != dimension) throw std::runtime_error("dimension mismatch");
        if (!is_loaded_ || (!index_ && !mg_index_)) return search_result_t{};

        // For large batches, skip dynamic batching
        if (num_queries > 16) {
            uint64_t job_id = worker->submit(
                [&, num_queries, limit, sp, queries_data](raft_handle_wrapper_t& handle) -> std::any {
                    return this->search_internal(handle, queries_data, num_queries, query_dimension, limit, sp);
                }
            );
            auto result_wait = worker->wait(job_id).get();
            if (result_wait.error) std::rethrow_exception(result_wait.error);
            return std::any_cast<search_result_t>(result_wait.result);
        }

        // Dynamic batching for small query counts
        struct search_req_t {
            const T* data;
            uint64_t n;
        };

        std::string batch_key = "ivf_pq_s_" + std::to_string((uintptr_t)this) + "_" + std::to_string(limit) + "_" + std::to_string(sp.n_probes);
        
        auto exec_fn = [this, limit, sp](cuvs_worker_t::raft_handle& handle, const std::vector<std::any>& reqs, const std::vector<std::function<void(std::any)>>& setters) {
            uint64_t total_queries = 0;
            for (const auto& r : reqs) total_queries += std::any_cast<search_req_t>(r).n;

            std::vector<T> aggregated_queries(total_queries * dimension);
            uint64_t offset = 0;
            for (const auto& r : reqs) {
                auto req = std::any_cast<search_req_t>(r);
                std::copy(req.data, req.data + (req.n * dimension), aggregated_queries.begin() + (offset * dimension));
                offset += req.n;
            }

            auto results = this->search_internal(handle, aggregated_queries.data(), total_queries, dimension, limit, sp);

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

        auto future = worker->submit_batched<search_result_t>(batch_key, search_req_t{queries_data, num_queries}, exec_fn);
        return future.get();
    }

    /**
     * @brief Internal search implementation (no worker submission)
     */
    search_result_t search_internal(raft_handle_wrapper_t& handle, const T* queries_data, uint64_t num_queries, uint32_t query_dimension, 
                        uint32_t limit, const ivf_pq_search_params_t& sp) {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto res = handle.get_raft_resources();

        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::ivf_pq::search_params search_params;
        search_params.n_probes = sp.n_probes;

        const ivf_pq_index* local_index = index_.get();
        if (!local_index && mg_index_) {
            int current_device;
            RAFT_CUDA_TRY(cudaGetDevice(&current_device));
            for (size_t i = 0; i < devices_.size(); ++i) {
                if (devices_[i] == current_device && i < mg_index_->ann_interfaces_.size()) {
                    if (mg_index_->ann_interfaces_[i].index_.has_value()) {
                        local_index = &mg_index_->ann_interfaces_[i].index_.value();
                        break;
                    }
                }
            }
        }

        if (is_snmg_handle(res) && mg_index_) {
            auto queries_host_view = raft::make_host_matrix_view<const T, int64_t>(
                queries_data, (int64_t)num_queries, (int64_t)dimension);
            auto neighbors_host_view = raft::make_host_matrix_view<int64_t, int64_t>(
                search_res.neighbors.data(), (int64_t)num_queries, (int64_t)limit);
            auto distances_host_view = raft::make_host_matrix_view<float, int64_t>(
                search_res.distances.data(), (int64_t)num_queries, (int64_t)limit);

            cuvs::neighbors::mg_search_params<cuvs::neighbors::ivf_pq::search_params> mg_search_params(search_params);
            cuvs::neighbors::ivf_pq::search(*res, *mg_index_, mg_search_params,
                                                queries_host_view, neighbors_host_view, distances_host_view);
        } else if (local_index) {
            auto queries_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(dimension));
            RAFT_CUDA_TRY(cudaMemcpyAsync(queries_device.data_handle(), queries_data,
                                        num_queries * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                        raft::resource::get_cuda_stream(*res)));

            auto neighbors_device = raft::make_device_matrix<int64_t, int64_t, raft::layout_c_contiguous>(
                *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
            auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

            cuvs::neighbors::ivf_pq::search(*res, search_params, *local_index,
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
     * @brief Performs IVF-PQ search for given float32 queries, with on-the-fly quantization if needed.
     */
    search_result_t search_float(const float* queries_data, uint64_t num_queries, uint32_t query_dimension, 
                        uint32_t limit, const ivf_pq_search_params_t& sp) {
        if constexpr (std::is_same_v<T, float>) {
            return search(queries_data, num_queries, query_dimension, limit, sp);
        }

        if (!queries_data || num_queries == 0 || dimension == 0) return search_result_t{};
        if (query_dimension != dimension) throw std::runtime_error("dimension mismatch");
        if (!is_loaded_ || (!index_ && !mg_index_)) return search_result_t{};

        // For large batches, skip dynamic batching
        if (num_queries > 16) {
            uint64_t job_id = worker->submit(
                [&, num_queries, limit, sp, queries_data](raft_handle_wrapper_t& handle) -> std::any {
                    return this->search_float_internal(handle, queries_data, num_queries, query_dimension, limit, sp);
                }
            );
            auto result_wait = worker->wait(job_id).get();
            if (result_wait.error) std::rethrow_exception(result_wait.error);
            return std::any_cast<search_result_t>(result_wait.result);
        }

        // Dynamic batching for small query counts
        struct search_req_t {
            const float* data;
            uint64_t n;
        };

        std::string batch_key = "ivf_pq_sf_" + std::to_string((uintptr_t)this) + "_" + std::to_string(limit) + "_" + std::to_string(sp.n_probes);
        
        auto exec_fn = [this, limit, sp](cuvs_worker_t::raft_handle& handle, const std::vector<std::any>& reqs, const std::vector<std::function<void(std::any)>>& setters) {
            uint64_t total_queries = 0;
            for (const auto& r : reqs) total_queries += std::any_cast<search_req_t>(r).n;

            std::vector<float> aggregated_queries(total_queries * dimension);
            uint64_t offset = 0;
            for (const auto& r : reqs) {
                auto req = std::any_cast<search_req_t>(r);
                std::copy(req.data, req.data + (req.n * dimension), aggregated_queries.begin() + (offset * dimension));
                offset += req.n;
            }

            auto results = this->search_float_internal(handle, aggregated_queries.data(), total_queries, dimension, limit, sp);

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

        auto future = worker->submit_batched<search_result_t>(batch_key, search_req_t{queries_data, num_queries}, exec_fn);
        return future.get();
    }

    /**
     * @brief Internal search_float implementation (no worker submission)
     */
    search_result_t search_float_internal(raft_handle_wrapper_t& handle, const float* queries_data, uint64_t num_queries, uint32_t query_dimension, 
                        uint32_t limit, const ivf_pq_search_params_t& sp) {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        auto res = handle.get_raft_resources();

        // 1. Quantize/Convert float queries to T on device
        auto queries_device_float = raft::make_device_matrix<float, int64_t>(*res, num_queries, dimension);
        raft::copy(*res, queries_device_float.view(), raft::make_host_matrix_view<const float, int64_t>(queries_data, num_queries, dimension));
        
        auto queries_device_target = raft::make_device_matrix<T, int64_t>(*res, num_queries, dimension);
        if constexpr (sizeof(T) == 1) {
            if (!quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
            quantizer_.template transform<T>(*res, queries_device_float.view(), queries_device_target.data_handle(), true);
        } else {
            raft::copy(*res, queries_device_target.view(), queries_device_float.view());
        }

        // 2. Perform search
        search_result_t search_res;
        search_res.neighbors.resize(num_queries * limit);
        search_res.distances.resize(num_queries * limit);

        cuvs::neighbors::ivf_pq::search_params search_params;
        search_params.n_probes = sp.n_probes;

        const ivf_pq_index* local_index = index_.get();
        if (!local_index && mg_index_) {
            int current_device;
            RAFT_CUDA_TRY(cudaGetDevice(&current_device));
            for (size_t i = 0; i < devices_.size(); ++i) {
                if (devices_[i] == current_device && i < mg_index_->ann_interfaces_.size()) {
                    if (mg_index_->ann_interfaces_[i].index_.has_value()) {
                        local_index = &mg_index_->ann_interfaces_[i].index_.value();
                        break;
                    }
                }
            }
        }

        if (is_snmg_handle(res) && mg_index_) {
            auto queries_host_target = raft::make_host_matrix<T, int64_t>(num_queries, dimension);
            raft::copy(*res, queries_host_target.view(), queries_device_target.view());
            raft::resource::sync_stream(*res);

            auto neighbors_host_view = raft::make_host_matrix_view<int64_t, int64_t>(
                search_res.neighbors.data(), (int64_t)num_queries, (int64_t)limit);
            auto distances_host_view = raft::make_host_matrix_view<float, int64_t>(
                search_res.distances.data(), (int64_t)num_queries, (int64_t)limit);

            cuvs::neighbors::mg_search_params<cuvs::neighbors::ivf_pq::search_params> mg_search_params(search_params);
            cuvs::neighbors::ivf_pq::search(*res, *mg_index_, mg_search_params,
                                                queries_host_target.view(), neighbors_host_view, distances_host_view);
        } else if (local_index) {
            auto neighbors_device = raft::make_device_matrix<int64_t, int64_t, raft::layout_c_contiguous>(
                *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
            auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

            cuvs::neighbors::ivf_pq::search(*res, search_params, *local_index,
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
        if (!is_loaded_ || (!index_ && !mg_index_)) return {};

        uint64_t job_id = worker->submit(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                auto res = handle.get_raft_resources();
                
                const ivf_pq_index* local_index = nullptr;
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

        cuvs_task_result_t result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<std::vector<T>>(result.result);
    }

    uint32_t get_n_list() {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (!is_loaded_) return build_params.n_lists;
        
        if (index_) return static_cast<uint32_t>(index_->n_lists());
        if (mg_index_) {
            for (const auto& iface : mg_index_->ann_interfaces_) {
                if (iface.index_.has_value()) return static_cast<uint32_t>(iface.index_.value().n_lists());
            }
        }
        return build_params.n_lists;
    }

    uint32_t get_dim() {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (!is_loaded_) return dimension;
        
        if (index_) return static_cast<uint32_t>(index_->dim());
        if (mg_index_) {
            for (const auto& iface : mg_index_->ann_interfaces_) {
                if (iface.index_.has_value()) return static_cast<uint32_t>(iface.index_.value().dim());
            }
        }
        return dimension;
    }

    uint32_t get_rot_dim() {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (!is_loaded_) return dimension;
        
        if (index_) return static_cast<uint32_t>(index_->rot_dim());
        if (mg_index_) {
            for (const auto& iface : mg_index_->ann_interfaces_) {
                if (iface.index_.has_value()) return static_cast<uint32_t>(iface.index_.value().rot_dim());
            }
        }
        return dimension;
    }

    uint32_t get_dim_ext() {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (!is_loaded_) return dimension;
        
        if (index_) return static_cast<uint32_t>(index_->dim_ext());
        if (mg_index_) {
            for (const auto& iface : mg_index_->ann_interfaces_) {
                if (iface.index_.has_value()) return static_cast<uint32_t>(iface.index_.value().dim_ext());
            }
        }
        return dimension;
    }

    void add_chunk(const T* chunk_data, uint64_t chunk_count) {
        if (current_offset_ + chunk_count > count) throw std::runtime_error("offset out of bounds");
        std::copy(chunk_data, chunk_data + (chunk_count * dimension), flattened_host_dataset.begin() + (current_offset_ * dimension));
        current_offset_ += chunk_count;
    }

    void add_chunk_float(const float* chunk_data, uint64_t chunk_count) {
        if (current_offset_ + chunk_count > count) throw std::runtime_error("offset out of bounds");
        
        uint64_t row_offset = current_offset_;
        uint64_t job_id = worker->submit(
            [&, chunk_data, chunk_count, row_offset](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                
                // If quantization is needed (T is 1-byte)
                if constexpr (sizeof(T) == 1) {
                    if (!quantizer_.is_trained()) {
                        int64_t n_train = std::min(static_cast<int64_t>(chunk_count), static_cast<int64_t>(500));
                        auto train_device = raft::make_device_matrix<float, int64_t>(*res, n_train, dimension);
                        raft::copy(*res, train_device.view(), raft::make_host_matrix_view<const float, int64_t>(chunk_data, n_train, dimension));
                        quantizer_.train(*res, train_device.view());
                    }

                    auto chunk_device_float = raft::make_device_matrix<float, int64_t>(*res, chunk_count, dimension);
                    raft::copy(*res, chunk_device_float.view(), raft::make_host_matrix_view<const float, int64_t>(chunk_data, chunk_count, dimension));
                    quantizer_.template transform<T>(*res, chunk_device_float.view(), flattened_host_dataset.data() + (row_offset * dimension), false);
                    raft::resource::sync_stream(*res);
                } else if constexpr (std::is_same_v<T, float>) {
                    std::copy(chunk_data, chunk_data + (chunk_count * dimension), flattened_host_dataset.begin() + (row_offset * dimension));
                } else {
                    auto chunk_device_float = raft::make_device_matrix<float, int64_t>(*res, chunk_count, dimension);
                    raft::copy(*res, chunk_device_float.view(), raft::make_host_matrix_view<const float, int64_t>(chunk_data, chunk_count, dimension));
                    auto out_view = raft::make_host_matrix_view<T, int64_t>(flattened_host_dataset.data() + (row_offset * dimension), chunk_count, dimension);
                    raft::copy(*res, out_view, chunk_device_float.view());
                    raft::resource::sync_stream(*res);
                }
                return std::any();
            }
        );
        
        auto result_wait = worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
        current_offset_ += chunk_count;
    }

    uint32_t cap() const {
        return count;
    }

    uint32_t len() const {
        return static_cast<uint32_t>(current_offset_);
    }

    void destroy() {
        if (worker) worker->stop();
    }

    void set_quantizer(float min, float max) {
        quantizer_ = scalar_quantizer_t<float>(min, max);
    }

    void get_quantizer(float* min, float* max) const {
        if (!quantizer_.is_trained()) throw std::runtime_error("Quantizer not trained");
        *min = quantizer_.min();
        *max = quantizer_.max();
    }

    void train_quantizer(const float* train_data, uint64_t n_samples) {
        if (!train_data || n_samples == 0) return;
        uint64_t job_id = worker->submit(
            [&, train_data, n_samples](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                auto train_device = raft::make_device_matrix<float, int64_t>(*res, n_samples, dimension);
                raft::copy(*res, train_device.view(), raft::make_host_matrix_view<const float, int64_t>(train_data, n_samples, dimension));
                quantizer_.train(*res, train_device.view());
                return std::any();
            }
        );
        auto result_wait = worker->wait(job_id).get();
        if (result_wait.error) std::rethrow_exception(result_wait.error);
    }

private:
    scalar_quantizer_t<float> quantizer_;
    uint64_t current_offset_ = 0;
};

} // namespace matrixone
