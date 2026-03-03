#pragma once

#include "cuvs_worker.hpp" // For cuvs_worker_t and raft_handle_wrapper_t
#include "../c/helper.h"   // For distance_type_t and ivf_flat_build_params_t
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
#pragma GCC diagnostic pop


namespace matrixone {

/**
 * @brief gpu_ivf_flat_t implements an IVF-Flat index that can run on a single GPU or sharded across multiple GPUs.
 * It automatically chooses between single-GPU and multi-GPU (SNMG) cuVS APIs based on the RAFT handle resources.
 */
template <typename T>
class gpu_ivf_flat_t {
public:
    using ivf_flat_index = cuvs::neighbors::ivf_flat::index<T, int64_t>;
    using mg_index = cuvs::neighbors::mg_index<ivf_flat_index, T, int64_t>;

    std::vector<T> flattened_host_dataset;
    std::vector<int> devices_;
    std::string filename_;
    
    // Internal index storage
    std::unique_ptr<ivf_flat_index> index_;
    std::unique_ptr<mg_index> mg_index_;

    cuvs::distance::DistanceType metric;
    uint32_t dimension;
    uint32_t count;
    ivf_flat_build_params_t build_params;
    distribution_mode_t dist_mode;

    std::unique_ptr<cuvs_worker_t> worker;
    std::shared_mutex mutex_;
    bool is_loaded_ = false;

    ~gpu_ivf_flat_t() {
        destroy();
    }

    // Unified Constructor for building from dataset
    gpu_ivf_flat_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                    cuvs::distance::DistanceType m, const ivf_flat_build_params_t& bp, 
                    const std::vector<int>& devices, uint32_t nthread, distribution_mode_t mode)
        : dimension(dimension), count(static_cast<uint32_t>(count_vectors)), metric(m), 
          build_params(bp), dist_mode(mode), devices_(devices) {
        
        bool force_mg = (mode == DistributionMode_SHARDED || mode == DistributionMode_REPLICATED);
        worker = std::make_unique<cuvs_worker_t>(nthread, devices_, force_mg || (devices_.size() > 1));

        flattened_host_dataset.resize(count * dimension);
        std::copy(dataset_data, dataset_data + (count * dimension), flattened_host_dataset.begin());
    }

    // Unified Constructor for loading from file
    gpu_ivf_flat_t(const std::string& filename, uint32_t dimension, cuvs::distance::DistanceType m, 
                    const std::vector<int>& devices, uint32_t nthread, distribution_mode_t mode)
        : filename_(filename), dimension(dimension), metric(m), count(0), 
          dist_mode(mode), devices_(devices) {
        
        bool force_mg = (mode == DistributionMode_SHARDED || mode == DistributionMode_REPLICATED);
        worker = std::make_unique<cuvs_worker_t>(nthread, devices_, force_mg || (devices_.size() > 1));
        build_params = {1024}; // Default values
    }

    void load() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (is_loaded_) return;

        std::promise<bool> init_complete_promise;
        std::future<bool> init_complete_future = init_complete_promise.get_future();

        auto init_fn = [&](raft_handle_wrapper_t& handle) -> std::any {
            auto res = handle.get_raft_resources();
            bool is_mg = is_snmg_handle(res);

            if (!filename_.empty()) {
                if (is_mg) {
                    mg_index_ = std::make_unique<mg_index>(
                        cuvs::neighbors::ivf_flat::deserialize<T, int64_t>(*res, filename_));
                    // Update metadata
                    count = 0;
                    for (const auto& iface : mg_index_->ann_interfaces_) {
                        if (iface.index_.has_value()) count += static_cast<uint32_t>(iface.index_.value().size());
                    }
                    if (!mg_index_->ann_interfaces_.empty() && mg_index_->ann_interfaces_[0].index_.has_value()) {
                        build_params.n_lists = static_cast<uint32_t>(mg_index_->ann_interfaces_[0].index_.value().n_lists());
                    }
                } else {
                    cuvs::neighbors::ivf_flat::index_params index_params;
                    index_params.metric = metric;
                    index_ = std::make_unique<ivf_flat_index>(*res, index_params, dimension);
                    cuvs::neighbors::ivf_flat::deserialize(*res, filename_, index_.get());
                    count = static_cast<uint32_t>(index_->size());
                    build_params.n_lists = static_cast<uint32_t>(index_->n_lists());
                }
                raft::resource::sync_stream(*res);
            } else if (!flattened_host_dataset.empty()) {
                if (count < build_params.n_lists) {
                    throw std::runtime_error("Dataset too small: count (" + std::to_string(count) + 
                                            ") must be >= n_list (" + std::to_string(build_params.n_lists) + 
                                            ") to build IVF index.");
                }

                if (is_mg) {
                    auto dataset_host_view = raft::make_host_matrix_view<const T, int64_t>(
                        flattened_host_dataset.data(), (int64_t)count, (int64_t)dimension);

                    cuvs::neighbors::ivf_flat::index_params index_params;
                    index_params.metric = metric;
                    index_params.n_lists = build_params.n_lists;

                    cuvs::neighbors::mg_index_params<cuvs::neighbors::ivf_flat::index_params> mg_params(index_params);
                    if (dist_mode == DistributionMode_REPLICATED) {
                        mg_params.mode = cuvs::neighbors::distribution_mode::REPLICATED;
                    } else {
                        mg_params.mode = cuvs::neighbors::distribution_mode::SHARDED;
                    }

                    mg_index_ = std::make_unique<mg_index>(
                        cuvs::neighbors::ivf_flat::build(*res, mg_params, dataset_host_view));
                } else {
                    auto dataset_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                        *res, static_cast<int64_t>(count), static_cast<int64_t>(dimension));
                    
                    RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device.data_handle(), flattened_host_dataset.data(),
                                             flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                             raft::resource::get_cuda_stream(*res)));

                    cuvs::neighbors::ivf_flat::index_params index_params;
                    index_params.metric = metric;
                    index_params.n_lists = build_params.n_lists;

                    index_ = std::make_unique<ivf_flat_index>(
                        cuvs::neighbors::ivf_flat::build(*res, index_params, raft::make_const_mdspan(dataset_device.view())));
                }
                raft::resource::sync_stream(*res);
            }

            init_complete_promise.set_value(true);
            return std::any();
        };

        auto stop_fn = [&](raft_handle_wrapper_t& handle) -> std::any {
            index_.reset();
            mg_index_.reset();
            return std::any();
        };

        worker->start(init_fn, stop_fn);
        init_complete_future.get();
        is_loaded_ = true;
    }

    void save(const std::string& filename) {
        if (!is_loaded_ || (!index_ && !mg_index_)) throw std::runtime_error("index not loaded");

        uint64_t job_id = worker->submit(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
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

        cuvs_task_result_t result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
    }

    struct search_result_t {
        std::vector<int64_t> neighbors;
        std::vector<float> distances;
    };

    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, 
                        uint32_t limit, const ivf_flat_search_params_t& sp) {
        if (!queries_data || num_queries == 0 || dimension == 0) return search_result_t{};
        if (query_dimension != dimension) throw std::runtime_error("dimension mismatch");
        if (!is_loaded_ || (!index_ && !mg_index_)) return search_result_t{};

        uint64_t job_id = worker->submit(
            [&, num_queries, limit, sp](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                auto res = handle.get_raft_resources();

                search_result_t search_res;
                search_res.neighbors.resize(num_queries * limit);
                search_res.distances.resize(num_queries * limit);

                cuvs::neighbors::ivf_flat::search_params search_params;
                search_params.n_probes = sp.n_probes;

                if (is_snmg_handle(res)) {
                    auto queries_host_view = raft::make_host_matrix_view<const T, int64_t>(
                        queries_data, (int64_t)num_queries, (int64_t)dimension);
                    auto neighbors_host_view = raft::make_host_matrix_view<int64_t, int64_t>(
                        search_res.neighbors.data(), (int64_t)num_queries, (int64_t)limit);
                    auto distances_host_view = raft::make_host_matrix_view<float, int64_t>(
                        search_res.distances.data(), (int64_t)num_queries, (int64_t)limit);

                    cuvs::neighbors::mg_search_params<cuvs::neighbors::ivf_flat::search_params> mg_search_params(search_params);
                    cuvs::neighbors::ivf_flat::search(*res, *mg_index_, mg_search_params,
                                                       queries_host_view, neighbors_host_view, distances_host_view);
                } else {
                    auto queries_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                        *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(dimension));
                    RAFT_CUDA_TRY(cudaMemcpyAsync(queries_device.data_handle(), queries_data,
                                             num_queries * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                             raft::resource::get_cuda_stream(*res)));

                    auto neighbors_device = raft::make_device_matrix<int64_t, int64_t, raft::layout_c_contiguous>(
                        *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
                    auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                        *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

                    cuvs::neighbors::ivf_flat::search(*res, search_params, *index_,
                                                       raft::make_const_mdspan(queries_device.view()), 
                                                       neighbors_device.view(), distances_device.view());

                    RAFT_CUDA_TRY(cudaMemcpyAsync(search_res.neighbors.data(), neighbors_device.data_handle(),
                                             search_res.neighbors.size() * sizeof(int64_t), cudaMemcpyDeviceToHost,
                                             raft::resource::get_cuda_stream(*res)));
                    RAFT_CUDA_TRY(cudaMemcpyAsync(search_res.distances.data(), distances_device.data_handle(),
                                             search_res.distances.size() * sizeof(float), cudaMemcpyDeviceToHost,
                                             raft::resource::get_cuda_stream(*res)));
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
        );

        cuvs_task_result_t result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<search_result_t>(result.result);
    }

    std::vector<T> get_centers() {
        if (!is_loaded_ || (!index_ && !mg_index_)) return {};

        uint64_t job_id = worker->submit(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
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

    void destroy() {
        if (worker) worker->stop();
    }
};

} // namespace matrixone
