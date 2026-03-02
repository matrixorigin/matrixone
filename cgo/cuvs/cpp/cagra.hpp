#pragma once

#include "cuvs_worker.hpp" // For cuvs_worker_t and raft_handle_wrapper_t
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
#include <raft/core/device_mdarray.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/resources.hpp>
#include <raft/core/copy.cuh>            // For raft::copy with type conversion
#include <raft/core/device_resources_snmg.hpp> // For checking SNMG type

// cuVS includes
#include <cuvs/distance/distance.hpp>
#include <cuvs/neighbors/cagra.hpp>
#pragma GCC diagnostic pop

namespace matrixone {

/**
 * @brief gpu_cagra_index_t implements a CAGRA index that can run on a single GPU or sharded across multiple GPUs.
 * It automatically chooses between single-GPU and multi-GPU (SNMG) cuVS APIs based on the RAFT handle resources.
 */
template <typename T>
class gpu_cagra_index_t {
public:
    using cagra_index = cuvs::neighbors::cagra::index<T, uint32_t>;
    using mg_index = cuvs::neighbors::mg_index<cagra_index, T, uint32_t>;

    std::vector<T> flattened_host_dataset;
    std::vector<int> devices_;
    std::string filename_;
    
    // Internal index storage
    std::unique_ptr<cagra_index> index_;
    std::unique_ptr<mg_index> mg_index_;

    cuvs::distance::DistanceType metric;
    uint32_t dimension;
    uint32_t count;
    size_t intermediate_graph_degree;
    size_t graph_degree;
    std::unique_ptr<cuvs_worker_t> worker;
    std::shared_mutex mutex_;
    bool is_loaded_ = false;
    std::shared_ptr<void> dataset_device_ptr_; // Keeps device dataset alive for single-GPU build

    ~gpu_cagra_index_t() {
        destroy();
    }

    // Unified Constructor for building from dataset
    gpu_cagra_index_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                  cuvs::distance::DistanceType m, size_t intermediate_graph_degree, 
                  size_t graph_degree, const std::vector<int>& devices, uint32_t nthread, bool force_mg = false)
        : dimension(dimension), count(static_cast<uint32_t>(count_vectors)), metric(m), 
          intermediate_graph_degree(intermediate_graph_degree), graph_degree(graph_degree), 
          devices_(devices) {
        
        worker = std::make_unique<cuvs_worker_t>(nthread, devices_, force_mg || (devices_.size() > 1));

        flattened_host_dataset.resize(count * dimension);
        std::copy(dataset_data, dataset_data + (count * dimension), flattened_host_dataset.begin());
    }

    // Unified Constructor for loading from file
    gpu_cagra_index_t(const std::string& filename, uint32_t dimension, cuvs::distance::DistanceType m, 
                    const std::vector<int>& devices, uint32_t nthread, bool force_mg = false)
        : filename_(filename), dimension(dimension), metric(m), count(0), 
          intermediate_graph_degree(0), graph_degree(0), devices_(devices) {
        
        worker = std::make_unique<cuvs_worker_t>(nthread, devices_, force_mg || (devices_.size() > 1));
    }

    // Private constructor for creating from an existing cuVS index (used by merge)
    gpu_cagra_index_t(std::unique_ptr<cagra_index> idx, 
                  uint32_t dim, cuvs::distance::DistanceType m, uint32_t nthread, const std::vector<int>& devices)
        : index_(std::move(idx)), metric(m), dimension(dim), devices_(devices) {
        
        // Merge result is currently a single-GPU index.
        worker = std::make_unique<cuvs_worker_t>(nthread, devices_, false);
        worker->start();
        count = static_cast<uint32_t>(index_->size());
        graph_degree = static_cast<size_t>(index_->graph_degree());
        is_loaded_ = true;
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
                        cuvs::neighbors::cagra::deserialize<T, uint32_t>(*res, filename_));
                    count = 0;
                    for (const auto& iface : mg_index_->ann_interfaces_) {
                        if (iface.index_.has_value()) count += static_cast<uint32_t>(iface.index_.value().size());
                    }
                    if (!mg_index_->ann_interfaces_.empty() && mg_index_->ann_interfaces_[0].index_.has_value()) {
                        graph_degree = static_cast<size_t>(mg_index_->ann_interfaces_[0].index_.value().graph_degree());
                    }
                } else {
                    index_ = std::make_unique<cagra_index>(*res);
                    cuvs::neighbors::cagra::deserialize(*res, filename_, index_.get());
                    count = static_cast<uint32_t>(index_->size());
                    graph_degree = static_cast<size_t>(index_->graph_degree());
                }
                raft::resource::sync_stream(*res);
            } else if (!flattened_host_dataset.empty()) {
                if (is_mg) {
                    auto dataset_host_view = raft::make_host_matrix_view<const T, int64_t>(
                        flattened_host_dataset.data(), (int64_t)count, (int64_t)dimension);

                    cuvs::neighbors::cagra::index_params index_params;
                    index_params.metric = metric;
                    index_params.intermediate_graph_degree = intermediate_graph_degree;
                    index_params.graph_degree = graph_degree;

                    cuvs::neighbors::mg_index_params<cuvs::neighbors::cagra::index_params> mg_params(index_params);
                    mg_params.mode = cuvs::neighbors::distribution_mode::SHARDED;

                    mg_index_ = std::make_unique<mg_index>(
                        cuvs::neighbors::cagra::build(*res, mg_params, dataset_host_view));
                } else {
                    auto dataset_device = new auto(raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                        *res, static_cast<int64_t>(count), static_cast<int64_t>(dimension)));
                    
                    dataset_device_ptr_ = std::shared_ptr<void>(dataset_device, [](void* ptr) {
                        delete static_cast<raft::device_matrix<T, int64_t, raft::layout_c_contiguous>*>(ptr);
                    });

                    RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device->data_handle(), flattened_host_dataset.data(),
                                             flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                             raft::resource::get_cuda_stream(*res)));

                    cuvs::neighbors::cagra::index_params index_params;
                    index_params.metric = metric;
                    index_params.intermediate_graph_degree = intermediate_graph_degree;
                    index_params.graph_degree = graph_degree;
                    index_params.attach_dataset_on_build = true; 

                    index_ = std::make_unique<cagra_index>(
                        cuvs::neighbors::cagra::build(*res, index_params, raft::make_const_mdspan(dataset_device->view())));
                }
                raft::resource::sync_stream(*res);
            }

            init_complete_promise.set_value(true);
            return std::any();
        };

        auto stop_fn = [&](raft_handle_wrapper_t& handle) -> std::any {
            index_.reset();
            mg_index_.reset();
            dataset_device_ptr_.reset();
            return std::any();
        };

        worker->start(init_fn, stop_fn);
        init_complete_future.get();
        is_loaded_ = true;
    }

    void extend(const T* additional_data, uint64_t num_vectors) {
        if constexpr (std::is_same_v<T, half>) {
             throw std::runtime_error("CAGRA single-GPU extend is not supported for float16 (half) by cuVS.");
        } else {
            if (!is_loaded_ || !index_) {
                throw std::runtime_error("index must be loaded before extending (or it is a multi-GPU index, which doesn't support extend).");
            }
            if (num_vectors == 0) return;

            std::unique_lock<std::shared_mutex> lock(mutex_);

            uint64_t job_id = worker->submit(
                [&, additional_data, num_vectors](raft_handle_wrapper_t& handle) -> std::any {
                    auto res = handle.get_raft_resources();
                    
                    auto additional_dataset_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                        *res, static_cast<int64_t>(num_vectors), static_cast<int64_t>(dimension));
                    
                    RAFT_CUDA_TRY(cudaMemcpyAsync(additional_dataset_device.data_handle(), additional_data,
                                            num_vectors * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                            raft::resource::get_cuda_stream(*res)));

                    cuvs::neighbors::cagra::extend_params params;
                    cuvs::neighbors::cagra::extend(*res, params, raft::make_const_mdspan(additional_dataset_device.view()), *index_);

                    raft::resource::sync_stream(*res);
                    return std::any();
                }
            );

            cuvs_task_result_t result = worker->wait(job_id).get();
            if (result.error) std::rethrow_exception(result.error);

            count += static_cast<uint32_t>(num_vectors);
            if (!flattened_host_dataset.empty()) {
                size_t old_size = flattened_host_dataset.size();
                flattened_host_dataset.resize(old_size + num_vectors * dimension);
                std::copy(additional_data, additional_data + num_vectors * dimension, flattened_host_dataset.begin() + old_size);
            }
        }
    }

    static std::unique_ptr<gpu_cagra_index_t<T>> merge(const std::vector<gpu_cagra_index_t<T>*>& indices, uint32_t nthread, const std::vector<int>& devices) {
        if (indices.empty()) return nullptr;
        
        uint32_t dim = indices[0]->dimension;
        cuvs::distance::DistanceType m = indices[0]->metric;

        cuvs_worker_t transient_worker(1, devices, false);
        transient_worker.start();

        uint64_t job_id = transient_worker.submit(
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
                cuvs::neighbors::cagra::merge_params params(index_params);
                
                auto merged_index = std::make_unique<cagra_index>(
                    cuvs::neighbors::cagra::merge(*res, params, cagra_indices)
                );

                raft::resource::sync_stream(*res);
                return merged_index.release(); 
            }
        );

        cuvs_task_result_t result = transient_worker.wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);

        auto* merged_index_raw = std::any_cast<cagra_index*>(result.result);
        auto merged_index_ptr = std::unique_ptr<cagra_index>(merged_index_raw);
        transient_worker.stop();

        return std::make_unique<gpu_cagra_index_t<T>>(std::move(merged_index_ptr), dim, m, nthread, devices);
    }

    void save(const std::string& filename) {
        if (!is_loaded_ || (!index_ && !mg_index_)) throw std::runtime_error("index not loaded");

        uint64_t job_id = worker->submit(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
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

        cuvs_task_result_t result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
    }

    struct search_result_t {
        std::vector<uint32_t> neighbors;
        std::vector<float> distances;
    };

    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, size_t itopk_size) {
        if (!queries_data || num_queries == 0 || dimension == 0) return search_result_t{};
        if (query_dimension != dimension) throw std::runtime_error("dimension mismatch");
        if (!is_loaded_ || (!index_ && !mg_index_)) return search_result_t{};

        uint64_t job_id = worker->submit(
            [&, num_queries, limit, itopk_size](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                auto res = handle.get_raft_resources();

                search_result_t search_res;
                search_res.neighbors.resize(num_queries * limit);
                search_res.distances.resize(num_queries * limit);

                cuvs::neighbors::cagra::search_params search_params;
                search_params.itopk_size = itopk_size;

                if (is_snmg_handle(res)) {
                    auto queries_host_view = raft::make_host_matrix_view<const T, int64_t>(
                        queries_data, (int64_t)num_queries, (int64_t)dimension);
                    auto neighbors_host_view = raft::make_host_matrix_view<uint32_t, int64_t>(
                        search_res.neighbors.data(), (int64_t)num_queries, (int64_t)limit);
                    auto distances_host_view = raft::make_host_matrix_view<float, int64_t>(
                        search_res.distances.data(), (int64_t)num_queries, (int64_t)limit);

                    cuvs::neighbors::mg_search_params<cuvs::neighbors::cagra::search_params> mg_search_params(search_params);
                    cuvs::neighbors::cagra::search(*res, *mg_index_, mg_search_params,
                                                       queries_host_view, neighbors_host_view, distances_host_view);
                } else {
                    auto queries_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                        *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(dimension));
                    RAFT_CUDA_TRY(cudaMemcpyAsync(queries_device.data_handle(), queries_data,
                                             num_queries * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                             raft::resource::get_cuda_stream(*res)));

                    auto neighbors_device = raft::make_device_matrix<uint32_t, int64_t, raft::layout_c_contiguous>(
                        *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));
                    auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                        *res, static_cast<int64_t>(num_queries), static_cast<int64_t>(limit));

                    cuvs::neighbors::cagra::search(*res, search_params, *index_,
                                                   raft::make_const_mdspan(queries_device.view()), 
                                                   neighbors_device.view(), distances_device.view());

                    RAFT_CUDA_TRY(cudaMemcpyAsync(search_res.neighbors.data(), neighbors_device.data_handle(),
                                             search_res.neighbors.size() * sizeof(uint32_t), cudaMemcpyDeviceToHost,
                                             raft::resource::get_cuda_stream(*res)));
                    RAFT_CUDA_TRY(cudaMemcpyAsync(search_res.distances.data(), distances_device.data_handle(),
                                             search_res.distances.size() * sizeof(float), cudaMemcpyDeviceToHost,
                                             raft::resource::get_cuda_stream(*res)));
                }

                raft::resource::sync_stream(*res);

                for (size_t i = 0; i < search_res.neighbors.size(); ++i) {
                    if (search_res.neighbors[i] == std::numeric_limits<uint32_t>::max()) {
                        search_res.neighbors[i] = static_cast<uint32_t>(-1); 
                    }
                }
                return search_res;
            }
        );

        cuvs_task_result_t result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<search_result_t>(result.result);
    }

    void destroy() {
        if (worker) worker->stop();
    }
};

} // namespace matrixone
