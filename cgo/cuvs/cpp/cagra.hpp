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

// cuVS includes
#include <cuvs/distance/distance.hpp>
#include <cuvs/neighbors/cagra.hpp>
#pragma GCC diagnostic pop

namespace matrixone {

// --- gpu_cagra_index_t Class ---
template <typename T>
class gpu_cagra_index_t {
public:
    std::vector<T> flattened_host_dataset;
    std::string filename_;
    std::unique_ptr<cuvs::neighbors::cagra::index<T, uint32_t>> index; 
    cuvs::distance::DistanceType metric;
    uint32_t dimension;
    uint32_t count;
    size_t intermediate_graph_degree;
    size_t graph_degree;
    int device_id_;
    std::unique_ptr<cuvs_worker_t> worker;
    std::shared_mutex mutex_;
    bool is_loaded_ = false;
    std::shared_ptr<void> dataset_device_ptr_; // Keeps device dataset alive for search

    ~gpu_cagra_index_t() {
        destroy();
    }

    // Constructor for building from dataset
    gpu_cagra_index_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                  cuvs::distance::DistanceType m, size_t intermediate_graph_degree, 
                  size_t graph_degree, uint32_t nthread, int device_id = 0)
        : dimension(dimension), count(static_cast<uint32_t>(count_vectors)), metric(m), 
          intermediate_graph_degree(intermediate_graph_degree), graph_degree(graph_degree), 
          device_id_(device_id) {
        worker = std::make_unique<cuvs_worker_t>(nthread, device_id_);

        flattened_host_dataset.resize(count * dimension);
        std::copy(dataset_data, dataset_data + (count * dimension), flattened_host_dataset.begin());
    }

    // Constructor for loading from file
    gpu_cagra_index_t(const std::string& filename, uint32_t dimension, cuvs::distance::DistanceType m, uint32_t nthread, int device_id = 0)
        : filename_(filename), dimension(dimension), metric(m), count(0), 
          intermediate_graph_degree(0), graph_degree(0), device_id_(device_id) {
        worker = std::make_unique<cuvs_worker_t>(nthread, device_id_);
    }

    // Private constructor for creating from an existing cuVS index (used by merge)
    gpu_cagra_index_t(std::unique_ptr<cuvs::neighbors::cagra::index<T, uint32_t>> idx, 
                  uint32_t dim, cuvs::distance::DistanceType m, uint32_t nthread, int dev_id)
        : index(std::move(idx)), metric(m), dimension(dim), device_id_(dev_id) {
        worker = std::make_unique<cuvs_worker_t>(nthread, device_id_);
        worker->start(); // MUST START WORKER
        count = static_cast<uint32_t>(index->size());
        graph_degree = static_cast<size_t>(index->graph_degree());
        is_loaded_ = true;
    }

    void load() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (is_loaded_) return;

        std::promise<bool> init_complete_promise;
        std::future<bool> init_complete_future = init_complete_promise.get_future();

        auto init_fn = [&](raft_handle_wrapper_t& handle) -> std::any {
            if (!filename_.empty()) {
                // load from file
                index = std::make_unique<cuvs::neighbors::cagra::index<T, uint32_t>>(
                    *handle.get_raft_resources()
                );
                cuvs::neighbors::cagra::deserialize(*handle.get_raft_resources(), filename_, index.get());
                raft::resource::sync_stream(*handle.get_raft_resources());
                
                count = static_cast<uint32_t>(index->size());
                graph_degree = static_cast<size_t>(index->graph_degree());
            } else if (!flattened_host_dataset.empty()) {
                auto dataset_device = new auto(raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                    *handle.get_raft_resources(), static_cast<int64_t>(count), static_cast<int64_t>(dimension)));
                
                dataset_device_ptr_ = std::shared_ptr<void>(dataset_device, [](void* ptr) {
                    delete static_cast<raft::device_matrix<T, int64_t, raft::layout_c_contiguous>*>(ptr);
                });

                RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device->data_handle(), flattened_host_dataset.data(),
                                         flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));

                cuvs::neighbors::cagra::index_params index_params;
                index_params.metric = metric;
                index_params.intermediate_graph_degree = intermediate_graph_degree;
                index_params.graph_degree = graph_degree;
                index_params.attach_dataset_on_build = true; 

                index = std::make_unique<cuvs::neighbors::cagra::index<T, uint32_t>>(
                    cuvs::neighbors::cagra::build(*handle.get_raft_resources(), index_params, raft::make_const_mdspan(dataset_device->view())));

                raft::resource::sync_stream(*handle.get_raft_resources());
            } else {
                index = nullptr; 
            }

            init_complete_promise.set_value(true); 
            return std::any();
        };
        auto stop_fn = [&](raft_handle_wrapper_t& handle) -> std::any {
            if (index) { 
                index.reset();
            }
            if (dataset_device_ptr_) {
                dataset_device_ptr_.reset();
            }
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
            if (!is_loaded_ || !index) {
                throw std::runtime_error("index must be loaded before extending.");
            }
            if (num_vectors == 0) return;

            std::unique_lock<std::shared_mutex> lock(mutex_);

            uint64_t job_id = worker->submit(
                [&, additional_data, num_vectors](raft_handle_wrapper_t& handle) -> std::any {
                    auto& res = *handle.get_raft_resources();
                    
                    auto additional_dataset_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                        res, static_cast<int64_t>(num_vectors), static_cast<int64_t>(dimension));
                    
                    RAFT_CUDA_TRY(cudaMemcpyAsync(additional_dataset_device.data_handle(), additional_data,
                                            num_vectors * dimension * sizeof(T), cudaMemcpyHostToDevice,
                                            raft::resource::get_cuda_stream(res)));

                    cuvs::neighbors::cagra::extend_params params;
                    auto view = additional_dataset_device.view();
                    cuvs::neighbors::cagra::extend(res, params, raft::make_const_mdspan(view), *index);

                    raft::resource::sync_stream(res);
                    return std::any();
                }
            );

            cuvs_task_result_t result = worker->wait(job_id).get();
            if (result.error) {
                std::rethrow_exception(result.error);
            }

            count += static_cast<uint32_t>(num_vectors);
            
            if (!flattened_host_dataset.empty()) {
                size_t old_size = flattened_host_dataset.size();
                flattened_host_dataset.resize(old_size + num_vectors * dimension);
                std::copy(additional_data, additional_data + num_vectors * dimension, flattened_host_dataset.begin() + old_size);
            }
        }
    }

    static std::unique_ptr<gpu_cagra_index_t<T>> merge(const std::vector<gpu_cagra_index_t<T>*>& indices, uint32_t nthread, int device_id) {
        if (indices.empty()) return nullptr;
        
        uint32_t dim = indices[0]->dimension;
        cuvs::distance::DistanceType m = indices[0]->metric;

        cuvs_worker_t transient_worker(1, device_id);
        transient_worker.start();

        uint64_t job_id = transient_worker.submit(
            [&indices](raft_handle_wrapper_t& handle) -> std::any {
                auto& res = *handle.get_raft_resources();
                
                std::vector<cuvs::neighbors::cagra::index<T, uint32_t>*> cagra_indices;
                for (auto* idx : indices) {
                    if (!idx->is_loaded_ || !idx->index) {
                        throw std::runtime_error("One of the indices to merge is not loaded.");
                    }
                    cagra_indices.push_back(idx->index.get());
                }

                cuvs::neighbors::cagra::index_params index_params;
                cuvs::neighbors::cagra::merge_params params(index_params);
                
                auto merged_index = std::make_unique<cuvs::neighbors::cagra::index<T, uint32_t>>(
                    cuvs::neighbors::cagra::merge(res, params, cagra_indices)
                );

                raft::resource::sync_stream(res);
                return merged_index.release(); 
            }
        );

        cuvs_task_result_t result = transient_worker.wait(job_id).get();
        if (result.error) {
            std::rethrow_exception(result.error);
        }

        auto* merged_index_raw = std::any_cast<cuvs::neighbors::cagra::index<T, uint32_t>*>(result.result);
        auto merged_index_ptr = std::unique_ptr<cuvs::neighbors::cagra::index<T, uint32_t>>(merged_index_raw);
        transient_worker.stop();

        return std::make_unique<gpu_cagra_index_t<T>>(std::move(merged_index_ptr), dim, m, nthread, device_id);
    }

    void save(const std::string& filename) {
        if (!is_loaded_ || !index) {
            throw std::runtime_error("index must be loaded before saving.");
        }

        uint64_t job_id = worker->submit(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_); 
                cuvs::neighbors::cagra::serialize(*handle.get_raft_resources(), filename, *index);
                raft::resource::sync_stream(*handle.get_raft_resources());
                return std::any();
            }
        );

        cuvs_task_result_t result = worker->wait(job_id).get();
        if (result.error) {
            std::rethrow_exception(result.error);
        }
    }

    struct search_result_t {
        std::vector<uint32_t> neighbors;
        std::vector<float> distances;
    };

    search_result_t search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, size_t itopk_size) {
        if (!queries_data || num_queries == 0 || dimension == 0) {
            return search_result_t{};
        }
        if (query_dimension != this->dimension) {
            throw std::runtime_error("Query dimension does not match index dimension.");
        }
        if (limit == 0) {
            return search_result_t{};
        }
        if (!index) {
            return search_result_t{};
        }

        size_t queries_rows = num_queries;
        size_t queries_cols = dimension; 

        uint64_t job_id = worker->submit(
            [&, queries_rows, queries_cols, limit, itopk_size](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                
                auto queries_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                    *handle.get_raft_resources(), static_cast<int64_t>(queries_rows), static_cast<int64_t>(queries_cols));
                RAFT_CUDA_TRY(cudaMemcpyAsync(queries_device.data_handle(), queries_data,
                                         queries_rows * queries_cols * sizeof(T), cudaMemcpyHostToDevice,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));

                auto neighbors_device = raft::make_device_matrix<uint32_t, int64_t, raft::layout_c_contiguous>(
                    *handle.get_raft_resources(), static_cast<int64_t>(queries_rows), static_cast<int64_t>(limit));
                auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                    *handle.get_raft_resources(), static_cast<int64_t>(queries_rows), static_cast<int64_t>(limit));

                cuvs::neighbors::cagra::search_params search_params;
                search_params.itopk_size = itopk_size;
                
                cuvs::neighbors::cagra::search(*handle.get_raft_resources(), search_params, *index,
                                               raft::make_const_mdspan(queries_device.view()), neighbors_device.view(), distances_device.view());

                search_result_t res;
                res.neighbors.resize(queries_rows * limit);
                res.distances.resize(queries_rows * limit);

                RAFT_CUDA_TRY(cudaMemcpyAsync(res.neighbors.data(), neighbors_device.data_handle(),
                                         res.neighbors.size() * sizeof(uint32_t), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));
                RAFT_CUDA_TRY(cudaMemcpyAsync(res.distances.data(), distances_device.data_handle(),
                                         res.distances.size() * sizeof(float), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));

                raft::resource::sync_stream(*handle.get_raft_resources());

                // Post-process to handle sentinels
                for (size_t i = 0; i < res.neighbors.size(); ++i) {
                    if (res.neighbors[i] == std::numeric_limits<uint32_t>::max()) {
                        res.neighbors[i] = static_cast<uint32_t>(-1); 
                    }
                }
                
                return res;
            }
        );

        cuvs_task_result_t result = worker->wait(job_id).get();
        if (result.error) {
            std::rethrow_exception(result.error);
        }

        return std::any_cast<search_result_t>(result.result);
    }

    void destroy() {
        if (worker) {
            worker->stop();
        }
    }
};

} // namespace matrixone
