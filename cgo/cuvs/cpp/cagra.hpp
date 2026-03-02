#pragma once

#include "cuvs_worker.hpp" // For CuvsWorker and RaftHandleWrapper
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

// --- GpuCagraIndex Class ---
template <typename T>
class GpuCagraIndex {
public:
    std::vector<T> flattened_host_dataset;
    std::string filename_;
    std::unique_ptr<cuvs::neighbors::cagra::index<T, uint32_t>> Index; 
    cuvs::distance::DistanceType Metric;
    uint32_t Dimension;
    uint32_t Count;
    size_t IntermediateGraphDegree;
    size_t GraphDegree;
    int device_id_;
    std::unique_ptr<CuvsWorker> Worker;
    std::shared_mutex mutex_;
    bool is_loaded_ = false;
    std::shared_ptr<void> dataset_device_ptr_; // Keeps device dataset alive for search

    ~GpuCagraIndex() {
        Destroy();
    }

    // Constructor for building from dataset
    GpuCagraIndex(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                  cuvs::distance::DistanceType m, size_t intermediate_graph_degree, 
                  size_t graph_degree, uint32_t nthread, int device_id = 0)
        : Dimension(dimension), Count(static_cast<uint32_t>(count_vectors)), Metric(m), 
          IntermediateGraphDegree(intermediate_graph_degree), GraphDegree(graph_degree), 
          device_id_(device_id) {
        Worker = std::make_unique<CuvsWorker>(nthread, device_id_);

        flattened_host_dataset.resize(Count * Dimension);
        std::copy(dataset_data, dataset_data + (Count * Dimension), flattened_host_dataset.begin());
    }

    // Constructor for loading from file
    GpuCagraIndex(const std::string& filename, uint32_t dimension, cuvs::distance::DistanceType m, uint32_t nthread, int device_id = 0)
        : filename_(filename), Dimension(dimension), Metric(m), Count(0), 
          IntermediateGraphDegree(0), GraphDegree(0), device_id_(device_id) {
        Worker = std::make_unique<CuvsWorker>(nthread, device_id_);
    }

    // Private constructor for creating from an existing cuVS index (used by Merge)
    GpuCagraIndex(std::unique_ptr<cuvs::neighbors::cagra::index<T, uint32_t>> index, 
                  uint32_t dimension, cuvs::distance::DistanceType m, uint32_t nthread, int device_id)
        : Index(std::move(index)), Metric(m), Dimension(dimension), device_id_(device_id) {
        Worker = std::make_unique<CuvsWorker>(nthread, device_id_);
        Count = static_cast<uint32_t>(Index->size());
        GraphDegree = static_cast<size_t>(Index->graph_degree());
        is_loaded_ = true;
    }

    void Load() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (is_loaded_) return;

        std::promise<bool> init_complete_promise;
        std::future<bool> init_complete_future = init_complete_promise.get_future();

        auto init_fn = [&](RaftHandleWrapper& handle) -> std::any {
            if (!filename_.empty()) {
                // Load from file
                Index = std::make_unique<cuvs::neighbors::cagra::index<T, uint32_t>>(
                    *handle.get_raft_resources()
                );
                cuvs::neighbors::cagra::deserialize(*handle.get_raft_resources(), filename_, Index.get());
                raft::resource::sync_stream(*handle.get_raft_resources());
                
                Count = static_cast<uint32_t>(Index->size());
                GraphDegree = static_cast<size_t>(Index->graph_degree());
            } else if (!flattened_host_dataset.empty()) {
                auto dataset_device = new auto(raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                    *handle.get_raft_resources(), static_cast<int64_t>(Count), static_cast<int64_t>(Dimension)));
                
                dataset_device_ptr_ = std::shared_ptr<void>(dataset_device, [](void* ptr) {
                    delete static_cast<raft::device_matrix<T, int64_t, raft::layout_c_contiguous>*>(ptr);
                });

                RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device->data_handle(), flattened_host_dataset.data(),
                                         flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));

                cuvs::neighbors::cagra::index_params index_params;
                index_params.metric = Metric;
                index_params.intermediate_graph_degree = IntermediateGraphDegree;
                index_params.graph_degree = GraphDegree;
                index_params.attach_dataset_on_build = true; 

                Index = std::make_unique<cuvs::neighbors::cagra::index<T, uint32_t>>(
                    cuvs::neighbors::cagra::build(*handle.get_raft_resources(), index_params, raft::make_const_mdspan(dataset_device->view())));

                raft::resource::sync_stream(*handle.get_raft_resources());
            } else {
                Index = nullptr; 
            }

            init_complete_promise.set_value(true); 
            return std::any();
        };
        auto stop_fn = [&](RaftHandleWrapper& handle) -> std::any {
            if (Index) { 
                Index.reset();
            }
            if (dataset_device_ptr_) {
                dataset_device_ptr_.reset();
            }
            return std::any();
        };
        Worker->Start(init_fn, stop_fn);

        init_complete_future.get();
        is_loaded_ = true;
    }

    void Extend(const T* additional_data, uint64_t num_vectors) {
        if constexpr (std::is_same_v<T, half>) {
             throw std::runtime_error("CAGRA single-GPU extend is not supported for float16 (half) by cuVS.");
        } else {
            if (!is_loaded_ || !Index) {
                throw std::runtime_error("Index must be loaded before extending.");
            }
            if (num_vectors == 0) return;

            std::unique_lock<std::shared_mutex> lock(mutex_);

            uint64_t jobID = Worker->Submit(
                [&, additional_data, num_vectors](RaftHandleWrapper& handle) -> std::any {
                    auto& res = *handle.get_raft_resources();
                    
                    auto additional_dataset_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                        res, static_cast<int64_t>(num_vectors), static_cast<int64_t>(Dimension));
                    
                    RAFT_CUDA_TRY(cudaMemcpyAsync(additional_dataset_device.data_handle(), additional_data,
                                            num_vectors * Dimension * sizeof(T), cudaMemcpyHostToDevice,
                                            raft::resource::get_cuda_stream(res)));

                    cuvs::neighbors::cagra::extend_params params;
                    auto view = additional_dataset_device.view();
                    cuvs::neighbors::cagra::extend(res, params, raft::make_const_mdspan(view), *Index);

                    raft::resource::sync_stream(res);
                    return std::any();
                }
            );

            CuvsTaskResult result = Worker->Wait(jobID).get();
            if (result.Error) {
                std::rethrow_exception(result.Error);
            }

            Count += static_cast<uint32_t>(num_vectors);
            
            if (!flattened_host_dataset.empty()) {
                size_t old_size = flattened_host_dataset.size();
                flattened_host_dataset.resize(old_size + num_vectors * Dimension);
                std::copy(additional_data, additional_data + num_vectors * Dimension, flattened_host_dataset.begin() + old_size);
            }
        }
    }

    static std::unique_ptr<GpuCagraIndex<T>> Merge(const std::vector<GpuCagraIndex<T>*>& indices, uint32_t nthread, int device_id) {
        if (indices.empty()) return nullptr;
        
        uint32_t dimension = indices[0]->Dimension;
        cuvs::distance::DistanceType metric = indices[0]->Metric;

        CuvsWorker transient_worker(1, device_id);
        transient_worker.Start();

        uint64_t jobID = transient_worker.Submit(
            [&indices](RaftHandleWrapper& handle) -> std::any {
                auto& res = *handle.get_raft_resources();
                
                std::vector<cuvs::neighbors::cagra::index<T, uint32_t>*> cagra_indices;
                for (auto* idx : indices) {
                    if (!idx->is_loaded_ || !idx->Index) {
                        throw std::runtime_error("One of the indices to merge is not loaded.");
                    }
                    cagra_indices.push_back(idx->Index.get());
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

        CuvsTaskResult result = transient_worker.Wait(jobID).get();
        if (result.Error) {
            std::rethrow_exception(result.Error);
        }

        auto* merged_index_raw = std::any_cast<cuvs::neighbors::cagra::index<T, uint32_t>*>(result.Result);
        auto merged_index_ptr = std::unique_ptr<cuvs::neighbors::cagra::index<T, uint32_t>>(merged_index_raw);
        transient_worker.Stop();

        return std::make_unique<GpuCagraIndex<T>>(std::move(merged_index_ptr), dimension, metric, nthread, device_id);
    }

    void Save(const std::string& filename) {
        if (!is_loaded_ || !Index) {
            throw std::runtime_error("Index must be loaded before saving.");
        }

        uint64_t jobID = Worker->Submit(
            [&](RaftHandleWrapper& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_); 
                cuvs::neighbors::cagra::serialize(*handle.get_raft_resources(), filename, *Index);
                raft::resource::sync_stream(*handle.get_raft_resources());
                return std::any();
            }
        );

        CuvsTaskResult result = Worker->Wait(jobID).get();
        if (result.Error) {
            std::rethrow_exception(result.Error);
        }
    }

    struct SearchResult {
        std::vector<uint32_t> Neighbors;
        std::vector<float> Distances;
    };

    SearchResult Search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, size_t itopk_size) {
        if (!queries_data || num_queries == 0 || Dimension == 0) {
            return SearchResult{};
        }
        if (query_dimension != this->Dimension) {
            throw std::runtime_error("Query dimension does not match index dimension.");
        }
        if (limit == 0) {
            return SearchResult{};
        }
        if (!Index) {
            return SearchResult{};
        }

        size_t queries_rows = num_queries;
        size_t queries_cols = Dimension; 

        uint64_t jobID = Worker->Submit(
            [&, queries_rows, queries_cols, limit, itopk_size](RaftHandleWrapper& handle) -> std::any {
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
                
                cuvs::neighbors::cagra::search(*handle.get_raft_resources(), search_params, *Index,
                                               raft::make_const_mdspan(queries_device.view()), neighbors_device.view(), distances_device.view());

                SearchResult res;
                res.Neighbors.resize(queries_rows * limit);
                res.Distances.resize(queries_rows * limit);

                RAFT_CUDA_TRY(cudaMemcpyAsync(res.Neighbors.data(), neighbors_device.data_handle(),
                                         res.Neighbors.size() * sizeof(uint32_t), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));
                RAFT_CUDA_TRY(cudaMemcpyAsync(res.Distances.data(), distances_device.data_handle(),
                                         res.Distances.size() * sizeof(float), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));

                raft::resource::sync_stream(*handle.get_raft_resources());

                // Post-process to handle sentinels
                for (size_t i = 0; i < res.Neighbors.size(); ++i) {
                    if (res.Neighbors[i] == std::numeric_limits<uint32_t>::max()) {
                        res.Neighbors[i] = static_cast<uint32_t>(-1); 
                    }
                }
                
                return res;
            }
        );

        CuvsTaskResult result = Worker->Wait(jobID).get();
        if (result.Error) {
            std::rethrow_exception(result.Error);
        }

        return std::any_cast<SearchResult>(result.Result);
    }

    void Destroy() {
        if (Worker) {
            Worker->Stop();
        }
    }
};

} // namespace matrixone
