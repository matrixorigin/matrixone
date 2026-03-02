#pragma once

#include "cuvs_worker.hpp" // For CuvsWorker and RaftHandleWrapper
#include <raft/util/cudart_utils.hpp> // For RAFT_CUDA_TRY

// Standard library includes
#include <algorithm>
#include <iostream>
#include <memory>
#include <numeric>
#include <stdexcept>
#include <string>
#include <type_traits>
#include <vector>
#include <future>
#include <limits>
#include <shared_mutex>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
// RAFT includes
#include <raft/core/device_mdarray.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/resources.hpp>

// cuVS includes
#include <cuvs/distance/distance.hpp>
#include <cuvs/neighbors/cagra.hpp>
#pragma GCC diagnostic pop

namespace matrixone {

// --- GpuCagraIndex Class ---
template <typename T>
class GpuCagraIndex {
    static_assert(std::is_floating_point<T>::value, "T must be a floating-point type.");

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
                        res.Neighbors[i] = static_cast<uint32_t>(-1); // Let the caller decide how to handle this max val
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
