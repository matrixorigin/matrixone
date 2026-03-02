#pragma once

#include "cuvs_worker.hpp" // For CuvsWorker and RaftHandleWrapper
#include <raft/util/cudart_utils.hpp> // For RAFT_CUDA_TRY

// Standard library includes
#include <algorithm>   // For std::copy
#include <iostream>    // For simulation debug logs
#include <memory>
#include <numeric>     // For std::iota
#include <stdexcept>   // For std::runtime_error
#include <string>      
#include <type_traits> // For std::is_floating_point
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

// cuVS includes
#include <cuvs/distance/distance.hpp>    // cuVS distance API
#include <cuvs/neighbors/ivf_flat.hpp>   // IVF-Flat include
#pragma GCC diagnostic pop


namespace matrixone {

// --- GpuIvfFlatIndex Class ---
template <typename T>
class GpuIvfFlatIndex {
    static_assert(std::is_floating_point<T>::value, "T must be a floating-point type.");

public:
    std::vector<T> flattened_host_dataset; // Store flattened data as std::vector
    std::string filename_;
    std::unique_ptr<cuvs::neighbors::ivf_flat::index<T, int64_t>> Index; 
    cuvs::distance::DistanceType Metric;
    uint32_t Dimension;
    uint32_t Count;
    uint32_t NList;
    int device_id_;
    std::unique_ptr<CuvsWorker> Worker;
    std::shared_mutex mutex_; // Mutex to protect Load() and Search()
    bool is_loaded_ = false;

    ~GpuIvfFlatIndex() {
        Destroy();
    }

    // Constructor for building from dataset
    GpuIvfFlatIndex(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, cuvs::distance::DistanceType m,
                    uint32_t n_list, uint32_t nthread, int device_id = 0)
        : Dimension(dimension), Count(static_cast<uint32_t>(count_vectors)), Metric(m), 
          NList(n_list), device_id_(device_id) {
        Worker = std::make_unique<CuvsWorker>(nthread, device_id_);

        // Resize flattened_host_dataset and copy data from the flattened array
        flattened_host_dataset.resize(Count * Dimension); // Total elements
        std::copy(dataset_data, dataset_data + (Count * Dimension), flattened_host_dataset.begin());
    }

    // Constructor for loading from file
    GpuIvfFlatIndex(const std::string& filename, uint32_t dimension, cuvs::distance::DistanceType m, uint32_t nthread, int device_id = 0)
        : filename_(filename), Dimension(dimension), Metric(m), Count(0), NList(0), device_id_(device_id) {
        Worker = std::make_unique<CuvsWorker>(nthread, device_id_);
    }

    void Load() {
        std::unique_lock<std::shared_mutex> lock(mutex_); // Acquire exclusive lock
        if (is_loaded_) return;

        std::promise<bool> init_complete_promise;
        std::future<bool> init_complete_future = init_complete_promise.get_future();

        auto init_fn = [&](RaftHandleWrapper& handle) -> std::any {
            if (!filename_.empty()) {
                // Load from file
                cuvs::neighbors::ivf_flat::index_params index_params;
                index_params.metric = Metric;
                Index = std::make_unique<cuvs::neighbors::ivf_flat::index<T, int64_t>>(*handle.get_raft_resources(), index_params, Dimension);
                cuvs::neighbors::ivf_flat::deserialize(*handle.get_raft_resources(), filename_, Index.get());
                raft::resource::sync_stream(*handle.get_raft_resources());
                
                // Update metadata from loaded index
                Count = static_cast<uint32_t>(Index->size());
                NList = static_cast<uint32_t>(Index->n_lists());
            } else if (!flattened_host_dataset.empty()) {
                // DATASET SIZE CHECK
                if (Count < NList) {
                    throw std::runtime_error("Dataset too small: Count (" + std::to_string(Count) + 
                                            ") must be >= NList (" + std::to_string(NList) + 
                                            ") to build IVF index.");
                }
                
                // Build from dataset
                auto dataset_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                    *handle.get_raft_resources(), static_cast<int64_t>(Count), static_cast<int64_t>(Dimension));
                
                RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device.data_handle(), flattened_host_dataset.data(),
                                         flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));

                cuvs::neighbors::ivf_flat::index_params index_params;
                index_params.metric = Metric;
                index_params.n_lists = NList;

                Index = std::make_unique<cuvs::neighbors::ivf_flat::index<T, int64_t>>(
                    cuvs::neighbors::ivf_flat::build(*handle.get_raft_resources(), index_params, raft::make_const_mdspan(dataset_device.view())));

                raft::resource::sync_stream(*handle.get_raft_resources()); // Synchronize after build
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
            return std::any();
        };
        Worker->Start(init_fn, stop_fn);

        init_complete_future.get(); // Wait for the init_fn to complete
        is_loaded_ = true;
    }

    void Save(const std::string& filename) {
        if (!is_loaded_ || !Index) {
            throw std::runtime_error("Index must be loaded before saving.");
        }

        uint64_t jobID = Worker->Submit(
            [&](RaftHandleWrapper& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_); 
                cuvs::neighbors::ivf_flat::serialize(*handle.get_raft_resources(), filename, *Index);
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
        std::vector<int64_t> Neighbors;
        std::vector<float> Distances;
    };

    SearchResult Search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit, uint32_t n_probes) {
        if (!queries_data || num_queries == 0 || Dimension == 0) { // Check for invalid input
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
            [&, queries_rows, queries_cols, limit, n_probes](RaftHandleWrapper& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_); // Acquire shared read-only lock inside worker thread
                
                auto queries_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                    *handle.get_raft_resources(), static_cast<int64_t>(queries_rows), static_cast<int64_t>(queries_cols));
                RAFT_CUDA_TRY(cudaMemcpyAsync(queries_device.data_handle(), queries_data,
                                         queries_rows * queries_cols * sizeof(T), cudaMemcpyHostToDevice,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));

                auto neighbors_device = raft::make_device_matrix<int64_t, int64_t, raft::layout_c_contiguous>(
                    *handle.get_raft_resources(), static_cast<int64_t>(queries_rows), static_cast<int64_t>(limit));
                auto distances_device = raft::make_device_matrix<float, int64_t, raft::layout_c_contiguous>(
                    *handle.get_raft_resources(), static_cast<int64_t>(queries_rows), static_cast<int64_t>(limit));

                cuvs::neighbors::ivf_flat::search_params search_params;
                search_params.n_probes = n_probes;
                
                cuvs::neighbors::ivf_flat::search(*handle.get_raft_resources(), search_params, *Index,
                                                   raft::make_const_mdspan(queries_device.view()), neighbors_device.view(), distances_device.view());

                SearchResult res;
                res.Neighbors.resize(queries_rows * limit);
                res.Distances.resize(queries_rows * limit);

                RAFT_CUDA_TRY(cudaMemcpyAsync(res.Neighbors.data(), neighbors_device.data_handle(),
                                         res.Neighbors.size() * sizeof(int64_t), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));
                RAFT_CUDA_TRY(cudaMemcpyAsync(res.Distances.data(), distances_device.data_handle(),
                                         res.Distances.size() * sizeof(float), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));

                raft::resource::sync_stream(*handle.get_raft_resources());

                // Post-process to handle sentinels
                for (size_t i = 0; i < res.Neighbors.size(); ++i) {
                    if (res.Neighbors[i] == std::numeric_limits<int64_t>::max() || 
                        res.Neighbors[i] == 4294967295LL || 
                        res.Neighbors[i] < 0) {
                        res.Neighbors[i] = -1;
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

    std::vector<T> GetCenters() {
        if (!is_loaded_ || !Index) return {};

        uint64_t jobID = Worker->Submit(
            [&](RaftHandleWrapper& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                auto centers_view = Index->centers();
                size_t n_centers = centers_view.extent(0);
                size_t dim = centers_view.extent(1);
                std::vector<T> host_centers(n_centers * dim);

                RAFT_CUDA_TRY(cudaMemcpyAsync(host_centers.data(), centers_view.data_handle(),
                                         host_centers.size() * sizeof(T), cudaMemcpyDeviceToHost,
                                         raft::resource::get_cuda_stream(*handle.get_raft_resources())));

                raft::resource::sync_stream(*handle.get_raft_resources());
                return host_centers;
            }
        );

        CuvsTaskResult result = Worker->Wait(jobID).get();
        if (result.Error) {
            std::rethrow_exception(result.Error);
        }

        return std::any_cast<std::vector<T>>(result.Result);
    }

    void Destroy() {
        if (Worker) {
            Worker->Stop();
        }
    }
};

} // namespace matrixone
