#pragma once

#include "cuvs_worker.hpp" // For CuvsWorker and RaftHandleWrapper
#include <raft/util/cudart_utils.hpp> // For RAFT_CUDA_TRY

// Standard library includes
#include <algorithm>   // For std::copy
#include <iostream>    // For simulation debug logs
#include <memory>
#include <numeric>     // For std::iota
#include <stdexcept>   // For std::runtime_error
#include <string>      // Corrected: was #string
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
#include <raft/linalg/map.cuh>           // RESTORED: map.cuh


// cuVS includes
#include <cuvs/distance/distance.hpp>    // cuVS distance API
#include <cuvs/neighbors/brute_force.hpp> // Correct include
#pragma GCC diagnostic pop


namespace matrixone {

// --- GpuBruteForceIndex Class ---
template <typename T>
class GpuBruteForceIndex {
    static_assert(std::is_floating_point<T>::value, "T must be a floating-point type.");

public:
    std::vector<T> flattened_host_dataset; // Store flattened data as std::vector
    std::unique_ptr<cuvs::neighbors::brute_force::index<T, float>> Index; // Use float for DistT
    cuvs::distance::DistanceType Metric;
    uint32_t Dimension;
    uint32_t Count;
    int device_id_;
    std::unique_ptr<CuvsWorker> Worker;
    std::shared_mutex mutex_; // Mutex to protect Load() and Search()
    bool is_loaded_ = false;

    ~GpuBruteForceIndex() {
        Destroy();
    }

    GpuBruteForceIndex(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, cuvs::distance::DistanceType m,
                       uint32_t nthread, int device_id = 0)
        : Dimension(dimension), Count(static_cast<uint32_t>(count_vectors)), Metric(m), device_id_(device_id) {
        Worker = std::make_unique<CuvsWorker>(nthread);

        // Resize flattened_host_dataset and copy data from the flattened array
        flattened_host_dataset.resize(Count * Dimension); // Total elements
        std::copy(dataset_data, dataset_data + (Count * Dimension), flattened_host_dataset.begin());
    }

    void Load() {
        std::unique_lock<std::shared_mutex> lock(mutex_); // Acquire exclusive lock
        if (is_loaded_) return;

        std::promise<bool> init_complete_promise;
        std::future<bool> init_complete_future = init_complete_promise.get_future();

        auto init_fn = [&](RaftHandleWrapper& _) -> std::any {
            // Re-initialize handle with specific device_id
            RaftHandleWrapper handle(device_id_);

            if (flattened_host_dataset.empty()) { // Use new member
                Index = nullptr; // Ensure Index is null if no data
                init_complete_promise.set_value(true); // Signal completion even if empty
                return std::any();
            }

            auto dataset_device = raft::make_device_matrix<T, int64_t, raft::layout_c_contiguous>(
                *handle.get_raft_resources(), static_cast<int64_t>(Count), static_cast<int64_t>(Dimension));
            
            RAFT_CUDA_TRY(cudaMemcpyAsync(dataset_device.data_handle(), flattened_host_dataset.data(),
                                     flattened_host_dataset.size() * sizeof(T), cudaMemcpyHostToDevice,
                                     raft::resource::get_cuda_stream(*handle.get_raft_resources())));

            cuvs::neighbors::brute_force::index_params index_params; // Correct brute_force namespace
            index_params.metric = Metric;

            Index = std::make_unique<cuvs::neighbors::brute_force::index<T, float>>(
                cuvs::neighbors::brute_force::build(*handle.get_raft_resources(), index_params, raft::make_const_mdspan(dataset_device.view()))); // Use raft::make_const_mdspan

            raft::resource::sync_stream(*handle.get_raft_resources()); // Synchronize after build

            init_complete_promise.set_value(true); // Signal that initialization is complete
            return std::any();
        };
        auto stop_fn = [&](RaftHandleWrapper& handle) -> std::any {
            if (Index) { // Check if unique_ptr holds an object
                Index.reset();
            }
            return std::any();
        };
        Worker->Start(init_fn, stop_fn);

        init_complete_future.get(); // Wait for the init_fn to complete
        is_loaded_ = true;
    }

    struct SearchResult {
        std::vector<int64_t> Neighbors;
        std::vector<float> Distances;
    };

    SearchResult Search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, uint32_t limit) {
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
        size_t queries_cols = Dimension; // Use the class's Dimension

        uint64_t jobID = Worker->Submit(
            [&, queries_rows, queries_cols, limit](RaftHandleWrapper& _) -> std::any {
                RaftHandleWrapper handle(device_id_);
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

                cuvs::neighbors::brute_force::search_params search_params;
                cuvs::neighbors::brute_force::search(*handle.get_raft_resources(), search_params, *Index,
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

    void Destroy() {
        if (Worker) {
            Worker->Stop();
        }
    }
};

} // namespace matrixone
