#pragma once

#include "cuvs_worker.hpp"
#include <raft/util/cudart_utils.hpp>

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
#include <raft/core/device_mdarray.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/resources.hpp>
#include <cuvs/distance/distance.hpp>
#include <cuvs/neighbors/ivf_flat.hpp>
#pragma GCC diagnostic pop

namespace matrixone {

/**
 * @brief GpuShardedIvfFlatIndex implements a sharded IVF-Flat index across multiple GPUs on a single node.
 * It uses the cuVS Multi-GPU (SNMG) API.
 */
template <typename T>
class GpuShardedIvfFlatIndex {
    static_assert(std::is_floating_point<T>::value, "T must be a floating-point type.");

public:
    using IvfFlatIndex = cuvs::neighbors::ivf_flat::index<T, int64_t>;
    using MgIndex = cuvs::neighbors::mg_index<IvfFlatIndex, T, int64_t>;

    std::vector<T> flattened_host_dataset;
    std::vector<int> devices_;
    std::string filename_;
    std::unique_ptr<MgIndex> Index;
    cuvs::distance::DistanceType Metric;
    uint32_t Dimension;
    uint32_t Count;
    uint32_t NList;
    std::unique_ptr<CuvsWorker> Worker;
    std::shared_mutex mutex_;
    bool is_loaded_ = false;

    ~GpuShardedIvfFlatIndex() {
        Destroy();
    }

    // Constructor for building from dataset across multiple GPUs
    GpuShardedIvfFlatIndex(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                           cuvs::distance::DistanceType m, uint32_t n_list, 
                           const std::vector<int>& devices, uint32_t nthread)
        : Dimension(dimension), Count(static_cast<uint32_t>(count_vectors)), Metric(m), 
          NList(n_list), devices_(devices) {
        Worker = std::make_unique<CuvsWorker>(nthread, devices_);

        flattened_host_dataset.resize(Count * Dimension);
        std::copy(dataset_data, dataset_data + (Count * Dimension), flattened_host_dataset.begin());
    }

    // Constructor for loading from file (multi-GPU)
    GpuShardedIvfFlatIndex(const std::string& filename, uint32_t dimension, 
                           cuvs::distance::DistanceType m, const std::vector<int>& devices, uint32_t nthread)
        : filename_(filename), Dimension(dimension), Metric(m), Count(0), NList(0), devices_(devices) {
        Worker = std::make_unique<CuvsWorker>(nthread, devices_);
    }

    void Load() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (is_loaded_) return;

        std::promise<bool> init_complete_promise;
        std::future<bool> init_complete_future = init_complete_promise.get_future();

        auto init_fn = [&](RaftHandleWrapper& handle) -> std::any {
            auto clique = handle.get_raft_resources();

            if (!filename_.empty()) {
                // Load MG index from file
                Index = std::make_unique<MgIndex>(
                    cuvs::neighbors::ivf_flat::deserialize<T, int64_t>(*clique, filename_));
                raft::resource::sync_stream(*clique);
                
                // Update metadata
                Count = 0;
                for (const auto& iface : Index->ann_interfaces_) {
                    if (iface.index_.has_value()) {
                        Count += static_cast<uint32_t>(iface.index_.value().size());
                    }
                }
                
                if (!Index->ann_interfaces_.empty() && Index->ann_interfaces_[0].index_.has_value()) {
                    NList = static_cast<uint32_t>(Index->ann_interfaces_[0].index_.value().n_lists());
                }
            } else if (!flattened_host_dataset.empty()) {
                // Build sharded index from host dataset
                auto dataset_host_view = raft::make_host_matrix_view<const T, int64_t>(
                    flattened_host_dataset.data(), (int64_t)Count, (int64_t)Dimension);

                cuvs::neighbors::ivf_flat::index_params index_params;
                index_params.metric = Metric;
                index_params.n_lists = NList;

                cuvs::neighbors::mg_index_params<cuvs::neighbors::ivf_flat::index_params> mg_params(index_params);
                mg_params.mode = cuvs::neighbors::distribution_mode::SHARDED;

                Index = std::make_unique<MgIndex>(
                    cuvs::neighbors::ivf_flat::build(*clique, mg_params, dataset_host_view));

                raft::resource::sync_stream(*clique);
            }

            init_complete_promise.set_value(true);
            return std::any();
        };

        auto stop_fn = [&](RaftHandleWrapper& handle) -> std::any {
            if (Index) Index.reset();
            return std::any();
        };

        Worker->Start(init_fn, stop_fn);
        init_complete_future.get();
        is_loaded_ = true;
    }

    void Save(const std::string& filename) {
        if (!is_loaded_ || !Index) throw std::runtime_error("Index not loaded");

        uint64_t jobID = Worker->Submit(
            [&](RaftHandleWrapper& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                cuvs::neighbors::ivf_flat::serialize(*handle.get_raft_resources(), *Index, filename);
                raft::resource::sync_stream(*handle.get_raft_resources());
                return std::any();
            }
        );

        CuvsTaskResult result = Worker->Wait(jobID).get();
        if (result.Error) std::rethrow_exception(result.Error);
    }

    struct SearchResult {
        std::vector<int64_t> Neighbors;
        std::vector<float> Distances;
    };

    SearchResult Search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, 
                        uint32_t limit, uint32_t n_probes) {
        if (!queries_data || num_queries == 0 || !Index) return SearchResult{};
        if (query_dimension != Dimension) throw std::runtime_error("Dimension mismatch");

        uint64_t jobID = Worker->Submit(
            [&, num_queries, limit, n_probes](RaftHandleWrapper& handle) -> std::any {
                auto clique = handle.get_raft_resources();
                std::shared_lock<std::shared_mutex> lock(mutex_);

                auto queries_host_view = raft::make_host_matrix_view<const T, int64_t>(
                    queries_data, (int64_t)num_queries, (int64_t)Dimension);

                SearchResult res;
                res.Neighbors.resize(num_queries * limit);
                res.Distances.resize(num_queries * limit);

                auto neighbors_host_view = raft::make_host_matrix_view<int64_t, int64_t>(
                    res.Neighbors.data(), (int64_t)num_queries, (int64_t)limit);
                auto distances_host_view = raft::make_host_matrix_view<float, int64_t>(
                    res.Distances.data(), (int64_t)num_queries, (int64_t)limit);

                cuvs::neighbors::ivf_flat::search_params search_params;
                search_params.n_probes = n_probes;

                cuvs::neighbors::mg_search_params<cuvs::neighbors::ivf_flat::search_params> mg_search_params(search_params);

                cuvs::neighbors::ivf_flat::search(*clique, *Index, mg_search_params,
                                                   queries_host_view, neighbors_host_view, distances_host_view);

                raft::resource::sync_stream(*clique);

                for (size_t i = 0; i < res.Neighbors.size(); ++i) {
                    if (res.Neighbors[i] == std::numeric_limits<int64_t>::max() || 
                        res.Neighbors[i] == 4294967295LL || res.Neighbors[i] < 0) {
                        res.Neighbors[i] = -1;
                    }
                }
                return res;
            }
        );

        CuvsTaskResult result = Worker->Wait(jobID).get();
        if (result.Error) std::rethrow_exception(result.Error);
        return std::any_cast<SearchResult>(result.Result);
    }

    std::vector<T> GetCenters() {
        if (!is_loaded_ || !Index) return {};

        uint64_t jobID = Worker->Submit(
            [&](RaftHandleWrapper& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                const IvfFlatIndex* local_index = nullptr;
                for (const auto& iface : Index->ann_interfaces_) {
                    if (iface.index_.has_value()) {
                        local_index = &iface.index_.value();
                        break;
                    }
                }

                if (!local_index) return std::vector<T>{};

                auto centers_view = local_index->centers();
                size_t n_centers = centers_view.extent(0);
                size_t dim = centers_view.extent(1);
                std::vector<T> host_centers(n_centers * dim);

                RAFT_CUDA_TRY(cudaMemcpy(host_centers.data(), centers_view.data_handle(),
                                         host_centers.size() * sizeof(T), cudaMemcpyDeviceToHost));

                return host_centers;
            }
        );

        CuvsTaskResult result = Worker->Wait(jobID).get();
        if (result.Error) std::rethrow_exception(result.Error);
        return std::any_cast<std::vector<T>>(result.Result);
    }

    void Destroy() {
        if (Worker) Worker->Stop();
    }
};

} // namespace matrixone
