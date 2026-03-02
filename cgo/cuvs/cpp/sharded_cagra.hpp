#pragma once

#include "cuvs_worker.hpp"
#include <raft/util/cudart_utils.hpp>
#include <cuda_fp16.h> // For half

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
#include <raft/core/copy.cuh>            // For raft::copy with type conversion
#include <cuvs/distance/distance.hpp>
#include <cuvs/neighbors/cagra.hpp>
#pragma GCC diagnostic pop

namespace matrixone {

/**
 * @brief GpuShardedCagraIndex implements a sharded CAGRA index across multiple GPUs on a single node.
 * It uses the cuVS Multi-GPU (SNMG) API.
 */
template <typename T>
class GpuShardedCagraIndex {
public:
    using CagraIndex = cuvs::neighbors::cagra::index<T, uint32_t>;
    using MgIndex = cuvs::neighbors::mg_index<CagraIndex, T, uint32_t>;

    std::vector<T> flattened_host_dataset;
    std::vector<int> devices_;
    std::string filename_;
    std::unique_ptr<MgIndex> Index;
    cuvs::distance::DistanceType Metric;
    uint32_t Dimension;
    uint32_t Count;
    size_t IntermediateGraphDegree;
    size_t GraphDegree;
    std::unique_ptr<CuvsWorker> Worker;
    std::shared_mutex mutex_;
    bool is_loaded_ = false;

    ~GpuShardedCagraIndex() {
        Destroy();
    }

    // Constructor for building from dataset across multiple GPUs
    GpuShardedCagraIndex(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                         cuvs::distance::DistanceType m, size_t intermediate_graph_degree, 
                         size_t graph_degree, const std::vector<int>& devices, uint32_t nthread)
        : Dimension(dimension), Count(static_cast<uint32_t>(count_vectors)), Metric(m), 
          IntermediateGraphDegree(intermediate_graph_degree), GraphDegree(graph_degree), devices_(devices) {
        Worker = std::make_unique<CuvsWorker>(nthread, devices_);

        flattened_host_dataset.resize(Count * Dimension);
        std::copy(dataset_data, dataset_data + (Count * Dimension), flattened_host_dataset.begin());
    }

    // Constructor for loading from file (multi-GPU)
    GpuShardedCagraIndex(const std::string& filename, uint32_t dimension, 
                         cuvs::distance::DistanceType m, const std::vector<int>& devices, uint32_t nthread)
        : filename_(filename), Dimension(dimension), Metric(m), Count(0), IntermediateGraphDegree(0), GraphDegree(0), devices_(devices) {
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
                    cuvs::neighbors::cagra::deserialize<T, uint32_t>(*clique, filename_));
                raft::resource::sync_stream(*clique);
                
                // Update metadata
                Count = 0;
                for (const auto& iface : Index->ann_interfaces_) {
                    if (iface.index_.has_value()) {
                        Count += static_cast<uint32_t>(iface.index_.value().size());
                    }
                }
                
                if (!Index->ann_interfaces_.empty() && Index->ann_interfaces_[0].index_.has_value()) {
                    GraphDegree = static_cast<size_t>(Index->ann_interfaces_[0].index_.value().graph_degree());
                }
            } else if (!flattened_host_dataset.empty()) {
                // Build sharded index from host dataset
                auto dataset_host_view = raft::make_host_matrix_view<const T, int64_t>(
                    flattened_host_dataset.data(), (int64_t)Count, (int64_t)Dimension);

                cuvs::neighbors::cagra::index_params index_params;
                index_params.metric = Metric;
                index_params.intermediate_graph_degree = IntermediateGraphDegree;
                index_params.graph_degree = GraphDegree;

                cuvs::neighbors::mg_index_params<cuvs::neighbors::cagra::index_params> mg_params(index_params);
                mg_params.mode = cuvs::neighbors::distribution_mode::SHARDED;

                Index = std::make_unique<MgIndex>(
                    cuvs::neighbors::cagra::build(*clique, mg_params, dataset_host_view));

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
                cuvs::neighbors::cagra::serialize(*handle.get_raft_resources(), *Index, filename);
                raft::resource::sync_stream(*handle.get_raft_resources());
                return std::any();
            }
        );

        CuvsTaskResult result = Worker->Wait(jobID).get();
        if (result.Error) std::rethrow_exception(result.Error);
    }

    struct SearchResult {
        std::vector<uint32_t> Neighbors;
        std::vector<float> Distances;
    };

    SearchResult Search(const T* queries_data, uint64_t num_queries, uint32_t query_dimension, 
                        uint32_t limit, size_t itopk_size) {
        if (!queries_data || num_queries == 0 || !Index) return SearchResult{};
        if (query_dimension != Dimension) throw std::runtime_error("Dimension mismatch");

        uint64_t jobID = Worker->Submit(
            [&, num_queries, limit, itopk_size](RaftHandleWrapper& handle) -> std::any {
                auto clique = handle.get_raft_resources();
                std::shared_lock<std::shared_mutex> lock(mutex_);

                auto queries_host_view = raft::make_host_matrix_view<const T, int64_t>(
                    queries_data, (int64_t)num_queries, (int64_t)Dimension);

                SearchResult res;
                res.Neighbors.resize(num_queries * limit);
                res.Distances.resize(num_queries * limit);

                auto neighbors_host_view = raft::make_host_matrix_view<uint32_t, int64_t>(
                    res.Neighbors.data(), (int64_t)num_queries, (int64_t)limit);
                auto distances_host_view = raft::make_host_matrix_view<float, int64_t>(
                    res.Distances.data(), (int64_t)num_queries, (int64_t)limit);

                cuvs::neighbors::cagra::search_params search_params;
                search_params.itopk_size = itopk_size;

                cuvs::neighbors::mg_search_params<cuvs::neighbors::cagra::search_params> mg_search_params(search_params);

                cuvs::neighbors::cagra::search(*clique, *Index, mg_search_params,
                                                   queries_host_view, neighbors_host_view, distances_host_view);

                raft::resource::sync_stream(*clique);

                for (size_t i = 0; i < res.Neighbors.size(); ++i) {
                    if (res.Neighbors[i] == std::numeric_limits<uint32_t>::max()) {
                        res.Neighbors[i] = static_cast<uint32_t>(-1);
                    }
                }
                return res;
            }
        );

        CuvsTaskResult result = Worker->Wait(jobID).get();
        if (result.Error) std::rethrow_exception(result.Error);
        return std::any_cast<SearchResult>(result.Result);
    }

    void Destroy() {
        if (Worker) Worker->Stop();
    }
};

} // namespace matrixone
