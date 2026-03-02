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
#include <cuvs/neighbors/ivf_flat.hpp>
#pragma GCC diagnostic pop

namespace matrixone {

/**
 * @brief gpu_sharded_ivf_flat_index_t implements a sharded IVF-Flat index across multiple GPUs on a single node.
 * It uses the cuVS Multi-GPU (SNMG) API.
 */
template <typename T>
class gpu_sharded_ivf_flat_index_t {
public:
    using ivf_flat_index = cuvs::neighbors::ivf_flat::index<T, int64_t>;
    using mg_index = cuvs::neighbors::mg_index<ivf_flat_index, T, int64_t>;

    std::vector<T> flattened_host_dataset;
    std::vector<int> devices_;
    std::string filename_;
    std::unique_ptr<mg_index> index;
    std::unique_ptr<raft_handle_wrapper_t> snmg_handle_; // Persistent SNMG handle
    cuvs::distance::DistanceType metric;
    uint32_t dimension;
    uint32_t count;
    uint32_t n_list;
    int device_id_;
    std::unique_ptr<cuvs_worker_t> worker;
    std::shared_mutex mutex_;
    bool is_loaded_ = false;

    ~gpu_sharded_ivf_flat_index_t() {
        destroy();
    }

    // Constructor for building from dataset across multiple GPUs
    gpu_sharded_ivf_flat_index_t(const T* dataset_data, uint64_t count_vectors, uint32_t dimension, 
                           cuvs::distance::DistanceType m, uint32_t n_list, 
                           const std::vector<int>& devices, uint32_t nthread)
        : dimension(dimension), count(static_cast<uint32_t>(count_vectors)), metric(m), 
          n_list(n_list), devices_(devices) {
        worker = std::make_unique<cuvs_worker_t>(nthread, devices_);

        flattened_host_dataset.resize(count * dimension);
        std::copy(dataset_data, dataset_data + (count * dimension), flattened_host_dataset.begin());
    }

    // Constructor for loading from file (multi-GPU)
    gpu_sharded_ivf_flat_index_t(const std::string& filename, uint32_t dimension, 
                           cuvs::distance::DistanceType m, const std::vector<int>& devices, uint32_t nthread)
        : filename_(filename), dimension(dimension), metric(m), count(0), n_list(0), devices_(devices) {
        worker = std::make_unique<cuvs_worker_t>(nthread, devices_);
    }

    void load() {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (is_loaded_) return;

        std::promise<bool> init_complete_promise;
        std::future<bool> init_complete_future = init_complete_promise.get_future();

        auto init_fn = [&](raft_handle_wrapper_t& handle) -> std::any {
            auto clique = handle.get_raft_resources();

            if (!filename_.empty()) {
                // load MG index from file
                index = std::make_unique<mg_index>(
                    cuvs::neighbors::ivf_flat::deserialize<T, int64_t>(*clique, filename_));
                raft::resource::sync_stream(*clique);
                
                // Update metadata
                count = 0;
                for (const auto& iface : index->ann_interfaces_) {
                    if (iface.index_.has_value()) {
                        count += static_cast<uint32_t>(iface.index_.value().size());
                    }
                }
                
                if (!index->ann_interfaces_.empty() && index->ann_interfaces_[0].index_.has_value()) {
                    n_list = static_cast<uint32_t>(index->ann_interfaces_[0].index_.value().n_lists());
                }
            } else if (!flattened_host_dataset.empty()) {
                // DATASET SIZE CHECK
                if (count < n_list) {
                    throw std::runtime_error("Dataset too small: count (" + std::to_string(count) + 
                                            ") must be >= n_list (" + std::to_string(n_list) + 
                                            ") to build IVF index.");
                }

                // Build sharded index from host dataset
                auto dataset_host_view = raft::make_host_matrix_view<const T, int64_t>(
                    flattened_host_dataset.data(), (int64_t)count, (int64_t)dimension);

                cuvs::neighbors::ivf_flat::index_params index_params;
                index_params.metric = metric;
                index_params.n_lists = n_list;

                cuvs::neighbors::mg_index_params<cuvs::neighbors::ivf_flat::index_params> mg_params(index_params);
                mg_params.mode = cuvs::neighbors::distribution_mode::SHARDED;

                index = std::make_unique<mg_index>(
                    cuvs::neighbors::ivf_flat::build(*clique, mg_params, dataset_host_view));

                raft::resource::sync_stream(*clique);
            }

            init_complete_promise.set_value(true);
            return std::any();
        };

        auto stop_fn = [&](raft_handle_wrapper_t& handle) -> std::any {
            if (index) index.reset();
            return std::any();
        };

        worker->start(init_fn, stop_fn);
        init_complete_future.get();
        is_loaded_ = true;
    }

    void save(const std::string& filename) {
        if (!is_loaded_ || !index) throw std::runtime_error("index not loaded");

        uint64_t job_id = worker->submit(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                cuvs::neighbors::ivf_flat::serialize(*handle.get_raft_resources(), *index, filename);
                raft::resource::sync_stream(*handle.get_raft_resources());
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
                        uint32_t limit, uint32_t n_probes) {
        if (!queries_data || num_queries == 0 || !index) return search_result_t{};
        if (query_dimension != dimension) throw std::runtime_error("dimension mismatch");

        uint64_t job_id = worker->submit(
            [&, num_queries, limit, n_probes](raft_handle_wrapper_t& handle) -> std::any {
                auto clique = handle.get_raft_resources();
                std::shared_lock<std::shared_mutex> lock(mutex_);

                auto queries_host_view = raft::make_host_matrix_view<const T, int64_t>(
                    queries_data, (int64_t)num_queries, (int64_t)dimension);

                search_result_t res;
                res.neighbors.resize(num_queries * limit);
                res.distances.resize(num_queries * limit);

                auto neighbors_host_view = raft::make_host_matrix_view<int64_t, int64_t>(
                    res.neighbors.data(), (int64_t)num_queries, (int64_t)limit);
                auto distances_host_view = raft::make_host_matrix_view<float, int64_t>(
                    res.distances.data(), (int64_t)num_queries, (int64_t)limit);

                cuvs::neighbors::ivf_flat::search_params search_params;
                search_params.n_probes = n_probes;

                cuvs::neighbors::mg_search_params<cuvs::neighbors::ivf_flat::search_params> mg_search_params(search_params);

                cuvs::neighbors::ivf_flat::search(*clique, *index, mg_search_params,
                                                   queries_host_view, neighbors_host_view, distances_host_view);

                raft::resource::sync_stream(*clique);

                for (size_t i = 0; i < res.neighbors.size(); ++i) {
                    if (res.neighbors[i] == std::numeric_limits<int64_t>::max() || 
                        res.neighbors[i] == 4294967295LL || res.neighbors[i] < 0) {
                        res.neighbors[i] = -1;
                    }
                }
                return res;
            }
        );

        cuvs_task_result_t result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<search_result_t>(result.result);
    }

    std::vector<T> get_centers() {
        if (!is_loaded_ || !index) return {};

        uint64_t job_id = worker->submit(
            [&](raft_handle_wrapper_t& handle) -> std::any {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                const ivf_flat_index* local_index = nullptr;
                for (const auto& iface : index->ann_interfaces_) {
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

        cuvs_task_result_t result = worker->wait(job_id).get();
        if (result.error) std::rethrow_exception(result.error);
        return std::any_cast<std::vector<T>>(result.result);
    }

    void destroy() {
        if (worker) worker->stop();
    }
};

} // namespace matrixone
