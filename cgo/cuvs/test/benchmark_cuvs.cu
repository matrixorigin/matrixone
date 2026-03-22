/* 
 * Copyright 2021 Matrix Origin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "cagra.hpp"
#include "ivf_flat.hpp"
#include "ivf_pq.hpp"
#include "brute_force.hpp"
#include "helper.h"
#include <chrono>
#include <iostream>
#include <vector>
#include <random>
#include <thread>
#include <atomic>
#include <iomanip>
#include <string>
#include <cuda_fp16.h>

using namespace matrixone;

struct benchmark_config_t {
    uint32_t dimension = 1024;
    uint64_t n_vectors = 50000; 
    uint32_t n_queries = 1000;
    uint32_t limit = 10;
    uint32_t n_threads = 16;
    std::vector<int> devices = {0};
};

template<typename T> const char* type_name();
template<> const char* type_name<float>() { return "float32"; }
template<> const char* type_name<half>() { return "half"; }
template<> const char* type_name<int8_t>() { return "int8"; }
template<> const char* type_name<uint8_t>() { return "uint8"; }

std::vector<float> generate_random_data(uint64_t count, uint32_t dim) {
    std::vector<float> data(count * dim);
    std::mt19937 gen(42);
    // Use a wider range to have more signal for int8 benchmarks
    std::uniform_real_distribution<float> dis(-100.0, 100.0);
    for (size_t i = 0; i < data.size(); ++i) {
        data[i] = dis(gen);
    }
    return data;
}

template<typename NeighborT>
double calculate_recall(const std::vector<NeighborT>& neighbors, uint32_t n_queries, uint32_t limit) {
    int hit_count = 0;
    for (uint32_t i = 0; i < n_queries; ++i) {
        bool found = false;
        for (uint32_t j = 0; j < limit; ++j) {
            if (static_cast<int64_t>(neighbors[i * limit + j]) == static_cast<int64_t>(i)) {
                found = true;
                break;
            }
        }
        if (found) hit_count++;
    }
    return static_cast<double>(hit_count) / n_queries;
}

const char* mode_name(distribution_mode_t mode) {
    switch (mode) {
        case DistributionMode_SHARDED: return "Sharded";
        case DistributionMode_REPLICATED: return "Replicated";
        default: return "Single";
    }
}

template<typename T>
std::vector<T> convert_dataset(const std::vector<float>& src, uint64_t n_vectors, uint32_t dim) {
    if constexpr (std::is_same_v<T, float>) {
        return src;
    } else if constexpr (std::is_same_v<T, half>) {
        std::vector<T> dst(src.size());
        for(size_t i = 0; i < src.size(); ++i) {
            dst[i] = __float2half(src[i]);
        }
        return dst;
    } else {
        std::vector<T> dst(src.size());
        for(size_t i = 0; i < src.size(); ++i) {
            dst[i] = static_cast<T>(std::round(src[i]));
        }
        return dst;
    }
}

template<typename IndexT, typename SearchParamsT, typename T>
void run_benchmark(const std::string& index_name, distribution_mode_t mode, 
                  IndexT& index, const std::vector<float>& dataset, const benchmark_config_t& cfg, const SearchParamsT& sp) {
    
    for (bool batching : {false}) {
        index.set_use_batching(batching);
        
        std::string full_name = index_name + "_" + mode_name(mode) + "_" + type_name<T>() + (batching ? "_BatchingON" : "_BatchingOFF");

        auto queries = generate_random_data(cfg.n_queries, cfg.dimension);
        
        // Warmup
        for (int i = 0; i < 5; ++i) {
            index.search_float(queries.data(), 1, cfg.dimension, cfg.limit, sp);
        }

        std::atomic<uint64_t> total_completed{0};
        auto start = std::chrono::high_resolution_clock::now();

        std::vector<std::thread> threads;
        uint32_t q_per_thread = cfg.n_queries / cfg.n_threads;
        for (uint32_t t = 0; t < cfg.n_threads; ++t) {
            threads.emplace_back([&, t, q_per_thread]() {
                for (uint32_t i = 0; i < q_per_thread; ++i) {
                    index.search_float(queries.data() + (t * q_per_thread + i) * cfg.dimension, 1, cfg.dimension, cfg.limit, sp);
                    total_completed++;
                }
            });
        }
        for (auto& t : threads) t.join();

        auto end = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> diff = end - start;

        double qps = total_completed.load() / diff.count();
        
        // Self-recall
        auto recall_queries = dataset.data(); // Use first n_queries from dataset
        auto res = index.search_float(recall_queries, cfg.n_queries, cfg.dimension, cfg.limit, sp);
        double recall = calculate_recall(res.neighbors, cfg.n_queries, cfg.limit);

        std::cout << std::left << std::setw(45) << full_name 
                  << ": QPS=" << std::fixed << std::setprecision(2) << std::right << std::setw(10) << qps 
                  << ", Recall=" << std::setprecision(4) << recall << std::endl;
    }
}

template<typename T>
void benchmark_all_indices(const std::vector<float>& dataset, const benchmark_config_t& cfg) {
    auto converted = convert_dataset<T>(dataset, cfg.n_vectors, cfg.dimension);

    std::vector<distribution_mode_t> modes = {DistributionMode_REPLICATED};

    // CAGRA
    {
        cagra_build_params_t bp = cagra_build_params_default();
        bp.intermediate_graph_degree = 256;
        bp.graph_degree = 128;
        
        for (auto mode : modes) {
            std::vector<int> active_devices = (mode == DistributionMode_SINGLE_GPU) ? 
                                              std::vector<int>{cfg.devices[0]} : cfg.devices;
            
            if (mode != DistributionMode_SINGLE_GPU && active_devices.size() < 2) continue;
            
            gpu_cagra_t<T> index(converted.data(), cfg.n_vectors, cfg.dimension, DistanceType_L2Expanded, bp, active_devices, cfg.n_threads, mode);
            index.start();
            index.build();
            
            cagra_search_params_t sp = cagra_search_params_default();
            sp.itopk_size = 128;
            run_benchmark<gpu_cagra_t<T>, cagra_search_params_t, T>("Cagra", mode, index, dataset, cfg, sp);
            index.destroy();
        }
    }

    // IVF-Flat
    {
        ivf_flat_build_params_t bp = ivf_flat_build_params_default();
        bp.n_lists = 1024;
        
        for (auto mode : modes) {
            std::vector<int> active_devices = (mode == DistributionMode_SINGLE_GPU) ? 
                                              std::vector<int>{cfg.devices[0]} : cfg.devices;

            if (mode != DistributionMode_SINGLE_GPU && active_devices.size() < 2) continue;

            gpu_ivf_flat_t<T> index(converted.data(), cfg.n_vectors, cfg.dimension, DistanceType_L2Expanded, bp, active_devices, cfg.n_threads, mode);
            index.start();
            index.build();

            ivf_flat_search_params_t sp = ivf_flat_search_params_default();
            sp.n_probes = 64;
            run_benchmark<gpu_ivf_flat_t<T>, ivf_flat_search_params_t, T>("IvfFlat", mode, index, dataset, cfg, sp);
            index.destroy();
        }
    }

    // IVF-PQ
    {
        ivf_pq_build_params_t bp = ivf_pq_build_params_default();
        bp.n_lists = 1024;
        bp.m = 64;
        
        for (auto mode : modes) {
            std::vector<int> active_devices = (mode == DistributionMode_SINGLE_GPU) ? 
                                              std::vector<int>{cfg.devices[0]} : cfg.devices;

            if (mode != DistributionMode_SINGLE_GPU && active_devices.size() < 2) continue;

            gpu_ivf_pq_t<T> index(converted.data(), cfg.n_vectors, cfg.dimension, DistanceType_L2Expanded, bp, active_devices, cfg.n_threads, mode);
            index.start();
            index.build();

            ivf_pq_search_params_t sp = ivf_pq_search_params_default();
            sp.n_probes = 64;
            run_benchmark<gpu_ivf_pq_t<T>, ivf_pq_search_params_t, T>("IvfPq", mode, index, dataset, cfg, sp);
            index.destroy();
        }
    }
}

int main() {
    benchmark_config_t cfg;
    
    int dev_count = 0;
    cudaGetDeviceCount(&dev_count);
    if (dev_count > 1) {
        cfg.devices.clear();
        for (int i = 0; i < std::min(dev_count, 4); ++i) cfg.devices.push_back(i);
    }

    std::cout << "Generating dataset (" << cfg.n_vectors << " vectors, " << cfg.dimension << " dim, " << cfg.devices.size() << " GPUs)..." << std::endl;
    auto dataset = generate_random_data(cfg.n_vectors, cfg.dimension);

    std::cout << "Starting Benchmarks (T=" << cfg.n_threads << " threads)" << std::endl;
    std::cout << "--------------------------------------------------------------------------------" << std::endl;

    benchmark_all_indices<float>(dataset, cfg);
    benchmark_all_indices<half>(dataset, cfg);
    benchmark_all_indices<int8_t>(dataset, cfg);

    return 0;
}
