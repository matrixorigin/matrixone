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

// Single-GPU float32 benchmark for filtered search on CAGRA / IVF-Flat / IVF-PQ.
// Sweeps selectivity via an IN predicate on a categorical INT64 column and
// reports QPS, mean per-query latency, and self-recall for each index type.
//
// "Self-recall" here: queries are sampled from the dataset, so the expected
// top-1 is the query's own row id — but only when that row's category is in
// the allowed set. Queries whose ground-truth row is filtered out are skipped
// from the recall denominator (they have no valid ground truth under the
// predicate).

#include "cagra.hpp"
#include "ivf_flat.hpp"
#include "ivf_pq.hpp"
#include "helper.h"

#include <atomic>
#include <chrono>
#include <cuda_runtime.h>
#include <iomanip>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <utility>
#include <vector>

using namespace matrixone;

namespace {

struct bench_cfg_t {
    uint32_t dimension = 1024;
    uint64_t n_vectors = 50000;
    uint32_t n_queries = 1000;
    uint32_t limit     = 10;
    uint32_t n_threads = 16;
    uint32_t warmup    = 5;
    int      device    = 0;
    int64_t  n_cats    = 10;  // cardinality of filter column
};

std::vector<float> gen_dataset(uint64_t n, uint32_t dim, uint32_t seed) {
    std::vector<float> out(n * dim);
    std::mt19937 rng(seed);
    std::uniform_real_distribution<float> dist(-100.0f, 100.0f);
    for (auto& x : out) x = dist(rng);
    return out;
}

std::vector<int64_t> gen_categories(uint64_t n, int64_t n_cats) {
    std::vector<int64_t> cats(n);
    for (uint64_t i = 0; i < n; ++i) cats[i] = static_cast<int64_t>(i % n_cats);
    return cats;
}

// Predicate "cat IN [0..k)" — rows with category < k pass. k=0 → empty string
// (unfiltered baseline, which skips the bitset path entirely).
std::string make_in_preds(int64_t k) {
    if (k <= 0) return "";
    std::string s = "[{\"col\":0,\"op\":\"in\",\"vals\":[";
    for (int64_t i = 0; i < k; ++i) {
        if (i) s += ",";
        s += std::to_string(i);
    }
    s += "]}]";
    return s;
}

struct recall_t { double recall; uint32_t n_valid; };

template <typename NeighborT>
recall_t self_recall(const std::vector<NeighborT>& neighbors,
                     const std::vector<int64_t>& expected_ids,
                     const std::vector<int64_t>& cats,
                     int64_t allowed_k,
                     uint32_t n_queries, uint32_t limit) {
    uint32_t hits = 0, valid = 0;
    for (uint32_t q = 0; q < n_queries; ++q) {
        int64_t eid = expected_ids[q];
        if (allowed_k > 0 && cats[eid] >= allowed_k) continue;
        ++valid;
        for (uint32_t j = 0; j < limit; ++j) {
            if (static_cast<int64_t>(neighbors[q * limit + j]) == eid) {
                ++hits;
                break;
            }
        }
    }
    return {valid ? static_cast<double>(hits) / valid : 0.0, valid};
}

template <typename Index, typename SP>
std::pair<double, double> run_throughput(Index& index,
                                         const std::vector<float>& queries,
                                         const bench_cfg_t& cfg, const SP& sp,
                                         const std::string& preds_json) {
    std::atomic<uint64_t> total{0};
    std::atomic<uint64_t> total_ns{0};
    uint32_t per_thread = cfg.n_queries / cfg.n_threads;

    auto start = std::chrono::high_resolution_clock::now();
    std::vector<std::thread> pool;
    for (uint32_t t = 0; t < cfg.n_threads; ++t) {
        pool.emplace_back([&, t, per_thread]() {
            for (uint32_t i = 0; i < per_thread; ++i) {
                auto t0 = std::chrono::high_resolution_clock::now();
                (void)index.search_float_with_filter(
                    queries.data() + (t * per_thread + i) * cfg.dimension,
                    1, cfg.dimension, cfg.limit, sp, preds_json);
                auto t1 = std::chrono::high_resolution_clock::now();
                total_ns.fetch_add(
                    std::chrono::duration_cast<std::chrono::nanoseconds>(t1 - t0).count(),
                    std::memory_order_relaxed);
                total.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }
    for (auto& th : pool) th.join();
    auto end = std::chrono::high_resolution_clock::now();

    std::chrono::duration<double> diff = end - start;
    double qps = total.load() / diff.count();
    double mean_us = total.load()
        ? total_ns.load() / static_cast<double>(total.load()) / 1000.0
        : 0.0;
    return {qps, mean_us};
}

template <typename Index, typename SP>
void sweep_selectivities(const std::string& tag, Index& index, const SP& sp,
                         const std::vector<float>& recall_queries,
                         const std::vector<int64_t>& recall_expected_ids,
                         const std::vector<int64_t>& cats,
                         const std::vector<float>& throughput_queries,
                         const bench_cfg_t& cfg) {
    // allowed_k: 0 (empty preds → unfiltered baseline), 1 (10%), 5 (50%), 9 (90%).
    const std::vector<int64_t> ks = {0, 1, 5, 9};
    // batch_window_us: 0 = batching off, 1000 = 1ms batching window.
    const std::vector<int64_t> batch_windows = {0, 1000};

    for (auto window_us : batch_windows) {
        index.set_batch_window(window_us);

        for (uint32_t w = 0; w < cfg.warmup; ++w) {
            (void)index.search_float_with_filter(throughput_queries.data(), 1,
                                                 cfg.dimension, cfg.limit, sp, "");
        }

        std::string full_tag = tag +
            (window_us > 0 ? "/batch" + std::to_string(window_us) : "/nobatch");

        for (auto k : ks) {
            auto preds = make_in_preds(k);
            double sel = (k == 0) ? 1.0 : static_cast<double>(k) / cfg.n_cats;

            auto qt  = run_throughput(index, throughput_queries, cfg, sp, preds);
            double qps    = qt.first;
            double lat_us = qt.second;

            auto res = index.search_float_with_filter(
                recall_queries.data(), cfg.n_queries, cfg.dimension, cfg.limit, sp, preds);
            auto r = self_recall(res.neighbors, recall_expected_ids, cats, k,
                                 cfg.n_queries, cfg.limit);

            std::cout << std::left  << std::setw(20) << full_tag
                      << "  sel="   << std::fixed << std::setprecision(2)
                                    << std::setw(5) << sel
                      << "  QPS="   << std::setprecision(1)
                                    << std::setw(9) << std::right << qps
                      << "  mean_us=" << std::setprecision(1)
                                      << std::setw(7) << lat_us
                      << "  recall@" << cfg.limit << "="
                                     << std::setprecision(4) << r.recall
                      << "  (n="    << r.n_valid << ")"
                      << std::endl;
        }
    }
}

}  // namespace

int main() {
    bench_cfg_t cfg;

    int dev_count = 0;
    cudaGetDeviceCount(&dev_count);
    if (dev_count <= cfg.device) {
        std::cerr << "No CUDA device " << cfg.device
                  << " available (found " << dev_count << ")" << std::endl;
        return 1;
    }

    std::cout << "Filtered-search benchmark (single GPU, float32)\n"
              << "  N=" << cfg.n_vectors
              << "  dim=" << cfg.dimension
              << "  queries=" << cfg.n_queries
              << "  threads=" << cfg.n_threads
              << "  n_cats=" << cfg.n_cats << std::endl;

    auto dataset = gen_dataset(cfg.n_vectors, cfg.dimension, 42);
    auto cats    = gen_categories(cfg.n_vectors, cfg.n_cats);
    auto throughput_queries = gen_dataset(cfg.n_queries, cfg.dimension, 7);

    // Recall queries sampled from the dataset (evenly-spaced rows) so each
    // query's expected top-1 is its own row id.
    std::vector<float>   recall_queries;
    std::vector<int64_t> recall_expected_ids;
    recall_queries.reserve(cfg.n_queries * cfg.dimension);
    recall_expected_ids.reserve(cfg.n_queries);
    for (uint32_t q = 0; q < cfg.n_queries; ++q) {
        uint64_t row = (static_cast<uint64_t>(q) * cfg.n_vectors) / cfg.n_queries;
        recall_expected_ids.push_back(static_cast<int64_t>(row));
        for (uint32_t d = 0; d < cfg.dimension; ++d) {
            recall_queries.push_back(dataset[row * cfg.dimension + d]);
        }
    }

    std::cout << std::string(96, '-') << std::endl;
    std::cout << "Filter column: cat INT64 in [0," << cfg.n_cats
              << "); predicate is cat IN [0..k). "
              << "selectivity 1.00 = unfiltered (empty preds_json)." << std::endl;
    std::cout << std::string(96, '-') << std::endl;

    std::vector<int> devices = {cfg.device};

    {
        cagra_build_params_t bp = cagra_build_params_default();
        gpu_cagra_t<float> index(dataset.data(), cfg.n_vectors, cfg.dimension,
                                 DistanceType_L2Expanded, bp, devices,
                                 cfg.n_threads, DistributionMode_SINGLE_GPU);
        index.start();
        index.set_filter_columns("[{\"name\":\"cat\",\"type\":1}]", cfg.n_vectors);
        index.add_filter_chunk(0, cats.data(), cfg.n_vectors);
        index.build();

        cagra_search_params_t sp = cagra_search_params_default();
        sp.itopk_size   = 128;
        sp.search_width = 1;
        sweep_selectivities("CAGRA", index, sp, recall_queries, recall_expected_ids,
                            cats, throughput_queries, cfg);
        index.destroy();
        cudaDeviceSynchronize();
    }

    {
        ivf_flat_build_params_t bp = ivf_flat_build_params_default();
        bp.n_lists = 1024;
        gpu_ivf_flat_t<float> index(dataset.data(), cfg.n_vectors, cfg.dimension,
                                    DistanceType_L2Expanded, bp, devices,
                                    cfg.n_threads, DistributionMode_SINGLE_GPU);
        index.start();
        index.set_filter_columns("[{\"name\":\"cat\",\"type\":1}]", cfg.n_vectors);
        index.add_filter_chunk(0, cats.data(), cfg.n_vectors);
        index.build();

        ivf_flat_search_params_t sp = ivf_flat_search_params_default();
        sp.n_probes = 64;
        sweep_selectivities("IVF-Flat", index, sp, recall_queries, recall_expected_ids,
                            cats, throughput_queries, cfg);
        index.destroy();
        cudaDeviceSynchronize();
    }

    {
        ivf_pq_build_params_t bp = ivf_pq_build_params_default();
        bp.n_lists = 1024;
        bp.m       = 64;
        gpu_ivf_pq_t<float> index(dataset.data(), cfg.n_vectors, cfg.dimension,
                                  DistanceType_L2Expanded, bp, devices,
                                  cfg.n_threads, DistributionMode_SINGLE_GPU);
        index.start();
        index.set_filter_columns("[{\"name\":\"cat\",\"type\":1}]", cfg.n_vectors);
        index.add_filter_chunk(0, cats.data(), cfg.n_vectors);
        index.build();

        ivf_pq_search_params_t sp = ivf_pq_search_params_default();
        sp.n_probes = 64;
        sweep_selectivities("IVF-PQ", index, sp, recall_queries, recall_expected_ids,
                            cats, throughput_queries, cfg);
        index.destroy();
        cudaDeviceSynchronize();
    }

    return 0;
}
