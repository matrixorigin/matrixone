// SPDX-License-Identifier: Apache-2.0
// -----------------------------------------------------------------------------
// bench_hostview.cu — what does MO give up by uploading the dataset itself?
//
// MO builds an IVF-PQ index by allocating count*dim device memory, raft::copy-ing
// the host buffer up, and calling the DEVICE build overload
// (cgo/cuvs/ivf_pq.hpp:555-571). cuVS also accepts a raft::host_matrix_view, and
// that path is materially different inside:
//
//   * raft::matrix::sample_rows inspects the pointer (cudaPointerGetAttributes)
//     and gathers the k-means trainset ON THE HOST — only the trainset is
//     uploaded (raft/matrix/detail/sample_rows.cuh:47-63).
//   * build_impl ends with detail::extend(..., dataset.data_handle(), ...) and
//     extend streams through utils::batch_load_iterator with a batch size that
//     halves until it fits free memory (ivf_pq_build.cuh:1050-1097, :1374).
//
// So the host path trades one bulk PCIe transfer for many 65536-row batches.
// This measures that trade on three axes:
//
//   A) wall-clock build time            — is streaming slower, and by how much?
//   B) peak device memory               — how much does NOT having the dataset
//                                         resident actually save?
//   C) index equivalence                — do the two paths produce indexes that
//                                         answer queries the same way? (cuVS
//                                         seeds its sampler with a hardcoded
//                                         RngState{137}, so the sample is
//                                         deterministic, but the host and device
//                                         gather paths differ.)
//
// A fourth variant adds a CUDA stream pool to the raft resources: extend only
// sets enable_prefetch when a stream pool exists (ivf_pq_build.cuh:1086-1093),
// so without one the batch copies never overlap compute. MO's resources have no
// stream pool today — this quantifies that lever.
//
// Deliberately standalone: no matrixone headers, only cuVS / RAFT / RMM / CUDA /
// STL, same constraint as test_dynb.cu and test_ivfpq_filter.cu.
//
// Build:  make -C cgo/cuvs bench_hostview
// Run:    ./bench_hostview [max_rows]
// -----------------------------------------------------------------------------

#include <cuvs/neighbors/ivf_pq.hpp>

#include <raft/core/device_mdarray.hpp>
#include <raft/core/device_resources.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/mdspan.hpp>
#include <raft/core/resource/cuda_stream.hpp>

#include <rmm/cuda_stream_pool.hpp>
#include <rmm/device_uvector.hpp>

#include <cuda_runtime.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <random>
#include <string>
#include <thread>
#include <unordered_set>
#include <vector>

#define CUDA_CHECK(call)                                                            \
    do {                                                                            \
        cudaError_t _e = (call);                                                    \
        if (_e != cudaSuccess) {                                                    \
            std::fprintf(stderr, "CUDA error %s at %s:%d\n", cudaGetErrorString(_e), \
                         __FILE__, __LINE__);                                       \
            std::exit(1);                                                           \
        }                                                                           \
    } while (0)

namespace {

constexpr int64_t kDim     = 960;   // wiki_all
constexpr int64_t kQueries = 100;
constexpr uint32_t kTopK   = 10;

size_t free_bytes()
{
    size_t f = 0, t = 0;
    CUDA_CHECK(cudaMemGetInfo(&f, &t));
    return f;
}

// Samples cudaMemGetInfo on a side thread so we capture the transient peak
// (the trainset allocation, the batch staging buffers) rather than just the
// steady state after build returns.
class PeakWatcher {
   public:
    explicit PeakWatcher(int device) : device_(device), min_free_(free_bytes()), stop_(false)
    {
        thread_ = std::thread([this] {
            CUDA_CHECK(cudaSetDevice(device_));
            while (!stop_.load(std::memory_order_relaxed)) {
                size_t f = free_bytes();
                size_t cur = min_free_.load(std::memory_order_relaxed);
                while (f < cur && !min_free_.compare_exchange_weak(cur, f)) {}
                std::this_thread::sleep_for(std::chrono::milliseconds(2));
            }
        });
    }
    // Bytes consumed relative to the baseline captured at construction.
    size_t stop(size_t baseline)
    {
        stop_.store(true, std::memory_order_relaxed);
        thread_.join();
        size_t lo = min_free_.load();
        return lo < baseline ? baseline - lo : 0;
    }

   private:
    int device_;
    std::atomic<size_t> min_free_;
    std::atomic<bool> stop_;
    std::thread thread_;
};

cuvs::neighbors::ivf_pq::index_params make_params(int64_t n_rows)
{
    cuvs::neighbors::ivf_pq::index_params p;
    // 4*sqrt(N) is the sizing rule the MO plan uses for n_lists.
    p.n_lists                   = static_cast<uint32_t>(4.0 * std::sqrt((double)n_rows));
    p.metric                    = cuvs::distance::DistanceType::L2Expanded;
    p.kmeans_trainset_fraction  = 0.1;
    p.kmeans_n_iters            = 20;
    p.pq_bits                   = 8;
    p.pq_dim                    = 0;  // let cuVS pick
    p.add_data_on_build         = true;
    return p;
}

struct Result {
    double seconds;
    size_t peak_bytes;
};

// Path A — what MO does today: allocate device memory, copy the whole dataset up,
// hand cuVS a device view.
Result build_device(raft::device_resources& res, const std::vector<float>& host,
                    int64_t n_rows, cuvs::neighbors::ivf_pq::index<int64_t>** out)
{
    size_t baseline = free_bytes();
    PeakWatcher watch(0);
    auto t0 = std::chrono::steady_clock::now();

    auto stream = raft::resource::get_cuda_stream(res);
    rmm::device_uvector<float> d(static_cast<size_t>(n_rows) * kDim, stream);
    auto dev = raft::make_device_matrix_view<float, int64_t>(d.data(), n_rows, kDim);
    raft::copy(d.data(), host.data(), d.size(), stream);
    res.sync_stream();

    auto idx = cuvs::neighbors::ivf_pq::build(res, make_params(n_rows),
                                              raft::make_const_mdspan(dev));
    res.sync_stream();

    auto t1 = std::chrono::steady_clock::now();
    size_t peak = watch.stop(baseline);
    *out = new cuvs::neighbors::ivf_pq::index<int64_t>(std::move(idx));
    return {std::chrono::duration<double>(t1 - t0).count(), peak};
}

// Path B — hand cuVS the host buffer and let it sample on the host and stream
// the encode in batches.
Result build_host(raft::device_resources& res, const std::vector<float>& host,
                  int64_t n_rows, cuvs::neighbors::ivf_pq::index<int64_t>** out)
{
    size_t baseline = free_bytes();
    PeakWatcher watch(0);
    auto t0 = std::chrono::steady_clock::now();

    auto hv = raft::make_host_matrix_view<const float, int64_t>(host.data(), n_rows, kDim);
    auto idx = cuvs::neighbors::ivf_pq::build(res, make_params(n_rows), hv);
    res.sync_stream();

    auto t1 = std::chrono::steady_clock::now();
    size_t peak = watch.stop(baseline);
    *out = new cuvs::neighbors::ivf_pq::index<int64_t>(std::move(idx));
    return {std::chrono::duration<double>(t1 - t0).count(), peak};
}

// Absolute quality metric: each query is a small perturbation of a KNOWN data
// row, so the correct top-1 is that row's id. recall@k = fraction of queries
// whose seed row appears in the top k. This replaces an earlier A-vs-B neighbor
// overlap, which on uniform random data in 960 dims measured mostly noise —
// distances concentrate, so top-k is near-arbitrary and even an A-vs-A control
// scored badly. An absolute per-index score needs no control and is comparable
// across builds.
double self_recall(raft::device_resources& res,
                   cuvs::neighbors::ivf_pq::index<int64_t>& idx,
                   const std::vector<float>& queries,
                   const std::vector<int64_t>& seeds)
{
    auto stream = raft::resource::get_cuda_stream(res);
    rmm::device_uvector<float> dq(static_cast<size_t>(kQueries) * kDim, stream);
    raft::copy(dq.data(), queries.data(), dq.size(), stream);
    auto qv = raft::make_device_matrix_view<const float, int64_t>(dq.data(), kQueries, kDim);

    cuvs::neighbors::ivf_pq::search_params sp;
    sp.n_probes = 32;

    auto nbr  = raft::make_device_matrix<int64_t, int64_t>(res, kQueries, kTopK);
    auto dist = raft::make_device_matrix<float, int64_t>(res, kQueries, kTopK);
    cuvs::neighbors::ivf_pq::search(res, sp, idx, qv, nbr.view(), dist.view());

    std::vector<int64_t> h(static_cast<size_t>(kQueries) * kTopK);
    raft::copy(h.data(), nbr.data_handle(), h.size(), stream);
    res.sync_stream();

    int hits = 0;
    for (int64_t q = 0; q < kQueries; ++q) {
        for (uint32_t j = 0; j < kTopK; ++j) {
            if (h[q * kTopK + j] == seeds[q]) { ++hits; break; }
        }
    }
    return double(hits) / double(kQueries);
}

}  // namespace

int main(int argc, char** argv)
{
    int64_t max_rows = (argc > 1) ? std::atoll(argv[1]) : 1000000;

    CUDA_CHECK(cudaSetDevice(0));
    cudaDeviceProp prop{};
    CUDA_CHECK(cudaGetDeviceProperties(&prop, 0));
    std::printf("GPU: %s   free: %.2f GB   dim=%ld  trainset_fraction=0.1  n_probes=32  k=%u\n",
                prop.name, free_bytes() / 1e9, (long)kDim, kTopK);
    std::printf("data: 1000 gaussian clusters (sigma=0.10); queries perturb a known row "
                "(sigma=0.01) so the correct top-1 is that row\n\n");

    std::vector<int64_t> sizes;
    for (int64_t n : {250000L, 500000L, 1000000L, 2500000L}) {
        if (n <= max_rows) sizes.push_back(n);
    }

    std::printf("%9s %7s | %-22s | %9s %9s %8s\n",
                "rows", "lists", "variant", "build(s)", "peak", "recall@10");
    std::printf("%s\n", std::string(76, '-').c_str());

    for (int64_t n : sizes) {
        // Clustered data — uniform noise in 960 dims has no nearest-neighbor
        // structure to recover, which made the earlier metric unreadable.
        std::mt19937 rng(42);
        std::uniform_real_distribution<float> uni(-1.f, 1.f);
        std::normal_distribution<float> jitter(0.f, 0.10f);

        constexpr int64_t kClusters = 1000;
        std::vector<float> centers(static_cast<size_t>(kClusters) * kDim);
        for (auto& v : centers) v = uni(rng);

        std::vector<float> data(static_cast<size_t>(n) * kDim);
        for (int64_t i = 0; i < n; ++i) {
            const float* c = centers.data() + (i % kClusters) * kDim;
            for (int64_t j = 0; j < kDim; ++j) data[i * kDim + j] = c[j] + jitter(rng);
        }

        std::normal_distribution<float> qjitter(0.f, 0.01f);
        std::uniform_int_distribution<int64_t> pick(0, n - 1);
        std::vector<int64_t> seeds(kQueries);
        std::vector<float> queries(static_cast<size_t>(kQueries) * kDim);
        for (int64_t q = 0; q < kQueries; ++q) {
            seeds[q] = pick(rng);
            for (int64_t j = 0; j < kDim; ++j)
                queries[q * kDim + j] = data[seeds[q] * kDim + j] + qjitter(rng);
        }

        // One index alive at a time: build, score, free. Holding two 1M indexes
        // concurrently starved the second build and corrupted the earlier control.
        auto run = [&](const char* label, bool host_path, bool stream_pool) {
            cuvs::neighbors::ivf_pq::index<int64_t>* idx = nullptr;
            Result r{};
            double recall = 0.0;
            {
                std::shared_ptr<rmm::cuda_stream_pool> pool =
                    stream_pool ? std::make_shared<rmm::cuda_stream_pool>(4) : nullptr;
                raft::device_resources res(rmm::cuda_stream_per_thread, pool);
                r = host_path ? build_host(res, data, n, &idx) : build_device(res, data, n, &idx);
                recall = self_recall(res, *idx, queries, seeds);
            }
            std::printf("%9ld %7u | %-22s | %9.2f %8.2fGB %8.3f\n",
                        (long)n, make_params(n).n_lists, label, r.seconds,
                        r.peak_bytes / 1e9, recall);
            std::fflush(stdout);
            delete idx;
            return r;
        };

        // Past the point where n*dim*4 exceeds free VRAM the device path cannot
        // run at all. Report that rather than aborting: the contrast IS the
        // measurement.
        double dataset_gb = double(n) * kDim * 4 / 1e9;
        try {
            run("device (MO today)", false, false);
            run("device (repeat)", false, false);
        } catch (const std::exception& e) {
            std::printf("%9ld %7u | %-22s | dataset %.2fGB > free VRAM: %.60s\n",
                        (long)n, make_params(n).n_lists, "device (MO today)", dataset_gb, e.what());
            std::fflush(stdout);
        }
        run("host view", true, false);
        run("host view + 4-stream", true, true);
        std::printf("\n");
    }
    return 0;
}
