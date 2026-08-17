// SPDX-License-Identifier: Apache-2.0
// -----------------------------------------------------------------------------
// bench_wiki88.cu — will the wiki_all 88M float16 index fit one 20 GB A200?
//
// Uses the REAL benchmark tuning, from mo_vector_benchmark/cfg/templates/88M.json:
//   dim 768, ivfpq_m 192, bits_per_code 8, ivfpq_lists 6000,
//   kmeans_train_percent 2, storage float16.
//
// The question is peak device memory, and the key structural fact is that IVF-PQ
// build has TWO phases whose allocations do NOT overlap:
//
//   phase A (train)   trainset (float32, n_train*dim*4) + for non-float T a
//                     trainset_tmp (n_train*dim*sizeof(T)) + k-means scratch.
//                     ALL of it is scoped to a block that closes at
//                     ivf_pq_build.cuh:1369 ...
//   phase B (encode)  ... BEFORE detail::extend at :1374 allocates the PQ list
//                     data (n*pq_dim codes + 8 B index per row) and streams the
//                     dataset in batches.
//
// So peak = max(A, B), not A + B. Extrapolating a single fitted curve conflates
// them and overstates the requirement, which is what an earlier estimate here
// did. This measures the two separately by building with
// add_data_on_build=false (phase A alone), then calling extend (phase B alone).
//
// That split is also the explicit train-then-extend shape, expressed through
// cuVS's own API rather than by hand — no sampling, no gather, no ord
// permutation, so the host_ids[ord] identity is untouched.
//
// Build:  make -C cgo/cuvs bench_wiki88
// Run:    ./bench_wiki88 [max_rows]      (default 5000000)
// -----------------------------------------------------------------------------

#include <cuvs/neighbors/ivf_pq.hpp>

#include <raft/core/device_mdarray.hpp>
#include <raft/core/device_resources.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/mdspan.hpp>
#include <raft/core/resource/cuda_stream.hpp>

#include <rmm/device_uvector.hpp>

#include <cuda_fp16.h>
#include <cuda_runtime.h>

#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <random>
#include <string>
#include <thread>
#include <vector>

#define CUDA_CHECK(call)                                                             \
    do {                                                                             \
        cudaError_t _e = (call);                                                     \
        if (_e != cudaSuccess) {                                                     \
            std::fprintf(stderr, "CUDA error %s at %s:%d\n", cudaGetErrorString(_e),  \
                         __FILE__, __LINE__);                                        \
            std::exit(1);                                                            \
        }                                                                            \
    } while (0)

namespace {

// mo_vector_benchmark/cfg/templates/88M.json
constexpr int64_t  kDim      = 768;
constexpr uint32_t kPqDim    = 192;
constexpr uint32_t kPqBits   = 8;
constexpr uint32_t kNLists   = 6000;
constexpr double   kTrainPct = 0.02;
constexpr int64_t  kTarget   = 88000000;

constexpr int64_t kQueries = 100;
constexpr uint32_t kTopK   = 10;

size_t free_bytes()
{
    size_t f = 0, t = 0;
    CUDA_CHECK(cudaMemGetInfo(&f, &t));
    return f;
}

class PeakWatcher {
   public:
    explicit PeakWatcher(int device) : device_(device), min_free_(free_bytes()), stop_(false)
    {
        thread_ = std::thread([this] {
            CUDA_CHECK(cudaSetDevice(device_));
            while (!stop_.load(std::memory_order_relaxed)) {
                size_t f   = free_bytes();
                size_t cur = min_free_.load(std::memory_order_relaxed);
                while (f < cur && !min_free_.compare_exchange_weak(cur, f)) {}
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }
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

bool g_conservative = false;

cuvs::neighbors::ivf_pq::index_params make_params(bool add_data)
{
    cuvs::neighbors::ivf_pq::index_params p;
    // Default false makes cuVS grow each IVF list geometrically, so allocated
    // list bytes overshoot used bytes by an amount that depends on where each
    // list sits in its doubling curve. That slack is what makes the measured
    // bytes-per-row non-monotone, and at 88M it is the difference between
    // fitting a 20 GB card and not.
    p.conservative_memory_allocation = g_conservative;
    p.n_lists                  = kNLists;
    p.pq_dim                   = kPqDim;
    p.pq_bits                  = kPqBits;
    p.metric                   = cuvs::distance::DistanceType::L2Expanded;
    p.kmeans_trainset_fraction = kTrainPct;
    p.kmeans_n_iters           = 20;
    p.add_data_on_build        = add_data;
    return p;
}

struct Phase {
    double seconds;
    size_t peak;
};

}  // namespace

int main(int argc, char** argv)
{
    int64_t max_rows = (argc > 1) ? std::atoll(argv[1]) : 5000000;

    CUDA_CHECK(cudaSetDevice(0));
    cudaDeviceProp prop{};
    CUDA_CHECK(cudaGetDeviceProperties(&prop, 0));
    std::printf("GPU: %s   free: %.2f GB\n", prop.name, free_bytes() / 1e9);
    std::printf("wiki_all 88M tuning: dim=%ld m=%u bits=%u lists=%u train=%.0f%% storage=float16\n\n",
                (long)kDim, kPqDim, kPqBits, kNLists, kTrainPct * 100);

    g_conservative = (argc > 2 && std::string(argv[2]) == "conservative");
    std::printf("conservative_memory_allocation = %s\n\n", g_conservative ? "true" : "false");

    std::vector<int64_t> sizes;
    for (int64_t n : {1000000L, 2500000L, 5000000L, 8000000L}) {
        if (n <= max_rows) sizes.push_back(n);
    }

    std::printf("%9s | %9s %9s | %9s %9s | %9s %9s\n",
                "rows", "trainA(s)", "peakA", "extB(s)", "peakB", "max", "recall@10");
    std::printf("%s\n", std::string(74, '-').c_str());

    std::vector<double> ns, peaksB;

    for (int64_t n : sizes) {
        std::mt19937 rng(42);
        std::uniform_real_distribution<float> uni(-1.f, 1.f);
        std::normal_distribution<float> jitter(0.f, 0.10f);

        constexpr int64_t kClusters = 1000;
        std::vector<float> centers(static_cast<size_t>(kClusters) * kDim);
        for (auto& v : centers) v = uni(rng);

        // Host dataset in the storage type, exactly as flattened_host_dataset holds it.
        std::vector<half> data(static_cast<size_t>(n) * kDim);
        for (int64_t i = 0; i < n; ++i) {
            const float* c = centers.data() + (i % kClusters) * kDim;
            for (int64_t j = 0; j < kDim; ++j)
                data[i * kDim + j] = __float2half(c[j] + jitter(rng));
        }

        std::normal_distribution<float> qj(0.f, 0.01f);
        std::uniform_int_distribution<int64_t> pick(0, n - 1);
        std::vector<int64_t> seeds(kQueries);
        std::vector<half> queries(static_cast<size_t>(kQueries) * kDim);
        for (int64_t q = 0; q < kQueries; ++q) {
            seeds[q] = pick(rng);
            for (int64_t j = 0; j < kDim; ++j)
                queries[q * kDim + j] =
                    __float2half(__half2float(data[seeds[q] * kDim + j]) + qj(rng));
        }

        auto hv = raft::make_host_matrix_view<const half, int64_t>(data.data(), n, kDim);

        raft::device_resources res;
        Phase A{}, B{};

        // ---- phase A: train only (add_data_on_build = false) ----
        cuvs::neighbors::ivf_pq::index<int64_t> idx(res, make_params(false), (uint32_t)kDim);
        {
            size_t base = free_bytes();
            PeakWatcher w(0);
            auto t0 = std::chrono::steady_clock::now();
            cuvs::neighbors::ivf_pq::build(res, make_params(false), hv, &idx);
            res.sync_stream();
            auto t1 = std::chrono::steady_clock::now();
            A = {std::chrono::duration<double>(t1 - t0).count(), w.stop(base)};
        }

        // ---- phase B: encode the dataset into the trained index ----
        {
            size_t base = free_bytes();
            PeakWatcher w(0);
            auto t0 = std::chrono::steady_clock::now();
            cuvs::neighbors::ivf_pq::extend(res, hv, std::nullopt, &idx);
            res.sync_stream();
            auto t1 = std::chrono::steady_clock::now();
            B = {std::chrono::duration<double>(t1 - t0).count(), w.stop(base)};
        }

        // ---- quality: each query perturbs a known row; that row must come back ----
        double recall = 0.0;
        {
            auto stream = raft::resource::get_cuda_stream(res);
            rmm::device_uvector<half> dq(static_cast<size_t>(kQueries) * kDim, stream);
            raft::copy(dq.data(), queries.data(), dq.size(), stream);
            auto qv = raft::make_device_matrix_view<const half, int64_t>(dq.data(), kQueries, kDim);

            cuvs::neighbors::ivf_pq::search_params sp;
            sp.n_probes = 16;  // probe_ivfpq from the same template
            auto nbr  = raft::make_device_matrix<int64_t, int64_t>(res, kQueries, kTopK);
            auto dist = raft::make_device_matrix<float, int64_t>(res, kQueries, kTopK);
            cuvs::neighbors::ivf_pq::search(res, sp, idx, qv, nbr.view(), dist.view());

            std::vector<int64_t> h(static_cast<size_t>(kQueries) * kTopK);
            raft::copy(h.data(), nbr.data_handle(), h.size(), stream);
            res.sync_stream();
            int hits = 0;
            for (int64_t q = 0; q < kQueries; ++q)
                for (uint32_t j = 0; j < kTopK; ++j)
                    if (h[q * kTopK + j] == seeds[q]) { ++hits; break; }
            recall = double(hits) / double(kQueries);
        }

        size_t mx = A.peak > B.peak ? A.peak : B.peak;
        std::printf("%9ld | %9.1f %8.2fGB | %9.1f %8.2fGB | %8.2fGB %9.3f\n",
                    (long)n, A.seconds, A.peak / 1e9, B.seconds, B.peak / 1e9,
                    mx / 1e9, recall);
        std::fflush(stdout);

        ns.push_back((double)n);
        peaksB.push_back((double)B.peak);
    }

    // Project phase B to 88M. Phase B is the one that scales with row count (PQ
    // codes + an 8 B index per row); phase A scales with n_train = 2% of rows.
    if (ns.size() >= 2) {
        size_t k = ns.size() - 1;
        double slope = (peaksB[k] - peaksB[0]) / (ns[k] - ns[0]);
        double icept = peaksB[0] - slope * ns[0];
        double projB = slope * kTarget + icept;
        double codes = double(kTarget) * (kPqDim + 8) / 1e9;
        double trainRows = kTrainPct * kTarget;
        double projA = trainRows * kDim * (4 + sizeof(half)) / 1e9;

        std::printf("\nprojection to %ld rows (linear fit on phase B over the measured range):\n",
                    (long)kTarget);
        std::printf("  phase A (train)  ~%.1f GB   [%.1fM train rows x %ld dim x (4B f32 + 2B half)]\n",
                    projA, trainRows / 1e6, (long)kDim);
        std::printf("  phase B (encode) ~%.1f GB   [raw codes+ids alone = %.1f GB]\n", projB / 1e9, codes);
        std::printf("  peak = max(A,B)  ~%.1f GB   vs 20 GB A200\n",
                    projA > projB / 1e9 ? projA : projB / 1e9);
        std::printf("\n  NOTE: extrapolated %.0fx beyond the largest measured point. The fit\n"
                    "  cannot see allocator behaviour that only appears near capacity.\n",
                    double(kTarget) / ns[k]);
    }
    return 0;
}
