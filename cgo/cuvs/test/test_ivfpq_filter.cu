// SPDX-License-Identifier: Apache-2.0
// -----------------------------------------------------------------------------
// test_ivfpq_filter.cu — standalone reproducer for a bitset-filter result-
// padding quirk in cuvs::neighbors::ivf_pq::search.
//
// Symptom
// -------
// When the number of rows passing the bitset filter is strictly less than the
// requested top-k, IVF-PQ does not write the (-1, +inf) sentinel into the
// remaining result slots. Instead it pads them with filter-EXCLUDED nearest
// neighbors (rows whose bit in the filter bitset is 0). The caller cannot tell
// these padding entries apart from real, filter-passing matches.
//
// IVF-Flat and CAGRA on the same input correctly emit the sentinel for the
// excluded slots — so this looks specific to the IVF-PQ search kernel's
// handling of `popcount(filter) < k`.
//
// Observation: in our run the *distance* slot is already FLT_MAX (+inf) for
// the leaked entries — cuVS evidently knows those slots are empty and writes
// the sentinel distance, but it forgets to also overwrite the matching
// neighbor index with -1. So the fix on the cuVS side may be a single store
// in the same kernel codepath that already writes the sentinel distance.
//
// Reproduces on libcuvs / libraft 26.02.000 + CUDA 13.0 against an
// RTX 5070 Laptop GPU.
//
// Configuration
// -------------
//   N = 200 row dataset, dim = 8, n_lists = 4, pq_dim = 4, n_probes = 4
//   Rows 0, 1, 2 are placed near the query; rows 3..199 are placed far away.
//   Bitset filter has only bits 0 and 2 set (popcount = 2).
//   Search asks for top-k = 5.
//   Expected:  neighbors = [0, 2, -1, -1, -1]  (only IDs 0 and 2 pass).
//   Observed:  trailing slots contain filter-excluded IDs (often 1, 3..),
//              never the sentinel.
//
// Workaround we ship in matrixone (cgo/cuvs/ivf_pq.hpp,
// apply_pq_post_filter_locked): copy the filter bitset back to the host and
// overwrite any slot whose row-id has a 0 bit with (-1, FLT_MAX). This file
// implements the same workaround in `postfilter_on_host` and prints the
// before/after for each query so the contrast is plain.
//
// What this file deliberately does NOT do
// ---------------------------------------
//   * No matrixone headers — only cuVS / RAFT / RMM / CUDA / STL.
//   * No worker pool, no batching, no SNMG. One device, one stream, one
//     query batch — the smallest setup that triggers the bug.
//
// Build
// -----
//   make -C cgo/cuvs test_ivfpq_filter
//
// Or directly:
/* nvcc -O2 -std=c++17 -x cu \
 *      -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE \
 *      -DRAFT_SYSTEM_LITTLE_ENDIAN=1 \
 *      --extended-lambda --expt-relaxed-constexpr \
 *      -I"${CONDA_PREFIX}/include" \
 *      -I"${CONDA_PREFIX}/include/rapids" \
 *      -I"${CONDA_PREFIX}/include/raft" \
 *      -I"${CONDA_PREFIX}/include/cuvs" \
 *      -L"${CONDA_PREFIX}/lib" -lcuvs -lrmm -lrapids_logger \
 *      -Xcompiler "-fPIC -pthread" \
 *      test_ivfpq_filter.cu -o test_ivfpq_filter
 */
//
// Run
// ---
//   ./test_ivfpq_filter
//
// Exit code is 0 if the workaround produces the expected sentinel-padded
// result, 1 if the bug is no longer present (raw cuVS already emits sentinels)
// — useful as a regression marker against future cuVS releases.
// -----------------------------------------------------------------------------

#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <random>
#include <vector>

#include <cuda_runtime.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#include <raft/core/bitset.cuh>
#include <raft/core/device_mdarray.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <raft/core/resources.hpp>
#include <cuvs/core/bitset.hpp>
#include <cuvs/neighbors/common.hpp>
#include <cuvs/neighbors/ivf_pq.hpp>
#pragma GCC diagnostic pop

namespace {

#define CHECK_CUDA(stmt)                                                        \
    do {                                                                        \
        cudaError_t _e = (stmt);                                                \
        if (_e != cudaSuccess) {                                                \
            std::fprintf(stderr, "CUDA error %s at %s:%d: %s\n",                \
                         cudaGetErrorName(_e), __FILE__, __LINE__,              \
                         cudaGetErrorString(_e));                               \
            std::abort();                                                       \
        }                                                                       \
    } while (0)

constexpr int64_t  kN          = 200;     // dataset rows
constexpr uint32_t kDim        = 8;
constexpr uint32_t kNLists     = 4;
constexpr uint32_t kPqDim      = 4;
constexpr uint32_t kNProbes    = 4;
constexpr uint32_t kK          = 5;       // request top-5
// Indices to mark as "passing" the user filter. popcount = 2 < kK.
constexpr int64_t  kPassingIds[]  = {0, 2};

// Build a deterministic dataset where rows 0,1,2 sit near the query and
// rows 3..N-1 are far away. The 3 "near" rows are intentionally close in
// PQ-quantized space too, so when the filter excludes some of them the
// pre-filter top-k easily slots a non-passing one in.
std::vector<float> make_dataset() {
    std::vector<float> ds(kN * kDim, 0.0f);
    for (uint32_t j = 0; j < kDim; ++j) ds[0 * kDim + j] = 1.0f;
    for (uint32_t j = 0; j < kDim; ++j) ds[1 * kDim + j] = 2.0f;
    for (uint32_t j = 0; j < kDim; ++j) ds[2 * kDim + j] = 3.0f;
    for (int64_t i = 3; i < kN; ++i) {
        for (uint32_t j = 0; j < kDim; ++j)
            ds[i * kDim + j] = 1.0e4f + static_cast<float>(i);
    }
    return ds;
}

// Re-apply the filter on the host: for every result slot whose row-id has
// a 0 bit in the filter, overwrite with (-1, FLT_MAX). This is exactly the
// fix-up that matrixone applies in cgo/cuvs/ivf_pq.hpp's
// apply_pq_post_filter_locked.
void postfilter_on_host(std::vector<int64_t>&         neighbors,
                        std::vector<float>&           distances,
                        const std::vector<uint32_t>&  host_filter_words,
                        int64_t                       n_rows)
{
    const float kDistSentinel = std::numeric_limits<float>::max();
    for (size_t i = 0; i < neighbors.size(); ++i) {
        int64_t raw = neighbors[i];
        if (raw < 0) continue;
        if (raw >= n_rows
            || !((host_filter_words[raw / 32] >> (raw % 32)) & 1U)) {
            neighbors[i]  = -1;
            distances[i]  = kDistSentinel;
        }
    }
}

void print_row(const char* label, const int64_t* nb, const float* d, uint32_t k) {
    std::printf("  %-22s neighbors = [", label);
    for (uint32_t j = 0; j < k; ++j) {
        std::printf("%4lld%s", (long long)nb[j], j + 1 < k ? ", " : "");
    }
    std::printf("]   distances = [");
    for (uint32_t j = 0; j < k; ++j) {
        if (d[j] == std::numeric_limits<float>::max())
            std::printf("  +inf%s", j + 1 < k ? ", " : "");
        else
            std::printf("%6.1f%s", d[j], j + 1 < k ? ", " : "");
    }
    std::printf("]\n");
}

}  // namespace

int main() {
    CHECK_CUDA(cudaSetDevice(0));
    raft::resources res;
    auto stream = raft::resource::get_cuda_stream(res);

    // ---- dataset H2D --------------------------------------------------------
    std::vector<float> host_ds = make_dataset();
    auto dataset_d = raft::make_device_matrix<float, int64_t>(res, kN, kDim);
    CHECK_CUDA(cudaMemcpyAsync(dataset_d.data_handle(), host_ds.data(),
                               host_ds.size() * sizeof(float),
                               cudaMemcpyHostToDevice, stream));
    CHECK_CUDA(cudaStreamSynchronize(stream));

    // ---- build IVF-PQ -------------------------------------------------------
    cuvs::neighbors::ivf_pq::index_params bp;
    bp.n_lists  = kNLists;
    bp.pq_dim   = kPqDim;
    bp.pq_bits  = 8;
    bp.metric   = cuvs::distance::DistanceType::L2Expanded;
    auto idx = cuvs::neighbors::ivf_pq::build(
        res, bp,
        raft::make_device_matrix_view<const float, int64_t>(
            dataset_d.data_handle(), kN, kDim));

    // ---- bitset filter: pass only IDs in kPassingIds ------------------------
    // Construct with default = false (nothing passes), then set the few IDs
    // we want to admit. The "set list" API takes a device vector of indices.
    std::vector<int64_t> passing(std::begin(kPassingIds), std::end(kPassingIds));
    auto pass_d = raft::make_device_vector<int64_t, int64_t>(res, passing.size());
    CHECK_CUDA(cudaMemcpyAsync(pass_d.data_handle(), passing.data(),
                               passing.size() * sizeof(int64_t),
                               cudaMemcpyHostToDevice, stream));
    CHECK_CUDA(cudaStreamSynchronize(stream));

    cuvs::core::bitset<uint32_t, int64_t> filter_bs(res, kN, /*default_value=*/false);
    filter_bs.set(res,
                  raft::make_device_vector_view<const int64_t, int64_t>(
                      pass_d.data_handle(), passing.size()),
                  /*set_value=*/true);
    CHECK_CUDA(cudaStreamSynchronize(stream));

    // Snapshot the bitset words on the host — used both for our printout and
    // for the post-filter workaround.
    const int64_t n_words = (kN + 31) / 32;
    std::vector<uint32_t> host_filter_words(n_words);
    CHECK_CUDA(cudaMemcpyAsync(host_filter_words.data(),
                               filter_bs.data(),
                               n_words * sizeof(uint32_t),
                               cudaMemcpyDeviceToHost, stream));
    CHECK_CUDA(cudaStreamSynchronize(stream));
    int popcount = 0;
    for (auto w : host_filter_words) popcount += __builtin_popcount(w);

    // ---- query: all-1s, single row -----------------------------------------
    std::vector<float> q(kDim, 1.0f);
    auto q_d         = raft::make_device_matrix<float,   int64_t>(res, 1, kDim);
    auto neighbors_d = raft::make_device_matrix<int64_t, int64_t>(res, 1, kK);
    auto distances_d = raft::make_device_matrix<float,   int64_t>(res, 1, kK);
    CHECK_CUDA(cudaMemcpyAsync(q_d.data_handle(), q.data(),
                               q.size() * sizeof(float),
                               cudaMemcpyHostToDevice, stream));
    CHECK_CUDA(cudaStreamSynchronize(stream));

    cuvs::neighbors::ivf_pq::search_params sp;
    sp.n_probes = kNProbes;

    // ---- search WITH bitset filter -----------------------------------------
    cuvs::neighbors::filtering::bitset_filter<uint32_t, int64_t> bs_filter(
        filter_bs.view());
    cuvs::neighbors::ivf_pq::search(
        res, sp, idx,
        raft::make_device_matrix_view<const float, int64_t>(
            q_d.data_handle(), 1, kDim),
        neighbors_d.view(), distances_d.view(),
        bs_filter);
    CHECK_CUDA(cudaStreamSynchronize(stream));

    std::vector<int64_t> raw_neighbors(kK);
    std::vector<float>   raw_distances(kK);
    CHECK_CUDA(cudaMemcpyAsync(raw_neighbors.data(), neighbors_d.data_handle(),
                               kK * sizeof(int64_t),
                               cudaMemcpyDeviceToHost, stream));
    CHECK_CUDA(cudaMemcpyAsync(raw_distances.data(), distances_d.data_handle(),
                               kK * sizeof(float),
                               cudaMemcpyDeviceToHost, stream));
    CHECK_CUDA(cudaStreamSynchronize(stream));

    // ---- report -------------------------------------------------------------
    std::printf("==============================================================\n");
    std::printf("cuVS IVF-PQ bitset-filter padding reproducer\n");
    std::printf("  rows=%lld dim=%u n_lists=%u pq_dim=%u n_probes=%u top_k=%u\n",
                (long long)kN, kDim, kNLists, kPqDim, kNProbes, kK);
    std::printf("  filter popcount = %d  (passing IDs:", popcount);
    for (auto id : kPassingIds) std::printf(" %lld", (long long)id);
    std::printf(")\n");
    std::printf("  query = (1,1,...,1).  Distance from query to each row:\n");
    std::printf("    row 0 (passing): 0.0   row 1 (excluded): 8.0   row 2 (passing): 32.0\n");
    std::printf("    row 3+ (excluded): ~10000+\n");
    std::printf("--------------------------------------------------------------\n");
    std::printf("Expected (correct):     neighbors = [   0,    2,   -1,   -1,   -1]\n");
    std::printf("--------------------------------------------------------------\n");

    print_row("raw cuVS:",          raw_neighbors.data(), raw_distances.data(), kK);

    // Detect the bug: any returned id whose bit is 0 in the filter.
    int leaked = 0;
    for (uint32_t i = 0; i < kK; ++i) {
        int64_t r = raw_neighbors[i];
        if (r >= 0 && r < kN
            && !((host_filter_words[r / 32] >> (r % 32)) & 1U)) {
            ++leaked;
        }
    }
    std::printf("  ==> %d filter-excluded id(s) leaked into the result.%s\n",
                leaked,
                leaked > 0 ? "  <-- bug" : "  (cuVS already correct)");

    // Apply the workaround.
    auto fixed_neighbors = raw_neighbors;
    auto fixed_distances = raw_distances;
    postfilter_on_host(fixed_neighbors, fixed_distances, host_filter_words, kN);
    print_row("after host post-filter:",
              fixed_neighbors.data(), fixed_distances.data(), kK);

    // Validate the workaround: only IDs 0 and 2 may appear; the other 3 slots
    // must be the -1 sentinel.
    bool ok = true;
    int  valid = 0;
    for (uint32_t i = 0; i < kK; ++i) {
        int64_t r = fixed_neighbors[i];
        if (r == -1) continue;
        if (r != 0 && r != 2) {
            std::printf("  POST-FILTER FAILED at slot %u: id=%lld\n",
                        i, (long long)r);
            ok = false;
        } else {
            ++valid;
        }
    }
    if (ok && valid != 2) {
        std::printf("  POST-FILTER FAILED: expected 2 valid slots, got %d\n", valid);
        ok = false;
    }
    std::printf("--------------------------------------------------------------\n");
    if (leaked == 0) {
        std::printf("RESULT: bug appears FIXED in this cuVS build "
                    "(no filter-excluded ids in raw output).\n");
        return 1;  // signal regression-marker so CI notices the cuVS fix landed
    }
    if (!ok) {
        std::printf("RESULT: workaround failed to produce the expected output.\n");
        return 2;
    }
    std::printf("RESULT: bug reproduced; host post-filter recovers the expected "
                "sentinel-padded result.\n");
    return 0;
}
