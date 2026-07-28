// SPDX-License-Identifier: Apache-2.0
// -----------------------------------------------------------------------------
// dynb.cu — standalone reproducer for a deadlock in
// cuvs::neighbors::dynamic_batching when conservative_dispatch=true
// and the number of concurrent client threads cannot naturally fill
// max_batch_size.
//
// Symptom
// -------
// Every client thread blocks inside cuvs::neighbors::dynamic_batching::search
// and never returns. The dispatch_timeout_ms codepath does not fire, so even
// when the batch can never fill (concurrent_clients < max_batch_size), the
// dispatcher never falls back to launching the upstream search at the partial
// size. The program hangs indefinitely.
//
// Configuration that reproduces it on our host (one RTX 5070 Laptop GPU,
// CUDA 13.0, libcuvs 26.02.000 / libraft 26.02.000):
//
//     max_batch_size       = 4
//     n_queues             = 3
//     conservative_dispatch= true
//     dispatch_timeout_ms  = 100   (fast for the test; doesn't fire either way)
//     concurrent clients   = 16    (>> max_batch_size, but each client opens
//                                   its own raft::resources / stream, so the
//                                   wrapper splits them across queues and no
//                                   single queue ever sees max_batch_size in
//                                   flight at once)
//
// The same program with conservative_dispatch = false (the cuVS default) runs
// to completion. That difference is the bug.
//
// What this file deliberately does NOT do
// ---------------------------------------
//   * No matrixone headers, no project utilities — only cuVS / RAFT / RMM /
//     CUDA / STL.
//   * No nontrivial worker pool, no custom batching scheduler. Each client
//     thread holds its own raft::resources and calls
//     cuvs::neighbors::dynamic_batching::search directly, which is the usage
//     pattern that the cuVS docs explicitly recommend for thread-safe batching
//     ("call the search function with copies of the same index in multiple
//     threads to increase the occupancy of the batches").
//
/* Build (against the conda-shipped cuVS / libraft / librmm)
 * ---------------------------------------------------------
 *   nvcc -O2 -std=c++17 -x cu \
 *        -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE \
 *        -DRAFT_SYSTEM_LITTLE_ENDIAN=1 \
 *        --extended-lambda --expt-relaxed-constexpr \
 *        -I"${CONDA_PREFIX}/include" \
 *        -I"${CONDA_PREFIX}/include/rapids" \
 *        -I"${CONDA_PREFIX}/include/raft" \
 *        -I"${CONDA_PREFIX}/include/cuvs" \
 *        -L"${CONDA_PREFIX}/lib" -lcuvs -lrmm -lrapids_logger \
 *        -Xcompiler "-fPIC -pthread" \
 *        test_dynb.cu -o test_dynb
 *
 * Or, in this tree:
 *   make -C cgo/cuvs test_dynb
 *
 * Verified to build with libcuvs/libraft 26.02.000 + CUDA 13.0.
 */
//
// Run
// ---
//   ./test_dynb                       # default config, both modes
//   ./test_dynb conservative=true     # conservative-only run
//   ./test_dynb conservative=false    # eager-only run
//   ./test_dynb threads=16 max_batch=4 n_queues=3 timeout_ms=100 iters=8
//
// Each run prints a per-second progress line (completed searches across all
// clients). The watchdog aborts after STALL_TIMEOUT_S seconds without any
// progress and prints which threads are stuck — that abort is the bug
// reproduction.
// -----------------------------------------------------------------------------

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <cuda_runtime.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#include <raft/core/device_mdarray.hpp>
#include <raft/core/device_mdspan.hpp>
#include <raft/core/host_mdarray.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <raft/core/resources.hpp>
#include <cuvs/neighbors/cagra.hpp>
#include <cuvs/neighbors/dynamic_batching.hpp>
#pragma GCC diagnostic pop

namespace {

// ---- knobs (overridable on the command line) --------------------------------
struct Config {
    int    n_clients      = 16;     // concurrent threads calling search
    int    max_batch_size = 4;      // dynamic_batching::index_params
    int    n_queues       = 3;      // dynamic_batching::index_params
    double timeout_ms     = 100.0;  // dynamic_batching::search_params
    int    iters_per_thread = 8;    // each client does this many searches
    int    k              = 5;      // neighbors per query
    int    dim            = 32;     // dataset dim
    int64_t n_rows        = 4096;   // dataset rows
    bool   run_conservative = true;
    bool   run_eager        = true;
};

constexpr int STALL_TIMEOUT_S = 30;  // watchdog: abort after this many idle s

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

bool parse_kv(const char* arg, const char* key, std::string& out) {
    size_t klen = std::strlen(key);
    if (std::strncmp(arg, key, klen) != 0 || arg[klen] != '=') return false;
    out.assign(arg + klen + 1);
    return true;
}

Config parse_args(int argc, char** argv) {
    Config c{};
    for (int i = 1; i < argc; ++i) {
        std::string v;
        if      (parse_kv(argv[i], "threads",      v)) c.n_clients       = std::stoi(v);
        else if (parse_kv(argv[i], "max_batch",    v)) c.max_batch_size  = std::stoi(v);
        else if (parse_kv(argv[i], "n_queues",     v)) c.n_queues        = std::stoi(v);
        else if (parse_kv(argv[i], "timeout_ms",   v)) c.timeout_ms      = std::stod(v);
        else if (parse_kv(argv[i], "iters",        v)) c.iters_per_thread= std::stoi(v);
        else if (parse_kv(argv[i], "k",            v)) c.k               = std::stoi(v);
        else if (parse_kv(argv[i], "dim",          v)) c.dim             = std::stoi(v);
        else if (parse_kv(argv[i], "rows",         v)) c.n_rows          = std::stoll(v);
        else if (parse_kv(argv[i], "conservative", v)) {
            bool b = (v == "true" || v == "1");
            c.run_conservative = b;
            c.run_eager        = !b;
        } else {
            std::fprintf(stderr, "unknown arg: %s\n", argv[i]);
            std::exit(2);
        }
    }
    return c;
}

// Build a CAGRA index over a synthetic dataset on device 0.
// Returns the index and keeps the underlying dataset alive in `dataset_dev`.
auto build_cagra_index(raft::resources&       res,
                       const Config&          c,
                       rmm::device_uvector<float>& dataset_dev)
    -> cuvs::neighbors::cagra::index<float, uint32_t>
{
    // Fill host dataset with deterministic pseudo-random floats.
    std::vector<float> host(static_cast<size_t>(c.n_rows) * c.dim);
    std::mt19937 rng(0xC0DEFACE);
    std::uniform_real_distribution<float> dist(0.f, 1.f);
    for (auto& x : host) x = dist(rng);

    dataset_dev.resize(host.size(), raft::resource::get_cuda_stream(res));
    CHECK_CUDA(cudaMemcpyAsync(dataset_dev.data(), host.data(),
                               host.size() * sizeof(float),
                               cudaMemcpyHostToDevice,
                               raft::resource::get_cuda_stream(res)));
    CHECK_CUDA(cudaStreamSynchronize(raft::resource::get_cuda_stream(res)));

    auto dataset_view = raft::make_device_matrix_view<const float, int64_t>(
        dataset_dev.data(), c.n_rows, c.dim);

    cuvs::neighbors::cagra::index_params bp;
    bp.intermediate_graph_degree = 64;
    bp.graph_degree              = 32;
    return cuvs::neighbors::cagra::build(res, bp, dataset_view);
}

struct ClientState {
    std::atomic<int>      done_iters{0};   // searches completed
    std::atomic<bool>     in_search{false};// currently inside dynamic_batching::search
    std::atomic<int>      enter_count{0};  // # times we have entered search
};

void client_thread(int                                         tid,
                   const Config&                               c,
                   const cuvs::neighbors::cagra::index<float, uint32_t>& upstream,
                   const cuvs::neighbors::dynamic_batching::index<float, uint32_t>& dynb,
                   ClientState&                                state)
{
    (void)upstream;
    // Each thread owns its own raft::resources => its own default stream.
    // This mirrors the recommended cuVS pattern for thread-safe batching.
    raft::resources res;
    auto stream = raft::resource::get_cuda_stream(res);

    // One query per call (n_queries=1) — the canonical case for dynamic
    // batching: many independent clients, each issuing single-vector searches.
    auto queries_d   = raft::make_device_matrix<float,    int64_t>(res, 1, c.dim);
    auto neighbors_d = raft::make_device_matrix<uint32_t, int64_t>(res, 1, c.k);
    auto distances_d = raft::make_device_matrix<float,    int64_t>(res, 1, c.k);

    std::vector<float> hq(c.dim);
    std::mt19937 rng(0xBEEF + tid);
    std::uniform_real_distribution<float> dist(0.f, 1.f);

    cuvs::neighbors::dynamic_batching::search_params sp;
    sp.dispatch_timeout_ms = c.timeout_ms;

    for (int it = 0; it < c.iters_per_thread; ++it) {
        for (auto& x : hq) x = dist(rng);
        CHECK_CUDA(cudaMemcpyAsync(queries_d.data_handle(), hq.data(),
                                   hq.size() * sizeof(float),
                                   cudaMemcpyHostToDevice, stream));
        CHECK_CUDA(cudaStreamSynchronize(stream));

        state.enter_count.fetch_add(1, std::memory_order_relaxed);
        state.in_search.store(true, std::memory_order_release);
        cuvs::neighbors::dynamic_batching::search(
            res, sp, dynb,
            raft::make_device_matrix_view<const float, int64_t>(
                queries_d.data_handle(), 1, c.dim),
            neighbors_d.view(),
            distances_d.view());
        // Per cuVS contract: results land on `res`'s stream once it is drained.
        CHECK_CUDA(cudaStreamSynchronize(stream));
        state.in_search.store(false, std::memory_order_release);
        state.done_iters.fetch_add(1, std::memory_order_relaxed);
    }
}

// Returns true on clean completion, false on watchdog-detected stall.
bool run_once(const Config& c, bool conservative)
{
    std::printf("\n========================================================\n");
    std::printf("dynamic_batching reproducer:\n"
                "  conservative_dispatch = %s\n"
                "  max_batch_size        = %d\n"
                "  n_queues              = %d\n"
                "  dispatch_timeout_ms   = %.1f\n"
                "  client threads        = %d  (each does %d searches)\n",
                conservative ? "true" : "false",
                c.max_batch_size, c.n_queues, c.timeout_ms,
                c.n_clients, c.iters_per_thread);
    std::printf("========================================================\n");
    std::fflush(stdout);

    raft::resources build_res;
    rmm::device_uvector<float> dataset_dev(0, raft::resource::get_cuda_stream(build_res));
    auto upstream = build_cagra_index(build_res, c, dataset_dev);

    cuvs::neighbors::cagra::search_params upstream_sp;
    upstream_sp.itopk_size = 64;

    cuvs::neighbors::dynamic_batching::index_params dynb_p{};
    dynb_p.k                    = c.k;
    dynb_p.max_batch_size       = c.max_batch_size;
    dynb_p.n_queues             = c.n_queues;
    dynb_p.conservative_dispatch = conservative;

    // Build the dynamic_batching wrapper on a distinct raft::resources from
    // the one we hand to client threads — we don't use `build_res` after this.
    raft::resources wrap_res;
    cuvs::neighbors::dynamic_batching::index<float, uint32_t> dynb(
        wrap_res, dynb_p, upstream, upstream_sp, /*sample_filter=*/nullptr);

    std::vector<ClientState> states(c.n_clients);
    std::vector<std::thread> ts;
    ts.reserve(c.n_clients);

    auto t0 = std::chrono::steady_clock::now();
    for (int i = 0; i < c.n_clients; ++i) {
        ts.emplace_back(client_thread, i, std::cref(c),
                        std::cref(upstream), std::cref(dynb),
                        std::ref(states[i]));
    }

    // ---- watchdog & progress ------------------------------------------------
    const int total = c.n_clients * c.iters_per_thread;
    int       last_done = 0;
    int       idle_s    = 0;
    bool      stalled   = false;
    while (true) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        int done = 0;
        int in_search = 0;
        for (auto& s : states) {
            done      += s.done_iters.load(std::memory_order_relaxed);
            in_search += s.in_search.load(std::memory_order_acquire) ? 1 : 0;
        }
        std::printf("[t=%lds] done=%d/%d  in_search=%d/%d\n",
                    (long)std::chrono::duration_cast<std::chrono::seconds>(
                        std::chrono::steady_clock::now() - t0).count(),
                    done, total, in_search, c.n_clients);
        std::fflush(stdout);
        if (done >= total) break;
        if (done == last_done) {
            if (++idle_s >= STALL_TIMEOUT_S) {
                std::printf("\n*** STALL: no progress for %d s — assuming "
                            "deadlock in dynamic_batching::search. ***\n",
                            STALL_TIMEOUT_S);
                std::printf("Per-thread state at stall:\n");
                for (int i = 0; i < c.n_clients; ++i) {
                    std::printf("  tid=%d  done=%d/%d  entered=%d  in_search=%d\n",
                                i,
                                states[i].done_iters.load(),
                                c.iters_per_thread,
                                states[i].enter_count.load(),
                                states[i].in_search.load() ? 1 : 0);
                }
                stalled = true;
                break;
            }
        } else {
            idle_s    = 0;
            last_done = done;
        }
    }

    if (stalled) {
        // detach so the program can exit despite stuck threads
        for (auto& t : ts) t.detach();
        return false;
    }
    for (auto& t : ts) t.join();

    auto t1 = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(t1 - t0).count();
    std::printf("OK: %d searches in %.2fs (%.0f q/s)\n",
                total, elapsed, total / elapsed);
    return true;
}

}  // namespace

int main(int argc, char** argv) {
    Config c = parse_args(argc, argv);

    // Pin to device 0 — single-GPU reproducer, no need for SNMG.
    CHECK_CUDA(cudaSetDevice(0));

    bool any_failed = false;
    if (c.run_eager) {
        bool ok = run_once(c, /*conservative=*/false);
        if (!ok) any_failed = true;
    }
    if (c.run_conservative) {
        bool ok = run_once(c, /*conservative=*/true);
        if (!ok) any_failed = true;
    }

    if (any_failed) {
        std::printf("\nRESULT: at least one configuration deadlocked.\n");
        // Use _Exit so we don't run global dtors over detached, stuck threads.
        std::_Exit(1);
    }
    std::printf("\nRESULT: all configurations completed.\n");
    return 0;
}
