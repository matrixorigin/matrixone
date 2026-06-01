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

#pragma once

// -----------------------------------------------------------------------------
// Shared cuVS dynamic-batching support, used by every GPU index family that
// offers request-level batching (IVF-PQ, CAGRA, IVF-Flat).
//
// cuVS ships cuvs::neighbors::dynamic_batching: a lightweight wrapper around a
// single-GPU ANN index that coalesces concurrent single-query searches from many
// threads into one launch, double-buffered across n_queues CUDA streams. It
// covers the fixed-param, unfiltered subset of search. We replaced the old
// hand-rolled "dequeue-time" batcher (single stream, blocked the worker thread
// during the straggler wait) with it.
//
// On/off + tuning, all runtime knobs on the index (gpu_index_base_t):
//   - set_batch_window(µs) / SetBatchWindow: 0 ⇒ batching disabled, every search
//     standalone; > 0 ⇒ enabled, and the value is the cuVS dynamic_batching
//     dispatch_timeout_ms (µs → ms).
//   - set_dynb_conservative_dispatch(bool) / SetDynbConservativeDispatch.
// The remaining knobs below (kDynBMaxBatchSize, kDynBNQueues) are cuVS index_params
// that are still compile-time — sweep them by editing + rebuilding.
//
// TUNING NOTES
//  - kDynBMaxBatchSize: a *ceiling* on the per-dispatch batch size. The value
//    actually used is min(this, per-GPU search concurrency ≈ ThreadsSearch/numGPU),
//    so by default it tracks the worker thread count and you only need to raise
//    this if ThreadsSearch/numGPU exceeds it. Sizing past the real concurrency is
//    pointless (the batch can't fill) and, with conservative_dispatch == false,
//    wasteful (the upstream search always runs at the full size).
//  - conservative_dispatch (runtime): false ⇒ the first thread to commit a query
//    dispatches immediately at max_batch_size (low latency, possible padding
//    waste). true ⇒ wait until the batch fills or the window elapses, then
//    dispatch at the real size (no waste, but if max_batch_size > the real
//    concurrency the batch never fills and every request eats the whole window).
//  - kDynBNQueues: independent (stream + IO buffer) queues; more ⇒ better GPU
//    overlap, costs ~kDynBNQueues × kDynBMaxBatchSize × (dim + k) of IO buffers
//    (dataset-size-independent).
//
// NOTE: request-level batching only helps when a single search under-utilizes
// the GPU (small index and/or low concurrency). On large indexes with high
// concurrency the unbatched path already saturates the GPU, so batching is a
// wash — keep set_batch_window at 0 there.
// -----------------------------------------------------------------------------

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <tuple>

#include <cuda_runtime.h>

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#include <raft/core/device_mdspan.hpp>
#include <raft/core/resources.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <cuvs/neighbors/dynamic_batching.hpp>
#pragma GCC diagnostic pop

namespace matrixone {

// Compile-time cuVS dynamic_batching::index_params. (The runtime knobs
// batch_window and conservative_dispatch live on gpu_index_base_t.)
inline constexpr int64_t kDynBMaxBatchSize          = 64;     // ceiling; see TUNING NOTES
inline constexpr std::size_t kDynBNQueues           = 3;

// Lazily-built, mutex-guarded cache of cuVS dynamic_batching wrappers, shared by
// all index families. One instance lives per gpu_*_t object.
//
//  T    = query element type (float / half / int8_t / uint8_t)
//  IdxT = cuVS neighbor-id type for this index family (int64_t for IVF-*,
//         uint32_t for CAGRA)
//
// Keyed by (device_id, k, cfg0, cfg1) where cfgN are the index-specific search
// parameters baked into the wrapper at construction time (n_probes for IVF;
// itopk_size / search_width for CAGRA — pass 0 for unused slots). In practice a
// handful of keys per index. Each wrapper holds the device-resident upstream
// index by const reference (no second copy of the dataset); it costs only its
// IO ring buffers. A cached wrapper is dropped + rebuilt if the upstream index
// pointer changes (e.g. extend()/build() replaced it).
template <typename T, typename IdxT>
class dynb_cache_t {
public:
    using wrapper_t = cuvs::neighbors::dynamic_batching::index<T, IdxT>;

    // Resolve (building on first use) the wrapper for (device_id, k, cfg0, cfg1)
    // over `upstream` with `upstream_params`, then run a batched search with
    // dispatch_timeout_ms. `max_batch_size_hint` is the caller's estimate of the
    // per-GPU search concurrency (≈ ThreadsSearch / numGPU); the wrapper's
    // max_batch_size is set to min(that, kDynBMaxBatchSize), floored at 1.
    // `conservative_dispatch` is baked into the wrapper at construction — a cached
    // wrapper is dropped + rebuilt if it (or the upstream index pointer) changed.
    // Must be called on a worker thread while `upstream` is resident. `Upstream`
    // is the cuVS index type (e.g. ivf_pq::index<int64_t>); `UpstreamSearchParams`
    // its search-params type.
    template <typename Upstream, typename UpstreamSearchParams>
    void search(const raft::resources& res,
                int device_id,
                const Upstream* upstream,
                const UpstreamSearchParams& upstream_params,
                int64_t k, std::uint32_t cfg0, std::uint32_t cfg1,
                int64_t max_batch_size_hint,
                bool conservative_dispatch,
                double dispatch_timeout_ms,
                raft::device_matrix_view<const T, std::int64_t, raft::row_major> queries,
                raft::device_matrix_view<IdxT, std::int64_t, raft::row_major> neighbors,
                raft::device_matrix_view<float, std::int64_t, raft::row_major> distances) {
        std::shared_ptr<wrapper_t> w;
        {
            std::lock_guard<std::mutex> lk(mtx_);
            key_t key{device_id, static_cast<std::uint32_t>(k), cfg0, cfg1};
            auto it = cache_.find(key);
            if (it != cache_.end() &&
                (it->second.upstream != static_cast<const void*>(upstream) ||
                 it->second.conservative != conservative_dispatch)) {
                cache_.erase(it);
                it = cache_.end();
            }
            if (it == cache_.end()) {
                cuvs::neighbors::dynamic_batching::index_params p{};
                p.k                    = k;
                p.max_batch_size        = std::max<int64_t>(1, std::min<int64_t>(max_batch_size_hint, kDynBMaxBatchSize));
                p.n_queues              = kDynBNQueues;
                p.conservative_dispatch = conservative_dispatch;
                auto built = std::make_shared<wrapper_t>(
                    res, p, *upstream, upstream_params, /*sample_filter=*/nullptr);
                it = cache_.emplace(key,
                        entry_t{static_cast<const void*>(upstream), conservative_dispatch, std::move(built)}).first;
            }
            w = it->second.wrapper;
        }
        // The caller queued the queries H2D (and possibly other prep) on `res`'s
        // stream; dynamic_batching gathers from `queries` on its own internal
        // streams, so drain `res` first or the gather can race the H2D.
        raft::resource::sync_stream(res);
        cuvs::neighbors::dynamic_batching::search_params sp;
        sp.dispatch_timeout_ms = dispatch_timeout_ms;
        cuvs::neighbors::dynamic_batching::search(res, sp, *w, queries, neighbors, distances);
        // dynamic_batching runs the gather → upstream search → scatter on its
        // own internal streams but, per the cuVS contract, makes the results on
        // `res`'s stream available once that stream is drained. Sync it so the
        // caller can read neighbors/distances and reuse its per-thread queries
        // workspace safely. (This replaces an earlier full-device-sync
        // workaround; re-evaluating whether the per-stream sync is sufficient.)
        raft::resource::sync_stream(res);
    }

    // Drop all cached wrappers. Call before the upstream indices they reference
    // by const-ref are destroyed (e.g. in the owning index's destroy()).
    void clear() {
        std::lock_guard<std::mutex> lk(mtx_);
        cache_.clear();
    }

private:
    using key_t = std::tuple<int, std::uint32_t, std::uint32_t, std::uint32_t>;
    struct entry_t {
        const void* upstream;          // upstream index ptr the wrapper was built over (staleness check)
        bool conservative;             // conservative_dispatch baked into the wrapper
        std::shared_ptr<wrapper_t> wrapper;
    };
    std::map<key_t, entry_t> cache_;
    std::mutex mtx_;
};

}  // namespace matrixone
