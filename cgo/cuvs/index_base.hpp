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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wunused-parameter"
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
#pragma GCC diagnostic ignored "-Wmissing-field-initializers"
#include <raft/core/bitset.cuh>
#include <raft/core/copy.cuh>
#include <raft/core/resources.hpp>
#include <raft/core/device_mdspan.hpp>
#include <thrust/fill.h>
#include <thrust/functional.h>
#include <thrust/transform.h>
#pragma GCC diagnostic pop

#include "cuvs_types.h"
#include "cuvs_worker.hpp"
#include "filter.hpp"
#include "quantize.hpp"
#include "json.hpp"
#include <cuvs/distance/distance.hpp>
#include <vector>
#include <string>
#include <memory>
#include <shared_mutex>
#include <algorithm>
#include <atomic>
#include <fstream>
#include <unordered_map>
#include <sys/stat.h>
#include <cerrno>
#include <iostream>

namespace matrixone {

using ::distance_type_t;
using ::quantization_t;
using ::distribution_mode_t;

// =============================================================================
// gpu_index_base_t — Developer Guide
// =============================================================================
//
// OVERVIEW
// --------
// gpu_index_base_t<T, BuildParams, IdT> is the CRTP-style base class shared by
// all three GPU index types:
//
//   gpu_ivf_flat_t<T>  (IdT = int64_t)
//   gpu_ivf_pq_t<T>    (IdT = int64_t)
//   gpu_cagra_t<T>     (IdT = uint32_t)
//
// It provides:
//   - Pre-build vector buffering (flattened_host_dataset)
//   - External-ID mapping (host_ids / id_to_index_)
//   - Soft-delete bitset (host + per-device GPU cache)
//   - Scalar quantizer for 1-byte types
//   - Serialization helpers (save/load ids, bitset, manifest)
//   - Worker lifecycle management
//
//
// LIFECYCLE
// ---------
// Every index goes through these stages in order:
//
//   1. Construct   — allocates host buffers, creates worker
//   2. start()     — starts worker threads and GPU resources
//   3. add_chunk() / add_chunk_float()
//                  — fills flattened_host_dataset (pre-build only)
//   4. build()     — uploads dataset to GPU, runs cuVS build, sets is_loaded_=true,
//                    clears flattened_host_dataset, calls init_deleted_bitset()
//   5. search() / search_float()
//                  — concurrent reads, no lock during GPU work
//   6. extend() / extend_float()
//                  — serialized by extend_mutex_; updates count+current_offset_
//                    under unique_lock after GPU work completes
//   7. delete_id() — soft-delete under unique_lock; increments bitset_version_
//   8. destroy()   — stops worker, frees GPU resources
//
// Calling extend() or delete_id() before build() is an error (throws).
// Calling add_chunk() after build() is an error (throws).
//
//
// DISTRIBUTION MODES
// ------------------
// SINGLE_GPU (default)
//   - One GPU, one cuVS index object (index_ unique_ptr).
//   - build: submit_main() + wait()
//   - search: submit() (round-robin load-balance across search threads)
//   - extend: submit_main() + wait(); GPU sequential indices required for cuVS.
//
// REPLICATED
//   - N GPUs, each holds a full copy of the index in replicated_indices_[dev_id].
//   - build: submit_all_devices() — concurrent build on all GPUs.
//   - search: submit() — dispatches to any GPU, uses per-thread cached index ptr.
//   - extend: submit_all_devices() — concurrent extend on all GPUs; set_ids() is
//             called ONCE (in extend(), not extend_internal()) after all GPUs done.
//   - WARNING: dataset_device_ptr_ / replicated_datasets_ are stale after extend
//              and must be reset immediately under unique_lock.
//
// SHARDED
//   - N GPUs, each holds a disjoint slice of the index.
//   - build: submit_all_devices() — each GPU builds its shard.
//   - search: submit_all_devices_no_wait() — all shards searched in parallel,
//             results merged via merge_sharded_results().
//   - extend: routes new rows to the last shard via submit_to_rank(last_rank).
//             shard-local seq_ids = [old_last_shard_size .. old_last_shard_size+n_rows).
//             replicated_datasets_[last_dev_id] erased (stale); other shards' entries untouched.
//   - SHARDED shard sizing: rows_per_shard is rounded DOWN to a multiple of 32
//     (i.e., (count / num_shards) & ~31). The last shard absorbs the remainder.
//     This is required for word-aligned bitset slicing in sync_shard_bitset().
//     The same rounded value must be used in both build_internal and search_internal.
//
//
// LOCKING RULES  (see also CLAUDE.md for the full table)
// -------------
// mutex_ is a std::shared_mutex covering all shared host-side state:
//   - is_loaded_, count, current_offset_
//   - host_ids, id_to_index_
//   - deleted_bitset_
//   - replicated_indices_, replicated_datasets_
//   - dataset_device_ptr_
//
// Use shared_lock  for: reading a pointer, checking is_loaded_, reads in search.
// Use unique_lock  for: any write to the above; count/current_offset_ increment.
// NO lock during GPU operations (build, extend, search kernel launch).
//
// extend_mutex_ (std::mutex, in derived classes) serializes concurrent extend()
// calls so that set_ids() offsets and GPU execution order always agree.
// It is acquired AFTER checking is_loaded_ under unique_lock, and held across
// the entire GPU operation + count update.
//
// Per-device bitset caches each have their own std::mutex (device_bitset_cache_t::mutex)
// protected by a double-check pattern: check version, acquire device mutex, recheck.
// The main mutex_ is acquired as shared_lock inside the device mutex to read
// deleted_bitset_ safely.
// Lock order for search/sync path: device_bitsets_mutex_ → device mutex → mutex_ (shared).
//   device_bitsets_mutex_ is released before mutex_ is acquired (lookup only), so the
//   effective nesting is: device mutex → mutex_ (shared).
// Lock order for init/load path: mutex_ (unique) is acquired first, then released before
//   device_bitsets_mutex_ or device_shard_bitsets_mutex_.  These two never overlap.
//
//
// ID MAPPING
// ----------
// Two modes, cannot mix within one index:
//
// Sequential IDs (host_ids is empty):
//   - Vectors are addressed by their insertion order (0, 1, 2, ...).
//   - delete_id(k) marks internal position k.
//   - search results are returned as raw internal positions.
//
// Custom IDs (host_ids non-empty, set via set_ids() or add_chunk(ids)):
//   - host_ids[internal_pos] = external_id
//   - id_to_index_[external_id] = internal_pos  (reverse map)
//   - delete_id(external_id) looks up id_to_index_ to find internal pos.
//   - search results are translated: neighbors[i] = host_ids[raw_result[i]].
//   - set_ids() must only be called under unique_lock (or before build).
//
//
// SOFT-DELETE BITSET
// ------------------
// deleted_bitset_ is a host vector<uint32_t> acting as a packed bit array.
// Bit layout: bit j = (deleted_bitset_[j/32] >> (j%32)) & 1
//   1 = alive (valid), 0 = deleted.
//
// Lifecycle:
//   - init_deleted_bitset() is called from build() after is_loaded_ = true.
//     Allocates ceil(current_offset_ / 32) words, all set to ~0U (all alive).
//   - delete_id() clears the bit for the target position and increments
//     deleted_count_ and bitset_version_.
//   - Before each GPU search, if deleted_count_ > 0, the host bitset is synced
//     to a per-device raft::core::bitset via sync_device_bitset() (non-SHARDED)
//     or sync_shard_bitset() (SHARDED). Uses version-based double-check caching.
//
// SHARDED bitset slicing (sync_shard_bitset):
//   Because SHARDED shards search shard-local IDs (0..shard_sz), the bitset
//   passed to the cuVS filter must be indexed locally.  sync_shard_bitset()
//   copies the word-aligned slice deleted_bitset_[start_word .. start_word+n_words)
//   where start_word = shard_offset / 32.  This works because rows_per_shard
//   is always a multiple of 32 (see above), so start_word is always an integer.
//
//
// QUANTIZER  (1-byte types only: int8_t, uint8_t)
// ------------------------------------------------
// scalar_quantizer_t<float> quantizer_ maps float32 values to [min, max] range
// and packs them into int8/uint8.  It must be trained before add_chunk_float()
// or extend_float() is called for 1-byte types.
//
// Training: quantizer_.train(res, train_matrix) or train_quantizer(data, n).
//   - Auto-training occurs in add_chunk_float if not yet trained (uses up to 500
//     samples from the first chunk).
//   - For extend_float, the quantizer MUST already be trained (throws otherwise).
//
// Extended vectors must lie within the trained [min, max] range; vectors outside
// this range will be clamped and produce degraded search quality.
//
//
// SERIALIZATION  (save_dir / load_dir)
// ------------------------------------
// save_dir(dir) writes:
//   manifest.json    — metadata (type, quantization, dim, count, components list)
//   index.<fmt>      — cuVS index serialized by the derived class
//   ids.bin          — host_ids (omitted if sequential IDs)
//   quantizer.bin    — quantizer params (omitted if not trained)
//   bitset.bin       — deleted_bitset_ (omitted if no deletions)
//
// load_dir(dir) reads manifest.json, deserializes each component, then calls
// init_deleted_bitset() to recreate GPU caches.
//
// =============================================================================

/**
 * @brief Base class for GPU-based vector indices (IVF-Flat, IVF-PQ, CAGRA).
 *
 * See the Developer Guide block above for full details on lifecycle, locking,
 * distribution modes, ID mapping, and the soft-delete bitset system.
 *
 * @tparam T           Element type: float, half (__half), int8_t, uint8_t
 * @tparam BuildParams Index-specific build parameter struct
 * @tparam IdT         Neighbor ID type: int64_t (IVF) or uint32_t (CAGRA)
 */
template <typename T, typename BuildParams, typename IdT = int64_t>
class gpu_index_base_t {
public:
    // ---- Index configuration (immutable after build) ----
    uint32_t dimension = 0;          ///< Vector dimensionality
    distance_type_t metric;          ///< Distance metric (L2, IP, cosine, ...)
    BuildParams build_params;        ///< Index-type-specific build parameters
    std::vector<int> devices_;       ///< GPU device IDs to use
    distribution_mode_t dist_mode;   ///< SINGLE_GPU / REPLICATED / SHARDED

    // ---- Mutable counters (protected by mutex_) ----
    uint64_t count = 0;              ///< cap(): total allocated slots (after build = total vectors)
    // current_offset_: number of vectors actually inserted; len() reads this.
    // Before build: incremented by add_chunk(). After build: incremented by extend().
    // Invariant: current_offset_ <= count always holds after build.

    // ---- Pre-build host buffer (cleared after build()) ----
    // Holds raw T vectors [count x dimension] during the add_chunk phase.
    // Released immediately after build_internal() completes to free host RAM.
    std::vector<T> flattened_host_dataset;

    // ---- Deferred float buffer for quantizer training (1-byte types only) ----
    // When T is int8_t or uint8_t the quantizer must be trained on a
    // representative sample before any vectors can be quantized.
    // Raw float chunks are accumulated here until kQuantizerTrainThreshold
    // vectors are available, then the quantizer is trained on all of them at
    // once and the buffer is flushed into flattened_host_dataset as T.
    // If build() is called before the threshold is reached the buffer is
    // force-flushed (trained on whatever is available).
    // Only ever accessed from submit_main() tasks (serialised), so no extra
    // locking is needed beyond what those tasks already take.
    // (Fields are in protected: — see below.)

    // ---- External ID mapping ----
    // If non-empty: host_ids[internal_pos] = external_id.
    // If empty: internal positions are used directly as IDs.
    // Written under unique_lock (during build AND during extend() for post-build appends).
    // Must be read under shared_lock in search (extend() may append after build).
    std::vector<IdT> host_ids;

    // ---- Worker and GPU resource management ----
    std::unique_ptr<cuvs_worker_t> worker;  ///< Thread pool + CUDA stream pool
    mutable std::shared_mutex mutex_;       ///< Guards all shared host-side state (see Locking Rules)
    bool is_loaded_ = false;                ///< True once build() has completed successfully
    int build_device_id_ = 0;              ///< Primary GPU used for SINGLE_GPU mode
    std::vector<uint64_t> shard_sizes_;    ///< Per-shard row counts established at build time (SHARDED mode)

    // SINGLE_GPU: points to the device copy of the build dataset (stale after extend, reset then).
    std::shared_ptr<void> dataset_device_ptr_;

    // REPLICATED: per-device index and dataset pointers (device_id → shared_ptr<DerivedIndex>).
    // Keyed by device id. Written under unique_lock, read under shared_lock in search.
    std::map<int, std::shared_ptr<void>> replicated_indices_;
    std::map<int, std::shared_ptr<void>> replicated_datasets_;

    // ---- Soft-delete bitset (host side, protected by mutex_) ----
    // Packed uint32 array: bit j = 1 means position j is alive, 0 means deleted.
    // Indexed by internal position (0-based), NOT by external host_id.
    // Bit word: deleted_bitset_[j/32], bit position: j%32.
    std::vector<uint32_t> deleted_bitset_;
    uint64_t deleted_count_ = 0;            ///< Number of soft-deleted vectors
    std::atomic<uint64_t> bitset_version_{0}; ///< Incremented on every delete; drives cache invalidation

    // Per-device GPU cache for the full bitset (non-SHARDED modes).
    // Each entry is invalidated when bitset_version_ advances.
    // Double-check pattern: check version → lock device mutex → recheck → rebuild if stale.
    struct device_bitset_cache_t {
        std::shared_ptr<void> ptr;  ///< raft::core::bitset<uint32_t, int64_t>* (type-erased)
        uint64_t version = 0;       ///< Last known bitset_version_ when ptr was synced
        std::mutex mutex;           ///< Per-device lock for rebuilding (never held during GPU build)
    };
    std::mutex device_bitsets_mutex_;  ///< Guards the map itself (not individual entries)
    std::map<int, std::shared_ptr<device_bitset_cache_t>> device_deleted_bitsets_;

    // Per-device GPU cache for shard-local bitset slices (SHARDED mode only).
    // Entry for device d covers global positions [shard_offset, shard_offset+shard_sz).
    // bit j of the shard bitset = global bit (shard_offset + j).
    // shard_offset is always a multiple of 32 (enforced by rows_per_shard rounding at build).
    std::mutex device_shard_bitsets_mutex_;
    std::map<int, std::shared_ptr<device_bitset_cache_t>> device_shard_bitsets_;

    // ---- External-to-internal ID reverse map (protected by mutex_) ----
    // Populated by set_ids() / add_chunk(ids). id_to_index_[external_id] = internal_pos.
    // Used only when host_ids is non-empty.
    std::unordered_map<IdT, uint64_t> id_to_index_;

    // ---- Host-resident filter columns for pre-filtered search (protected by mutex_) ----
    // Populated before build() via set_filter_columns() + add_filter_chunk().
    // Retained for the lifetime of the index — search-time predicate eval reads
    // directly from this store (see filter.hpp / eval_filter_bitmap_cpu).
    // Empty means the index has no INCLUDE columns and only unfiltered search applies.
    FilterStore filter_host_;

    gpu_index_base_t() = default;
    virtual ~gpu_index_base_t() {
        destroy();
    }
    
    // Helper to get or create a device-specific bitset cache info
    std::shared_ptr<device_bitset_cache_t> get_device_shard_bitset_info(int dev_id) {
        std::lock_guard<std::mutex> lock(device_shard_bitsets_mutex_);
        auto it = device_shard_bitsets_.find(dev_id);
        if (it == device_shard_bitsets_.end()) {
            auto info = std::make_shared<device_bitset_cache_t>();
            device_shard_bitsets_[dev_id] = info;
            return info;
        }
        return it->second;
    }

    std::shared_ptr<device_bitset_cache_t> get_device_bitset_info(int dev_id) {
        std::lock_guard<std::mutex> lock(device_bitsets_mutex_);
        auto it = device_deleted_bitsets_.find(dev_id);
        if (it == device_deleted_bitsets_.end()) {
            auto info = std::make_shared<device_bitset_cache_t>();
            device_deleted_bitsets_[dev_id] = info;
            return info;
        }
        return it->second;
    }

    // Sync a shard-local slice of the deleted bitset to device (SHARDED mode).
    // shard_offset must be a multiple of 32 (enforced at build time).
    // Bit j of the resulting device bitset = global bit (shard_offset + j).
    void sync_shard_bitset(int dev_id, uint64_t shard_offset, uint64_t shard_sz, raft::resources const& res) {
        auto info = get_device_shard_bitset_info(dev_id);
        uint64_t current_ver = bitset_version_.load();

        if (info->version < current_ver || !info->ptr) {
            std::lock_guard<std::mutex> lock(info->mutex);
            if (info->version < current_ver || !info->ptr) {
                std::shared_lock<std::shared_mutex> base_lock(mutex_);

                using bs_t = raft::core::bitset<uint32_t, int64_t>;
                auto* bs = new bs_t(res, static_cast<int64_t>(shard_sz));
                uint64_t n_words   = (shard_sz + 31) / 32;
                uint64_t start_word = shard_offset / 32; // always integer since shard_offset % 32 == 0

                if (deleted_bitset_.empty() || start_word >= deleted_bitset_.size()) {
                    // No deletions recorded in this shard's range — mark all alive
                    thrust::fill_n(raft::resource::get_thrust_policy(res),
                                   bs->data(), static_cast<int64_t>(n_words), ~0U);
                } else {
                    uint64_t avail      = deleted_bitset_.size() - start_word;
                    uint64_t copy_words = std::min(n_words, avail);
                    raft::copy(res,
                        raft::make_device_vector_view<uint32_t, int64_t>(bs->data(), static_cast<int64_t>(copy_words)),
                        raft::make_host_vector_view<const uint32_t, int64_t>(
                            deleted_bitset_.data() + start_word, static_cast<int64_t>(copy_words)));
                    if (copy_words < n_words) {
                        thrust::fill_n(raft::resource::get_thrust_policy(res),
                                       bs->data() + static_cast<int64_t>(copy_words),
                                       static_cast<int64_t>(n_words - copy_words), ~0U);
                    }
                }

                info->ptr     = std::shared_ptr<void>(bs, [](void* p){ delete static_cast<bs_t*>(p); });
                info->version = current_ver;
            }
        }
    }

    // Helper to sync host bitset to device if stale. Should be called within search.
    void sync_device_bitset(int dev_id, raft::resources const& res) {
        auto info = get_device_bitset_info(dev_id);
        uint64_t current_ver = bitset_version_.load();

        if (info->version < current_ver || !info->ptr) {
            std::lock_guard<std::mutex> lock(info->mutex);
            if (info->version < current_ver || !info->ptr) {
                std::shared_lock<std::shared_mutex> base_lock(mutex_);

                using bs_t = raft::core::bitset<uint32_t, int64_t>;
                auto* bs = new bs_t(res, static_cast<int64_t>(current_offset_));
                uint64_t n_words = (current_offset_ + 31) / 32;

                if (deleted_bitset_.empty()) {
                    thrust::fill_n(raft::resource::get_thrust_policy(res),
                                   bs->data(), static_cast<int64_t>(n_words), ~0U);
                } else {
                    // Copy the recorded portion first, then fill any tail beyond it.
                    // Both ops use the same CUDA stream (from res) so ordering is guaranteed.
                    uint64_t copy_words = std::min<uint64_t>(n_words, deleted_bitset_.size());
                    raft::copy(res,
                        raft::make_device_vector_view<uint32_t, int64_t>(bs->data(), static_cast<int64_t>(copy_words)),
                        raft::make_host_vector_view<const uint32_t, int64_t>(deleted_bitset_.data(), static_cast<int64_t>(copy_words)));
                    if (copy_words < n_words) {
                        thrust::fill_n(raft::resource::get_thrust_policy(res),
                                       bs->data() + static_cast<int64_t>(copy_words),
                                       static_cast<int64_t>(n_words - copy_words), ~0U);
                    }
                }

                info->ptr = std::shared_ptr<void>(bs, [](void* p){ delete static_cast<bs_t*>(p); });
                info->version = current_ver;
            }
        }
    }

    // Build a raft::core::bitset<uint32_t, int64_t> for rows [start_row, start_row+shard_sz)
    // that represents (user_filter AND NOT deleted). Dispatches on four cases:
    //
    //   no filter, no deletes    → returns nullptr (caller runs the unfiltered search path)
    //   no filter, has deletes   → reuses the cached device delete bitset (shared_ptr aliased)
    //   filter, no deletes       → evaluates CPU bitmap, uploads H2D, returns a new owning bitset
    //   filter + deletes         → evaluates CPU bitmap, ANDs with host delete slice on the CPU,
    //                              uploads the already-combined bitmap in one H2D copy —
    //                              no device-side thrust::transform. Faster than the old
    //                              device-AND path: one fewer kernel launch per search, and
    //                              the IVF-PQ post-filter can reuse *out_user_mask directly
    //                              without re-ANDing.
    //
    // `start_row`, `shard_sz` match the shard-local slicing used by sync_shard_bitset:
    //   - SINGLE_GPU / REPLICATED: start_row=0, shard_sz=current_offset_
    //   - SHARDED:                 start_row aligned to 32, shard_sz from shard_sizes_
    //
    // Caller is responsible for holding the returned shared_ptr alive for the duration
    // of the cuVS search call.
    //
    // `out_user_mask` (optional): if non-null AND a user filter is present, the
    // function populates *out_user_mask with the packed host bitmap uploaded to
    // the device — i.e. (user_filter AND NOT deleted) when both are present, or
    // user_filter alone when no deletes. The IVF-PQ post-filter reuses this to
    // suppress the bitset_filter padding quirk. Left empty on the deletes-only
    // and unfiltered paths (the cached device delete bitset is enough there).
    std::shared_ptr<raft::core::bitset<uint32_t, int64_t>>
    build_search_bitset(raft_handle_wrapper_t& handle,
                        const std::string& preds_json,
                        uint64_t start_row,
                        uint64_t shard_sz,
                        std::vector<uint32_t>* out_user_mask = nullptr) {
        using bs_t = raft::core::bitset<uint32_t, int64_t>;
        auto res   = handle.get_raft_resources();  // shared_ptr<raft::resources>
        int dev_id = handle.get_device_id();

        // Parse user preds outside the lock; empty JSON → no user filter.
        std::vector<PredOp> preds;
        if (!preds_json.empty()) preds = parse_preds(preds_json);
        const bool has_user = !preds.empty();

        uint64_t del_count;
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            del_count = this->deleted_count_;
        }
        const bool has_del = del_count > 0;

        if (!has_user && !has_del) {
            return nullptr;  // unfiltered path — caller skips the bitset arg
        }

        const bool sharded = (this->dist_mode == DistributionMode_SHARDED);

        // Deletes-only path: reuse the cached delete bitset without copying.
        if (!has_user) {
            if (sharded) {
                this->sync_shard_bitset(dev_id, start_row, shard_sz, *res);
                return std::static_pointer_cast<bs_t>(
                    this->get_device_shard_bitset_info(dev_id)->ptr);
            }
            this->sync_device_bitset(dev_id, *res);
            return std::static_pointer_cast<bs_t>(
                this->get_device_bitset_info(dev_id)->ptr);
        }

        // User-filter path: evaluate on CPU, AND in the delete slice on CPU
        // (when present), then upload the already-combined bitmap. Keeping the
        // AND on the host is cheaper than a device thrust::transform — one
        // fewer kernel launch — and lets the IVF-PQ post-filter reuse the
        // combined host bitmap without any further work.
        std::vector<uint32_t> host_mask;
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            host_mask = eval_filter_bitmap_cpu(this->filter_host_, preds, start_row, shard_sz);
            if (has_del && !this->deleted_bitset_.empty()) {
                // start_row is 0 (non-SHARDED) or a multiple of 32 (SHARDED),
                // so start_word is always an integer (see class-level doc).
                const uint64_t start_word = start_row / 32;
                const uint64_t del_words  = this->deleted_bitset_.size();
                for (uint64_t w = 0; w < host_mask.size(); ++w) {
                    uint32_t del_w = (start_word + w < del_words)
                                       ? this->deleted_bitset_[start_word + w]
                                       : 0xFFFFFFFFu;
                    host_mask[w] &= del_w;
                }
            }
        }
        const uint64_t nwords = host_mask.size();

        auto bs = std::make_shared<bs_t>(*res, static_cast<int64_t>(shard_sz));

        // Upload H2D on the search stream (same mechanism as sync_device_bitset).
        raft::copy(
            *res,
            raft::make_device_vector_view<uint32_t, int64_t>(
                bs->data(), static_cast<int64_t>(nwords)),
            raft::make_host_vector_view<const uint32_t, int64_t>(
                host_mask.data(), static_cast<int64_t>(nwords)));
        // Drain the H2D DMA before host_mask (stack-local) goes out of scope at return.
        raft::resource::sync_stream(*res);

        if (out_user_mask) *out_user_mask = std::move(host_mask);
        return bs;
    }

    void set_ids(const IdT* ids, uint64_t count_vectors, uint64_t offset = 0) {
        if (!ids) return;
        std::unique_lock<std::shared_mutex> lock(mutex_);
        set_ids_internal(ids, count_vectors, offset);
    }

    void set_ids_internal(const IdT* ids, uint64_t count_vectors, uint64_t offset = 0) {
        if (!ids) return;
        // std::cout << "[DEBUG] set_ids: count=" << count_vectors << " offset=" << offset 
        //           << " first_id=" << ids[0] << " last_id=" << ids[count_vectors-1] 
        //           << " sizeof(IdT)=" << sizeof(IdT) << std::endl;
        if (this->host_ids.size() < offset + count_vectors) {
            this->host_ids.resize(offset + count_vectors);
        }
        std::copy(ids, ids + count_vectors, this->host_ids.begin() + offset);
        for (uint64_t i = 0; i < count_vectors; ++i) {
            this->id_to_index_[ids[i]] = offset + i;
        }
    }

    virtual void start() {}
    virtual void build() {}

    // Common management methods
    virtual void destroy() {
        if (worker) worker->stop();
    }

    void set_per_thread_device(bool enable) {
        if (worker) worker->set_per_thread_device(enable);
    }

    void transform_distance(distance_type_t metric, std::vector<float>& distances) const {
        if (metric == DistanceType_InnerProduct) {
            for (auto& d : distances) {
                if (d != std::numeric_limits<float>::max() && d != -std::numeric_limits<float>::max()) {
                    d *= -1.0f;
                }
            }
        }
    }

    void set_batch_window(int64_t window_us) {
        if (worker) worker->set_batch_window(window_us);
    }

    uint64_t cap() const { 
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return count; 
    }
    uint64_t len() const { 
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return current_offset_; 
    }

    void add_chunk(const T* chunk_data, uint64_t chunk_count, int64_t offset = -1, const IdT* ids = nullptr) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (is_loaded_) throw std::runtime_error("Cannot add chunk to built index");

        uint64_t target_offset;
        if (offset == -1) {
            target_offset = current_offset_;
            current_offset_ += chunk_count;
        } else {
            target_offset = static_cast<uint64_t>(offset);
            if (target_offset + chunk_count > current_offset_) {
                current_offset_ = target_offset + chunk_count;
            }
        }
        if (current_offset_ > count) count = current_offset_;

        size_t required_elements = static_cast<size_t>(current_offset_) * dimension;
        if (flattened_host_dataset.size() < required_elements) {
            flattened_host_dataset.resize(required_elements);
        }

        std::copy(chunk_data, chunk_data + chunk_count * dimension, flattened_host_dataset.begin() + (target_offset * dimension));

        if (this->dist_mode == DistributionMode_SHARDED) {
            // Pre-calculate shard distribution if we're in sharded mode.
            // Note: This will be re-calculated/finalized in build().
            int num_shards = static_cast<int>(this->devices_.size());
            if (this->shard_sizes_.size() != (size_t)num_shards) {
                this->shard_sizes_.assign(num_shards, 0);
            }
            uint64_t total = this->current_offset_;
            uint64_t rows_per_shard = (total / num_shards) & ~static_cast<uint64_t>(31);
            for (int i = 0; i < num_shards - 1; ++i) this->shard_sizes_[i] = rows_per_shard;
            this->shard_sizes_.back() = total - rows_per_shard * (num_shards - 1);
        }

        if (ids) {
            if (host_ids.size() < current_offset_) {
                host_ids.resize(current_offset_);
            }
            std::copy(ids, ids + chunk_count, host_ids.begin() + target_offset);
            for (uint64_t i = 0; i < chunk_count; ++i) {
                id_to_index_[ids[i]] = target_offset + i;
            }
        }
    }

    // ---- Filter column ingest (build-time only) ----
    //
    // Typical call sequence (mirrors add_chunk for vectors):
    //   idx.set_filter_columns("[{\"name\":\"price\",\"type\":2}, ...]", total_count);
    //   for each batch:
    //       idx.add_filter_chunk(0, prices_bytes, price_null_bm, nrows);
    //       idx.add_filter_chunk(1, cats_bytes,   nullptr,       nrows);
    //   idx.build();
    //
    // Both throw if the index is already built.  filter_host_ is read-only
    // after build() and is persisted alongside the index via save_dir().

    void set_filter_columns(const std::string& col_meta_json, uint64_t total_count) {
        auto cols = parse_filter_col_meta(col_meta_json);
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (is_loaded_) throw std::runtime_error("Cannot set filter columns on built index");
        filter_host_.init(std::move(cols), total_count);
    }

    // null_bitmap: packed uint32 words, LSB-first (bit i = row i is not-null).
    //              nullptr means the chunk has no nulls.
    void add_filter_chunk(uint32_t col_idx, const void* data,
                          const uint32_t* null_bitmap, uint64_t nrows) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        if (is_loaded_) throw std::runtime_error("Cannot add filter chunk to built index");
        filter_host_.add_chunk(col_idx, data, null_bitmap, nrows);
    }

    // Initialize (or reset) the deleted bitset after index build.
    // All positions are marked valid (1). Must be called after is_loaded_ = true.
    void init_deleted_bitset() {
        {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            uint64_t n_bits = current_offset_;
            uint64_t n_words = (n_bits + 31) / 32;
            if (deleted_bitset_.size() < n_words) {
                std::vector<uint32_t> new_bitset(n_words, ~0U);
                if (!deleted_bitset_.empty()) {
                    std::copy(deleted_bitset_.begin(), deleted_bitset_.end(), new_bitset.begin());
                }
                deleted_bitset_ = std::move(new_bitset);
            }
            bitset_version_.fetch_add(1);
        } // release mutex_ before acquiring device cache locks (lock-order: device_*_mutex_ must not be held while waiting for mutex_ unique_lock)
        {
            std::lock_guard<std::mutex> ds_lock(device_bitsets_mutex_);
            device_deleted_bitsets_.clear();
        }
        {
            std::lock_guard<std::mutex> ss_lock(device_shard_bitsets_mutex_);
            device_shard_bitsets_.clear();
        }
    }

    // Soft-delete by external ID (or internal position if no custom IDs).
    void delete_id(IdT id) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        uint64_t pos;
        if (!host_ids.empty()) {
            auto it = id_to_index_.find(id);
            if (it == id_to_index_.end()) return; // not found
            pos = it->second;
        } else {
            pos = static_cast<uint64_t>(id);
        }
        if (pos >= current_offset_) return;

        // Ensure bitset is large enough (lazy allocation)
        uint64_t n_words = (current_offset_ + 31) / 32;
        if (deleted_bitset_.size() < n_words) {
            deleted_bitset_.resize(n_words, ~0U);
        }

        uint32_t word = static_cast<uint32_t>(pos / 32);
        uint32_t bit  = static_cast<uint32_t>(pos % 32);
        if ((deleted_bitset_[word] >> bit) & 1U) {
            deleted_bitset_[word] &= ~(1U << bit); // clear bit: mark deleted
            ++deleted_count_;
            bitset_version_.fetch_add(1);
        }
    }

    // Flush all pending float chunks: train the quantizer on the combined data,
    // then quantize each chunk and store into flattened_host_dataset.
    // Must be called only from inside a submit_main() task (GPU work is legal there).
    // GPU operations are performed without holding mutex_; shared state is updated
    // under unique_lock after each chunk's GPU work completes.
    void flush_pending_float_chunks_internal(raft_handle_wrapper_t& handle) {
        std::vector<pending_float_chunk_t> chunks;
        uint64_t total;
        {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            if (pending_float_chunks_.empty()) return;
            chunks = std::move(pending_float_chunks_);
            total = pending_total_count_;
            pending_total_count_ = 0;
            pending_float_chunks_.clear();
        }

        auto res = handle.get_raft_resources();

        // --- GPU work: train quantizer on ALL pending float data — NO LOCK ---
        std::vector<float> all_floats;
        all_floats.reserve(total * dimension);
        for (auto& c : chunks) {
            all_floats.insert(all_floats.end(), c.data.begin(), c.data.end());
        }
        auto train_host_view = raft::make_host_matrix_view<const float, int64_t>(
            all_floats.data(), static_cast<int64_t>(total), static_cast<int64_t>(dimension));
        auto train_device = raft::make_device_matrix<float, int64_t>(*res, total, dimension);
        raft::copy(*res, train_device.view(), train_host_view);
        // Train without holding the lock: GPU kernels run while lock is not held,
        // so concurrent readers are not blocked for the duration of training.
        quantizer_.train(*res, train_device.view());
        handle.sync();
        // Brief unique_lock after sync to publish the completed quantizer state.
        // The lock/unlock acts as a memory barrier: any subsequent shared_lock
        // acquisition by a reader is guaranteed to see is_trained() == true.
        { std::unique_lock<std::shared_mutex> _pub_lock(mutex_); }

        // --- GPU work + locked store: process each buffered chunk ---
        for (auto& c : chunks) {
            // Upload and quantize — NO LOCK
            auto chunk_host_view = raft::make_host_matrix_view<const float, int64_t>(
                c.data.data(), static_cast<int64_t>(c.count), static_cast<int64_t>(dimension));
            auto chunk_device = raft::make_device_matrix<float, int64_t>(*res, c.count, dimension);
            raft::copy(*res, chunk_device.view(), chunk_host_view);

            auto chunk_device_target = raft::make_device_matrix<T, int64_t>(*res, c.count, dimension);
            
            {
                std::shared_lock<std::shared_mutex> lock(mutex_);
                quantizer_.template transform<T>(*res, chunk_device.view(), chunk_device_target.data_handle(), true);
            }

            std::vector<T> chunk_host_target(c.count * dimension);
            raft::copy(*res,
                raft::make_host_matrix_view<T, int64_t>(chunk_host_target.data(), static_cast<int64_t>(c.count), static_cast<int64_t>(dimension)),
                chunk_device_target.view());
            handle.sync();

            // Store into shared state — unique_lock
            std::unique_lock<std::shared_mutex> lock(mutex_);
            uint64_t target_offset;
            if (c.offset == -1) {
                target_offset = current_offset_;
                current_offset_ += c.count;
            } else {
                target_offset = static_cast<uint64_t>(c.offset);
                if (target_offset + c.count > current_offset_) {
                    current_offset_ = target_offset + c.count;
                }
            }
            if (current_offset_ > count) count = current_offset_;

            size_t required_elements = static_cast<size_t>(current_offset_) * dimension;
            if (flattened_host_dataset.size() < required_elements) {
                flattened_host_dataset.resize(required_elements);
            }
            std::copy(chunk_host_target.begin(), chunk_host_target.end(),
                      flattened_host_dataset.begin() + target_offset * dimension);

            if (this->dist_mode == DistributionMode_SHARDED) {
                int num_shards = static_cast<int>(this->devices_.size());
                if (this->shard_sizes_.size() != (size_t)num_shards) {
                    this->shard_sizes_.assign(num_shards, 0);
                }
                uint64_t total = this->current_offset_;
                uint64_t rows_per_shard = (total / num_shards) & ~static_cast<uint64_t>(31);
                for (int i = 0; i < num_shards - 1; ++i) this->shard_sizes_[i] = rows_per_shard;
                this->shard_sizes_.back() = total - rows_per_shard * (num_shards - 1);
            }

            if (!c.ids.empty()) {
                if (host_ids.size() < current_offset_) {
                    host_ids.resize(current_offset_);
                }
                std::copy(c.ids.begin(), c.ids.end(), host_ids.begin() + target_offset);
                for (uint64_t i = 0; i < c.count; ++i) {
                    id_to_index_[c.ids[i]] = target_offset + i;
                }
            }
        }
    }

    void add_chunk_float(const float* chunk_data, uint64_t chunk_count, int64_t offset = -1, const IdT* ids = nullptr) {
        uint64_t job_id = worker->submit_main(
            [this, chunk_data, chunk_count, offset, ids](raft_handle_wrapper_t& handle) -> std::any {
                {
                    std::shared_lock<std::shared_mutex> lock(mutex_);
                    if (is_loaded_) throw std::runtime_error("Cannot add chunk to built index");
                }

                auto res = handle.get_raft_resources();
                
                // If quantization is needed (T is 1-byte)
                if constexpr (sizeof(T) == 1) {
                    bool trained;
                    {
                        std::shared_lock<std::shared_mutex> lock(mutex_);
                        trained = quantizer_.is_trained();
                    }

                    if (!trained) {
                        // Buffer this chunk for deferred training.
                        pending_float_chunk_t c;
                        c.data.assign(chunk_data, chunk_data + chunk_count * dimension);
                        c.count  = chunk_count;
                        c.offset = offset;
                        if (ids) c.ids.assign(ids, ids + chunk_count);
                        
                        bool should_flush = false;
                        {
                            std::unique_lock<std::shared_mutex> lock(mutex_);
                            // Re-check trained under unique_lock to be absolutely safe
                            if (!quantizer_.is_trained()) {
                                pending_total_count_ += chunk_count;
                                pending_float_chunks_.push_back(std::move(c));
                                if (pending_total_count_ >= kQuantizerTrainThreshold) {
                                    should_flush = true;
                                }
                            } else {
                                trained = true; // Someone trained it while we were copying
                            }
                        }
                        
                        if (should_flush) {
                            flush_pending_float_chunks_internal(handle);
                        }
                        
                        if (!trained) return std::any();
                        // trained=true here means set_quantizer() was called on another thread
                        // between the first check (shared_lock) and the re-check (unique_lock).
                        // c was NOT pushed to pending, so fall through to process chunk_data directly.
                    }

                    // Quantizer already trained: quantize this chunk immediately.
                    auto queries_host_view = raft::make_host_matrix_view<const float, int64_t>(chunk_data, chunk_count, dimension);
                    auto queries_device = raft::make_device_matrix<float, int64_t>(*res, chunk_count, dimension);
                    raft::copy(*res, queries_device.view(), queries_host_view);

                    auto chunk_device_target = raft::make_device_matrix<T, int64_t>(*res, chunk_count, dimension);
                    
                    {
                        std::shared_lock<std::shared_mutex> lock(mutex_);
                        quantizer_.template transform<T>(*res, queries_device.view(), chunk_device_target.data_handle(), true);
                    }

                    std::vector<T> chunk_host_target(chunk_count * dimension);
                    raft::copy(*res, raft::make_host_matrix_view<T, int64_t>(chunk_host_target.data(), chunk_count, dimension), chunk_device_target.view());
                    handle.sync();

                    std::unique_lock<std::shared_mutex> lock(mutex_);
                    uint64_t target_offset;
                    if (offset == -1) {
                        target_offset = current_offset_;
                        current_offset_ += chunk_count;
                    } else {
                        target_offset = static_cast<uint64_t>(offset);
                        if (target_offset + chunk_count > current_offset_) {
                            current_offset_ = target_offset + chunk_count;
                        }
                    }
                    if (current_offset_ > count) count = current_offset_;

                    size_t required_elements = static_cast<size_t>(current_offset_) * dimension;
                    if (flattened_host_dataset.size() < required_elements) {
                        flattened_host_dataset.resize(required_elements);
                    }
                    std::copy(chunk_host_target.begin(), chunk_host_target.end(), flattened_host_dataset.begin() + (target_offset * dimension));

                    if (this->dist_mode == DistributionMode_SHARDED) {
                        int num_shards = static_cast<int>(this->devices_.size());
                        if (this->shard_sizes_.size() != (size_t)num_shards) {
                            this->shard_sizes_.assign(num_shards, 0);
                        }
                        uint64_t total = this->current_offset_;
                        uint64_t rows_per_shard = (total / num_shards) & ~static_cast<uint64_t>(31);
                        for (int i = 0; i < num_shards - 1; ++i) this->shard_sizes_[i] = rows_per_shard;
                        this->shard_sizes_.back() = total - rows_per_shard * (num_shards - 1);
                    }

                    if (ids) {
                        this->set_ids_internal(ids, chunk_count, target_offset);
                    }                
		} else {
                    std::unique_lock<std::shared_mutex> lock(mutex_);
                    uint64_t target_offset;
                    if (offset == -1) {
                        target_offset = current_offset_;
                        current_offset_ += chunk_count;
                    } else {
                        target_offset = static_cast<uint64_t>(offset);
                        if (target_offset + chunk_count > current_offset_) {
                            current_offset_ = target_offset + chunk_count;
                        }
                    }
                    if (current_offset_ > count) count = current_offset_;

                    size_t required_elements = static_cast<size_t>(current_offset_) * dimension;
                    if (flattened_host_dataset.size() < required_elements) {
                        flattened_host_dataset.resize(required_elements);
                    }
                    // For T=float: trivial copy. For T=__half: implicit float→half per element
                    // via __half::operator=(float), which is correct (float32→float16 narrowing).
                    std::copy(chunk_data, chunk_data + chunk_count * dimension, flattened_host_dataset.begin() + (target_offset * dimension));

                    if (this->dist_mode == DistributionMode_SHARDED) {
                        int num_shards = static_cast<int>(this->devices_.size());
                        if (this->shard_sizes_.size() != (size_t)num_shards) {
                            this->shard_sizes_.assign(num_shards, 0);
                        }
                        uint64_t total = this->current_offset_;
                        uint64_t rows_per_shard = (total / num_shards) & ~static_cast<uint64_t>(31);
                        for (int i = 0; i < num_shards - 1; ++i) this->shard_sizes_[i] = rows_per_shard;
                        this->shard_sizes_.back() = total - rows_per_shard * (num_shards - 1);
                    }

                    if (ids) {
                        this->set_ids_internal(ids, chunk_count, target_offset);
                    }
                }
                return std::any();
            }
        );
        worker->wait(job_id).get();
    }

    void train_quantizer(const float* train_data, uint64_t n_samples) {
        uint64_t job_id = worker->submit_main(
            [this, train_data, n_samples](raft_handle_wrapper_t& handle) -> std::any {
                auto res = handle.get_raft_resources();
                auto train_host_view = raft::make_host_matrix_view<const float, int64_t>(train_data, n_samples, dimension);
                auto train_device = raft::make_device_matrix<float, int64_t>(*res, n_samples, dimension);
                raft::copy(*res, train_device.view(), train_host_view);
                quantizer_.train(*res, train_device.view());
                handle.sync();
                return std::any();
            }
        );
        worker->wait(job_id).get();
    }

    void train_quantizer_if_needed() {
        if constexpr (sizeof(T) == 1) {
            uint64_t job_id = worker->submit_main(
                [this](raft_handle_wrapper_t& handle) -> std::any {
                    // 1. Flush any buffered chunks first
                    flush_pending_float_chunks_internal(handle);

                    // 2. Check if still not trained (might have used add_chunk<T> instead of float).
                    // WARNING: if data was added via add_chunk(T*) rather than add_chunk_float(),
                    // flattened_host_dataset already holds T values (e.g. int8 in [-128,127]).
                    // Casting them to float trains the quantizer on the compressed range, not the
                    // original float range.  extend_float() will then clamp to the wrong range.
                    // If extend_float() is needed after add_chunk(T*), call train_quantizer()
                    // explicitly with representative original float data before calling build().
                    bool needs_training;
                    uint64_t n_train = 0;
                    {
                        std::shared_lock<std::shared_mutex> lock(mutex_);
                        needs_training = !quantizer_.is_trained() && !flattened_host_dataset.empty();
                        if (needs_training) {
                            n_train = std::min(static_cast<uint64_t>(500), count);
                            if (n_train == 0) needs_training = false;
                        }
                    }

                    if (needs_training) {
                        std::vector<float> train_data(n_train * dimension);
                        {
                            std::shared_lock<std::shared_mutex> lock(mutex_);
                            for (size_t i = 0; i < n_train * dimension; ++i) {
                                train_data[i] = static_cast<float>(flattened_host_dataset[i]);
                            }
                        }
                        
                        auto res = handle.get_raft_resources();
                        auto train_host_view = raft::make_host_matrix_view<const float, int64_t>(train_data.data(), n_train, dimension);
                        auto train_device = raft::make_device_matrix<float, int64_t>(*res, n_train, dimension);
                        raft::copy(*res, train_device.view(), train_host_view);
                        
                        {
                            std::unique_lock<std::shared_mutex> lock(mutex_);
                            quantizer_.train(*res, train_device.view());
                        }
                        handle.sync();
                    }
                    return std::any();
                }
            );
            worker->wait(job_id).get();
        }
    }

    void set_quantizer(float min, float max) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        quantizer_.set_quantizer(min, max);
    }

    void get_quantizer(float* min, float* max) {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        *min = quantizer_.min();
        *max = quantizer_.max();
    }

    const IdT* get_host_ids() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        return host_ids.empty() ? nullptr : host_ids.data();
    }

    void save_ids(const std::string& filename) const {
        std::ofstream os(filename, std::ios::binary);
        if (!os) throw std::runtime_error("Failed to open file for saving IDs: " + filename);
        // Hold a single lock for the entire snapshot to avoid TOCTOU: another
        // thread modifying host_ids between the size read and the data write
        // would produce a file whose header and body disagree.
        std::shared_lock<std::shared_mutex> lock(mutex_);
        uint64_t size = host_ids.size();
        os.write(reinterpret_cast<const char*>(&size), sizeof(size));
        if (size > 0) {
            os.write(reinterpret_cast<const char*>(host_ids.data()), size * sizeof(IdT));
        }
    }

    void load_ids(const std::string& filename) {
        std::ifstream is(filename, std::ios::binary);
        if (!is) throw std::runtime_error("Failed to open file for loading IDs: " + filename);
        uint64_t size;
        is.read(reinterpret_cast<char*>(&size), sizeof(size));
        {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            this->host_ids.clear();
            this->id_to_index_.clear();
        }
        if (size > 0) {
            std::vector<IdT> temp_ids(size);
            is.read(reinterpret_cast<char*>(temp_ids.data()), size * sizeof(IdT));
            this->set_ids(temp_ids.data(), size);
        }
    }

    // Returns a string name for the template element type T.
    std::string element_type_name() const {
        if constexpr (std::is_same_v<T, float>) return "float32";
        else if constexpr (sizeof(T) == 2)      return "float16";
        else if constexpr (std::is_same_v<T, int8_t>) return "int8";
        else return "uint8";
    }

    // Creates a directory and all its parents. Ignores EEXIST at each level.
    static void ensure_dir(const std::string& dir) {
        for (size_t pos = 1; pos <= dir.size(); ++pos) {
            if (pos == dir.size() || dir[pos] == '/') {
                std::string partial = dir.substr(0, pos);
                if (partial.empty() || partial == ".") continue;
                if (::mkdir(partial.c_str(), 0755) != 0 && errno != EEXIST) {
                    throw std::runtime_error("Failed to create directory: " + partial +
                                             " (errno=" + std::to_string(errno) + ")");
                }
            }
        }
    }

    // Writes the soft-delete bitset to {dir}/bitset.bin.
    // Format: [uint64 n_bits][uint64 n_words][uint64 deleted_count][uint32 words...]
    void save_bitset(const std::string& dir) const {
        std::string filename = dir + "/bitset.bin";
        std::ofstream os(filename, std::ios::binary);
        if (!os) throw std::runtime_error("Failed to open bitset file for writing: " + filename);
        uint64_t n_bits, n_words, d_count;
        std::vector<uint32_t> bs_copy;
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            n_bits  = current_offset_;
            n_words = deleted_bitset_.size();
            d_count = deleted_count_;
            bs_copy = deleted_bitset_;
        }
        os.write(reinterpret_cast<const char*>(&n_bits),       sizeof(n_bits));
        os.write(reinterpret_cast<const char*>(&n_words),      sizeof(n_words));
        os.write(reinterpret_cast<const char*>(&d_count), sizeof(d_count));
        if (n_words > 0) {
            os.write(reinterpret_cast<const char*>(bs_copy.data()),
                     n_words * sizeof(uint32_t));
        }
    }

    // Restores the soft-delete bitset from a file written by save_bitset().
    void load_bitset_from_file(const std::string& filename) {
        std::ifstream is(filename, std::ios::binary);
        if (!is) throw std::runtime_error("Failed to open bitset file for reading: " + filename);
        uint64_t n_bits = 0, n_words = 0, d_count = 0;
        is.read(reinterpret_cast<char*>(&n_bits),       sizeof(n_bits));
        is.read(reinterpret_cast<char*>(&n_words),      sizeof(n_words));
        is.read(reinterpret_cast<char*>(&d_count), sizeof(d_count));
        std::vector<uint32_t> temp_bs(n_words);
        if (n_words > 0) {
            is.read(reinterpret_cast<char*>(temp_bs.data()),
                    n_words * sizeof(uint32_t));
        }
        
        {
            std::unique_lock<std::shared_mutex> lock(mutex_);
            deleted_bitset_ = std::move(temp_bs);
            deleted_count_ = d_count;
            bitset_version_.fetch_add(1);
        } // release mutex_ before acquiring device cache locks
        {
            std::lock_guard<std::mutex> ds_lock(device_bitsets_mutex_);
            device_deleted_bitsets_.clear();
        }
        {
            std::lock_guard<std::mutex> ss_lock(device_shard_bitsets_mutex_);
            device_shard_bitsets_.clear();
        }
    }

    // -------------------------------------------------------------------------
    // Manifest helpers — shared by save_dir / load_dir in all derived classes
    // -------------------------------------------------------------------------

    struct manifest_data_t {
        std::string raw;          // full manifest.json content
        std::string comp_json;    // "components" sub-object
        bool has_ids       = false;
        bool has_quantizer = false;
        bool has_bitset    = false;
        bool has_filter    = false;
    };

    // Saves ids, quantizer, bitset, and filter data (when present) to dir.
    // Returns comp_entry strings for each saved file.
    std::vector<std::string> save_common_components(const std::string& dir) const {
        bool has_ids, has_quantizer, has_bitset, has_filter;
        // Snapshot the filter data under the lock; writing to disk happens without
        // holding the lock since FilterStore::save only reads from its buffers.
        FilterStore filter_snapshot;
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            has_ids       = !this->host_ids.empty();
            has_quantizer = this->quantizer_.is_trained();
            has_bitset    = !this->deleted_bitset_.empty();
            has_filter    = !this->filter_host_.empty();
            if (has_filter) filter_snapshot = this->filter_host_;  // copy
        }

        if (has_ids)       this->save_ids(dir + "/ids.bin");
        if (has_quantizer) this->quantizer_.save_to_file(dir + "/quantizer.bin");
        if (has_bitset)    this->save_bitset(dir);
        if (has_filter)    filter_snapshot.save(dir + "/filter_data.bin");

        std::vector<std::string> entries;
        if (has_ids)       entries.push_back("    \"ids\": \"ids.bin\"");
        if (has_quantizer) entries.push_back("    \"quantizer\": \"quantizer.bin\"");
        if (has_bitset)    entries.push_back("    \"bitset\": \"bitset.bin\"");
        if (has_filter)    entries.push_back("    \"filter_data\": \"filter_data.bin\"");
        return entries;
    }

    // Returns the JSON component entry for sharded index files.
    std::string shards_comp_entry() const {
        std::string s = "    \"shards\": [";
        for (int i = 0; i < static_cast<int>(this->devices_.size()); ++i) {
            s += "\"shard_" + std::to_string(i) + ".bin\"";
            if (i + 1 < static_cast<int>(this->devices_.size())) s += ", ";
        }
        s += "]";
        return s;
    }

    // Writes manifest.json to dir.
    // build_params_json: inner key:value lines for the "build_params" object.
    // comp_entries: per-component JSON lines for the "components" object.
    void write_manifest(const std::string& dir, const std::string& index_type,
                        const std::string& build_params_json,
                        const std::vector<std::string>& comp_entries) const {
        bool has_ids, has_quantizer, has_bitset, has_filter;
        uint64_t cap_val, len_val, del_count, bs_ver;
        {
            std::shared_lock<std::shared_mutex> lock(mutex_);
            has_ids       = !this->host_ids.empty();
            has_quantizer = this->quantizer_.is_trained();
            has_bitset    = !this->deleted_bitset_.empty();
            has_filter    = !this->filter_host_.empty();
            cap_val       = this->count;
            len_val       = this->current_offset_;
            del_count     = this->deleted_count_;
            bs_ver        = this->bitset_version_.load();
        }

        std::ofstream mf(dir + "/manifest.json");
        if (!mf) throw std::runtime_error("Failed to create manifest.json in: " + dir);

        mf << "{\n";
        mf << "  \"schema_version\": 1,\n";
        mf << "  \"index_type\": \""    << index_type                    << "\",\n";
        mf << "  \"element_type\": \""  << this->element_type_name()     << "\",\n";
        mf << "  \"dimension\": "       << this->dimension               << ",\n";
        mf << "  \"metric\": "          << static_cast<int>(this->metric) << ",\n";
        mf << "  \"dist_mode\": "       << static_cast<int>(this->dist_mode) << ",\n";
        mf << "  \"capacity\": "        << cap_val                       << ",\n";
        mf << "  \"length\": "          << len_val                       << ",\n";
        mf << "  \"has_ids\": "         << (has_ids       ? "true" : "false") << ",\n";
        mf << "  \"has_quantizer\": "   << (has_quantizer ? "true" : "false") << ",\n";
        mf << "  \"has_bitset\": "      << (has_bitset    ? "true" : "false") << ",\n";
        mf << "  \"has_filter\": "      << (has_filter    ? "true" : "false") << ",\n";
        mf << "  \"deleted_count\": "   << del_count                     << ",\n";
        mf << "  \"bitset_version\": "  << bs_ver                        << ",\n";
        mf << "  \"devices\": [";
        for (size_t i = 0; i < this->devices_.size(); ++i) {
            mf << this->devices_[i];
            if (i + 1 < this->devices_.size()) mf << ", ";
        }
        mf << "],\n";
        mf << "  \"build_params\": {\n" << build_params_json << "\n  },\n";
        mf << "  \"components\": {\n";
        for (size_t i = 0; i < comp_entries.size(); ++i) {
            mf << comp_entries[i];
            if (i + 1 < comp_entries.size()) mf << ",";
            mf << "\n";
        }
        mf << "  }\n}\n";
    }

    // Reads manifest.json from dir, validates schema and index_type,
    // restores common index fields, and returns parsed manifest data.
    manifest_data_t read_manifest(const std::string& dir, const std::string& expected_type) {
        std::ifstream mf(dir + "/manifest.json");
        if (!mf) throw std::runtime_error("Failed to open manifest.json in: " + dir);
        std::string raw((std::istreambuf_iterator<char>(mf)),
                         std::istreambuf_iterator<char>());

        int64_t schema_ver = json_int(raw, "schema_version");
        if (schema_ver != 1)
            throw std::runtime_error("Unsupported manifest schema_version: " +
                                     std::to_string(schema_ver));
        std::string idx_type = json_value(raw, "index_type");
        if (idx_type != expected_type)
            throw std::runtime_error("manifest index_type is '" + idx_type +
                                     "', expected '" + expected_type + "'");

        this->dimension       = static_cast<uint32_t>(json_int(raw, "dimension"));
        this->count           = static_cast<uint64_t>(json_int(raw, "capacity"));
        this->current_offset_ = static_cast<uint64_t>(json_int(raw, "length"));
        this->metric          = static_cast<distance_type_t>(json_int(raw, "metric"));
        this->dist_mode       = static_cast<distribution_mode_t>(json_int(raw, "dist_mode"));
        this->deleted_count_  = static_cast<uint64_t>(json_int(raw, "deleted_count"));

        manifest_data_t m;
        m.raw           = raw;
        m.comp_json     = json_object(raw, "components");
        m.has_ids       = json_bool(raw, "has_ids");
        m.has_quantizer = json_bool(raw, "has_quantizer");
        m.has_bitset    = json_bool(raw, "has_bitset");
        m.has_filter    = json_bool(raw, "has_filter");
        return m;
    }

    // Loads ids, quantizer, bitset, and filter data from dir using the parsed manifest data.
    void load_common_components(const std::string& dir, const manifest_data_t& m) {
        if (m.has_ids) {
            this->load_ids(dir + "/" + json_value(m.comp_json, "ids"));
        }
        if (m.has_quantizer) {
            this->quantizer_.load_from_file(dir + "/" + json_value(m.comp_json, "quantizer"));
        }
        if (m.has_bitset) {
            this->load_bitset_from_file(dir + "/" + json_value(m.comp_json, "bitset"));
        }
        if (m.has_filter) {
            std::string fname = json_value(m.comp_json, "filter_data");
            std::unique_lock<std::shared_mutex> lock(mutex_);
            this->filter_host_.load(dir + "/" + fname);
        }
    }

    virtual std::string info() const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        std::string json = "{";
        json += "\"element_size\": " + std::to_string(sizeof(T)) + ", ";
        json += "\"dimension\": " + std::to_string(dimension) + ", ";
        json += "\"metric\": " + std::to_string((int)metric) + ", ";
        json += "\"status\": \"" + std::string(is_loaded_ ? "Loaded" : "Empty") + "\", ";
        json += "\"capacity\": " + std::to_string(count) + ", ";
        json += "\"current_length\": " + std::to_string(current_offset_) + ", ";
        json += "\"dist_mode\": " + std::to_string((int)dist_mode) + ", ";
        json += "\"has_ids\": " + std::string(host_ids.empty() ? "false" : "true") + ", ";
        json += "\"devices\": [";
        for (size_t i = 0; i < devices_.size(); ++i) {
            json += std::to_string(devices_[i]) + (i == devices_.size() - 1 ? "" : ", ");
        }
        json += "]";
        return json; // Caller will close the object or add more fields
    }

protected:
    scalar_quantizer_t<float> quantizer_;
    uint64_t current_offset_ = 0;
    // Serializes concurrent extend() calls. Held across GPU work and count update so that
    // set_ids() offsets always match the GPU execution order. Does NOT block searches.
    std::mutex extend_mutex_;

    // Deferred float chunk buffer for quantizer training (1-byte types only).
    // See class-level comment block above for full description.
    struct pending_float_chunk_t {
        std::vector<float> data;   ///< count * dimension floats
        uint64_t           count;
        int64_t            offset; ///< -1 = append; >= 0 = explicit position
        std::vector<IdT>   ids;    ///< empty if caller supplied no IDs
    };
    static constexpr uint64_t kQuantizerTrainThreshold = 1000;
    std::vector<pending_float_chunk_t> pending_float_chunks_;
    uint64_t pending_total_count_ = 0;
};

} // namespace matrixone
