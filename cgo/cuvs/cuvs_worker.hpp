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

#include <raft/core/resource/comms.hpp>
#include <raft/core/resources.hpp>
#include "helper.h"
#include "cuvs_types.h"

#include <rmm/mr/cuda_memory_resource.hpp>
#include <rmm/mr/pool_memory_resource.hpp>
#include <rmm/mr/per_device_resource.hpp>
#include <rmm/cuda_device.hpp>
#include <rmm/device_uvector.hpp>
#include <raft/core/resource/cuda_stream.hpp>
#include <raft/core/device_mdspan.hpp>
#include <cuda_fp16.h>
#include <cstdint>

#include <any>
#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>
#include <unordered_map>
#include <chrono>
#include <sstream>
#include <iostream>

namespace matrixone {

// Process-wide RMM pool memory resources, one per device, lazy-initialized the
// first time a worker thread runs on that device. Without this, every
// raft::device_resources() falls back to cuda_memory_resource, which calls raw
// cudaMalloc/cudaFree per allocation — a global driver lock that dominates
// search wall time when ~18 device_uvectors are allocated/freed per query.
// The pool services thousands of allocations from a small number of up-front
// cudaMallocs and is lock-light on the hot path.
//
// Pool storage is intentionally never torn down: shared_ptrs in `keepalives_`
// hold the pool alive for process lifetime, so we cannot free into a destroyed
// pool from a `device_uvector` whose lifetime crosses cuvs_worker_t teardown.
// On process exit the OS reclaims everything.
inline void ensure_rmm_pool_for_device(int device_id) {
    constexpr int kMaxDevices = 16;
    static std::once_flag flags[kMaxDevices];
    static std::mutex keepalive_mu;
    static std::vector<std::shared_ptr<void>> keepalives;
    if (device_id < 0 || device_id >= kMaxDevices) return;
    std::call_once(flags[device_id], [device_id] {
        try {
            cudaSetDevice(device_id);
            auto base = std::make_shared<rmm::mr::cuda_memory_resource>();
            // Initial pool = 10% of free GPU memory; max = unbounded — pool
            // grows by allocating more from the upstream as needed.
            // Kept small so a subsequent huge-index load (e.g. an IVF-PQ or
            // CAGRA index needing ≥¾ VRAM in a single allocation) can still
            // claim a contiguous upstream slab: the pool's initial reservation
            // is not relocatable, so an allocation larger than the pool must
            // be satisfied by a fresh upstream cudaMalloc, which only succeeds
            // if (1 − initial_pct) of VRAM is still free.
            auto pool = std::make_shared<
                rmm::mr::pool_memory_resource<rmm::mr::cuda_memory_resource>>(
                    base.get(),
                    rmm::percent_of_free_device_memory(10));
            rmm::mr::set_per_device_resource(rmm::cuda_device_id{device_id}, pool.get());
            std::lock_guard<std::mutex> lk(keepalive_mu);
            keepalives.push_back(pool);   // pool outlives every device_uvector
            keepalives.push_back(base);   // base outlives the pool
        } catch (const std::exception& e) {
            std::cerr << "[ensure_rmm_pool_for_device] device=" << device_id
                      << " failed to install pool MR: " << e.what()
                      << " — falling back to cuda_memory_resource" << std::endl;
        } catch (...) {
            std::cerr << "[ensure_rmm_pool_for_device] device=" << device_id
                      << " failed with unknown error — falling back to cuda_memory_resource"
                      << std::endl;
        }
    });
}

// Process-static raw cuda_memory_resource that bypasses the per-device pool.
// Use this for transient huge allocations whose lifetime is "load → build/extend
// → drop" (e.g. the training-vector device matrix in build_internal). Routing
// these through the pool would pin the pool's high-water mark at training-set
// size forever — pool memory is never released to the driver — eating the
// upstream headroom that big single allocations need (see ensure_rmm_pool_for_device
// comment). With this MR, the device_uvector dtor returns memory straight to the
// driver, keeping the pool small and the upstream free pool large.
//
// Per-allocation override: rmm::device_uvector captures the MR at construction
// and frees back to it. No global state changes, so concurrent search threads
// on the same device keep using the per-device pool MR via raft handles.
inline rmm::mr::cuda_memory_resource* raw_device_mr() {
    static rmm::mr::cuda_memory_resource mr;
    return &mr;
}

// =============================================================================
// cuvs_worker_t — Developer Guide
// =============================================================================
//
// PURPOSE
// -------
// cuvs_worker_t manages a pool of CPU threads, each pinned to a GPU device,
// that execute GPU work (build, extend, search) asynchronously.  Each index
// type (CAGRA, IVF-Flat, IVF-PQ) owns one cuvs_worker_t for its lifetime.
//
// THREAD MODEL
// ------------
// On start(), two categories of threads are spawned:
//
//   Main thread (1):
//     - Pinned to devices_[0].
//     - Drains main_tasks_ queue exclusively.
//     - Used for serialized operations that must run on the primary GPU:
//       build, extend, serialize/deserialize, merge setup.
//     - submit_main() enqueues to this thread.
//
//   Device worker pool (nthread_ threads):
//     - Distributed across devices_ round-robin: thread i → devices_[i % n].
//     - Each thread drains device_queues_[i % n] (one queue per physical GPU).
//     - Used for parallel/concurrent operations: search round-robin, SHARDED
//       build/extend (one job per GPU simultaneously).
//     - submit() enqueues to the next device queue (round-robin via next_device_idx_).
//     - submit_all_devices_no_wait() pushes one job to EACH device queue.
//     - submit_all_devices() = submit_all_devices_no_wait() + wait for all.
//
//   Each thread gets its own raft_handle_wrapper_t with its own raft::resources
//   (and therefore its own CUDA stream) so GPU ops on different threads do not
//   serialize on the same stream.
//
// TASK SUBMISSION SUMMARY
// -----------------------
//   submit_main(fn)               → main thread, primary GPU, serialized
//   submit(fn)                    → round-robin device thread, load-balanced
//   submit_all_devices_no_wait(fn)→ one task per GPU, concurrent, returns job IDs
//   submit_all_devices(fn)        → same + blocks until all complete
//
// RESULT TRACKING
// ---------------
// Every submit* returns a uint64_t job ID.  call wait(id).get() to block until
// the task completes and retrieve cuvs_task_result_t { result: std::any, error }.
// cuvs_task_result_store_t is a sharded (64 shards) lock-striped map of
// promise/future pairs.  If wait() is called before the task completes, a
// placeholder promise is registered; the worker fulfills it on completion.
// If wait() is called after the task completes, the stored result is returned
// immediately via a pre-fulfilled future.
//
// LIFECYCLE
// ---------
//   1. Construct: cuvs_worker_t(nthread, devices, mode)
//      - Creates one thread_safe_queue_t per GPU (capacity 1000 tasks each).
//      - Does NOT start any threads yet.
//   2. start(init_fn, stop_fn):
//      - Spawns main_thread_ + nthread_ device_threads_.
//      - init_fn (optional) is called once per thread with its raft_handle.
//        Used by index types to register the cuVS index pointer on the handle
//        (handle.set_index_ptr) so per-thread searches avoid map lookups.
//      - stop_fn (optional) is called once per thread when its queue is drained
//        and the worker is stopping.  Used to clean up device-side state.
//   3. submit / submit_main / submit_all_devices — enqueue work.
//   4. wait(id).get() — block until work completes, retrieve result or rethrow.
//   5. stop():
//      - Sets stopping_ = true (blocks new external submit* calls immediately).
//      - Waits (via condition variable) for all in-flight tasks to complete.
//      - Sets running_ = false, stops queues, joins all threads.
//      - Fulfills any pending placeholder futures with a "Worker stopped" error.
//
// raft_handle_wrapper_t
// ---------------------
// Wraps raft::resources (CUDA stream + allocators) with device metadata:
//   - device_id_: physical CUDA device ID
//   - rank_:      logical rank (index into devices_ vector)
//   - mode_:      distribution mode (SINGLE_GPU / REPLICATED / SHARDED)
//   - index_ptr_: std::any — used by REPLICATED mode to cache a per-thread
//                 pointer to the local GPU index (set in init_fn, read in search)
//                 so hot-path searches avoid taking the index map mutex.
//   - sync():     calls raft::resource::sync_stream(); with force_all_ranks=true
//                 also syncs all other ranks (used by build completion).
//
// DEQUEUE-TIME BATCHING
// ---------------------
// Optional path for fusing multiple concurrent search requests into a single
// cuVS call to improve GPU utilization.  Batching happens *inside* the worker
// loop (run_device_loop), not at submit time:
//   - Enabled by set_batch_window(window_us > 0) on the index; 0 = disabled
//     (the default — every task then runs standalone, byte-identical behavior).
//   - submit_batchable(batch_key, req, nq, bexec) enqueues an ordinary task
//     tagged with a batch_key (a process-unique id for "this index + variant +
//     limit"; see gpu_index_base_t::batch_key_for) and a fused-search runner.
//   - When the worker pops a batchable task it drains all already-queued tasks
//     with the same key (zero wait), then waits up to a total batch_window_us
//     budget for stragglers (caps: kMaxBatchCount, kMaxBatchQueries), then runs
//     them as ONE fused search via bexec, fulfilling each request's result.
//   - bexec(handle, reqs, setters): gets the per-request payloads and a vector
//     of per-request setters (each writes one result/exception into results_store_).
//   - A non-matching head task is left in the queue and becomes the first task
//     of the next group, preserving FIFO between batch groups.
//
// DISTRIBUTION MODE USAGE BY INDEX TYPES
// ----------------------------------------
//   SINGLE_GPU:
//     build    → submit_main
//     extend   → submit_main
//     search   → submit (round-robin; with multiple threads, multiple searches
//                can overlap on the same GPU via separate streams)
//
//   REPLICATED:
//     build    → submit_all_devices (each GPU builds its own full copy)
//     extend   → submit_all_devices (each GPU extends its copy concurrently)
//     search   → submit (round-robin across GPU replicas)
//
//   SHARDED:
//     build    → submit_all_devices (each GPU builds its shard)
//     extend   → submit_to_rank (only extend to last shard)
//     search   → submit_all_devices_no_wait (all shards search concurrently)
//                → results collected and merged by merge_sharded_results()
//
// THREAD SAFETY
// -------------
// - thread_safe_queue_t: all operations protected by internal mutex + condvars.
//   Bounded capacity (1000) — push blocks if full, pop blocks if empty.
// - cuvs_task_result_store_t: 64-shard lock-striped; each shard has its own
//   mutex so high-concurrency wait/store calls rarely contend.
// - next_device_idx_: atomic uint32_t, incremented without lock for round-robin.
// - Dequeue-time batch grouping happens entirely on the worker thread inside
//   run_device_loop; no extra shared state / mutex is needed for it.
//
// =============================================================================

inline const char* mode_name(distribution_mode_t mode) {
    switch (mode) {
        case DistributionMode_SINGLE_GPU: return "SINGLE_GPU";
        case DistributionMode_SHARDED: return "SHARDED";
        case DistributionMode_REPLICATED: return "REPLICATED";
        default: return "UNKNOWN";
    }
}

/**
 * @brief Thread-safe queue for worker tasks.
 */
template <typename T>
class thread_safe_queue_t {
public:
    void push(T item) {
        std::unique_lock<std::mutex> lock(mu_);
        cond_can_push_.wait(lock, [this]() { return stopped_ || (capacity_ == 0 || queue_.size() < capacity_); });
        if (stopped_) throw std::runtime_error("Queue is stopped");
        queue_.push(std::move(item));
        cond_can_pop_.notify_one();
    }

    bool pop(T& item) {
        std::unique_lock<std::mutex> lock(mu_);
        cond_can_pop_.wait(lock, [this]() { return stopped_ || !queue_.empty(); });
        if (queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        cond_can_push_.notify_one();
        return true;
    }

    bool pop_wait(T& item, std::chrono::microseconds timeout) {
        std::unique_lock<std::mutex> lock(mu_);
        if (!cond_can_pop_.wait_for(lock, timeout, [this]() { return stopped_ || !queue_.empty(); })) {
            return false;
        }
        if (queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        cond_can_push_.notify_one();
        return true;
    }

    bool try_pop(T& item) {
        std::unique_lock<std::mutex> lock(mu_);
        if (queue_.empty()) return false;
        item = std::move(queue_.front());
        queue_.pop();
        cond_can_push_.notify_one();
        return true;
    }

    // Non-blocking pop, but only if the head satisfies `pred`. A non-matching
    // (or empty) head is left untouched and false is returned. Used by the
    // worker to drain already-queued same-batch-key tasks with zero wait.
    template <class Pred>
    bool try_pop_if(Pred&& pred, T& item) {
        std::unique_lock<std::mutex> lock(mu_);
        if (queue_.empty()) return false;
        if (!pred(static_cast<const T&>(queue_.front()))) return false;
        item = std::move(queue_.front());
        queue_.pop();
        cond_can_push_.notify_one();
        return true;
    }

    // Blocking pop with timeout, but only if the head satisfies `pred`. If the
    // head does not match (different batch key), it is left in the queue and
    // false is returned immediately — this is what preserves FIFO between batch
    // groups: the non-matching task becomes the first task of the next group.
    template <class Pred>
    bool pop_wait_if(Pred&& pred, T& item, std::chrono::microseconds timeout) {
        std::unique_lock<std::mutex> lock(mu_);
        if (!cond_can_pop_.wait_for(lock, timeout, [this]() { return stopped_ || !queue_.empty(); })) {
            return false;  // timed out, still empty
        }
        if (queue_.empty()) return false;                                 // stopped / spurious
        if (!pred(static_cast<const T&>(queue_.front()))) return false;   // different key: leave it
        item = std::move(queue_.front());
        queue_.pop();
        cond_can_push_.notify_one();
        return true;
    }

    void stop() {
        std::lock_guard<std::mutex> lock(mu_);
        stopped_ = true;
        cond_can_pop_.notify_all();
        cond_can_push_.notify_all();
    }

    bool is_stopped() const {
        std::lock_guard<std::mutex> lock(mu_);
        return stopped_;
    }

    void set_capacity(size_t capacity) {
        std::lock_guard<std::mutex> lock(mu_);
        capacity_ = capacity;
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mu_);
        return queue_.size();
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mu_);
        return queue_.empty();
    }

private:
    std::queue<T> queue_;
    mutable std::mutex mu_;
    std::condition_variable cond_can_pop_;
    std::condition_variable cond_can_push_;
    size_t capacity_ = 0;
    bool stopped_ = false;
};

struct cuvs_task_result_t {
    std::any result;
    std::exception_ptr error;
};

/**
 * @brief Store for tracking and waiting on async task results.
 */
class cuvs_task_result_store_t {
public:
    uint64_t get_next_job_id() { return next_id_++; }

    void store(uint64_t id, cuvs_task_result_t result) {
        auto& shard = shards_[id % num_shards];
        std::shared_ptr<std::promise<cuvs_task_result_t>> promise;
        {
            std::lock_guard<std::mutex> lock(shard.mu);
            if (stopped_) return; 

            auto it = shard.placeholders.find(id);
            if (it != shard.placeholders.end()) {
                if (it->second.promise) {
                    promise = std::move(it->second.promise);
                }
                shard.placeholders.erase(it);  // prevent leak: placeholder served its purpose
            } else {
                shard.results[id] = result;
            }
        }
        if (promise) {
            try { promise->set_value(result); } catch (...) {}
        }
    }

    std::shared_future<cuvs_task_result_t> wait(uint64_t id) {
        auto& shard = shards_[id % num_shards];
        std::lock_guard<std::mutex> lock(shard.mu);

        if (stopped_) {
            auto p = std::make_shared<std::promise<cuvs_task_result_t>>();
            auto f = p->get_future().share();
            p->set_exception(std::make_exception_ptr(std::runtime_error("Worker stopped")));
            return f;
        }

        auto pit = shard.placeholders.find(id);
        if (pit != shard.placeholders.end()) {
            return pit->second.future;
        }

        auto rit = shard.results.find(id);
        if (rit != shard.results.end()) {
            auto p = std::make_shared<std::promise<cuvs_task_result_t>>();
            auto f = p->get_future().share();
            p->set_value(std::move(rit->second));
            shard.results.erase(rit);
            // Do not cache into placeholders — result is consumed once; callers
            // must hold the returned shared_future for multiple .get() calls.
            return f;
        }

        auto p = std::make_shared<std::promise<cuvs_task_result_t>>();
        auto f = p->get_future().share();
        shard.placeholders[id] = {p, f};
        return f;
    }

    // Releases all bookkeeping for `id`. Must be called when the result is no
    // longer needed (e.g. fire-and-forget tasks) or when the caller abandons
    // the future. Any subsequent wait(id) will create a new unfulfilled future.
    void discard(uint64_t id) {
        auto& shard = shards_[id % num_shards];
        std::lock_guard<std::mutex> lock(shard.mu);
        shard.results.erase(id);
        shard.placeholders.erase(id);
    }

    void stop() {
        for (uint32_t i = 0; i < num_shards; ++i) {
            auto& shard = shards_[i];
            decltype(shard.placeholders) phs;
            {
                std::lock_guard<std::mutex> lock(shard.mu);
                stopped_ = true;
                phs = std::move(shard.placeholders);
                shard.results.clear();
            }
            for (auto& [id, ph] : phs) {
                try {
                    if (ph.promise) {
                        ph.promise->set_exception(
                            std::make_exception_ptr(std::runtime_error("Worker stopped")));
                    }
                } catch (...) {}
            }
        }
    }

private:
    static constexpr uint32_t num_shards = 64;
    std::atomic<bool> stopped_{false};

    struct placeholder_t {
        std::shared_ptr<std::promise<cuvs_task_result_t>> promise;
        std::shared_future<cuvs_task_result_t> future;
    };

    struct shard_t {
        std::unordered_map<uint64_t, cuvs_task_result_t> results;
        std::unordered_map<uint64_t, placeholder_t> placeholders;
        std::mutex mu;
    };

    std::atomic<uint64_t> next_id_{0};
    shard_t shards_[num_shards];
};

/**
 * @brief Wrapper around raft::resources to provide a consistent interface for workers.
 *
 * Owns per-thread reusable buffers (pinned host scratch for the fp32→fp16
 * query path). One handle is created per worker thread and lives for the
 * thread's lifetime, so these buffers grow once and are reused across
 * thousands of searches.
 */
class raft_handle_wrapper_t {
public:
    raft_handle_wrapper_t(int device_id, int rank = 0,
                         distribution_mode_t mode = DistributionMode_SINGLE_GPU)
        : device_id_(device_id), rank_(rank), mode_(mode) {
        if (device_id >= 0) {
            res_ = std::make_shared<raft::resources>();
        } else {
            // CPU Context
            res_ = nullptr;
        }
    }

    ~raft_handle_wrapper_t() {
        if (host_half_buf_) {
            // cudaFreeHost requires a live CUDA context on this device.
            // The worker thread bound to this handle has already called
            // cudaSetDevice(device_id_) once during start(); reset it here
            // because handle dtor may run on the joining thread which may
            // have switched device contexts since.
            if (device_id_ >= 0) cudaSetDevice(device_id_);
            cudaFreeHost(host_half_buf_);
            host_half_buf_ = nullptr;
        }
    }

    std::shared_ptr<raft::resources> get_raft_resources() const { return res_; }
    int get_device_id() const { return device_id_; }
    int get_rank() const { return rank_; }
    distribution_mode_t get_mode() const { return mode_; }

    /**
     * @brief Performs synchronization.
     * @param force_all_ranks If true, performs a collective sync across all ranks.
     */
    void sync() {
        if (!res_) return;
        raft::resource::sync_stream(*res_);
    }

    void set_index_ptr(std::any ptr) { index_ptr_ = ptr; }
    std::any get_index_ptr() const { return index_ptr_; }

    /**
     * @brief Grow-only pinned-host scratch buffer for fp16 query staging.
     *
     * Returns a pointer to at least `n` halves of pinned (page-locked) host
     * memory. Pinned memory makes the subsequent H2D copy ~2× faster on the
     * wire than pageable memory. The buffer is per-thread (one
     * raft_handle_wrapper_t per worker thread), so no synchronization is
     * needed on access.
     */
    half* ensure_host_half_buf(size_t n) {
        if (n <= host_half_capacity_) return host_half_buf_;
        if (host_half_buf_) {
            cudaFreeHost(host_half_buf_);
            host_half_buf_ = nullptr;
            host_half_capacity_ = 0;
        }
        cudaError_t err = cudaMallocHost(reinterpret_cast<void**>(&host_half_buf_), n * sizeof(half));
        if (err != cudaSuccess) {
            host_half_buf_ = nullptr;
            host_half_capacity_ = 0;
            throw std::runtime_error(std::string("cudaMallocHost failed for host_half_buf: ") +
                                     cudaGetErrorString(err));
        }
        host_half_capacity_ = n;
        return host_half_buf_;
    }

    // ------------------------------------------------------------------
    // Grow-only device-side workspace buffers
    //
    // These hold the per-search query / neighbor / distance staging memory
    // that used to be allocated via raft::make_device_matrix on every call.
    // With Step A's RMM pool the underlying cost is small but non-zero;
    // reusing the same uvector eliminates the pool free-list traffic
    // entirely on the hot path.
    //
    // Each accessor is grow-only — once the buffer reaches the largest n
    // seen on this thread, subsequent calls return without resizing. All
    // resizes are stream-ordered on the handle's CUDA stream, so they
    // serialize correctly with searches issued on the same handle.
    //
    // Lifetime: these uvectors are destroyed in the handle's dtor. The RMM
    // pool installed in start() is process-lifetime, so the deallocations
    // always release into a live pool — no ordering hazard at shutdown.
    //
    // Thread-safety: one handle per worker thread; no synchronization
    // needed on access. Dequeue-time batching aggregates queries from
    // multiple callers into one search, but the fused search runs on the
    // worker thread holding the handle, so concurrent access is impossible
    // by construction.
    // ------------------------------------------------------------------

    rmm::device_uvector<float>& q_dev_buf_float(size_t n) {
        return ensure_uvec_(q_buf_f_, n);
    }
    rmm::device_uvector<__half>& q_dev_buf_half(size_t n) {
        return ensure_uvec_(q_buf_h_, n);
    }
    rmm::device_uvector<int8_t>& q_dev_buf_int8(size_t n) {
        return ensure_uvec_(q_buf_i8_, n);
    }
    rmm::device_uvector<uint8_t>& q_dev_buf_uint8(size_t n) {
        return ensure_uvec_(q_buf_u8_, n);
    }
    rmm::device_uvector<int64_t>& neighbors_buf(size_t n) {
        return ensure_uvec_(neigh_buf_, n);
    }
    rmm::device_uvector<float>& distances_buf(size_t n) {
        return ensure_uvec_(dist_buf_, n);
    }
    rmm::device_uvector<uint32_t>& cagra_neighbors_buf(size_t n) {
        return ensure_uvec_(cagra_neigh_buf_, n);
    }

    /**
     * @brief Templated dispatch to the per-type query workspace buffer.
     *
     * Used in templated search bodies (ivf_pq / ivf_flat / cagra) to fetch
     * the right grow-only uvector for `T` without an `if constexpr` chain
     * at every call site.
     */
    template <typename U>
    rmm::device_uvector<U>& q_dev_buf(size_t n) {
        if constexpr (std::is_same_v<U, float>)        return q_dev_buf_float(n);
        else if constexpr (std::is_same_v<U, __half>)  return q_dev_buf_half(n);
        else if constexpr (std::is_same_v<U, int8_t>)  return q_dev_buf_int8(n);
        else if constexpr (std::is_same_v<U, uint8_t>) return q_dev_buf_uint8(n);
        else static_assert(sizeof(U) == 0, "q_dev_buf: unsupported query element type");
    }

private:
    template <typename U>
    rmm::device_uvector<U>& ensure_uvec_(std::unique_ptr<rmm::device_uvector<U>>& slot, size_t n) {
        auto stream = raft::resource::get_cuda_stream(*res_);
        if (!slot) {
            slot = std::make_unique<rmm::device_uvector<U>>(n, stream);
            return *slot;
        }
        if (slot->size() < n) {
            slot->resize(n, stream);
        }
        return *slot;
    }

    int device_id_;
    int rank_;
    std::shared_ptr<raft::resources> res_;
    distribution_mode_t mode_;
    std::any index_ptr_;

    // Pinned-host fp16 staging buffer for the fp32-input → fp16-index search
    // path. Sized to (max num_queries × dimension) seen so far on this thread.
    half*  host_half_buf_      = nullptr;
    size_t host_half_capacity_ = 0;

    // Grow-only device workspace buffers (allocated on the handle's stream
    // out of the RMM pool installed by ensure_rmm_pool_for_device).
    std::unique_ptr<rmm::device_uvector<float>>    q_buf_f_;
    std::unique_ptr<rmm::device_uvector<__half>>   q_buf_h_;
    std::unique_ptr<rmm::device_uvector<int8_t>>   q_buf_i8_;
    std::unique_ptr<rmm::device_uvector<uint8_t>>  q_buf_u8_;
    std::unique_ptr<rmm::device_uvector<int64_t>>  neigh_buf_;
    std::unique_ptr<rmm::device_uvector<float>>    dist_buf_;
    std::unique_ptr<rmm::device_uvector<uint32_t>> cagra_neigh_buf_;
};

class cuvs_worker_t {
public:
    using raft_handle = raft_handle_wrapper_t;
    using task_fn_t = std::function<std::any(raft_handle&)>;

    // Fused-search runner for a batch group: given the worker handle, the
    // per-request payloads (each carrying that request's queries), and one
    // setter per request (which writes its individual result/exception into
    // results_store_), run a single fused search and resolve every setter.
    using batch_exec_fn_t = std::function<void(raft_handle& /*handle*/,
                                               const std::vector<std::any>& /*reqs*/,
                                               const std::vector<std::function<void(std::any)>>& /*setters*/)>;

    struct cuvs_task_t {
        uint64_t id = 0;
        task_fn_t fn{};                  // empty for batchable tasks
        bool fire_and_forget = false;    // skip results_store_ — used for internal tasks
        // ---- batchable-task fields (meaningful only when batch_key != 0) ----
        uint64_t batch_key = 0;          // 0 = non-batchable (ordinary task: run fn)
        uint64_t batch_nq  = 0;          // this request's query count (for kMaxBatchQueries)
        std::any  breq{};                // per-request payload (the per-index search_req_t)
        batch_exec_fn_t bexec{};         // fused-search runner
        // (default member initializers above keep the factories below
        //  warning-free under -Wmissing-field-initializers)

        // An ordinary task: runs `fn` and (unless fire_and_forget) stores its result.
        static cuvs_task_t ordinary(task_fn_t fn, bool fire_and_forget = false) {
            cuvs_task_t t;
            t.fn = std::move(fn);
            t.fire_and_forget = fire_and_forget;
            return t;
        }
        // A batchable task: `fn` stays empty; the worker fuses it with same-key
        // peers and runs them via `exec`. `key` must be non-zero (0 = ordinary).
        static cuvs_task_t batchable(uint64_t key, uint64_t nq, std::any req, batch_exec_fn_t exec) {
            cuvs_task_t t;
            t.batch_key = key;
            t.batch_nq  = nq;
            t.breq      = std::move(req);
            t.bexec     = std::move(exec);
            return t;
        }
    };

    cuvs_worker_t(uint32_t nthread, const std::vector<int>& devices, distribution_mode_t mode = DistributionMode_SINGLE_GPU)
        : nthread_(std::max(nthread, (uint32_t)devices.size())), devices_(devices), mode_(mode), running_(false), stopping_(false), batch_window_us_(0), per_thread_device_(false), next_device_idx_(0), in_flight_tasks_(0) {
        
        // One queue per physical GPU device
        for (size_t i = 0; i < devices_.size(); ++i) {
            auto q = std::make_unique<thread_safe_queue_t<cuvs_task_t>>();
            q->set_capacity(kQueueCapacity);
            device_queues_.push_back(std::move(q));
        }
        main_tasks_.set_capacity(kQueueCapacity);
    }

    ~cuvs_worker_t() { try { stop(); } catch (...) {} }

    void start(std::function<std::any(raft_handle&)> init_fn = nullptr,
               std::function<std::any(raft_handle&)> stop_fn = nullptr) {
        if (running_) return;
        stopping_.store(false);
        running_ = true;

        // Start Main Thread (only for main_tasks_)
        main_thread_ = std::thread([this, init_fn, stop_fn] {
            int device_id = devices_.empty() ? -1 : devices_[0];
            if (device_id >= 0) {
                cudaSetDevice(device_id);
                // Install the per-device RMM pool BEFORE constructing
                // raft_handle so any rmm::device_uvector created via the
                // handle's allocator pulls from the pool from the start.
                matrixone::ensure_rmm_pool_for_device(device_id);
            }
            raft_handle handle(device_id, 0, mode_);
            if (init_fn) init_fn(handle);
            this->run_main_loop(handle, stop_fn);
        });

        // Start Pool of Device Worker Threads
        for (uint32_t i = 0; i < nthread_; ++i) {
            if (devices_.empty()) break;
            
            // Shared Pool with Device Affinity
            int device_idx = i % devices_.size(); 
            int device_id = devices_[device_idx];
            int rank = device_idx; 

            device_threads_.emplace_back([this, device_id, device_idx, rank, init_fn, stop_fn] {
                cudaSetDevice(device_id);
                // Same as the main thread: install the RMM pool BEFORE the
                // handle so all device-side allocations route through it.
                matrixone::ensure_rmm_pool_for_device(device_id);

                // Each thread in the pool gets its own raft::resources (so separate CUDA streams)
                raft_handle handle(device_id, rank, mode_);

		// only main thread will run init_fn and stop_fn
                this->run_device_loop(handle, nullptr, device_idx);
            });
        }
    }

    void stop() {
        bool was_stopping = stopping_.exchange(true);
        if (was_stopping) return;  // already stopping or stopped

        // 1. Wait for all in-flight tasks to complete. stopping_ = true already
        //    blocks new external submit* calls; queued batchable tasks still run
        //    (and are fused normally) on the worker threads.
        this->sync();

        // 2. Now block all further submissions and drain the thread queues
        running_.store(false);
        for (auto& q : device_queues_) q->stop();
        main_tasks_.stop();

        if (main_thread_.joinable()) main_thread_.join();
        for (auto& w : device_threads_) {
            if (w.joinable()) w.join();
        }
        device_threads_.clear();

        // 3. Drain physical queues (defensive; should be empty after sync).
        //    Each task — ordinary or batchable — counts as exactly one
        //    in-flight unit (submit_batchable does one ++), so one -- per task.
        cuvs_task_t task;
        while (main_tasks_.try_pop(task)) {
            in_flight_tasks_--;
            if (!task.fire_and_forget) {
                cuvs_task_result_t result;
                result.error = std::make_exception_ptr(std::runtime_error("Worker stopped"));
                results_store_.store(task.id, result);
            }
        }
        for (auto& q : device_queues_) {
            while (q->try_pop(task)) {
                in_flight_tasks_--;
                if (!task.fire_and_forget) {
                    cuvs_task_result_t result;
                    result.error = std::make_exception_ptr(std::runtime_error("Worker stopped"));
                    results_store_.store(task.id, result);
                }
            }
        }

        results_store_.stop();
    }

    void sync() {
        std::unique_lock<std::mutex> lock(sync_mu_);
        sync_cv_.wait(lock, [this]() { return in_flight_tasks_.load() == 0; });
    }

    uint64_t submit(task_fn_t fn) {
        if (!running_ || stopping_) throw std::runtime_error("Worker is not running");
        if (devices_.empty()) throw std::runtime_error("No devices configured");
        auto& q = *device_queues_[next_device_idx_++ % devices_.size()];
        return enqueue_(q, cuvs_task_t::ordinary(std::move(fn)));
    }

    uint64_t submit_main(task_fn_t fn) {
        if (!running_ || stopping_) throw std::runtime_error("Worker is not running");
        return enqueue_(main_tasks_, cuvs_task_t::ordinary(std::move(fn)));
    }

    uint64_t submit_to_rank(size_t rank, task_fn_t fn) {
        if (!running_ || stopping_) throw std::runtime_error("Worker is not running");
        if (rank >= device_queues_.size())
            throw std::runtime_error("submit_to_rank: rank out of range");
        return enqueue_(*device_queues_[rank], cuvs_task_t::ordinary(std::move(fn)));
    }

    std::vector<uint64_t> submit_all_devices_no_wait(task_fn_t fn) {
        if (!running_ || stopping_) throw std::runtime_error("Worker is not running");
        std::vector<uint64_t> ids;
        ids.reserve(devices_.size());
        for (size_t i = 0; i < devices_.size(); ++i) {
            ids.push_back(enqueue_(*device_queues_[i], cuvs_task_t::ordinary(fn)));  // copy fn per device
        }
        return ids;
    }

    void submit_all_devices(task_fn_t fn) {
        auto ids = submit_all_devices_no_wait(fn);
        std::exception_ptr first_error;
        int err_count = 0;
        for (size_t i = 0; i < ids.size(); ++i) {
            auto res = wait(ids[i]).get();
            if (!res.error) continue;
            ++err_count;
            if (!first_error) first_error = res.error;
            try { std::rethrow_exception(res.error); }
            catch (const std::exception& e) {
                std::cerr << "[submit_all_devices ERROR] rank=" << i
                          << " what=" << e.what() << std::endl;
            } catch (...) {
                std::cerr << "[submit_all_devices ERROR] rank=" << i
                          << " unknown exception" << std::endl;
            }
        }
        if (first_error) {
            std::cerr << "[submit_all_devices] " << err_count << "/" << ids.size()
                      << " device tasks failed; rethrowing first" << std::endl;
            std::rethrow_exception(first_error);
        }
    }


    std::shared_future<cuvs_task_result_t> wait(uint64_t task_id) {
        return results_store_.wait(task_id);
    }

    distribution_mode_t get_mode() const { return mode_; }

    uint32_t nthread() const { return nthread_; }

    void set_batch_window(int64_t window_us) {
        // Drain in-flight work first so no batch group is mid-straggler-wait
        // when the budget changes, then flip it. 0 disables dequeue-time
        // batching entirely (every task runs standalone).
        this->sync();
        batch_window_us_ = window_us;
    }
    int64_t batch_window() const { return batch_window_us_; }
    // NOTE: per_thread_device_ is wired through gpu_*_set_per_thread_device in the
    // C wrappers but is currently consulted by no worker code path; kept only for
    // API stability.
    void set_per_thread_device(bool enable) { per_thread_device_ = enable; }

    // Stats: number of fused batch groups executed, and total requests fused
    // across all groups. (reqs - groups) > 0 iff fusion actually happened.
    uint64_t batched_groups_total() const { return batched_groups_total_.load(std::memory_order_relaxed); }
    uint64_t batched_reqs_total()   const { return batched_reqs_total_.load(std::memory_order_relaxed); }

    // Submit a batchable search task. batch_key (must be non-zero) identifies a
    // fusable group ("this index + variant + limit"; see batch_key_for); breq is
    // the per-request payload; batch_nq is its query-row count (used to cap a
    // fused batch at kMaxBatchQueries); bexec is the fused-search runner.
    // Returns a job id resolvable via wait(id). When batch_window() == 0 the
    // task still runs (standalone) — bexec is just invoked with a 1-element group.
    uint64_t submit_batchable(uint64_t batch_key, std::any breq, uint64_t batch_nq, batch_exec_fn_t bexec) {
        if (!running_ || stopping_) throw std::runtime_error("Worker is not running");
        if (devices_.empty())       throw std::runtime_error("No devices configured");
        if (batch_key == 0)         throw std::runtime_error("submit_batchable: batch_key must be non-zero");
        auto& q = *device_queues_[next_device_idx_++ % devices_.size()];
        return enqueue_(q, cuvs_task_t::batchable(batch_key, batch_nq, std::move(breq), std::move(bexec)));
    }

private:
    // Caps on a single fused batch group (dequeue-time batching).
    static constexpr size_t   kMaxBatchCount   = 64;     // max tasks fused into one search
    static constexpr uint64_t kMaxBatchQueries = 4096;   // max total query rows in one fused search
    // Bounded capacity of every task queue (per-device + main); push blocks when full.
    static constexpr size_t   kQueueCapacity   = 1000;

    // Reserve a job id, count it in-flight, push `t` (id filled in) onto `q`.
    // On push failure roll the reservation back and rethrow. The one place the
    // in-flight / results_store invariant for submission lives.
    uint64_t enqueue_(thread_safe_queue_t<cuvs_task_t>& q, cuvs_task_t t) {
        in_flight_tasks_++;
        uint64_t id = results_store_.get_next_job_id();
        t.id = id;
        try {
            q.push(std::move(t));
        } catch (...) {
            in_flight_tasks_--;
            results_store_.discard(id);
            throw;
        }
        return id;
    }

    // RAII: subtract n from in_flight_tasks_ on exit; if it reaches 0, wake sync().
    // sync_mu_ is held across notify to close the lost-wakeup window between sync()'s
    // predicate check and its first cv.wait().
    struct flight_guard {
        std::atomic<int64_t>& counter;
        std::condition_variable& cv;
        std::mutex& mu;
        int64_t n = 1;
        ~flight_guard() {
            if ((counter -= n) == 0) {
                std::lock_guard<std::mutex> lk(mu);
                cv.notify_all();
            }
        }
    };

    // Format the arguments into one line and emit it via matrixone::log_err
    // (stderr, "[ERROR <timestamp>] " prefix; see helper.cpp).
    template <typename... Args>
    static void log_err(Args&&... a) {
        std::ostringstream os;
        (os << ... << a);
        matrixone::log_err(os.str());
    }

    void run_device_loop(raft_handle& handle, std::function<std::any(raft_handle&)> stop_fn, uint32_t d_idx) {
        auto& q = *device_queues_[d_idx];
        cuvs_task_t task;
        // Reused across iterations: clear() keeps capacity, so the kMaxBatchCount
        // backing store is allocated exactly once per worker thread (not per
        // batchable search). Moved-from cuvs_task_t slots from the previous batch
        // are destroyed by the clear() at the top of the next batchable iteration.
        std::vector<cuvs_task_t> group;
        group.reserve(kMaxBatchCount);
        while (q.pop(task)) {
            if (task.batch_key == 0) { execute_task(task, handle); continue; }

            group.clear();
            group.push_back(std::move(task));
            gather_batch_group(q, group);

            // execute_batch is effectively noexcept (bexec's exceptions are
            // caught inside it, and execute_batch's own backstops resolve every
            // group id before any exception unwinds). Catch defensively anyway
            // so a setup-phase bad_alloc can't escape the worker thread fn and
            // std::terminate the process — the in-flight count and result store
            // are already consistent by the time we get here.
            try {
                execute_batch(group, handle);
            } catch (const std::exception& e) {
                log_err("run_device_loop: execute_batch escaped: ", e.what());
            } catch (...) {
                log_err("run_device_loop: execute_batch escaped (unknown)");
            }
        }
        if (stop_fn) stop_fn(handle);
    }

    // Grow `group` (which already holds the head batchable task at [0]) with
    // same-batch-key peers: first a zero-wait drain of everything already queued,
    // then a bounded straggler wait sharing ONE total batch_window_us_ budget, so
    // the head request's worst-case extra latency is exactly that window. Caps:
    // kMaxBatchCount tasks / kMaxBatchQueries total query rows. A non-matching
    // head encountered along the way is left in `q` and becomes the first task of
    // the next group, preserving FIFO between batch groups.
    //
    // No-op when batching is disabled (batch_window_us_ == 0): a batchable task
    // can only reach the worker then via a set_batch_window(0) race, and
    // execute_batch (a 1-element group) is the only path that knows how to run
    // one — its fn is empty; bexec is the runner.
    void gather_batch_group(thread_safe_queue_t<cuvs_task_t>& q, std::vector<cuvs_task_t>& group) {
        if (batch_window_us_ == 0) return;

        const uint64_t key = group[0].batch_key;
        uint64_t nq = group[0].batch_nq;
        auto same_key = [key](const cuvs_task_t& t) noexcept { return t.batch_key == key; };

        // (a) zero-wait drain of everything already queued under this key
        while (group.size() < kMaxBatchCount && nq < kMaxBatchQueries) {
            cuvs_task_t t;
            if (!q.try_pop_if(same_key, t)) break;
            nq += t.batch_nq;
            group.push_back(std::move(t));
        }
        // (b) straggler wait — ONE total budget (not reset per iteration),
        // so the first request's worst-case extra latency is exactly
        // batch_window_us_.
        if (group.size() < kMaxBatchCount && nq < kMaxBatchQueries) {
            auto deadline = std::chrono::steady_clock::now() + std::chrono::microseconds(batch_window_us_);
            while (group.size() < kMaxBatchCount && nq < kMaxBatchQueries) {
                auto now = std::chrono::steady_clock::now();
                if (now >= deadline) break;
                cuvs_task_t t;
                if (!q.pop_wait_if(same_key, t,
                        std::chrono::duration_cast<std::chrono::microseconds>(deadline - now))) break;
                nq += t.batch_nq;
                group.push_back(std::move(t));
            }
        }
    }

    void run_main_loop(raft_handle& handle, std::function<std::any(raft_handle&)> stop_fn) {
        cuvs_task_t task;
        while (main_tasks_.pop(task)) {
            execute_task(task, handle);
        }
        if (stop_fn) stop_fn(handle);
    }

    void execute_task(const cuvs_task_t& task, raft_handle& handle) {
        flight_guard guard{in_flight_tasks_, sync_cv_, sync_mu_};

        cuvs_task_result_t result;
        try {
            result.result = task.fn(handle);
        } catch (const std::exception& e) {
            log_err("Worker execute_task error id=", task.id, " rank=", handle.get_rank(), ": ", e.what());
            result.error = std::current_exception();
        } catch (...) {
            log_err("Worker execute_task unknown error id=", task.id, " rank=", handle.get_rank());
            result.error = std::current_exception();
        }
        if (!task.fire_and_forget) {
            results_store_.store(task.id, result);
        }
    }

    // Run a group of batchable tasks as ONE fused search, then resolve each
    // request's result into results_store_. The fused-search runner is the
    // first arrival's bexec (it closed over that request's search params; per
    // the carried-over precondition, params are constant per (index, variant,
    // limit) — so any group member's bexec would do). Each task — whether part
    // of a multi-element group or run standalone — counts as exactly one
    // in-flight unit, matching submit_batchable's single ++.
    void execute_batch(std::vector<cuvs_task_t>& group, raft_handle& handle) {
        flight_guard guard{in_flight_tasks_, sync_cv_, sync_mu_, (int64_t)group.size()};

        // Early backstop: covers the *setup* phase (building per-request setters,
        // copying bexec). If anything there throws — e.g. bad_alloc from a
        // std::function ctor or make_shared — every group member still gets a
        // result stored before the exception unwinds, so no wait(id) hangs. No
        // setter has fired yet at this point (setters only fire inside bexec),
        // so this never overwrites a real result. Disarmed just before bexec.
        struct early_backstop_t {
            cuvs_task_result_store_t* store;
            const std::vector<cuvs_task_t>* group;
            bool armed = true;
            ~early_backstop_t() {
                if (!armed) return;
                try {
                    auto e = std::make_exception_ptr(std::runtime_error("Batch setup failed"));
                    for (const auto& t : *group) {
                        try { cuvs_task_result_t r; r.error = e; store->store(t.id, std::move(r)); } catch (...) {}
                    }
                } catch (...) {}
            }
        } eb{&results_store_, &group};

        std::vector<std::any> reqs;
        reqs.reserve(group.size());
        std::vector<std::function<void(std::any)>> setters;
        setters.reserve(group.size());
        for (auto& t : group) {
            reqs.push_back(std::move(t.breq));
            const uint64_t id = t.id;
            auto fulfilled = std::make_shared<std::atomic<bool>>(false);
            setters.push_back([this, id, fulfilled](std::any res) {
                if (fulfilled->exchange(true)) return;
                cuvs_task_result_t s;
                if (res.type() == typeid(std::exception_ptr)) s.error  = std::any_cast<std::exception_ptr>(res);
                else                                          s.result = std::move(res);
                results_store_.store(id, std::move(s));
            });
        }
        auto bexec = group[0].bexec;
        const uint64_t key = group[0].batch_key;

        // Late backstop: if bexec returns without resolving every setter (a buggy
        // exec_fn), the leftovers get an error. Setters are idempotent, so a
        // partial run (some resolved by bexec, some not) is handled cleanly.
        struct setters_guard_t {
            std::vector<std::function<void(std::any)>>* setters;
            bool fulfilled = false;
            ~setters_guard_t() {
                if (!fulfilled && setters) {
                    auto e = std::make_exception_ptr(std::runtime_error("Batch task cancelled"));
                    for (auto& s : *setters) { try { s(std::any(e)); } catch (...) {} }
                }
            }
        } sg{&setters};

        eb.armed = false;  // committed: the setters / late backstop now own resolution.

        try {
            bexec(handle, reqs, setters);
        } catch (const std::exception& e) {
            log_err("execute_batch error key=", key, " count=", group.size(),
                    " rank=", handle.get_rank(), ": ", e.what());
            auto err = std::current_exception();
            for (auto& s : setters) { try { s(std::any(err)); } catch (...) {} }
        } catch (...) {
            log_err("execute_batch unknown error key=", key, " count=", group.size(),
                    " rank=", handle.get_rank());
            auto err = std::current_exception();
            for (auto& s : setters) { try { s(std::any(err)); } catch (...) {} }
        }
        sg.fulfilled = true;
        batched_groups_total_.fetch_add(1, std::memory_order_relaxed);
        batched_reqs_total_.fetch_add(group.size(), std::memory_order_relaxed);
    }

    uint32_t nthread_;
    std::vector<int> devices_;
    distribution_mode_t mode_;
    std::thread main_thread_;
    std::vector<std::thread> device_threads_;
    std::atomic<bool> running_;
    std::atomic<bool> stopping_;  // set true at start of stop(); blocks new external submits
    int64_t batch_window_us_;  // batching window in microseconds; 0 = disabled
    // Set via set_per_thread_device() (wired to the C wrappers) but not read by
    // any worker code path; kept only for API stability.
    bool per_thread_device_;

    std::vector<std::unique_ptr<thread_safe_queue_t<cuvs_task_t>>> device_queues_;
    thread_safe_queue_t<cuvs_task_t> main_tasks_;
    std::atomic<uint32_t> next_device_idx_;
    std::atomic<int64_t> in_flight_tasks_;
    std::mutex            sync_mu_;
    std::condition_variable sync_cv_;

    cuvs_task_result_store_t results_store_;

    // Dequeue-time batching stats (relaxed; informational / test assertions).
    std::atomic<uint64_t> batched_groups_total_{0};
    std::atomic<uint64_t> batched_reqs_total_{0};
};

} // namespace matrixone
