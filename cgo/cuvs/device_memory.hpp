// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cuda_runtime.h>

#include <array>
#include <cstddef>
#include <filesystem>
#include <atomic>
#include <stdexcept>
#include <string>
#include <system_error>

namespace matrixone {

// ---------------------------------------------------------------------------
// device_memory_governor — the account large DEVICE allocations claim through.
//
// WHAT CLAIMS HERE. Only large allocations, and only these: build peaks, index
// loads, row uploads (upload_T_matrix / upload_float_matrix_as_T) and the
// scalar-quantizer training upload. A site joins the list when its size becomes
// knowable before the allocation.
//
// THE PROBLEM. cudaMemGetInfo reports memory that is already resident. An
// allocation that has been DECIDED but not yet made is invisible to it, so two
// callers can read the same free bytes, each conclude its allocation fits, and
// then both allocate. Two shapes of that:
//
//   load  vs load   the index cache deduplicates by cache key only, so two
//                   different cold indexes load concurrently.
//   build vs load   worse: a build's decided-but-unallocated window spans
//                   minutes (plan capacity -> scan source -> add chunks -> cuVS
//                   build), and nothing serialises it against a load.
//
// THE RULE. A claim is registered before the allocation and dropped only after
// the memory is resident. So at every instant
//
//     free(device) + reserved(device)
//
// is a complete account of what is spoken for, and the check-and-claim below is
// a single CAS so two callers cannot both spend the same headroom.
//
// WHY LOCK-FREE. There is no lock here at all. cudaMemGetInfo is thread-safe on
// its own, so it needs no protection, and the check-and-claim is a CAS loop on
// a per-device counter. Nothing in this file can contend with, invert, or
// deadlock against any existing lock — in particular it deliberately does NOT
// reuse device_build_mutex, which serialises builds for reasons of its own
// (cuVS kmeans workspace is not re-entrant per device) and is documented as the
// INNERMOST lock. Widening that lock to cover loads would have changed the
// locking semantics of every existing build path.
//
// RESIDUAL WINDOW, stated plainly. The free sample and the CAS are not one
// atomic step, so in principle a peer could allocate AND release between them,
// leaving this caller deciding on a stale budget. Reaching it requires a peer
// to finish an entire allocation inside the gap between two adjacent
// operations — microseconds at most, against allocations that run for seconds.
// The defect this exists to fix had a window spanning the whole load, which the
// CAS closes completely. Serialising the pair would also close the residue, at
// the cost of a lock; that trade was considered and declined deliberately.
//
// WHAT IT DOES NOT DO. It does not serialise the allocations themselves. Two
// loads that genuinely both fit still proceed concurrently; only the accounting
// is ordered.
// ---------------------------------------------------------------------------
class device_memory_governor {
public:
    // DEFAULT fraction of currently-free VRAM a governed allocation may consume,
    // for callers with no cost class to ask. The real value is per index --
    // index_cost_base::budget_percent in index_cost.hpp -- because how much of an
    // algorithm's build peak its per-row cost accounts for differs between them;
    // IVF-PQ holds back more, CAGRA takes this default.
    //
    // The fraction is headroom for ONE large allocation to succeed, not a
    // reservation for a second build. cudaMemGetInfo reports total free bytes, but
    // the largest contiguous block is smaller -- fragmentation, allocator
    // granularity and the driver's own reserve -- so a build sized at 100% of free
    // fails on the single allocation that needs it. CONCURRENCY is handled by the
    // ledger below instead: in-flight claims are summed, so two builds cannot both
    // pass the same free-memory snapshot however generous this fraction is.
    // THE default fraction, as a percentage. index_cost_base::kDefaultBudgetPercent
    // derives from this rather than restating it -- two independent 75s in two
    // headers is the same drift this subsystem keeps getting caught by, one header
    // apart instead of one language apart.
    //
    // Was a 3/4 numerator/denominator pair, which only ever existed to compute this
    // number and made the arithmetic differ by rounding (pool/4*3 is not
    // pool/100*75). Every fraction is a percent now, so the pair is gone.
    static constexpr size_t kBudgetPercent = 75;

    // effective_percent / budget_bytes are the ONLY places the fraction is
    // resolved and applied. Both used to be open-coded at each admission site --
    // three copies of "0 means default" and three of "pool / 100 * percent" --
    // and one copy drifting is not a hypothetical: this whole subsystem's bugs
    // have been two places disagreeing about the same number, never the
    // arithmetic itself.
    //
    // pool_bytes is FREE VRAM for an admission, TOTAL VRAM for the permanent
    // ceiling; percent 0 means "no cost class asked", i.e. the default.
    static size_t effective_percent(size_t percent) {
        return percent == 0 ? kBudgetPercent : percent;
    }
    static size_t budget_bytes(size_t pool_bytes, size_t percent) {
        return pool_bytes / 100 * effective_percent(percent);
    }

    // RAII claim. Move-only; releasing twice is a no-op, so an explicit
    // release() alongside scope exit is safe.
    class reservation {
    public:
        reservation() = default;
        reservation(int device_id, size_t bytes)
            : device_id_(device_id), bytes_(bytes), held_(bytes > 0) {}

        reservation(const reservation&)            = delete;
        reservation& operator=(const reservation&) = delete;

        reservation(reservation&& other) noexcept { *this = std::move(other); }
        reservation& operator=(reservation&& other) noexcept {
            if (this != &other) {
                release();
                device_id_ = other.device_id_;
                bytes_     = other.bytes_;
                held_      = other.held_;
                other.held_ = false;
            }
            return *this;
        }

        ~reservation() { release(); }

        // Drops the claim. Called on scope exit, including during stack
        // unwinding if the allocation throws — a claim that outlived its
        // failed allocation would shrink the device's budget permanently.
        void release() {
            if (!held_) return;
            held_ = false;
            auto&  slot = reserved(device_id_);
            size_t cur  = slot.load(std::memory_order_relaxed);
            // Clamp at zero rather than wrapping: size_t underflow here would
            // read as a colossal reservation and refuse every later load on the
            // device until restart.
            while (!slot.compare_exchange_weak(cur, cur > bytes_ ? cur - bytes_ : 0,
                                               std::memory_order_acq_rel,
                                               std::memory_order_relaxed)) {
            }
        }

    private:
        int    device_id_ = -1;
        size_t bytes_     = 0;
        bool   held_      = false;
    };

    // Admits need_bytes on the CURRENT device and returns the claim.
    //
    // The current device is the ledger key, read via cudaGetDevice: callers are
    // already inside a worker task with their device bound, and keying off the
    // ambient device avoids a cudaSetDevice here that would leave the calling
    // thread bound somewhere the caller did not ask for.
    //
    // need_bytes == 0 is REFUSED, not treated as "unknown demand". Admitting a
    // zero claim would overload one value with two meanings — "nothing to
    // allocate" and "I could not work out the size" — and the second is a defect
    // every caller here can detect locally (see required_path_bytes). A caller
    // with genuinely nothing to allocate must not call this at all.
    //
    // THROWS std::runtime_error naming the demand, the budget and the free
    // figure when it does not fit, or when need_bytes is 0.
    // budget_percent is the caller's per-index fraction (index_cost.hpp,
    // index_cost_base::budget_percent). 0 falls back to the default below, for
    // callers with no cost class to ask. Capacity sizing reads the same value, so
    // a build cannot be sized against one fraction and admitted against another.
    static reservation reserve(size_t need_bytes, const char* who, size_t budget_percent = 0) {
        int device_id = 0;
        if (cudaGetDevice(&device_id) != cudaSuccess) device_id = 0;
        return reserve_on(device_id, need_bytes, who, budget_percent);
    }

    // reserve_on names the device explicitly, for callers that are not running
    // on it — in particular the Go build path, which decides how much a build
    // will need long before any worker thread has bound a device. Reserving
    // from there is what covers the decided-but-not-yet-allocated window: a
    // claim taken inside the C++ build would only start at the allocation,
    // leaving the minutes between planning and allocating uncovered.
    static reservation reserve_on(int device_id, size_t need_bytes, const char* who,
                                  size_t budget_percent = 0) {
        if (need_bytes == 0) {
            throw std::runtime_error(
                std::string(who) + ": refusing a zero-byte VRAM claim; a zero demand means the "
                "caller could not determine the size, which is a defect, not permission to skip "
                "admission. Callers with nothing to allocate must not reserve at all.");
        }

        // cudaMemGetInfo reports the CURRENT device, so bind the requested one
        // and put it back: this runs on a caller's thread (a Go goroutine's OS
        // thread, for the build path) that did not ask to be moved.
        int prev_device = device_id;
        cudaGetDevice(&prev_device);
        const bool rebind = (prev_device != device_id);
        if (rebind) {
            const cudaError_t serr = cudaSetDevice(device_id);
            if (serr != cudaSuccess) {
                // CONSUME the error before leaving. A failed CUDA call latches its
                // status in the context, and the next cudaPeekAtLastError() —
                // anywhere, in unrelated code — reports it. Throwing without
                // clearing turns "this reservation asked for a device that does
                // not exist" into a spurious failure in whatever runs next.
                cudaGetLastError();
                throw std::runtime_error(std::string(who) + ": cannot select device " +
                                         std::to_string(device_id) + " to admit " +
                                         std::to_string(need_bytes) +
                                         " bytes: " + cudaGetErrorString(serr));
            }
        }

        size_t free_bytes = 0, total_bytes = 0;
        cudaError_t err = cudaMemGetInfo(&free_bytes, &total_bytes);
        if (rebind) cudaSetDevice(prev_device);
        if (err != cudaSuccess) {
            cudaGetLastError();  // consume, as above
            // Same fail-loud policy as rows_fitting_gpu_mem: an allocation that
            // OOMs at first search is worse than one that never happens.
            throw std::runtime_error(std::string(who) + ": cudaMemGetInfo failed while admitting " +
                                     std::to_string(need_bytes) +
                                     " bytes: " + cudaGetErrorString(err));
        }

        // Normalise first: the refusal message below quotes budget_percent, and an
        // un-normalised 0 would print "0% of N free".
        budget_percent = effective_percent(budget_percent);
        const size_t budget = budget_bytes(free_bytes, budget_percent);

        // Check-and-claim as one CAS so two concurrent callers cannot both pass
        // against the same ledger value. On CAS failure `inflight` is refreshed
        // with the winner's value and the budget is re-checked — a loser that no
        // longer fits is refused rather than squeezed in.
        auto&  slot     = reserved(device_id);
        size_t inflight = slot.load(std::memory_order_relaxed);
        for (;;) {
            // Subtract rather than add: inflight + need_bytes can wrap size_t,
            // and a wrapped sum compares SMALL, so the overflow would admit the
            // one claim least able to be honoured.
            const size_t left = budget > inflight ? budget - inflight : 0;
            if (need_bytes > left) {
                throw std::runtime_error(
                    std::string(who) + ": needs " + std::to_string(need_bytes) +
                    " bytes of VRAM on device " + std::to_string(device_id) + " but only " +
                    std::to_string(left) + " are available (" + std::to_string(budget_percent) +
                    "% of " + std::to_string(free_bytes) +
                    " free, " + std::to_string(inflight) + " already reserved by concurrent " +
                    "builds/loads); evict cached indexes, drop and rebuild at a smaller " +
                    "max_index_capacity, or use a larger GPU / more GPUs");
            }
            if (slot.compare_exchange_weak(inflight, inflight + need_bytes,
                                           std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
                break;
            }
        }
        return reservation(device_id, need_bytes);
    }

    // Bytes currently claimed on a device. Test/diagnostic only.
    static size_t reserved_bytes(int device_id) {
        return reserved(device_id).load(std::memory_order_relaxed);
    }

private:
    static constexpr int kMaxDevices = 64;

    static std::atomic<size_t>& reserved(int device_id) {
        static std::array<std::atomic<size_t>, kMaxDevices> slots{};
        static std::atomic<size_t> fallback{0};
        if (device_id < 0 || device_id >= kMaxDevices) return fallback;
        return slots[static_cast<size_t>(device_id)];
    }
};

// path_bytes returns the on-disk size of `path`: the file size for a regular
// file, or the summed size of the regular files directly inside a directory.
// 0 when the path cannot be inspected, which reserve() reads as "unknown".
//
// This is how a load learns what it is about to pull into VRAM without the Go
// caller passing a size down through the C API: the serialized index on disk is
// what deserialize() materialises.
inline size_t path_bytes(const std::string& path) {
    std::error_code ec;
    auto st = std::filesystem::status(path, ec);
    if (ec) return 0;
    if (std::filesystem::is_regular_file(st)) {
        auto n = std::filesystem::file_size(path, ec);
        return ec ? 0 : static_cast<size_t>(n);
    }
    if (!std::filesystem::is_directory(st)) return 0;
    size_t total = 0;
    for (std::filesystem::directory_iterator it(path, ec), end; !ec && it != end; it.increment(ec)) {
        std::error_code fec;
        if (!it->is_regular_file(fec) || fec) continue;
        auto n = it->file_size(fec);
        if (!fec) total += static_cast<size_t>(n);
    }
    return total;
}


// required_path_bytes is path_bytes for callers that KNOW the artifact must be
// there — an index file that was just unpacked, for instance.
//
// path_bytes reports 0 for "no size I can determine", which reserve() reads as
// "unknown demand, do not guess" and admits without a claim. At a load site that
// reading is wrong in both directions: the file is supposed to exist, so 0 means
// missing, unreadable, or truncated, and admitting the load anyway skips the
// admission AND defers the real complaint to a later, more cryptic deserialize
// failure. THROWS std::runtime_error naming the path instead.
inline size_t required_path_bytes(const std::string& path, const char* who) {
    const size_t n = path_bytes(path);
    if (n == 0) {
        throw std::runtime_error(
            std::string(who) + ": index artifact \"" + path +
            "\" is missing, unreadable, or empty; cannot size its VRAM before loading");
    }
    return n;
}

}  // namespace matrixone
