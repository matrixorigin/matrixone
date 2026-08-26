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

#include "host_meminfo.h"

#include <atomic>
#include <cstddef>
#include <stdexcept>
#include <string>

namespace matrixone {

// ---------------------------------------------------------------------------
// host_memory_governor — the account large HOST allocations claim through.
//
// The device twin of this lives in device_memory.hpp and the two work the same
// way: sample what is available, add the claims already in flight, and admit or
// refuse under one CAS so two callers cannot spend the same headroom.
//
// WHY IT IS HERE AND NOT IN GO. The bytes are allocated here. A ledger in Go
// can only bracket a cgo call, so it covers the window it can see -- the one
// around InitEmpty -- and anything the native side allocates outside that
// window is charged to nobody. Growing an allocation early so it would land
// inside the Go window worked, but it is the allocation bending to fit the
// accounting. A claim taken at the allocation is accurate wherever the
// allocation happens to be.
//
// THE PROBLEM IT SOLVES is the same one as on the device side. Availability
// reports memory that is already taken; an allocation that has been DECIDED but
// not yet made is invisible to it. Two builds each read the same headroom, each
// conclude they fit, and both allocate.
//
// WHAT IT DOES NOT DO. It does not free anything, does not serialise the
// allocations, and does not track the memory's lifetime -- see release() for
// why a claim held past its allocation is worse than useless.
// ---------------------------------------------------------------------------
class host_memory_governor {
public:
    // Fraction of AVAILABLE host memory a governed allocation may consume.
    //
    // Deliberately looser than the device side's default: VRAM is contested by
    // the RMM pool, the graph build workspace and kernel scratch, which host
    // memory is not. The remaining 25% is headroom for concurrent queries, the
    // mpool, and allocator slack.
    //
    // The Go capacity model uses the same 75% (hostBudgetNumerator/Denominator
    // in hostbudget.go). It expresses it as /4*3 where this is /100*75, so the
    // two can differ by a few bytes of truncation on the same input; this side
    // is the smaller of the two, so admission cannot be looser than the sizing
    // that led to it.
    static constexpr size_t kBudgetPercent = 75;

    static size_t budget_bytes(size_t avail_bytes) {
        return avail_bytes / 100 * kBudgetPercent;
    }

    // RAII claim. Move-only; releasing twice is a no-op, so an explicit
    // release() alongside scope exit is safe.
    class reservation {
    public:
        reservation() = default;
        explicit reservation(size_t bytes) : bytes_(bytes), held_(bytes > 0) {}

        reservation(const reservation&)            = delete;
        reservation& operator=(const reservation&) = delete;

        reservation(reservation&& other) noexcept { *this = std::move(other); }
        reservation& operator=(reservation&& other) noexcept {
            if (this != &other) {
                release();
                bytes_      = other.bytes_;
                held_       = other.held_;
                other.held_ = false;
            }
            return *this;
        }

        ~reservation() { release(); }

        size_t bytes() const { return held_ ? bytes_ : 0; }

        // Drops the claim, declaring that the bytes it stood for have now been
        // allocated -- or that nothing will allocate them after all, on a
        // failure path. It frees nothing: the name is about the ledger, and the
        // memory it covered usually outlives this call by the whole build.
        //
        // WHERE this happens is the whole contract. Once the allocator has
        // taken the bytes, availability has already dropped by that amount, so
        // a claim still on the ledger is counted TWICE -- once in the ledger,
        // once in the lowered availability -- and the headroom lost is the full
        // size of the claim for as long as it is held. Released too early and
        // it is counted nowhere, and the next caller is admitted against bytes
        // that are already spoken for.
        //
        // So the window a claim covers is exactly the one availability cannot
        // see: decided, not yet allocated.
        void release() {
            if (!held_) return;
            held_       = false;
            auto&  slot = reserved();
            size_t cur  = slot.load(std::memory_order_relaxed);
            // Clamp at zero rather than wrapping: an underflow here would read
            // as a colossal claim and refuse every later allocation until
            // restart.
            while (!slot.compare_exchange_weak(cur, cur > bytes_ ? cur - bytes_ : 0,
                                               std::memory_order_acq_rel,
                                               std::memory_order_relaxed)) {
            }
        }

    private:
        size_t bytes_ = 0;
        bool   held_  = false;
    };

    // Admits need_bytes against the ledger, or THROWS std::runtime_error naming
    // the demand, what is already claimed, and the budget. `who` names the
    // caller and appears in the refusal.
    //
    // A refusal is an ordinary outcome -- it means another build holds the
    // headroom -- not a malfunction.
    static reservation reserve(size_t need_bytes, const char* who) {
        return reserve_against(host_available_bytes(), need_bytes, who);
    }

    // reserve with the availability reading supplied rather than sampled.
    //
    // This is the whole of the budgeting rule, so it is also what the tests
    // drive: sampling live memory inside the rule would make every assertion
    // about admission depend on what else the machine is doing. Production
    // callers want reserve() above.
    static reservation reserve_against(host_available_t avail, size_t need_bytes,
                                       const char* who) {
        if (need_bytes == 0) {
            // Same policy as the device governor: a zero demand means the
            // caller could not size its allocation, which is a defect, not
            // permission to skip admission. A caller with nothing to allocate
            // must not reserve at all.
            throw std::runtime_error(
                std::string(who) +
                ": refusing a zero-byte host memory claim; a caller that cannot size its "
                "allocation must not be admitted");
        }

        // Unmeasurable availability yields a no-op claim rather than a refusal.
        // The capacity model already treats an unmeasured host as "bound by
        // device memory only", and failing here instead would refuse builds
        // that work today wherever /proc and the cgroup files are unreadable.
        // Such a host keeps the pre-existing race; it gains no new failure mode.
        if (!avail.measured) return reservation();

        const size_t budget = budget_bytes(static_cast<size_t>(avail.bytes));

        // Check-and-claim as one CAS so two concurrent callers cannot both pass
        // against the same ledger value. On CAS failure `inflight` is refreshed
        // with the winner's value and the budget re-checked, so a loser that no
        // longer fits is refused rather than squeezed in.
        auto&  slot     = reserved();
        size_t inflight = slot.load(std::memory_order_relaxed);
        for (;;) {
            if (inflight + need_bytes > budget) {
                throw std::runtime_error(
                    std::string(who) + ": host memory admission refused: " +
                    std::to_string(need_bytes) + " bytes requested, " +
                    std::to_string(inflight) +
                    " already claimed by in-flight builds, budget is " + std::to_string(budget) +
                    " bytes (" + std::to_string(kBudgetPercent) + "% of " +
                    std::to_string(avail.bytes) +
                    " available). Retry when the other build finishes, lower "
                    "max_index_capacity, or run on a larger CN");
            }
            if (slot.compare_exchange_weak(inflight, inflight + need_bytes,
                                           std::memory_order_acq_rel,
                                           std::memory_order_relaxed)) {
                break;
            }
        }
        return reservation(need_bytes);
    }

    // Bytes currently claimed. Test and diagnostic only: acting on it is a
    // race, since it can change between the read and the act.
    static size_t reserved_bytes() { return reserved().load(std::memory_order_relaxed); }

private:
    static std::atomic<size_t>& reserved() {
        static std::atomic<size_t> slot{0};
        return slot;
    }
};

}  // namespace matrixone
