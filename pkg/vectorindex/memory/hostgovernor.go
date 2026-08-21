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

package memory

import (
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/system"
)

// ---------------------------------------------------------------------------
// Host memory governor
//
// HostRowsFitting answers "does this build fit?" from a SNAPSHOT of available
// memory. That answer is only true for one build at a time: two concurrent
// CREATE INDEX statements each snapshot the same headroom, each conclude they
// fit, and then both allocate it. The window is not small -- it spans from the
// capacity decision through InitEmpty, whose native constructor eagerly resizes
// the capacity-sized host buffers (index_base.hpp reserves host_ids to capacity;
// FilterStore::init resizes every INCLUDE column to capacity * elem_size). Two
// builds that each fit can therefore OOM-kill the CN together.
//
// This ledger closes that window the same way device_memory_governor closes it
// for VRAM: a claim is taken BEFORE the allocation and held until the memory is
// actually released, and admission is check-and-claim under a single CAS so two
// callers cannot both pass against the same ledger value.
//
// Deliberate differences from the device side:
//
//   - The ledger lives in Go, not C++. The device governor had to be native
//     because loads allocate deep inside cuVS deserialize, where Go cannot wrap
//     the allocation. The host allocation is triggered BY Go (InitEmpty), so a
//     Go claim can span snapshot -> allocation -> release exactly, and stays
//     reachable from ordinary non-gpu-tagged CI like the rest of this package.
//   - Budget is 75% (hostBudgetNumerator/Denominator), matching HostRowsFitting
//     so the admission and the capacity model cannot disagree.
//
// LIFETIME CONTRACT -- hold a claim across the ALLOCATION, not for the lifetime
// of the memory. Once the bytes are actually allocated, the live availability
// figure has already dropped by that amount, so a claim still on the ledger is
// counted TWICE: once in `inflight`, once in the lowered `avail`. The cost is
// not a rounding error, it is the full size of the claim, for as long as it is
// held:
//
//	total 512G, a build claims 100G and allocates it
//	  avail  -> 412G, budget -> 309G, inflight -> 100G
//	  a second build can now get 209G, when 309G is genuinely free
//
// For a build that runs for tens of minutes that is tens of minutes of a
// concurrent build being refused memory that exists. So callers release as soon
// as the allocation returns; the window the ledger has to cover is only the one
// where a decision has been made and the memory not yet taken, which is exactly
// the window the live figure cannot see.
// ---------------------------------------------------------------------------

// hostReserved is the per-CN ledger of host bytes claimed but not yet released.
var hostReserved atomic.Uint64

// hostAvailFn is the availability source, indirected so the budgeting rules are
// testable without depending on the machine's live memory. Mirrors the device
// side taking a free-bytes callback rather than calling CUDA itself.
var hostAvailFn = system.MemoryAvailableIncludingCache

// HostReservation is a claim on host memory. Release is idempotent and safe from
// any goroutine; a zero-byte reservation (returned when availability cannot be
// measured) releases to nothing.
type HostReservation struct {
	bytes uint64
	once  sync.Once
}

// Bytes reports what this reservation is holding. Zero means the claim was a
// no-op because availability could not be measured.
func (r *HostReservation) Bytes() uint64 {
	if r == nil {
		return 0
	}
	return r.bytes
}

// Release drops the claim from the ledger. WHERE it is called is the whole
// contract: immediately after the allocation returns, not when the memory is
// finally freed. See the lifetime note above -- a claim held past the allocation
// is counted twice, and the headroom lost is its full size.
//
// Idempotent, so the canonical shape needs no flag to tell the paths apart:
//
//	claim, err := ReserveHostMemory(n, "who")
//	if err != nil { return err }
//	defer claim.Release()   // covers an early return or a panic
//	... allocate ...
//	claim.Release()         // allocated: stop counting it now
//
// whichever runs first wins and the other is a no-op.
func (r *HostReservation) Release() {
	if r == nil {
		return
	}
	r.once.Do(func() {
		if r.bytes == 0 {
			return
		}
		// Subtracting via Add(-x) on the unsigned counter is exact as long as
		// every claim is dropped once, which once.Do guarantees.
		hostReserved.Add(^(r.bytes - 1))
		r.bytes = 0
	})
}

// ReserveHostMemory admits needBytes of host memory against the per-CN ledger,
// or refuses. `who` names the caller and appears in the refusal.
//
// A refusal is an ordinary, expected outcome -- it means another build already
// holds the headroom -- so it returns an error rather than blocking. Callers
// must release the claim once the memory is genuinely freed, on EVERY path
// (success, error, and panic).
//
// When availability cannot be measured, this returns a zero-byte reservation
// rather than an error: HostRowsFitting already treats an unmeasurable host as
// "bound by device memory only", and turning the same condition into a hard
// failure here would refuse builds that work today on hosts where /proc and the
// cgroup files are unreadable. Such a host keeps the pre-existing race; it does
// not gain a new failure mode.
func ReserveHostMemory(needBytes uint64, who string) (*HostReservation, error) {
	if needBytes == 0 {
		// Same policy as the device governor: a zero demand means the caller
		// could not size its allocation, which is a defect, not permission to
		// skip admission. Callers with nothing to allocate must not reserve.
		return nil, moerr.NewInternalErrorNoCtx(
			who + ": refusing a zero-byte host memory claim; a caller that cannot size its " +
				"allocation must not be admitted")
	}

	avail, measured := hostAvailFn()
	if !measured {
		return &HostReservation{}, nil
	}
	budget := avail / hostBudgetDenominator * hostBudgetNumerator

	// Check-and-claim as one CAS so two concurrent callers cannot both pass
	// against the same ledger value. On CAS failure the observed value is
	// refreshed with the winner's and the budget re-checked, so a loser that no
	// longer fits is refused rather than squeezed in.
	for {
		inflight := hostReserved.Load()
		if inflight+needBytes > budget {
			return nil, moerr.NewInternalErrorNoCtxf(
				"%s: host memory admission refused: %d bytes requested, %d already claimed by "+
					"in-flight builds, budget is %d bytes (75%% of %d available). Retry when the "+
					"other build finishes, lower max_index_capacity, or run on a larger CN",
				who, needBytes, inflight, budget, avail)
		}
		if hostReserved.CompareAndSwap(inflight, inflight+needBytes) {
			return &HostReservation{bytes: needBytes}, nil
		}
	}
}

// HostReservedBytes reports the ledger total. Diagnostic and test use only:
// acting on it is a race, since it can change between the read and the act.
func HostReservedBytes() uint64 {
	return hostReserved.Load()
}
