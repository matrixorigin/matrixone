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

// Package memory owns the vector index's memory budgeting: how much HOST memory
// a build may pre-allocate, how much DEVICE memory a load may claim, and the
// on-disk scratch area that exists so neither has to be held in RAM.
//
// NAMING: every exported identifier starts with Host or Device, so a call site
// says which memory it is spending without the reader opening this package.
//
//	Host*    host RAM, and the local disk scratch that exists to avoid it
//	Device*  GPU VRAM
//
// Nothing here is GPU-specific in the build-tag sense -- the device side takes a
// free-bytes callback rather than calling CUDA itself, so every budgeting rule
// stays reachable from ordinary (non-gpu-tagged) CI. That is deliberate: these
// are exactly the lifecycle rules that GPU-gated tests were failing to close.
package memory

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/system"
)

// HostIDBytesPerRow is the HOST cost of the per-row identity bookkeeping every GPU
// index keeps, charged by the capacity model on top of vector + INCLUDE bytes.
//
//	host_ids   8   one int64 per row, sized to capacity alongside the vector
//	               buffer by index_base.hpp's allocate_host_capacity. This
//	               constant is what the CAPACITY model charges; the admission
//	               claim for the same bytes is taken natively, at the allocation.
//
// id_to_index_ is deliberately NOT charged. It used to be, at ~40 bytes/row (24
// map node + 8 allocator header + 8 bucket slot), and that was honest at the time:
// add_chunk inserted one node per row across the whole ingest. It is not allocated
// during a build any more -- index_base.hpp builds the map on demand in
// ensure_id_index(), because delete_id() is its only reader, the lifecycle forbids
// delete_id() before build(), and save_ids() never serializes it. Charging for it
// would now reserve host memory against a structure the build never allocates, and
// take that capacity away from the index for nothing.
//
// Keep this constant and that laziness together: if the map is ever populated
// during ingest again, this must go back to 48 AND the build must hold the map's
// share on its own lifetime, because those bytes would once more be claimed before
// the allocator had been asked for them.
//
// The 8 that remains is not a rounding error either. With a narrow int8 vector the
// ID storage is a real fraction of the row, so a capacity sized on vector+INCLUDE
// alone overcommits the host budget.
const HostIDBytesPerRow = 8

// hostAvailFn is the availability source, indirected so the budgeting rule is
// testable without depending on the machine's live memory.
var hostAvailFn = system.MemoryAvailableIncludingCache

// hostBudgetNumerator/Denominator take 75% of what is actually available. The
// budget is now derived from an accurate baseline — cgroup limit (regardless of
// PID) or MemAvailable (cache-aware) — and the per-row cost model includes every
// eager capacity-sized allocation (vector staging + INCLUDE columns + host_ids), so
// the safety margin does not also need to absorb measurement error. 25% still leaves
// headroom for concurrent queries, allocator slack, and the mpool. This is a
// deliberate divergence from the device-side 60%: VRAM is contested by the RMM
// pool + graph build workspace + kernel scratch, which host memory is not.
const hostBudgetNumerator, hostBudgetDenominator = 3, 4

// HostRowsFitting returns how many rows of perRowBytes fit the host budget, and the
// available figure it was derived from.
//
// perRowBytes here is the HOST cost of a row, and MUST cover every eager
// capacity-sized host allocation the build makes:
//   - the flattened vector staging buffer: dim * sizeof(storage-Q)
//   - every INCLUDE column's FilterStore column (FilterStore::init resizes
//     each of them to capacity * elem_size up front)
//
// A model that only charges the vector width lets a narrow vector plus several
// fixed-width INCLUDE columns allocate far beyond the claimed 60% budget.
// Callers must sum both terms before calling this.
//
// Uses system.MemoryAvailableIncludingCache() — MemFree + reclaimable buffers/cache
// (= /proc/meminfo:MemAvailable) on bare-metal, and cgroup-limit-minus-cgroup-used
// wherever a process cgroup is discoverable (regardless of whether mo-service runs
// as PID 1). A plain MemFree reading false-aborts on any warm-cache host, and a
// plain /proc/meminfo reading inside a container reports HOST memory not the
// cgroup limit and would allow pre-allocations sized against the whole node.
//
// Unlike the GPU query, an unavailable measurement here is NOT fatal:
// (rows=0, availBytes=0, err=nil) signals "unmeasured" and the caller falls
// back to the GPU / srcRowCount bounds.
//
// A SUCCESSFUL measurement that cannot hold one row is a hard error, and that
// deliberately includes avail==0 (a cgroup already at its limit). Those two
// zeros used to be indistinguishable — MemoryAvailableIncludingCache returned 0
// for both — so a full cgroup read as "unmeasured" and DISABLED the very bound
// that was supposed to stop the build, handing it to the OOM killer. The
// measured flag now separates them, and callers can treat any non-nil error as
// fatal without inspecting availBytes.
// reservedBytes is host memory that will be live AT THE SAME TIME as the
// capacity allocation and is not per-row -- today, the int8/uint8 quantizer
// staging arena. It is subtracted from the budget before the division, so the
// two allocations cannot be promised the same bytes.
//
// This is why a clamp on the arena's own size would not have worked: the arena
// and the capacity buffer are concurrent, so bounding either against the WHOLE
// budget lets their sum exceed it. Charging the arena first and deriving
// capacity from what is left is the only ordering that bounds the sum, and it
// puts the cost where it belongs -- ask for a bigger training sample and you get
// less index capacity, rather than silently overcommitting the node.
//
// A reservation that swallows the whole budget is a hard error, the same as a
// per-row cost that cannot fit: it means the requested sample alone cannot be
// held, which is a configuration to reject, not to round down.
// HostRowsFittingStaged solves the capacity and the quantizer staging arena
// TOGETHER, for builds where the arena is sized per sub-index.
//
// HostRowsFitting takes the arena as a fixed reservation, which forces the
// caller to size it before capacity is known. That is circular whenever the
// HOST is the binding constraint: the arena is capped by the final per-sub-index
// capacity (native staging_bound_rows), but that capacity is what this function
// is being asked to produce. Charging the whole source instead can refuse a
// rotation that fits -- 1 GiB of availability with a 1M-row train limit charges
// 3.07 GB of staging against an 805 MB budget, though 209,279-row sub-indexes
// fit it with room to spare.
//
// No fixed point is needed, because the arena is min(stageLimitRows, capacity):
//
//	capacity <= stageLimitRows:  budget >= capacity*(perRow + perTrainRow)
//	                             -> capacity = budget / (perRow + perTrainRow)
//	capacity >  stageLimitRows:  the arena is constant at stageLimitRows*perTrainRow
//	                             -> capacity = (budget - arena) / perRow
//
// Exactly one branch is self-consistent, so it is solved directly. perTrainRow
// is 0 for storage wider than a byte, where nothing stages and this reduces to
// plain division.
//
// Returns (0, 0, nil) when availability cannot be measured -- the caller falls
// back to the device bound, as with HostRowsFitting.
func HostRowsFittingStaged(perRowBytes, perTrainRowBytes, stageLimitRows uint64) (rows int64, availBytes uint64, err error) {
	if perRowBytes == 0 {
		return 0, 0, nil
	}
	avail, measured := hostAvailFn()
	if !measured {
		return 0, 0, nil
	}
	budget := avail / hostBudgetDenominator * hostBudgetNumerator

	if perTrainRowBytes == 0 || stageLimitRows == 0 {
		rows = int64(budget / perRowBytes)
	} else if small := budget / (perRowBytes + perTrainRowBytes); small <= stageLimitRows {
		// The arena grows with capacity, so both scale together.
		rows = int64(small)
	} else {
		// The arena is pinned at its limit; the rest of the budget is capacity.
		//
		// The arena cannot starve capacity here, and that falls out of the branch
		// condition rather than needing a guard: reaching this branch means
		// stageLimitRows < budget/(perRow + perTrainRow), so
		// stageLimitRows*perTrainRow < budget. Solving the two together is what
		// makes "the sample alone exceeds the budget" unreachable -- the shape
		// that made HostRowsFitting refuse a rotation that fits.
		rows = int64((budget - stageLimitRows*perTrainRowBytes) / perRowBytes)
	}

	if rows <= 0 {
		return 0, avail, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"host memory budget of %d bytes (75%% of %d available) cannot hold one row of %d "+
				"bytes alongside the quantizer training sample; free memory on this node or run "+
				"the build on a larger CN", budget, avail, perRowBytes))
	}
	return rows, avail, nil
}

func HostRowsFitting(perRowBytes uint64, reservedBytes uint64) (rows int64, availBytes uint64, err error) {
	if perRowBytes == 0 {
		return 0, 0, nil
	}
	// Through hostAvailFn rather than system.MemoryAvailableIncludingCache
	// directly, so the budget rule can be tested without depending on the
	// machine's live memory.
	avail, measured := hostAvailFn()
	if !measured {
		return 0, 0, nil
	}
	budget := avail / hostBudgetDenominator * hostBudgetNumerator
	if reservedBytes > 0 {
		if reservedBytes >= budget {
			return 0, avail, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
				"host memory budget of %d bytes (75%% of %d available) cannot hold the %d bytes "+
					"reserved before capacity (the int8/uint8 quantizer training sample); lower "+
					"quantizer_train_limit, or lower max_index_capacity (the sample is capped by "+
					"a sub-index's capacity, so a smaller one costs less), or free memory on this node",
				budget, avail, reservedBytes))
		}
		budget -= reservedBytes
	}
	rows = int64(budget / perRowBytes)
	if rows == 0 {
		return 0, avail, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"host memory budget of %d bytes (75%% of %d available, less %d reserved) cannot hold "+
				"one row of %d bytes; free memory on this node or run the build on a larger CN",
			budget, avail, reservedBytes, perRowBytes))
	}
	return rows, avail, nil
}
