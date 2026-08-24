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
)

// HostIDBytesPerRow is the HOST cost of the per-row identity bookkeeping every GPU
// index keeps, charged by the capacity model on top of vector + INCLUDE bytes.
//
//	host_ids   8   one int64 per row, from host_ids.reserve(capacity) in both
//	               chunked constructors (ivf_pq.hpp:266, cagra.hpp:318). reserve()
//	               mallocs the whole span, so this is taken by the time InitEmpty
//	               returns -- which is what lets the build hold ONE claim with one
//	               lifetime (see reserveBuildHost).
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

// HostIDMapBytesPerRow is the id_to_index_ cost -- 24 map node + 8 allocator
// header + 8 bucket slot -- charged ONLY where that map is actually allocated.
//
// A build never allocates it: ensure_id_index builds the map on demand and no
// build path reads it (index_base.hpp), which is why HostIDBytesPerRow above is
// 8 and not 48. A LOAD can: LoadIndex replays CDC deletes right after Unpack,
// and the first delete_id materialises the whole map. At 88M rows that is ~3.5 GB
// appearing on a path that used to charge nothing for it, where an allocation
// failure leaves the index unloadable rather than merely refused.
const HostIDMapBytesPerRow = 40

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
func HostRowsFitting(perRowBytes uint64) (rows int64, availBytes uint64, err error) {
	if perRowBytes == 0 {
		return 0, 0, nil
	}
	// Through hostAvailFn, the same seam ReserveHostMemory uses. Calling
	// system.MemoryAvailableIncludingCache directly left the package with two
	// sources of truth for availability, only one of them injectable: the budget
	// rule could not be tested without depending on the machine's live memory,
	// and two consecutive calls could legitimately disagree.
	avail, measured := hostAvailFn()
	if !measured {
		return 0, 0, nil
	}
	budget := avail / hostBudgetDenominator * hostBudgetNumerator
	rows = int64(budget / perRowBytes)
	if rows == 0 {
		return 0, avail, moerr.NewInternalErrorNoCtx(fmt.Sprintf(
			"host memory budget of %d bytes (75%% of %d available) cannot hold one row of %d bytes; "+
				"free memory on this node or run the build on a larger CN",
			budget, avail, perRowBytes))
	}
	return rows, avail, nil
}
