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
// index keeps, which the vector+INCLUDE cost model omitted entirely.
//
// Both chunked constructors reserve host_ids to capacity up front
// (cgo/cuvs/index_base.hpp:484, std::vector<int64_t>), and AddRow additionally
// inserts into id_to_index_ (:539, std::unordered_map<int64_t,uint64_t>):
//
//	host_ids             8   one int64 per row
//	map node            24   next pointer + pair<const int64_t,uint64_t>
//	allocator header     8   glibc malloc bookkeeping, per node
//	bucket slot          8   one pointer per element at load factor 1.0
//	                    --
//	                    48
//
// This is not a rounding error: with a narrow int8 vector the IDs cost several
// times the vector itself, so a capacity sized on vector+INCLUDE alone can spend
// the entire advertised host budget on ID storage and still be declared to fit.
const HostIDBytesPerRow = 48

// hostBudgetNumerator/Denominator take 75% of what is actually available. The
// budget is now derived from an accurate baseline — cgroup limit (regardless of
// PID) or MemAvailable (cache-aware) — and the per-row cost model includes every
// eager capacity-sized allocation (vector staging + INCLUDE columns), so the
// safety margin does not also need to absorb measurement error. 25% still leaves
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
	avail, measured := system.MemoryAvailableIncludingCache()
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
