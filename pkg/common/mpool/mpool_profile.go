// Copyright 2021 - 2022 Matrix Origin
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

package mpool

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

var profilingEnabled atomic.Bool

// EnableProfiling turns on tracking for off-heap mpool allocations. Ordinary
// allocations are grouped by sampled call stack; accounted allocations are
// grouped by their explicit owner/site provenance.
func EnableProfiling() { profilingEnabled.Store(true) }

// DisableProfiling turns off per-allocation stack tracking.
func DisableProfiling() { profilingEnabled.Store(false) }

// ProfilingEnabled reports whether mpool profiling is active.
func ProfilingEnabled() bool { return profilingEnabled.Load() }

// Sharded map to track per-pointer profile sample values for inuse tracking.
const numProfileShards = 128

type profileShard struct {
	mu sync.Mutex
	m  map[uintptr]*malloc.HeapSampleValues
}

var globalProfileShards [numProfileShards]profileShard

// profileTracked is the global count of live entries across all shards. It
// backs a bounded empty-store fast path so that plain Free/Realloc retirement
// of pointer-keyed records can skip the shard lock when no record can exist:
// profiling disabled from start, or every recorded pointer already retired.
// Invariant: profileTracked is always >= the number of live entries. Every
// mutation happens inside the owning shard's critical section; Add(1) is
// ordered before the map insert and Add(-1) after the map delete. Therefore a
// load of 0 proves the shards are empty, and a transient overcount only costs
// the full lookup, whose miss is a no-op nil.
var profileTracked atomic.Int64

// Accounted allocations already carry stable, bounded provenance. Reusing one
// synthetic sample per owner/site avoids collecting and hashing the same
// runtime stack for every vector growth in a hash build.
var accountedProfileSamples [AllocationOwnerCatalogMax + 1][256]atomic.Pointer[malloc.HeapSampleValues]

func init() {
	for i := range globalProfileShards {
		globalProfileShards[i].m = make(map[uintptr]*malloc.HeapSampleValues, 64)
	}
	// Register with the malloc config system so that patching MpoolProfiling
	// via malloc.SetDefaultConfig dynamically enables/disables profiling.
	malloc.SetMpoolProfilingHandler(func(enabled bool) {
		profilingEnabled.Store(enabled)
	})
}

func getProfileShard(ptr uintptr) *profileShard {
	hash := ptr >> 4
	hash ^= hash >> 17
	hash *= 0x85ebca6b
	hash ^= hash >> 13
	hash *= 0xc2b2ae35
	hash ^= hash >> 16
	return &globalProfileShards[hash%numProfileShards]
}

func profileRecordAlloc(skip int, ptr uintptr, sz int64) {
	if !profilingEnabled.Load() {
		return
	}
	profiler := malloc.GlobalProfiler()
	values := profiler.Sample(skip, 1)
	values.Bytes.Allocated.Add(uint64(sz))
	values.Objects.Allocated.Add(1)
	values.Bytes.Inuse.Add(sz)
	values.Objects.Inuse.Add(1)

	profileRestore(ptr, values)
}

func profileRecordFree(ptr uintptr, sz int64) {
	if values := profileDetach(ptr); values != nil {
		values.Bytes.Inuse.Add(-sz)
		values.Objects.Inuse.Add(-1)
	}
}

// Detach address-keyed state before the physical allocator can reuse its address.
// The caller owns the returned sample until rollback or successful retirement.
func profileDetach(ptr uintptr) *malloc.HeapSampleValues {
	// Retirement is record-driven, not toggle-driven: a pointer recorded while
	// profiling was on must still retire on Free/teardown/Realloc when
	// profiling is turned off later, otherwise the released address keeps a
	// phantom sample with stale inuse accounting. The empty-store count keeps
	// the profiling-off default at one atomic read, exactly like the alloc
	// direction's enabled gate; a non-empty store takes the same shard-lock
	// lookup that the enabled path has always paid.
	if profileTracked.Load() == 0 {
		return nil
	}
	shard := getProfileShard(ptr)
	shard.mu.Lock()
	values, ok := shard.m[ptr]
	if ok {
		delete(shard.m, ptr)
		profileTracked.Add(-1)
	}
	shard.mu.Unlock()
	return values
}

func profileRestore(ptr uintptr, values *malloc.HeapSampleValues) {
	if values != nil {
		shard := getProfileShard(ptr)
		shard.mu.Lock()
		if _, ok := shard.m[ptr]; !ok {
			profileTracked.Add(1)
		}
		shard.m[ptr] = values
		shard.mu.Unlock()
	}
}

func accountedProfileSample(
	owner AllocationOwner,
	site AllocationSite,
) *malloc.HeapSampleValues {
	slot := &accountedProfileSamples[owner][site]
	if values := slot.Load(); values != nil {
		return values
	}
	values := malloc.GlobalProfiler().SampleNamed(fmt.Sprintf(
		"| mpool accounted owner=%s(%d) site=%d |",
		owner,
		owner,
		site,
	))
	if slot.CompareAndSwap(nil, values) {
		return values
	}
	return slot.Load()
}

func profileRecordAccountedAlloc(lease allocationLease, sz int64) {
	if !lease.profiled {
		return
	}
	values := accountedProfileSample(lease.owner, lease.site)
	values.Bytes.Allocated.Add(uint64(sz))
	values.Objects.Allocated.Add(1)
	values.Bytes.Inuse.Add(sz)
	values.Objects.Inuse.Add(1)
}

func profileRecordAccountedFree(lease allocationLease, sz int64) {
	if !lease.profiled {
		return
	}
	values := accountedProfileSample(lease.owner, lease.site)
	values.Bytes.Inuse.Add(-sz)
	values.Objects.Inuse.Add(-1)
}

func profileRecordRealloc(skip int, old *malloc.HeapSampleValues, newPtr uintptr, oldSz, newSz int64) {
	if old != nil {
		old.Bytes.Inuse.Add(-oldSz)
		old.Objects.Inuse.Add(-1)
	}
	profileRecordAlloc(skip, newPtr, newSz)
}

// ProfileTrackedCount returns the total number of tracked pointers across all shards.
func ProfileTrackedCount() int {
	total := 0
	for i := range globalProfileShards {
		shard := &globalProfileShards[i]
		shard.mu.Lock()
		total += len(shard.m)
		shard.mu.Unlock()
	}
	return total
}
