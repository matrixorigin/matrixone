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

// Accounted allocations already carry stable, bounded provenance. Reusing one
// synthetic sample per owner/site avoids collecting and hashing the same
// runtime stack for every vector growth in a hash build.
var accountedProfileSamples [AllocationOwnerMax + 1][256]atomic.Pointer[malloc.HeapSampleValues]

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

	shard := getProfileShard(ptr)
	shard.mu.Lock()
	shard.m[ptr] = values
	shard.mu.Unlock()
}

func profileRecordFree(ptr uintptr, sz int64) {
	if !profilingEnabled.Load() {
		return
	}
	shard := getProfileShard(ptr)
	shard.mu.Lock()
	values, ok := shard.m[ptr]
	if ok {
		delete(shard.m, ptr)
	}
	shard.mu.Unlock()
	if ok {
		values.Bytes.Inuse.Add(-sz)
		values.Objects.Inuse.Add(-1)
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
		"| mpool accounted owner=%d site=%d |",
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

func profileRecordRealloc(skip int, oldPtr, newPtr uintptr, oldSz, newSz int64) {
	if !profilingEnabled.Load() {
		return
	}
	profileRecordFree(oldPtr, oldSz)
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
