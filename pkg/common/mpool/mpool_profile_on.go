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

//go:build mpoolprofile

package mpool

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
)

// Sharded map to track per-pointer profile sample values for inuse tracking.
const numProfileShards = 128

type profileShard struct {
	mu sync.Mutex
	m  map[uintptr]*malloc.HeapSampleValues
}

var globalProfileShards [numProfileShards]profileShard

func init() {
	for i := range globalProfileShards {
		globalProfileShards[i].m = make(map[uintptr]*malloc.HeapSampleValues, 64)
	}
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

func profileRecordRealloc(skip int, oldPtr, newPtr uintptr, oldSz, newSz int64) {
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
