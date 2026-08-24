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

package mpool

import (
	"bytes"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/stretchr/testify/require"
)

func TestProfileAllocFree(t *testing.T) {
	EnableProfiling()
	defer DisableProfiling()

	mp, err := NewMPool("test-profile", 0, NoFixed)
	require.NoError(t, err)
	defer DeleteMPool(mp)

	before := ProfileTrackedCount()

	// Off-heap allocation should increase tracked count
	bs, err := mp.Alloc(4096, true)
	require.NoError(t, err)
	require.Equal(t, before+1, ProfileTrackedCount())

	// Free should decrease tracked count
	mp.Free(bs)
	require.Equal(t, before, ProfileTrackedCount())
}

func TestProfileOnHeapNotTracked(t *testing.T) {
	EnableProfiling()
	defer DisableProfiling()

	mp, err := NewMPool("test-profile-onheap", 0, NoFixed)
	require.NoError(t, err)
	defer DeleteMPool(mp)

	before := ProfileTrackedCount()

	// On-heap allocation should NOT change tracked count
	bs, err := mp.Alloc(1024, false)
	require.NoError(t, err)
	require.Equal(t, before, ProfileTrackedCount())

	mp.Free(bs)
}

func TestProfileMultipleAllocs(t *testing.T) {
	EnableProfiling()
	defer DisableProfiling()

	mp, err := NewMPool("test-profile-multi", 0, NoFixed)
	require.NoError(t, err)
	defer DeleteMPool(mp)

	before := ProfileTrackedCount()

	// Multiple off-heap allocations
	allocs := make([][]byte, 10)
	for i := range allocs {
		allocs[i], err = mp.Alloc(1024, true)
		require.NoError(t, err)
	}
	require.Equal(t, before+10, ProfileTrackedCount())

	// Free all
	for _, bs := range allocs {
		mp.Free(bs)
	}
	require.Equal(t, before, ProfileTrackedCount())
}

func TestProfileWritable(t *testing.T) {
	EnableProfiling()
	defer DisableProfiling()

	mp, err := NewMPool("test-profile-write", 0, NoFixed)
	require.NoError(t, err)
	defer DeleteMPool(mp)

	bs, err := mp.Alloc(8192, true)
	require.NoError(t, err)

	var buf bytes.Buffer
	profiler := malloc.GlobalProfiler()
	require.NoError(t, profiler.Write(&buf))
	require.True(t, buf.Len() > 0, "profile output should be non-empty")

	mp.Free(bs)
}

func TestAccountedProfileUsesProvenanceAcrossProfilingToggle(t *testing.T) {
	DisableProfiling()
	defer DisableProfiling()
	registry, account := newTestAllocationAccount(t, 1024, 1)
	mp := MustNew("accounted-profile")
	defer DeleteMPool(mp)
	values := accountedProfileSample(testAllocationOwner, testAllocationSite)
	before := values.Values()
	trackedBefore := ProfileTrackedCount()

	EnableProfiling()
	buffer, err := mp.AllocAccounted(
		64,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.NoError(t, err)
	require.Equal(t, trackedBefore, ProfileTrackedCount(),
		"accounted provenance does not need a per-pointer stack entry")
	afterAlloc := values.Values()
	require.Equal(t, int64(1), afterAlloc[0]-before[0])
	require.Equal(t, int64(64), afterAlloc[1]-before[1])
	require.Equal(t, int64(1), afterAlloc[2]-before[2])
	require.Equal(t, int64(64), afterAlloc[3]-before[3])

	// The allocation lease remembers whether it was profiled, so disabling
	// collection cannot strand an existing in-use sample.
	DisableProfiling()
	mp.Free(buffer)
	afterFree := values.Values()
	require.Equal(t, before[2], afterFree[2])
	require.Equal(t, before[3], afterFree[3])
	finalizeTestAllocationAccount(t, registry, account)
}

func TestAccountedProfileNoLockPoolTeardown(t *testing.T) {
	DisableProfiling()
	defer DisableProfiling()
	registry, account := newTestAllocationAccount(t, 1024, 1)
	mp := MustNewNoLock("accounted-profile-no-lock-teardown")
	require.NoError(t, mp.BindAllocationAccount(account))
	values := accountedProfileSample(testAllocationOwner, testAllocationSite)
	before := values.Values()

	EnableProfiling()
	_, err := mp.AllocAccounted(
		64,
		account,
		testAllocationOwner,
		testAllocationSite,
	)
	require.NoError(t, err)
	afterAlloc := values.Values()
	require.Equal(t, int64(1), afterAlloc[2]-before[2])
	require.Equal(t, int64(64), afterAlloc[3]-before[3])

	// Pool teardown, like an ordinary Free, must retire the synthetic
	// owner/site in-use sample even when profiling is disabled meanwhile.
	DisableProfiling()
	DeleteMPool(mp)
	afterDelete := values.Values()
	require.Equal(t, before[2], afterDelete[2])
	require.Equal(t, before[3], afterDelete[3])
	finalizeTestAllocationAccount(t, registry, account)
}

func TestPointerMetadataRejectsDuplicateAcrossKinds(t *testing.T) {
	ptr := unsafe.Pointer(new(byte))
	hdr := memHdr{allocSz: 1}
	lease := allocationLease{}

	t.Run("no-lock", func(t *testing.T) {
		mp := &MPool{
			noLock: true,
			ptrs:   make(map[unsafe.Pointer]memHdr),
		}
		require.NoError(t, mp.recordPtrHdr(ptr, hdr))
		require.Error(t, mp.recordAccountedPtrMetadata(ptr, hdr, lease))

		delete(mp.ptrs, ptr)
		require.NoError(t, mp.recordAccountedPtrMetadata(ptr, hdr, lease))
		require.Error(t, mp.recordAccountedPtrMetadata(ptr, hdr, lease))
		require.Error(t, mp.recordPtrHdr(ptr, hdr))
	})

	t.Run("global", func(t *testing.T) {
		shard := getPtrShard(ptr)
		shard.mu.Lock()
		delete(shard.m, ptr)
		delete(shard.accounted, ptr)
		shard.mu.Unlock()
		defer func() {
			shard.mu.Lock()
			delete(shard.m, ptr)
			delete(shard.accounted, ptr)
			shard.mu.Unlock()
		}()

		require.NoError(t, gRecordPtr(ptr, hdr))
		require.Error(t, gRecordAccountedPtrMetadata(ptr, hdr, lease))
		shard.mu.Lock()
		delete(shard.m, ptr)
		shard.mu.Unlock()
		require.NoError(t, gRecordAccountedPtrMetadata(ptr, hdr, lease))
		require.Error(t, gRecordAccountedPtrMetadata(ptr, hdr, lease))
		require.Error(t, gRecordPtr(ptr, hdr))
	})
}

func BenchmarkProfileAllocFree(b *testing.B) {
	EnableProfiling()
	defer DisableProfiling()
	for _, accounted := range []bool{false, true} {
		name := "stack"
		if accounted {
			name = "accounted-provenance"
		}
		b.Run(name, func(b *testing.B) {
			mp := MustNew("profile-benchmark")
			defer DeleteMPool(mp)
			var registry *AllocationAccountRegistry
			var account *AllocationAccount
			if accounted {
				registry, account = newTestAllocationAccount(b, 1<<60, 1)
				// Initialize the bounded owner/site sample outside the measured loop.
				accountedProfileSample(testAllocationOwner, testAllocationSite)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var buffer []byte
				var err error
				if accounted {
					buffer, err = mp.AllocAccounted(
						64,
						account,
						testAllocationOwner,
						testAllocationSite,
					)
				} else {
					buffer, err = mp.Alloc(64, true)
				}
				if err != nil {
					b.Fatal(err)
				}
				mp.Free(buffer)
			}
			b.StopTimer()
			if accounted {
				finalizeTestAllocationAccount(b, registry, account)
			}
		})
	}
}
