// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package mpool

import (
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type reallocReleaseGauge struct {
	prometheus.Gauge
	observe func(float64)
}

func (g reallocReleaseGauge) Set(value float64) { g.observe(value) }

func TestReallocRetiresMetadataBeforeCachePublication(t *testing.T) {
	EnableProfiling()
	defer DisableProfiling()
	var capacity atomic.Uint64
	capacity.Store(1 << 20)
	allocator := malloc.NewSimpleCAllocator(nil, nil, nil, nil, nil)
	previous := simpleCAllocator
	simpleCAllocator = func() *malloc.SimpleCAllocator { return allocator }
	defer func() { simpleCAllocator = previous }()
	mp := MustNew("realloc-release-boundary")
	defer DeleteMPool(mp)
	var target atomic.Pointer[byte]
	var observed, metadataPresent, profilePresent atomic.Bool
	allocator.EnableMmapCache(capacity.Load, reallocReleaseGauge{observe: func(value float64) {
		ptr := target.Load()
		if value == 0 || ptr == nil {
			return
		}
		// Set runs synchronously after the freed mapping enters the cache.
		// Do not allocate here: the cache mutex is still held.
		var lease allocationLease
		_, found := mp.getPtrMetadata(unsafe.Pointer(ptr), &lease)
		metadataPresent.Store(found)
		shard := getProfileShard(uintptr(unsafe.Pointer(ptr)))
		shard.mu.Lock()
		_, found = shard.m[uintptr(unsafe.Pointer(ptr))]
		shard.mu.Unlock()
		profilePresent.Store(found)
		observed.Store(true)
	}})
	old, err := mp.Alloc(128<<10, true)
	require.NoError(t, err)
	defer func() { capacity.Store(0); target.Store(nil); mp.Free(old) }()
	target.Store(unsafe.SliceData(old))
	next, err := mp.ReallocZero(old, 256<<10, true)
	require.NoError(t, err)
	old = next
	target.Store(nil)
	// Drain the exact-size cached mapping, then disable caching for cleanup.
	reused, err := mp.Alloc(128<<10, true)
	require.NoError(t, err)
	capacity.Store(0)
	mp.Free(reused)
	require.True(t, observed.Load(), "must exercise the physical release boundary")
	require.False(t, metadataPresent.Load(), "released address must not retain old ownership")
	require.False(t, profilePresent.Load(), "released address must not retain old profile identity")
}

func TestReallocPhysicalFailureRestoresOwnership(t *testing.T) {
	const child = "MO_TEST_REALLOC_PHYSICAL_FAILURE"
	if os.Getenv(child) != "1" {
		// A process-local address-space limit injects real OS allocation failure
		// without a production hot-path hook or affecting other package tests.
		cmd := exec.Command(os.Args[0], "-test.run=^TestReallocPhysicalFailureRestoresOwnership$", "-test.v", "-test.timeout=30s")
		cmd.Env = append(os.Environ(), child+"=1")
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
		return
	}
	EnableProfiling()
	defer DisableProfiling()
	for _, accounted := range []bool{false, true} {
		for _, size := range []int{64 << 10, 128 << 10} {
			registry, account := newTestAllocationAccount(t, 2<<30, 2)
			mp := MustNew("physical-realloc-failure")
			var old []byte
			t.Cleanup(func() {
				mp.Free(old)
				DeleteMPool(mp)
			})
			var err error
			if accounted {
				old, err = mp.AllocAccounted(size, account, testAllocationOwner, testAllocationSite)
			} else {
				old, err = mp.Alloc(size, true)
			}
			require.NoError(t, err)
			old[0], old[size-1] = 0x5a, 0x7f
			var lease allocationLease
			ptr := unsafe.Pointer(unsafe.SliceData(old))
			hdr, ok := mp.getPtrMetadata(ptr, &lease)
			require.True(t, ok)
			before := GlobalStats().NumCurrBytes.Load()
			profileBefore := ProfileTrackedCount()
			stat, err := os.ReadFile("/proc/self/statm")
			require.NoError(t, err)
			pages, err := strconv.ParseUint(strings.Fields(string(stat))[0], 10, 64)
			require.NoError(t, err)
			var limit unix.Rlimit
			require.NoError(t, unix.Getrlimit(unix.RLIMIT_AS, &limit))
			restricted := limit
			restricted.Cur = pages*uint64(os.Getpagesize()) + 64<<20
			require.NoError(t, unix.Setrlimit(unix.RLIMIT_AS, &restricted))
			next, resizeErr := mp.ReallocZero(old, 1<<30, true)
			// Restore before assertions or other test work can allocate memory.
			restoreErr := unix.Setrlimit(unix.RLIMIT_AS, &limit)
			require.NoError(t, restoreErr)
			require.ErrorContains(t, resizeErr, "physical allocator rejected")
			require.Nil(t, next)
			var restoredLease allocationLease
			restoredHdr, ok := mp.getPtrMetadata(ptr, &restoredLease)
			require.True(t, ok)
			require.Equal(t, hdr, restoredHdr)
			require.Equal(t, lease, restoredLease)
			require.Equal(t, byte(0x5a), old[0])
			require.Equal(t, byte(0x7f), old[size-1])
			require.Equal(t, before, GlobalStats().NumCurrBytes.Load())
			require.Equal(t, profileBefore, ProfileTrackedCount())
			require.Equal(t, int64(size), mp.CurrNB())
			mp.Free(old)
			old = nil
			require.Zero(t, account.Snapshot().Used)
			require.Zero(t, registry.LiveAllocationMetadata())
			finalizeTestAllocationAccount(t, registry, account)
		}
	}
}
