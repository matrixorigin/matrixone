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

// restrictAddressSpaceBelow tightens RLIMIT_AS to the current resident set
// plus headroom so the next oversized physical request fails inside the OS
// allocator: a real post-detach native failure, never a capacity precheck
// rejection. The returned release restores the original limit; it must run
// before any further allocation or assertion, and the registered cleanup
// keeps every failure path closed (double restore is idempotent).
func restrictAddressSpaceBelow(t *testing.T, headroom uint64) func() {
	stat, err := os.ReadFile("/proc/self/statm")
	require.NoError(t, err)
	pages, err := strconv.ParseUint(strings.Fields(string(stat))[0], 10, 64)
	require.NoError(t, err)
	var limit unix.Rlimit
	require.NoError(t, unix.Getrlimit(unix.RLIMIT_AS, &limit))
	restricted := limit
	restricted.Cur = pages*uint64(os.Getpagesize()) + headroom
	require.NoError(t, unix.Setrlimit(unix.RLIMIT_AS, &restricted))
	release := func() {
		require.NoError(t, unix.Setrlimit(unix.RLIMIT_AS, &limit))
	}
	t.Cleanup(func() {
		_ = unix.Setrlimit(unix.RLIMIT_AS, &limit)
	})
	return release
}

func TestReallocPhysicalFailureRestoresOwnership(t *testing.T) {
	const (
		child                     = "MO_TEST_REALLOC_PHYSICAL_FAILURE"
		physicalFailureTargetSize = 512 << 20
		physicalFailureHeadroom   = 256 << 20
	)
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
				if old != nil {
					mp.Free(old)
					old = nil
				}
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
			// Stay below MPoolStats' 1 GiB high-watermark logging boundary: the
			// RLIMIT_AS budget remains below the requested mapping, while
			// leaving enough headroom for the Go runtime to construct and
			// return the expected allocation error in coverage-instrumented
			// test binaries. The helper registers its own cleanup, so every
			// failure path restores the limit.
			release := restrictAddressSpaceBelow(t, physicalFailureHeadroom)
			next, resizeErr := mp.ReallocZero(old, physicalFailureTargetSize, true)
			// Restore before assertions or other test work can allocate memory.
			release()
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

	// Named toggle case inside the same child-process fixture: profiling is
	// disabled between a plain alloc and the real post-detach physical
	// failure, and the following Free must still retire the restored record -
	// the transition the profileDetach toggle guard leaked forever. No second
	// subprocess or duplicated rlimit setup: it shares this fixture's own
	// restrictAddressSpaceBelow seam and failure discriminators.
	runReallocFailureAfterDisableCase(t, physicalFailureTargetSize, physicalFailureHeadroom)
}

// runReallocFailureAfterDisableCase runs the disable-before-failure variant of
// TestReallocPhysicalFailureRestoresOwnership. It keeps every discriminator of
// the loop cases - the physical allocator rejection itself (never a capacity
// precheck), restored ownership metadata, contents, byte counts - and adds the
// exact-same-sample rollback plus the final Free-retirement assertions.
func runReallocFailureAfterDisableCase(t *testing.T, targetSize int, headroom uint64) {
	wasEnabled := ProfilingEnabled()
	defer func() {
		if wasEnabled {
			EnableProfiling()
		} else {
			DisableProfiling()
		}
	}()
	EnableProfiling()
	size := 128 << 10
	mp := MustNew("physical-realloc-failure-after-disable")
	var old []byte
	// FailNow resource closure: the live slice is the real owner handle and
	// must be freed before pool deletion on every path.
	t.Cleanup(func() {
		if old != nil {
			mp.Free(old)
			old = nil
		}
		DeleteMPool(mp)
	})
	trackedBefore := ProfileTrackedCount()
	poolBefore := mp.CurrNB()
	globalBefore := GlobalStats().NumCurrBytes.Load()

	var err error
	old, err = mp.Alloc(size, true)
	require.NoError(t, err)
	old[0], old[size-1] = 0x5a, 0x7f
	ptr := unsafe.Pointer(unsafe.SliceData(old))
	oldPtr := uintptr(ptr)
	require.Equal(t, trackedBefore+1, ProfileTrackedCount(),
		"alloc while profiling must record the pointer identity")
	recorded, found := lookupProfileRecord(oldPtr)
	require.True(t, found)
	var lease allocationLease
	hdr, ok := mp.getPtrMetadata(ptr, &lease)
	require.True(t, ok)
	bytesInuse := recorded.Bytes.Inuse.Load()
	objectsInuse := recorded.Objects.Inuse.Load()

	DisableProfiling()
	require.False(t, ProfilingEnabled())
	release := restrictAddressSpaceBelow(t, headroom)
	next, resizeErr := mp.ReallocZero(old, targetSize, true)
	// Restore before assertions or other test work can allocate memory; the
	// helper's own registered cleanup keeps every failure path closed.
	release()
	require.ErrorContains(t, resizeErr, "physical allocator rejected")
	require.Nil(t, next)
	require.Equal(t, byte(0x5a), old[0])
	require.Equal(t, byte(0x7f), old[size-1])

	// The failed realloc must roll the exact detached sample back in and
	// leave ownership metadata untouched while profiling is disabled.
	require.Equal(t, trackedBefore+1, ProfileTrackedCount(),
		"failed realloc must not lose the disabled-switch record")
	restored, stillThere := lookupProfileRecord(oldPtr)
	require.True(t, stillThere,
		"failed realloc must restore the address-keyed record")
	require.Same(t, recorded, restored,
		"rollback must restore the exact sample detached before the native call")
	var restoredLease allocationLease
	restoredHdr, restoredOk := mp.getPtrMetadata(ptr, &restoredLease)
	require.True(t, restoredOk)
	require.Equal(t, hdr, restoredHdr)
	require.Equal(t, lease, restoredLease)
	require.Equal(t, poolBefore+int64(size), mp.CurrNB())
	require.Equal(t, globalBefore+int64(size), GlobalStats().NumCurrBytes.Load())

	// Final retire through the same real Free while profiling stays disabled:
	// the recovery loop only closes when rollback and retirement both hold.
	mp.Free(old)
	old = nil
	require.Equal(t, trackedBefore, ProfileTrackedCount(),
		"free after failed-realloc rollback must retire the restored record")
	_, retired := lookupProfileRecord(oldPtr)
	require.False(t, retired)
	require.Equal(t, bytesInuse-int64(size), recorded.Bytes.Inuse.Load())
	require.Equal(t, objectsInuse-1, recorded.Objects.Inuse.Load())
	require.Equal(t, poolBefore, mp.CurrNB())
	require.Equal(t, globalBefore, GlobalStats().NumCurrBytes.Load())
}
