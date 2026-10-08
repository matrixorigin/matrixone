// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:build linux && mmap_reclaim_integration

package malloc

import (
	"bufio"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// Explicit opt-in pressure experiment, not an ordinary UT: up to 1.1M VMAs and
// ~4GiB virtual (untouched) address space. Run in an isolated process on a test
// host with kernel-memory headroom, never in a production service. No sysctl is
// changed. The ordinary deterministic suite covers scheduling/error injection.
func TestMmapReclaimRealVMAExhaustion(t *testing.T) {
	limitRaw, err := os.ReadFile("/proc/sys/vm/max_map_count")
	require.NoError(t, err)
	limit, err := strconv.Atoi(strings.TrimSpace(string(limitRaw)))
	require.NoError(t, err)
	require.LessOrEqual(t, limit, 1100000, "experiment resource budget")
	require.False(t, mmapReclaimer.blocked.Load())
	mmapReclaimer.mu.Lock()
	initialFailures := mmapReclaimer.failures
	mmapReclaimer.mu.Unlock()
	a := newCachedTestSimpleCAllocator(func() uint64 { return 1 << 20 }, time.Hour)
	t.Cleanup(a.mmapCache.drain)
	const block = 344064
	var allocations [][]byte
	t.Cleanup(func() {
		for _, data := range allocations {
			if data != nil {
				a.Deallocate(data, block)
			}
		}
	})
	for range 3 {
		data, err := a.Allocate(block)
		require.NoError(t, err)
		allocations = append(allocations, data)
	}
	slices.SortFunc(allocations, func(a, b []byte) int {
		if uintptr(unsafe.Pointer(&a[0])) < uintptr(unsafe.Pointer(&b[0])) {
			return -1
		}
		return 1
	})
	base := uintptr(unsafe.Pointer(&allocations[0][0]))
	require.Equal(t, base+block, uintptr(unsafe.Pointer(&allocations[1][0])))
	require.Equal(t, base+2*block, uintptr(unsafe.Pointer(&allocations[2][0])))
	f, err := os.Open("/proc/self/maps")
	require.NoError(t, err)
	merged := false
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var start, end uintptr
		if _, err := fmt.Sscanf(scanner.Text(), "%x-%x", &start, &end); err == nil && start <= base && end >= base+3*block {
			merged = true
			t.Log("three allocator mappings merged:", scanner.Text())
		}
	}
	require.NoError(t, scanner.Err())
	require.NoError(t, f.Close())
	require.True(t, merged)
	a.Deallocate(allocations[1], block)
	allocations[1] = nil // ownership transferred to the cache
	require.Equal(t, uint64(block), a.mmapCache.cachedBytes())

	page := os.Getpagesize()
	arena, err := unix.Mmap(-1, 0, (limit+128)*page, unix.PROT_NONE, unix.MAP_PRIVATE|unix.MAP_ANONYMOUS|unix.MAP_NORESERVE)
	require.NoError(t, err)
	// Always remove VMA pressure before assertion cleanup, including FailNow.
	t.Cleanup(func() {
		if arena != nil {
			_ = unix.Munmap(arena)
		}
	})
	for i := 1; i < limit+127; i += 2 {
		err = unix.Mprotect(arena[i*page:(i+1)*page], unix.PROT_READ)
		if err != nil {
			break
		}
	}
	require.ErrorIs(t, err, unix.ENOMEM)
	a.mmapCache.mu.Lock()
	generation := a.mmapCache.timerGeneration
	a.mmapCache.timer.Stop() // manual expiry owns the test's timer invocation
	a.mmapCache.lastPut = time.Now().Add(-2 * time.Hour)
	a.mmapCache.mu.Unlock()
	a.mmapCache.expire(generation)
	require.True(t, mmapReclaimer.blocked.Load(), "real cached munmap must encounter ENOMEM")
	mmapReclaimer.mu.Lock()
	pending, bytes := mmapReclaimer.count, mmapReclaimer.bytes
	mmapReclaimer.mu.Unlock()
	require.Equal(t, uint64(1), pending)
	require.Equal(t, uint64(block), bytes)
	t.Logf("real ENOMEM retained: mappings=%d bytes=%d", pending, bytes)
	_, err = mmapMemory(block)
	require.ErrorIs(t, err, unix.ENOMEM)
	// Wait for the real timer's first failed retry/report, not a scheduling
	// assumption. Pressure remains until that observable transition occurs.
	require.Eventually(t, func() bool {
		mmapReclaimer.mu.Lock()
		defer mmapReclaimer.mu.Unlock()
		return mmapReclaimer.failures >= initialFailures+2
	}, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, unix.Munmap(arena))
	arena = nil
	require.Eventually(t, func() bool { return !mmapReclaimer.blocked.Load() }, 10*time.Second, 10*time.Millisecond)
	mmapReclaimer.mu.Lock()
	pending, bytes = mmapReclaimer.count, mmapReclaimer.bytes
	mmapReclaimer.mu.Unlock()
	require.Zero(t, pending)
	require.Zero(t, bytes)
	a.mmapCache.drain()
	a.mmapCache = nil
	a.Deallocate(allocations[0], block)
	a.Deallocate(allocations[2], block)
	allocations[0], allocations[2] = nil, nil
	require.Zero(t, a.currentInuse.Load())
	data, err := a.Allocate(block)
	require.NoError(t, err)
	a.Deallocate(data, block)
	t.Log("recovered: no pending mappings, allocator accepts fresh allocations")
}
