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

package malloc

import (
	"runtime"
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

type reclaimHarness struct {
	r        *mmapReclaim
	now      time.Time
	callback func()
	delay    time.Duration
	reports  []mmapReclaimReport
}

func newReclaimHarness(t *testing.T, unmap func([]byte) error) *reclaimHarness {
	h := &reclaimHarness{r: newMmapReclaimer(), now: time.Unix(1000, 0)}
	h.r.now = func() time.Time { return h.now }
	h.r.unmap = unmap
	h.r.schedule = func(delay time.Duration, f func()) {
		require.Nil(t, h.callback, "only one scheduled retry owner")
		h.callback, h.delay = f, delay
	}
	h.r.report = func(r mmapReclaimReport) { h.reports = append(h.reports, r) }
	return h
}

func (h *reclaimHarness) tick(t *testing.T) {
	t.Helper()
	require.NotNil(t, h.callback)
	f := h.callback
	h.callback = nil
	h.now = h.now.Add(h.delay)
	f()
}

func TestMmapReclaimFailureRecovery(t *testing.T) {
	fail := true
	freed := make(map[*byte]int)
	h := newReclaimHarness(t, func(data []byte) error {
		if fail && data[0] == 1 {
			return unix.ENOMEM
		}
		freed[&data[0]]++
		return nil
	})
	a, b := []byte{1}, []byte{2, 2}
	h.r.deferUnmap(a, unix.ENOMEM)
	h.r.deferUnmap(b, unix.ENOMEM)
	require.True(t, h.r.blocked.Load())
	h.tick(t)
	require.Equal(t, uint64(1), h.r.count)
	require.Equal(t, uint64(1), h.r.bytes)
	require.Equal(t, 1, freed[&b[0]])
	require.Zero(t, freed[&a[0]])
	require.Len(t, h.reports, 1)
	require.Equal(t, uint64(2), h.reports[0].Pending)
	require.Equal(t, uint64(3), h.reports[0].Bytes)
	require.NotZero(t, h.reports[0].Address)
	require.Positive(t, h.reports[0].StackLen)
	fail = false
	h.tick(t)
	require.Equal(t, 1, freed[&a[0]])
	require.Equal(t, 1, freed[&b[0]])
	require.Zero(t, h.r.count)
	require.Zero(t, h.r.bytes)
	require.Nil(t, h.r.head)
	require.Nil(t, h.r.tail)
	require.Nil(t, h.callback)
	require.False(t, h.r.blocked.Load())
	require.Equal(t, uint64(3), h.r.failures)
}

func TestMmapReclaimBoundedRetriesAndLogs(t *testing.T) {
	calls := 0
	h := newReclaimHarness(t, func([]byte) error { calls++; return unix.ENOMEM })
	for range mmapReclaimBatch + 1 {
		h.r.deferUnmap([]byte{1}, unix.ENOMEM)
	}
	for _, delay := range []time.Duration{1, 2, 4, 8, 16, 30, 30} {
		require.Equal(t, delay*time.Second, h.delay)
		before := calls
		h.tick(t)
		require.Equal(t, mmapReclaimBatch, calls-before)
		require.Equal(t, uint64(mmapReclaimBatch+1), h.r.count)
	}
	require.Len(t, h.reports, 4) // t=1,31,61,91, independent of block count
	// New failures do not reset a running timer/backoff or spawn more workers.
	h.r.deferUnmap([]byte{2}, unix.ENOMEM)
	require.Equal(t, 30*time.Second, h.delay)
	h.r.unmap = func([]byte) error { return nil }
	h.tick(t)
	require.Equal(t, time.Second, h.delay)
	h.tick(t)
	require.False(t, h.r.blocked.Load())
	// Repeated short failure/recovery episodes share the same log budget.
	reports := len(h.reports)
	for range 10 {
		h.r.deferUnmap([]byte{3}, unix.ENOMEM)
		h.tick(t)
	}
	require.Len(t, h.reports, reports)
}

func TestMmapReclaimConcurrentOwnership(t *testing.T) {
	entered, resume := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(resume) })
	calls := 0
	h := newReclaimHarness(t, func([]byte) error {
		calls++
		if calls == 1 {
			close(entered)
			<-resume
		}
		return nil
	})
	h.r.deferUnmap([]byte{1}, unix.ENOMEM)
	done := make(chan struct{})
	go func() { h.tick(t); close(done) }()
	<-entered
	// A detached batch remains accounted for and blocks new mmap admission.
	h.r.mu.Lock()
	require.Equal(t, uint64(1), h.r.count)
	h.r.mu.Unlock()
	require.True(t, h.r.blocked.Load())
	var producers sync.WaitGroup
	for range 16 {
		producers.Go(func() { h.r.deferUnmap([]byte{2}, unix.ENOMEM) })
	}
	producers.Wait()
	once.Do(func() { close(resume) })
	<-done
	require.Equal(t, uint64(16), h.r.count)
	require.True(t, h.r.blocked.Load())
	h.tick(t)
	require.Equal(t, 17, calls)
	require.False(t, h.r.blocked.Load())
	require.Nil(t, h.callback)
}

func TestMmapReclaimInvalidOwnership(t *testing.T) {
	h := newReclaimHarness(t, nil)
	require.PanicsWithValue(t, unix.EINVAL, func() { h.r.deferUnmap([]byte{1}, unix.EINVAL) })
	require.Nil(t, h.callback)
	require.Zero(t, h.r.count)
	require.False(t, h.r.blocked.Load())
}

func TestMmapReclaimAdmissionAndPoolReuse(t *testing.T) {
	// This test changes only the atomic admission flag, never swaps the global
	// reclaimer or its dependencies. The package's background workers keep their
	// actual owner. No synthetic mapping is queued on the production singleton.
	require.False(t, mmapReclaimer.blocked.Load())
	a := newTestSimpleCAllocator()
	a.EnableMmapCache(func() uint64 { return 1 << 20 }, nil)
	a.mmapCache.idle = time.Hour
	data, err := a.Allocate(simpleCAllocatorMmapThreshold)
	require.NoError(t, err)
	a.Deallocate(data, uint64(len(data)))
	a.mmapCache.mu.Lock()
	cached := a.mmapCache.bytes != 0
	a.mmapCache.mu.Unlock()
	t.Cleanup(func() {
		a.mmapCache.mu.Lock()
		if a.mmapCache.timer != nil {
			a.mmapCache.timer.Stop()
		}
		a.mmapCache.timer = nil
		a.mmapCache.timerGeneration++
		entries := a.mmapCache.bySize
		a.mmapCache.bySize = nil
		a.mmapCache.bytes = 0
		a.mmapCache.mu.Unlock()
		unmapSimpleCAllocatorCacheEntries(entries)
	})
	f := NewFixedSizeMmapAllocator(4096)
	_, dec, err := f.Allocate(0, 4096)
	require.NoError(t, err)
	dec.Deallocate()
	mmapReclaimer.blocked.Store(true)
	t.Cleanup(func() { mmapReclaimer.blocked.Store(false) })
	_, err = mmapMemory(4096)
	require.ErrorIs(t, err, unix.ENOMEM)
	_, err = a.Allocate(simpleCAllocatorMmapThreshold + 1)
	require.Error(t, err)
	small, err := a.Allocate(32)
	require.NoError(t, err)
	a.Deallocate(small, 32)
	if cached {
		// Linux cache reuse remains possible even while fresh mappings are paused.
		reused, err := a.Allocate(simpleCAllocatorMmapThreshold)
		require.NoError(t, err)
		require.Equal(t, &data[0], &reused[0])
		a.Deallocate(reused, uint64(len(reused)))
	}
	_, dec, err = f.Allocate(0, 4096)
	require.NoError(t, err)
	_, _, err = f.Allocate(0, 4096)
	require.ErrorIs(t, err, unix.ENOMEM)
	dec.Deallocate()
	unmapMemory(unsafe.Slice((*byte)(<-f.buffer1), 4096))
	mmapReclaimer.blocked.Store(false)
	data, err = mmapMemory(4096)
	require.NoError(t, err)
	unmapMemory(data)
}

func TestMmapReclaimDiagnostics(t *testing.T) {
	diagnostic := mmapReclaimDiagnostics()
	if runtime.GOOS == "linux" {
		for _, field := range []string{"maps=", "maps_complete=", "max_map_count=", "kernel=", "memory=[", "cgroup=["} {
			require.Contains(t, diagnostic, field)
		}
	} else {
		require.Contains(t, diagnostic, "unavailable")
	}
}

func TestMmapReclaimBlockedLogSink(t *testing.T) {
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	writes := 0
	w := &reclaimLogWriter{entries: make(chan mmapReclaimLog, 1)}
	w.write = func(mmapReclaimLog) {
		writes++
		if writes == 1 {
			close(entered)
			<-release
		}
		if writes == 2 {
			close(done)
		}
	}
	defer close(w.entries)
	w.submit(mmapReclaimLog{})
	<-entered
	for range 10000 {
		w.submit(mmapReclaimLog{})
	}
	require.Len(t, w.entries, 1, "a blocked sink retains only one waiting report")
	once.Do(func() { close(release) })
	<-done
	require.Equal(t, 2, writes)
}
