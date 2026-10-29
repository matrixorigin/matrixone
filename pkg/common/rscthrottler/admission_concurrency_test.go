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

package rscthrottler

import (
	"math"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConcurrentDefaultAdmission(t *testing.T) {
	const limit int64 = 1024
	m := &memThrottler{}
	m.limit.Store(limit)
	m.actualTotalMemory.Store(1 << 30)
	m.lastRefresh.Store(math.MaxInt64)
	const workers = 32
	var wg sync.WaitGroup
	var granted atomic.Int64
	start := make(chan struct{})
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, ok := m.Acquire(limit); ok {
				granted.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	require.Equal(t, int64(1), granted.Load())
	require.Equal(t, granted.Load()*limit, m.reserved.Load())
	require.Equal(t, int64(0), m.Available())
	require.Equal(t, limit, m.Release(granted.Load()*limit))
	left, ok := m.Acquire(limit)
	require.True(t, ok)
	require.Zero(t, left)
	m.Release(limit)
}

func admissionFixture(limit, total, rss, reserved int64, merge bool) *memThrottler {
	m := &memThrottler{}
	m.limit.Store(limit)
	m.actualTotalMemory.Store(uint64(total))
	m.rss.Store(rss)
	m.reserved.Store(reserved)
	m.lastRefresh.Store(math.MaxInt64)
	m.mergeAvailDebounce.Store(math.MaxInt64)
	m.options.specializedForMerge = merge
	return m
}

func TestDefaultAdmissionStaleSnapshot(t *testing.T) {
	m := admissionFixture(1024, 2048, 0, 0, false)
	observed := m.reserved.Load()
	_, ok := m.Acquire(1024)
	require.True(t, ok)
	_, granted, retry := m.tryAcquire(1024, observed)
	require.False(t, granted)
	require.True(t, retry)
	require.Equal(t, int64(1024), m.reserved.Load())
	left, ok := m.Acquire(1024)
	require.False(t, ok)
	require.Zero(t, left)
	m.Release(1024)
}

func TestDefaultAdmissionBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name                               string
		limit, total, rss, reserved, avail int64
		merge                              bool
	}{
		{"limit", 100, 200, 20, 10, 90, false},
		{"physical", 100, 100, 30, 10, 60, false},
		{"exhausted", 100, 100, 100, 0, 0, false},
		{"merge", 100, 200, 30, 10, 60, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, ask := range []int64{0, max(0, tc.avail-1), tc.avail, tc.avail + 1} {
				m := admissionFixture(tc.limit, tc.total, tc.rss, tc.reserved, tc.merge)
				require.Equal(t, tc.avail, m.Available())
				left, ok := m.Acquire(ask)
				if ask <= tc.avail {
					require.True(t, ok)
					require.Equal(t, tc.avail-ask, left)
					require.Equal(t, tc.reserved+ask, m.reserved.Load())
				} else {
					require.False(t, ok)
					require.Equal(t, tc.avail, left)
					require.Equal(t, tc.reserved, m.reserved.Load())
				}
			}
		})
	}
}

func TestDefaultAdmissionRetryAndForce(t *testing.T) {
	m := admissionFixture(100, 200, 0, 50, false)
	observed := m.reserved.Load()
	m.Release(50)
	_, granted, retry := m.tryAcquire(1, observed)
	require.False(t, granted)
	require.True(t, retry)
	observed = m.reserved.Load()
	m.reserved.Store(1)
	_, granted, retry = m.tryAcquire(1, observed)
	require.False(t, granted)
	require.True(t, retry)
	_, granted = m.Acquire(99)
	require.True(t, granted)
	m.options.allowOutOfMemoryAcquire = true
	_, granted, retry = m.tryAcquire(10, 0)
	require.False(t, granted)
	require.True(t, retry)
	left, granted := m.Acquire(10)
	require.True(t, granted)
	require.Equal(t, int64(-10), left)
	require.Equal(t, int64(110), m.reserved.Load())
	m.Release(110)
	_, granted = m.Acquire(100)
	require.True(t, granted)
	m.Release(100)
	m.reserved.Store(-5)
	_, granted = m.Acquire(10)
	require.True(t, granted)
	require.Equal(t, int64(10), m.reserved.Load())
}

func TestDefaultAdmissionMaxInt64(t *testing.T) {
	m := admissionFixture(math.MaxInt64, math.MaxInt64, 0, math.MaxInt64-1, false)
	left, ok := m.Acquire(1)
	require.True(t, ok)
	require.Zero(t, left)
	left, ok = m.Acquire(1)
	require.False(t, ok)
	require.Zero(t, left)
	require.Equal(t, int64(math.MaxInt64), m.reserved.Load())
}

func TestDefaultAdmissionExhaustedMemoryDoesNotWrap(t *testing.T) {
	for _, merge := range []bool{false, true} {
		t.Run(map[bool]string{false: "physical", true: "merge"}[merge], func(t *testing.T) {
			// A prior reservation may remain after RSS rises above the budget.
			m := admissionFixture(math.MaxInt64, math.MaxInt64, 0, 0, merge)
			_, ok := m.Acquire(math.MaxInt64)
			require.True(t, ok)
			m.limit.Store(100)
			m.actualTotalMemory.Store(100)
			m.rss.Store(102)
			left, ok := m.Acquire(1)
			if ok || left != 0 || m.reserved.Load() != math.MaxInt64 {
				t.Errorf("exhausted budget: left=%d, granted=%t, reserved=%d", left, ok, m.reserved.Load())
			}
		})
	}
}
