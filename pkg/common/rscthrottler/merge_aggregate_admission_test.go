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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMergeAggregateAdmissionKeepsHeadroom(t *testing.T) {
	// Use a fixed sampled RSS so the test exercises admission, not the host's
	// own memory pressure or the one-second process sampling interval.
	m := &memThrottler{}
	m.options.specializedForMerge = true
	m.limit.Store(90)
	m.rss.Store(10)
	m.lastRefresh.Store(math.MaxInt64)
	m.mergeAvailDebounce.Store(math.MaxInt64)

	// With 80 bytes of live headroom, the existing two-thirds safety margin
	// must cover *all* concurrent Merge reservations, not each in isolation.
	_, granted := m.Acquire(40)
	require.True(t, granted)
	left, granted := m.Acquire(14)
	require.False(t, granted)
	require.Equal(t, int64(13), left)
	require.Equal(t, int64(40), m.reserved.Load())
	left, granted = m.Acquire(13)
	require.True(t, granted)
	require.Zero(t, left)
	_, granted = m.Acquire(1)
	require.False(t, granted)
	m.Release(13)

	// Memory used by the rest of TN is part of the budget even before a Merge
	// reservation changes; a denial must not own or release a grant.
	m.rss.Store(50)
	left, granted = m.Acquire(1)
	require.False(t, granted)
	require.Zero(t, left)
	require.Equal(t, int64(40), m.reserved.Load())

	m.Release(40)
	left, granted = m.Acquire(26)
	require.True(t, granted)
	require.Zero(t, left)
	m.Release(26)
	require.Zero(t, m.reserved.Load())

	// The aggregate margin is Merge-specific; ordinary throttlers still
	// admit against their full available budget.
	ordinary := &memThrottler{}
	ordinary.limit.Store(90)
	ordinary.rss.Store(10)
	ordinary.actualTotalMemory.Store(90)
	ordinary.lastRefresh.Store(math.MaxInt64)
	_, granted = ordinary.Acquire(40)
	require.True(t, granted)
	_, granted = ordinary.Acquire(14)
	require.True(t, granted)
	ordinary.Release(54)
}

func TestMergeConcurrentAdmissionKeepsAggregateBound(t *testing.T) {
	m := &memThrottler{}
	m.options.specializedForMerge = true
	m.limit.Store(90)
	m.lastRefresh.Store(math.MaxInt64)
	m.mergeAvailDebounce.Store(math.MaxInt64)

	start := make(chan struct{})
	results := make(chan bool, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, granted := m.Acquire(40)
			results <- granted
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	grantedCount := 0
	for granted := range results {
		if granted {
			grantedCount++
		}
	}
	require.Equal(t, 1, grantedCount)
	require.Equal(t, int64(40), m.reserved.Load())
	m.Release(40)
	require.Zero(t, m.reserved.Load())
}

func TestMergeAdmissionCASRetryAndRounding(t *testing.T) {
	m := &memThrottler{}
	m.options.specializedForMerge = true
	m.limit.Store(90)
	m.rss.Store(10)
	m.lastRefresh.Store(math.MaxInt64)
	m.mergeAvailDebounce.Store(math.MaxInt64)
	observed := m.reserved.Load()
	_, granted := m.Acquire(40)
	require.True(t, granted)
	_, granted, retry := m.tryAcquire(40, observed)
	require.False(t, granted)
	require.True(t, retry, "a stale reservation snapshot must retry after CAS contention")
	_, granted = m.Acquire(40)
	require.False(t, granted, "the retry must see the aggregate budget already consumed")
	require.Equal(t, int64(40), m.reserved.Load())
	m.Release(40)

	for _, tc := range []struct {
		headroom, budget int64
	}{
		{0, 0}, {1, 0}, {2, 1}, {3, 2},
	} {
		m := &memThrottler{}
		m.options.specializedForMerge = true
		m.limit.Store(tc.headroom)
		m.lastRefresh.Store(math.MaxInt64)
		m.mergeAvailDebounce.Store(math.MaxInt64)
		left, granted := m.Acquire(tc.budget)
		require.True(t, granted, "headroom=%d", tc.headroom)
		require.Zero(t, left)
		left, granted = m.Acquire(1)
		require.False(t, granted, "headroom=%d", tc.headroom)
		require.Zero(t, left)
		m.Release(tc.budget)
	}
}
