// Copyright 2026 Matrix Origin
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

package cache

import (
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

// The result is always min(shared memo, this caller's own durable): the first cold caller seeds the
// memo with its own; a later caller whose snapshot can see a NEWER generation inherits the smaller
// memo (the in-flight generation execution will reuse); a caller whose snapshot is OLDER than the
// memo clamps to its OWN, never above what it can load. A zero VectorIndexCache never has a warm
// entry, so tier 1 (indexFound) is not exercised here.
func TestGetMaxTSClampToOwn(t *testing.T) {
	c := &VectorIndexCache{}

	ts, indexFound, memoFound, err := c.GetMaxTS("k", func() (int64, error) { return 100, nil })
	require.NoError(t, err)
	require.Equal(t, int64(100), ts) // first caller seeds the memo with own
	require.False(t, indexFound)
	require.False(t, memoFound)

	// own (200) > memo (100): inherit the smaller in-flight generation.
	ts, _, memoFound, err = c.GetMaxTS("k", func() (int64, error) { return 200, nil })
	require.NoError(t, err)
	require.Equal(t, int64(100), ts)
	require.True(t, memoFound)

	// own (50) < memo (100): clamp DOWN to own -- never return a generation this snapshot can't load.
	ts, _, memoFound, err = c.GetMaxTS("k", func() (int64, error) { return 50, nil })
	require.NoError(t, err)
	require.Equal(t, int64(50), ts)
	require.False(t, memoFound)

	// after removal the memo is reseeded by the next caller's own.
	c.RemoveMaxTSMemo("k")
	ts, _, _, err = c.GetMaxTS("k", func() (int64, error) { return 300, nil })
	require.NoError(t, err)
	require.Equal(t, int64(300), ts)
}

// A compute error propagates and leaves no entry (compute runs before LoadOrStore), so the next
// caller recomputes cleanly rather than inheriting a bogus/zero memo.
func TestGetMaxTSErrorPropagates(t *testing.T) {
	c := &VectorIndexCache{}
	_, _, _, err := c.GetMaxTS("k", func() (int64, error) { return 0, moerr.NewInternalErrorNoCtx("boom") })
	require.Error(t, err)

	ts, _, memoFound, err := c.GetMaxTS("k", func() (int64, error) { return 42, nil })
	require.NoError(t, err)
	require.Equal(t, int64(42), ts)
	require.False(t, memoFound) // proves no entry lingered from the failed call
}

// The clamp safety invariant under concurrency: whatever the interleaving, every caller's result is
// <= its own durable (never a generation it can't load), with no data race on the shared memo entry.
func TestGetMaxTSConcurrentClampNoRace(t *testing.T) {
	c := &VectorIndexCache{}
	const n = 16
	got := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			own := int64(100 + i)
			ts, _, _, _ := c.GetMaxTS("k", func() (int64, error) { return own, nil })
			got[i] = ts
		}(i)
	}
	wg.Wait()
	for i, ts := range got {
		require.LessOrEqual(t, ts, int64(100+i), "result must never exceed the caller's own durable")
	}
}

// Different keys are independent; each seeds its own memo.
func TestGetMaxTSDistinctKeysIndependent(t *testing.T) {
	c := &VectorIndexCache{}
	a, _, _, err := c.GetMaxTS("a", func() (int64, error) { return 1, nil })
	require.NoError(t, err)
	b, _, _, err := c.GetMaxTS("b", func() (int64, error) { return 2, nil })
	require.NoError(t, err)
	require.Equal(t, int64(1), a)
	require.Equal(t, int64(2), b)
}
