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
	"sync/atomic"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/stretchr/testify/require"
)

// A cold key computes once (tier 3), the next call reads the memo (tier 2) without recomputing, and
// after RemoveMaxTSMemo it recomputes. A zero VectorIndexCache never has a warm entry, so tier 1
// (indexFound) is not exercised here -- GetBuildTS is covered elsewhere.
func TestGetMaxTSMemoTiers(t *testing.T) {
	c := &VectorIndexCache{}
	var calls int32
	compute := func() (int64, error) { atomic.AddInt32(&calls, 1); return 100, nil }

	ts, indexFound, memoFound, err := c.GetMaxTS("k", compute)
	require.NoError(t, err)
	require.Equal(t, int64(100), ts)
	require.False(t, indexFound)
	require.False(t, memoFound) // tier 3: this caller computed it
	require.Equal(t, int32(1), atomic.LoadInt32(&calls))

	// tier 2: memo hit; a different compute must NOT run and must NOT change the value.
	ts, indexFound, memoFound, err = c.GetMaxTS("k", func() (int64, error) { atomic.AddInt32(&calls, 1); return 999, nil })
	require.NoError(t, err)
	require.Equal(t, int64(100), ts)
	require.False(t, indexFound)
	require.True(t, memoFound)
	require.Equal(t, int32(1), atomic.LoadInt32(&calls)) // still one compute

	c.RemoveMaxTSMemo("k")
	ts, _, memoFound, err = c.GetMaxTS("k", func() (int64, error) { return 200, nil })
	require.NoError(t, err)
	require.Equal(t, int64(200), ts) // recomputed after removal
	require.False(t, memoFound)
}

// A compute error is not memoized and leaves no placeholder: the next caller recomputes cleanly.
func TestGetMaxTSErrorSelfCleans(t *testing.T) {
	c := &VectorIndexCache{}
	_, _, _, err := c.GetMaxTS("k", func() (int64, error) { return 0, moerr.NewInternalErrorNoCtx("boom") })
	require.Error(t, err)

	ts, _, memoFound, err := c.GetMaxTS("k", func() (int64, error) { return 50, nil })
	require.NoError(t, err)
	require.Equal(t, int64(50), ts)
	require.False(t, memoFound) // memoFound=false proves the failed entry was dropped, not reused
}

// Concurrent callers for one key run compute exactly once (per-key singleflight): the winner holds
// the entry mutex while computing, the rest block and then read the memo.
func TestGetMaxTSSingleflightPerKey(t *testing.T) {
	c := &VectorIndexCache{}
	var calls int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})
	compute := func() (int64, error) {
		atomic.AddInt32(&calls, 1)
		started <- struct{}{}
		<-release // hold so the other callers pile up on the per-key mutex
		return 77, nil
	}

	const n = 8
	got := make([]int64, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ts, _, _, _ := c.GetMaxTS("k", compute)
			got[i] = ts
		}(i)
	}
	<-started      // one compute has begun
	close(release) // let it finish; the blocked callers then read the memo
	wg.Wait()

	require.Equal(t, int32(1), atomic.LoadInt32(&calls), "compute must run exactly once per key")
	for _, ts := range got {
		require.Equal(t, int64(77), ts)
	}
}

// Different keys do not serialize on each other: each computes independently, no cross-key blocking.
func TestGetMaxTSDistinctKeysIndependent(t *testing.T) {
	c := &VectorIndexCache{}
	a, _, _, err := c.GetMaxTS("a", func() (int64, error) { return 1, nil })
	require.NoError(t, err)
	b, _, _, err := c.GetMaxTS("b", func() (int64, error) { return 2, nil })
	require.NoError(t, err)
	require.Equal(t, int64(1), a)
	require.Equal(t, int64(2), b)
}
