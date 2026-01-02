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
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMPoolLimitExceed(t *testing.T) {
	m, err := NewMPool("test-mpool-small", 0, NoFixed)
	require.Nil(t, err)

	_, err = m.Alloc(7775731712, false)
	require.NotNil(t, err)
}

func TestMPool(t *testing.T) {
	m, err := NewMPool("test-mpool-small", 0, NoFixed)
	require.True(t, err == nil, "new mpool failed %v", err)

	nb0 := m.CurrNB()
	nalloc0 := m.Stats().NumAlloc.Load()
	nfree0 := m.Stats().NumFree.Load()

	require.True(t, nalloc0 == 0, "bad nalloc")
	require.True(t, nfree0 == 0, "bad nfree")

	for i := 1; i <= 10000; i++ {
		a, err := m.Alloc(i*10, true)
		require.True(t, err == nil, "alloc failure, %v", err)
		require.True(t, len(a) == i*10, "allocation i size error")
		a[0] = 0xF0
		require.True(t, a[1] == 0, "allocation result not zeroed.")
		a[i*10-1] = 0xBA
		a, err = m.reAllocWithDetailK(m.getDetailK(), a, int64(i*20), true, true)
		require.True(t, err == nil, "realloc failure %v", err)
		require.True(t, len(a) == i*20, "allocation i size error")
		require.True(t, a[0] == 0xF0, "reallocation not copied")
		require.True(t, a[i*10-1] == 0xBA, "reallocation not copied")
		require.True(t, a[i*10] == 0, "reallocation not zeroed")
		require.True(t, a[i*20-1] == 0, "reallocation not zeroed")
		m.Free(a)
	}

	require.True(t, nb0 == m.CurrNB(), "leak")
	require.True(t, nalloc0+10000*2 == m.Stats().NumAlloc.Load(), "alloc")
	require.True(t, nalloc0-nfree0 == m.Stats().NumAlloc.Load()-m.Stats().NumFree.Load(), "free")
}

func TestReportMemUsage(t *testing.T) {
	// Just test a mid sized
	m, err := NewMPool("testjson", 0, NoFixed)
	m.EnableDetailRecording()

	require.True(t, err == nil, "new mpool failed %v", err)
	mem, err := m.Alloc(1000000, false)
	require.True(t, err == nil, "mpool alloc failed %v", err)

	j1 := ReportMemUsage("")
	j2 := ReportMemUsage("global")
	j3 := ReportMemUsage("testjson")
	t.Logf("mem usage: %s", j1)
	t.Logf("global mem usage: %s", j2)
	t.Logf("testjson mem usage: %s", j3)

	m.Free(mem)
	j1 = ReportMemUsage("")
	j2 = ReportMemUsage("global")
	j3 = ReportMemUsage("testjson")
	t.Logf("mem usage: %s", j1)
	t.Logf("global mem usage: %s", j2)
	t.Logf("testjson mem usage: %s", j3)

	DeleteMPool(m)
	j1 = ReportMemUsage("")
	j2 = ReportMemUsage("global")
	j3 = ReportMemUsage("testjson")
	t.Logf("mem usage: %s", j1)
	t.Logf("global mem usage: %s", j2)
	t.Logf("testjson mem usage: %s", j3)
}

func TestMP(t *testing.T) {
	pool, err := NewMPool("default", 0, NoFixed)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	run := func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			buf, err := pool.Alloc(10, false)
			if err != nil {
				panic(err)
			}
			pool.Free(buf)
		}
	}
	for i := 0; i < 800; i++ {
		wg.Add(1)
		go run()
	}
	wg.Wait()

}

func TestMpoolReAllocate(t *testing.T) {
	m := MustNewZero()
	d1, err := m.Alloc(1023, true)
	require.NoError(t, err)
	require.Equal(t, int64(cap(d1)), m.CurrNB())

	d2, err := m.reAllocWithDetailK(m.getDetailK(), d1, int64(cap(d1)-1), true, true)
	require.NoError(t, err)
	require.Equal(t, cap(d1), cap(d2))
	require.Equal(t, int64(cap(d1)), m.CurrNB())

	d3, err := m.reAllocWithDetailK(m.getDetailK(), d2, int64(cap(d2)+1025), true, true)
	require.NoError(t, err)
	require.Equal(t, int64(cap(d3)), m.CurrNB())

	if cap(d3) > 5 {
		d3 = d3[:cap(d3)-4]
		var d3_1 []byte
		d3_1, err = m.Grow(d3, cap(d3)-2, true)
		require.NoError(t, err)
		require.Equal(t, cap(d3), cap(d3_1))
		require.Equal(t, int64(cap(d3)), m.CurrNB())
		d3 = d3_1
	}

	d4, err := m.Grow(d3, cap(d3)+10, true)
	require.NoError(t, err)
	require.Equal(t, int64(cap(d4)), m.CurrNB())

	if cap(d4) > 0 {
		d4 = d4[:cap(d4)-1]
	}
	m.Free(d4)
	require.Equal(t, int64(0), m.CurrNB())
}

func TestUseMalloc(t *testing.T) {
	pool, err := NewMPool("test", 1<<20, NoFixed)
	require.Nil(t, err)
	bs, err := pool.Alloc(8, true)
	require.Nil(t, err)
	pool.Free(bs)
}

func TestMPoolNoLock(t *testing.T) {
	mp1 := MustNewNoLock("test-nolock-1")
	mp2 := MustNewNoLock("test-nolock-2")

	bs1, err := mp1.Alloc(100, true)
	require.NoError(t, err)
	require.Equal(t, int64(100), mp1.CurrNB())

	bs1, err = mp1.ReallocZero(bs1, 200, true)
	require.NoError(t, err)
	require.Equal(t, int64(200), mp1.CurrNB())

	mp1.Free(bs1)
	require.Equal(t, int64(0), mp1.CurrNB())

	bs2, err := mp2.Alloc(100, true)
	require.NoError(t, err)
	require.Equal(t, int64(100), mp2.CurrNB())

	bs22, err := mp2.ReallocZero(bs2, 2000000, true)
	require.NoError(t, err)
	require.Equal(t, int64(2000000), mp2.CurrNB())

	// should not free bs1, because it is Reallocated.

	// cross pool free is not allowed for no lock mpool.
	// mp1.Free(bs22)
	bs22, err = mp2.ReallocZero(bs22, 100, true)
	require.NoError(t, err)
	require.Equal(t, int64(2000000), mp2.CurrNB())

	mp2.Free(bs22)
	require.Equal(t, int64(0), mp2.CurrNB())
}

// TestCrossPoolFreeOffHeap tests that cross-pool free correctly deallocates offHeap memory.
// This catches the bug where cross-pool free only recorded stats but didn't actually free memory.
func TestCrossPoolFreeOffHeap(t *testing.T) {
	mp1 := MustNew("cross-pool-test-1")
	mp2 := MustNew("cross-pool-test-2")

	// Allocate from mp1
	bs, err := mp1.Alloc(1024, true)
	require.NoError(t, err)
	require.Equal(t, int64(1024), mp1.CurrNB())

	globalBefore := GlobalStats().NumCurrBytes.Load()

	// Free from mp2 (cross-pool free)
	mp2.Free(bs)

	// Verify cross-pool free count was recorded (NumCrossPoolFree counts occurrences, not bytes)
	require.Equal(t, int64(1), mp2.Stats().NumCrossPoolFree.Load())

	// Verify global stats decreased (memory was actually freed)
	globalAfter := GlobalStats().NumCurrBytes.Load()
	require.Equal(t, globalBefore-1024, globalAfter, "offHeap memory should be deallocated on cross-pool free")

	DeleteMPool(mp1)
	DeleteMPool(mp2)
}

// TestCrossPoolFreeOnHeap tests that cross-pool free works correctly for on-heap memory.
func TestCrossPoolFreeOnHeap(t *testing.T) {
	mp1 := MustNew("cross-pool-onheap-1")
	mp2 := MustNew("cross-pool-onheap-2")

	// Allocate on-heap from mp1
	bs, err := mp1.Alloc(1024, false)
	require.NoError(t, err)

	// Free from mp2 (cross-pool free) - should not panic
	mp2.Free(bs)

	// On-heap cross-pool free: no stats recorded since offHeap=false returns early
	// This is expected behavior - on-heap memory is managed by Go GC

	DeleteMPool(mp1)
	DeleteMPool(mp2)
}

// TestDoubleFree tests that double free is detected and panics.
func TestDoubleFree(t *testing.T) {
	mp := MustNew("double-free-test")

	bs, err := mp.Alloc(1024, true)
	require.NoError(t, err)

	mp.Free(bs)

	// Second free should panic
	require.Panics(t, func() {
		mp.Free(bs)
	}, "double free should panic")

	DeleteMPool(mp)
}

// TestConcurrentAllocFree tests concurrent allocation and free with sharded locks.
func TestConcurrentAllocFree(t *testing.T) {
	mp := MustNew("concurrent-test")
	var wg sync.WaitGroup

	numGoroutines := 100
	numOps := 1000

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				bs, err := mp.Alloc(64, true)
				if err != nil {
					t.Errorf("alloc failed: %v", err)
					return
				}
				mp.Free(bs)
			}
		}()
	}

	wg.Wait()
	require.Equal(t, int64(0), mp.CurrNB(), "all memory should be freed")
	DeleteMPool(mp)
}

// TestConcurrentCrossPoolFree tests concurrent cross-pool free operations.
func TestConcurrentCrossPoolFree(t *testing.T) {
	mp1 := MustNew("concurrent-cross-1")
	mp2 := MustNew("concurrent-cross-2")
	var wg sync.WaitGroup

	numGoroutines := 50
	numOps := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numOps; j++ {
				// Allocate from mp1
				bs, err := mp1.Alloc(64, true)
				if err != nil {
					t.Errorf("alloc failed: %v", err)
					return
				}
				// Free from mp2 (cross-pool)
				mp2.Free(bs)
			}
		}()
	}

	wg.Wait()

	totalCrossPoolFrees := mp2.Stats().NumCrossPoolFree.Load()
	require.Equal(t, int64(numGoroutines*numOps), totalCrossPoolFrees)

	DeleteMPool(mp1)
	DeleteMPool(mp2)
}

// TestAllocZeroSize tests allocation of zero size.
func TestAllocZeroSize(t *testing.T) {
	mp := MustNew("zero-size-test")

	bs, err := mp.Alloc(0, true)
	require.NoError(t, err)
	require.Nil(t, bs)

	bs, err = mp.Alloc(0, false)
	require.NoError(t, err)
	require.Nil(t, bs)

	DeleteMPool(mp)
}

// TestFreeNil tests that freeing nil slice doesn't panic.
func TestFreeNil(t *testing.T) {
	mp := MustNew("free-nil-test")

	// Should not panic
	mp.Free(nil)

	var emptySlice []byte
	mp.Free(emptySlice)

	DeleteMPool(mp)
}

// TestShardDistribution tests that pointer sharding distributes load.
func TestShardDistribution(t *testing.T) {
	mp := MustNew("shard-dist-test")

	numAllocs := 10000
	ptrs := make([][]byte, numAllocs)

	for i := 0; i < numAllocs; i++ {
		bs, err := mp.Alloc(64, true)
		require.NoError(t, err)
		ptrs[i] = bs
	}

	// Free all
	for _, bs := range ptrs {
		mp.Free(bs)
	}

	require.Equal(t, int64(0), mp.CurrNB())
	DeleteMPool(mp)
}
