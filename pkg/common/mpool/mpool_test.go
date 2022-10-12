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
	"sync/atomic"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/require"
)

func TestMPool(t *testing.T) {
	// Just test a mid sized
	m, err := NewMPool("test-mpool-small", 0, Small)
	require.True(t, err == nil, "new mpool failed %v", err)

	nb0 := m.CurrNB()
	hw0 := m.Stats().HighWaterMark.Load()
	nalloc0 := m.Stats().NumAlloc.Load()
	nfree0 := m.Stats().NumFree.Load()

	// Small has 5 non-zero fixed pool.
	require.True(t, nalloc0 == 5, "bad nalloc")
	require.True(t, nfree0 == 0, "bad nfree")

	for i := 1; i <= 10000; i++ {
		a, err := m.Alloc(i * 10)
		require.True(t, err == nil, "alloc failure, %v", err)
		require.True(t, len(a) == i*10, "allocation i size error")
		a[0] = 0xF0
		require.True(t, a[1] == 0, "allocation result not zeroed.")
		a[i*10-1] = 0xBA
		a, err = m.Realloc(a, i*20)
		require.True(t, err == nil, "realloc failure %v", err)
		require.True(t, len(a) == i*20, "allocation i size error")
		require.True(t, a[0] == 0xF0, "reallocation not copied")
		require.True(t, a[i*10-1] == 0xBA, "reallocation not copied")
		require.True(t, a[i*10] == 0, "reallocation not zeroed")
		require.True(t, a[i*20-1] == 0, "reallocation not zeroed")
		m.Free(a)
	}

	require.True(t, nb0 == m.CurrNB(), "leak")
	// 30 -- we realloc, therefore, 10 + 20, need alloc first, then copy.
	require.True(t, hw0+10000*30 == m.Stats().HighWaterMark.Load(), "hw")
	// >, because some alloc is absorbed by fixed pool
	require.True(t, nalloc0+10000*2 > m.Stats().NumAlloc.Load(), "alloc")
	require.True(t, nalloc0-nfree0 == m.Stats().NumAlloc.Load()-m.Stats().NumFree.Load(), "free")
	require.True(t, m.FixedPoolStats(0).NumAlloc.Load() > 0, "use 64b pool")
	require.True(t, m.FixedPoolStats(0).NumGoAlloc.Load() == 0, "use 64b pool")
	require.True(t, m.FixedPoolStats(6).NumAlloc.Load() == 0, "use 64b pool")
}

func TestFreelist(t *testing.T) {
	// Too much for CI
	t.Skip()

	// a list of ten items
	type item struct {
		lastWriter  int32
		updateCount int64
	}
	var items [10]item
	fl := make_freelist(10)
	for i := 0; i < 10; i++ {
		fl.put(unsafe.Pointer(&items[i]))
	}

	require.Equal(t, fl.head.Load(), fl.next(fl.tail.Load()), "freelist should be full")

	// let 20 threads run for it, each looping 1 million times.
	type result struct {
		okCount   int64
		missCount int64
	}
	var results [20]result
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(ii int) {
			defer wg.Done()
			for j := 0; j < 1000000; j++ {
				ptr := fl.get()
				if ptr == nil {
					results[ii].missCount += 1
				} else {
					results[ii].okCount += 1
					pitem := (*item)(ptr)
					pitem.lastWriter = int32(ii)
					// this must be atomic -- the following could possibly
					// lost an update count.
					// pitem.updateCount += 1
					atomic.AddInt64(&pitem.updateCount, 1)
					fl.put(ptr)
				}
			}
		}(i)
	}
	wg.Wait()

	var totalOK1, totalOK2, totalMiss int64
	for i := 0; i < 10; i++ {
		require.True(t, items[i].lastWriter >= 0, "bad writer")
		require.True(t, items[i].lastWriter < 20, "bad writer")
		totalOK1 += items[i].updateCount
	}

	for i := 0; i < 20; i++ {
		require.True(t, results[i].okCount >= 0, "bad result")
		require.True(t, results[i].okCount < 1000000, "bad result")
		require.True(t, results[i].missCount >= 0, "bad result")
		require.True(t, results[i].missCount < 1000000, "bad result")
		totalOK2 += results[i].okCount
		totalMiss += results[i].missCount
	}

	require.True(t, totalOK1 == totalOK2, "wrong ok counter")
	require.True(t, totalOK2+totalMiss == 20*1000000, "wrong counter")
}

func TestReportMemUsage(t *testing.T) {
	// Just test a mid sized
	m, err := NewMPool("testjson", 0, Small)
	m.EnableDetailRecording()

	require.True(t, err == nil, "new mpool failed %v", err)
	mem, err := m.Alloc(1000000)
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
