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

func TestMPool(t *testing.T) {
	m, err := NewMPool("test-mpool-small", 0, 0)
	require.True(t, err == nil, "new mpool failed %v", err)

	nb0 := m.CurrNB()
	hw0 := m.Stats().HighWaterMark.Load()
	nalloc0 := m.Stats().NumAlloc.Load()
	nfree0 := m.Stats().NumFree.Load()

	require.True(t, nalloc0 == 0, "bad nalloc")
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
}

func TestReportMemUsage(t *testing.T) {
	// Just test a mid sized
	m, err := NewMPool("testjson", 0, 0)
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

func TestMP(t *testing.T) {
	pool, err := NewMPool("default", 0, 0)
	if err != nil {
		panic(err)
	}
	var wg sync.WaitGroup
	run := func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			buf, err := pool.Alloc(10)
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
