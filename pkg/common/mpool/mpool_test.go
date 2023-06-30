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
	"runtime"
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

type node plan.Node

func runPool[T any](p *pool, b *testing.B, routines int,
	allocs int, allocStep int, withGC bool) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		run := func() {
			defer wg.Done()
			vs := make([]*T, allocStep)
			for i := 0; i < allocs; i += allocStep {
				for j := 0; j < allocStep; j++ {
					vs[j] = alloc[T](p)
				}
				for j := range vs {
					free(p, vs[j])
				}
				if withGC {
					runtime.GC()
				}
			}
		}
		for i := 0; i < routines; i++ {
			wg.Add(1)
			go run()
		}
		wg.Wait()
	}
}

func runSyncpool[T any](p *sync.Pool, b *testing.B, routines int,
	allocs int, allocStep int, withGC bool) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var wg sync.WaitGroup
		run := func() {
			defer wg.Done()
			vs := make([]*T, allocStep)
			for i := 0; i < allocs; i += allocStep {
				for j := 0; j < allocStep; j++ {
					vs[j] = p.Get().(*T)
				}
				for j := range vs {
					p.Put(vs[j])
				}
				if withGC {
					runtime.GC()
				}
			}
		}
		for i := 0; i < routines; i++ {
			wg.Add(1)
			go run()
		}
		wg.Wait()
	}
}

func BenchmarkPool(b *testing.B) {
	p := newPool(0)
	b.Run("pool_1_10000_1_withoutGC", func(b *testing.B) {
		runPool[node](p, b, 1, 10000, 1, false)
	})
	b.Run("pool_1_10000_100_withoutGC", func(b *testing.B) {
		runPool[node](p, b, 1, 10000, 100, false)
	})
	b.Run("pool_1_10000_1000_withoutGC", func(b *testing.B) {
		runPool[node](p, b, 1, 10000, 1000, false)
	})
	b.Run("pool_10_10000_1_withoutGC", func(b *testing.B) {
		runPool[node](p, b, 10, 10000, 1, false)
	})
	b.Run("pool_10_10000_100_withoutGC", func(b *testing.B) {
		runPool[node](p, b, 10, 10000, 100, false)
	})
	b.Run("pool_10_10000_1000_withoutGC", func(b *testing.B) {
		runPool[node](p, b, 10, 10000, 1000, false)
	})
	b.Run("pool_100_10000_1_withoutGC", func(b *testing.B) {
		runPool[node](p, b, 100, 10000, 1, false)
	})
	b.Run("pool_100_10000_100_withoutGC", func(b *testing.B) {
		runPool[node](p, b, 100, 10000, 100, false)
	})
	b.Run("pool_100_10000_1000_withoutGC", func(b *testing.B) {
		runPool[node](p, b, 100, 10000, 1000, false)
	})
	b.Run("pool_1_10000_1_withGC", func(b *testing.B) {
		runPool[node](p, b, 1, 10000, 1, true)
	})
	b.Run("pool_1_10000_100_withGC", func(b *testing.B) {
		runPool[node](p, b, 1, 10000, 100, true)
	})
	b.Run("pool_1_10000_1000_withGC", func(b *testing.B) {
		runPool[node](p, b, 1, 10000, 1000, true)
	})
	b.Run("pool_10_10000_1_withGC", func(b *testing.B) {
		runPool[node](p, b, 10, 10000, 1, true)
	})
	b.Run("pool_10_10000_100_withGC", func(b *testing.B) {
		runPool[node](p, b, 10, 10000, 100, true)
	})
	b.Run("pool_10_10000_1000_withGC", func(b *testing.B) {
		runPool[node](p, b, 10, 10000, 1000, true)
	})
	b.Run("pool_100_10000_1_withGC", func(b *testing.B) {
		runPool[node](p, b, 100, 10000, 1, true)
	})
	b.Run("pool_100_10000_100_withGC", func(b *testing.B) {
		runPool[node](p, b, 100, 10000, 100, true)
	})
	b.Run("pool_100_10000_1000_withGC", func(b *testing.B) {
		runPool[node](p, b, 100, 10000, 1000, true)
	})
}

func BenchmarkSyncPool(b *testing.B) {
	var p = &sync.Pool{
		New: func() any {
			return new(node)
		},
	}
	b.Run("pool_1_10000_1_withoutGC", func(b *testing.B) {
		runSyncpool[node](p, b, 1, 10000, 1, false)
	})
	b.Run("pool_1_10000_100_withoutGC", func(b *testing.B) {
		runSyncpool[node](p, b, 1, 10000, 100, false)
	})
	b.Run("pool_1_10000_1000_withoutGC", func(b *testing.B) {
		runSyncpool[node](p, b, 1, 10000, 1000, false)
	})
	b.Run("pool_10_10000_1_withoutGC", func(b *testing.B) {
		runSyncpool[node](p, b, 10, 10000, 1, false)
	})
	b.Run("pool_10_10000_100_withoutGC", func(b *testing.B) {
		runSyncpool[node](p, b, 10, 10000, 100, false)
	})
	b.Run("pool_10_10000_1000_withoutGC", func(b *testing.B) {
		runSyncpool[node](p, b, 10, 10000, 1000, false)
	})
	b.Run("pool_100_10000_1_withoutGC", func(b *testing.B) {
		runSyncpool[node](p, b, 100, 10000, 1, false)
	})
	b.Run("pool_100_10000_100_withoutGC", func(b *testing.B) {
		runSyncpool[node](p, b, 100, 10000, 100, false)
	})
	b.Run("pool_100_10000_1000_withoutGC", func(b *testing.B) {
		runSyncpool[node](p, b, 100, 10000, 1000, false)
	})
	b.Run("pool_1_10000_1_withGC", func(b *testing.B) {
		runSyncpool[node](p, b, 1, 10000, 1, true)
	})
	b.Run("pool_1_10000_100_withGC", func(b *testing.B) {
		runSyncpool[node](p, b, 1, 10000, 100, true)
	})
	b.Run("pool_1_10000_1000_withGC", func(b *testing.B) {
		runSyncpool[node](p, b, 1, 10000, 1000, true)
	})
	b.Run("pool_10_10000_1_withGC", func(b *testing.B) {
		runSyncpool[node](p, b, 10, 10000, 1, true)
	})
	b.Run("pool_10_10000_100_withGC", func(b *testing.B) {
		runSyncpool[node](p, b, 10, 10000, 100, true)
	})
	b.Run("pool_10_10000_1000_withGC", func(b *testing.B) {
		runSyncpool[node](p, b, 10, 10000, 1000, true)
	})
	b.Run("pool_100_10000_1_withGC", func(b *testing.B) {
		runSyncpool[node](p, b, 100, 10000, 1, true)
	})
	b.Run("pool_100_10000_100_withGC", func(b *testing.B) {
		runSyncpool[node](p, b, 100, 10000, 100, true)
	})
	b.Run("pool_100_10000_1000_withGC", func(b *testing.B) {
		runSyncpool[node](p, b, 100, 10000, 1000, true)
	})
}

func TestPool(t *testing.T) {
	type test1 struct {
		e0 int8
		e1 int8
	}
	type test2 struct {
		e0 int32
		e1 test1
	}
	cl := newPool(0)
	for i := 0; i < 10000; i++ {
		t1 := alloc[test1](cl)
		t1.e0 = 1
		t1.e1 = 2
		require.Equal(t, int8(1), t1.e0)
		require.Equal(t, int8(2), t1.e1)
		free(cl, t1)
		t2 := alloc[test2](cl)
		t2.e0 = 1
		t2.e1.e0 = 2
		t2.e1.e1 = 3
		require.Equal(t, int32(1), t2.e0)
		require.Equal(t, int8(2), t2.e1.e0)
		require.Equal(t, int8(3), t2.e1.e1)
	}
}

// test race
func TestPoolForRace(t *testing.T) {
	cl := newPool(0)
	var wg sync.WaitGroup
	run := func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			v := alloc[int64](cl)
			free(cl, v)
		}
	}
	for i := 0; i < 800; i++ {
		wg.Add(1)
		go run()
	}
	wg.Wait()

}

func TestMPool(t *testing.T) {
	m, err := NewMPool("test-mpool-small", 0, 0, 0)
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
	m, err := NewMPool("testjson", 0, 0, 0)
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
	pool, err := NewMPool("default", 0, 0, 0)
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
