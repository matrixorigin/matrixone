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

package objectio

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestArenaPoolByteBudget(t *testing.T) {
	for _, tc := range []struct {
		name      string
		count     int32
		budget    int64
		wantCount int32
	}{
		{"exact byte boundary", 8, 512, 2},
		{"compression and serialization count toward budget", 8, 255, 0},
		{"count limit rolls back byte reservation", 1, 4096, 1},
		{"zero budget", 8, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := &arenaFreeList{maxCount: tc.count, maxBytes: tc.budget}
			// Tiny arenas prove the same capacity boundary as wide-column writers.
			for i := 0; i < 3; i++ {
				a := NewArena(128)
				t.Cleanup(a.FreeBuffers)
				a.CompressBuf(64)
				a.serialBuf.Grow(64)
				p.put(a)
				if i >= int(tc.wantCount) {
					require.Zero(t, a.Allocated())
					a.FreeBuffers() // released arenas remain safe to clean again
				}
			}
			t.Cleanup(p.drain)
			require.Equal(t, tc.wantCount, p.count.Load())
			require.Equal(t, int64(tc.wantCount)*256, p.bytes.Load())
			for i := tc.wantCount; i > 0; i-- {
				a := p.get()
				require.NotNil(t, a)
				require.Equal(t, 256, a.Allocated())
				require.Equal(t, int64(i-1)*256, p.bytes.Load())
			}
			require.Nil(t, p.get())
			p.drain()
			require.Zero(t, p.bytes.Load())
			require.Zero(t, p.count.Load())
		})
	}
}

func TestArenaPoolConcurrentReuseAndDrain(t *testing.T) {
	p := &arenaFreeList{maxCount: 8, maxBytes: 2048}
	t.Cleanup(p.drain)
	var wg sync.WaitGroup
	var invalid atomic.Bool
	start := make(chan struct{})
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for i := 0; i < 100; i++ {
				a := p.get()
				if a == nil {
					a = NewArena(128)
				}
				if len(a.data) != 128 {
					invalid.Store(true)
				}
				a.Reset()
				a.Alloc(64)[0] = byte(i)
				a.CompressBuf(64)[0] = byte(i)
				a.serialBuf.Grow(64)
				p.put(a)
				if used := p.bytes.Load(); used < 0 || used > p.maxBytes {
					invalid.Store(true)
				}
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 0; i < 100; i++ {
			p.drain()
		}
	}()
	close(start)
	wg.Wait()
	p.drain()
	require.False(t, invalid.Load(), "checked-out buffers stay owned; idle bytes stay bounded")
	require.Zero(t, p.count.Load())
	require.Zero(t, p.bytes.Load())
}

func BenchmarkArenaPoolReuse(b *testing.B) {
	p := &arenaFreeList{maxCount: 1, maxBytes: 4096}
	p.put(NewArena(128))
	b.Cleanup(p.drain)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a := p.get()
		p.put(a)
	}
}

// Either tier can use the entire idle byte budget while it is available.
func TestArenaPoolSharedTierBudget(t *testing.T) {
	shared := &arenaByteBudget{limit: 512}
	small := &arenaFreeList{maxCount: 8, maxBytes: 512, budget: shared}
	large := &arenaFreeList{maxCount: 8, maxBytes: 512, budget: shared}
	defer small.drain()
	defer large.drain()
	small.put(NewArena(256))
	small.put(NewArena(256))
	require.Equal(t, int64(512), shared.used.Load())
	rejected := NewArena(128)
	large.put(rejected)
	require.Zero(t, rejected.Allocated())
	a := small.get()
	require.NotNil(t, a)
	defer a.FreeBuffers()
	large.put(NewArena(256))
	require.Equal(t, int64(256), small.bytes.Load())
	require.Equal(t, int64(256), large.bytes.Load())
	small.drain()
	large.drain()
	require.Zero(t, shared.used.Load())
}

func TestArenaPoolSharedBudgetConcurrentDrain(t *testing.T) {
	shared := &arenaByteBudget{limit: 512}
	pools := []*arenaFreeList{{maxCount: 8, maxBytes: 512, budget: shared}, {maxCount: 8, maxBytes: 512, budget: shared}}
	var wg sync.WaitGroup
	var invalid atomic.Bool
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			p := pools[index%2]
			for i := 0; i < 100; i++ {
				a := p.get()
				if a == nil {
					a = NewArena(128)
				}
				a.Reset()
				a.Alloc(64)[0] = byte(i)
				p.put(a)
				used := shared.used.Load()
				if used < 0 || used > shared.limit {
					invalid.Store(true)
				}
				if i%7 == 0 {
					p.drain()
				}
			}
		}(worker)
	}
	wg.Wait()
	for _, p := range pools {
		p.drain()
		require.Zero(t, p.bytes.Load())
		require.Zero(t, p.count.Load())
	}
	require.False(t, invalid.Load())
	require.Zero(t, shared.used.Load())
}

func TestArenaCheckoutFitsWorkspace(t *testing.T) {
	// All global pool users in this package are serial tests; do not make this
	// test parallel because it intentionally verifies the real checkout boundary.
	ReleaseIdleArenas()
	defer ReleaseIdleArenas()
	large := NewArena(1024)
	large.sizeLimit = arenaMaxSize
	PutArena(large)
	a := GetArenaWithin(ArenaLarge, 128)
	defer a.FreeBuffers()
	require.LessOrEqual(t, a.Allocated(), 128)
	a.Alloc(256)
	a.Reset()
	require.LessOrEqual(t, a.Allocated(), 128, "adaptive data growth obeys the admitted workspace")
	zero := GetArenaWithin(ArenaSmall, 0)
	defer zero.FreeBuffers()
	require.Zero(t, zero.Allocated())
	zero.Alloc(64)
	zero.Reset()
	require.Zero(t, zero.Allocated())
}
