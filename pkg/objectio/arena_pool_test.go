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
	"testing"
	"time"

	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// withArenaIdleTTL runs a test with a short idle TTL and an empty large-tier
// pool, and empties the pool again afterwards.
func withArenaIdleTTL(t *testing.T, ttl time.Duration) {
	old := arenaIdleTTL
	arenaIdleTTL = ttl
	emptyArenaPool(ArenaLarge)
	t.Cleanup(func() {
		emptyArenaPool(ArenaLarge)
		arenaIdleTTL = old
	})
}

func emptyArenaPool(tier int) {
	pool := &arenaPools[tier]
	pool.mu.Lock()
	parked := pool.parked
	pool.parked = nil
	for _, p := range parked {
		metric.MemObjectIOPooledSerialBytesGauge.Sub(float64(p.arena.serialBuf.Cap()))
	}
	if pool.reaper != nil {
		pool.reaper.Stop()
		pool.reaper = nil
	}
	pool.mu.Unlock()
	for _, p := range parked {
		p.arena.FreeBuffers()
	}
}

func parkedCount(tier int) int {
	pool := &arenaPools[tier]
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return len(pool.parked)
}

func TestArenaReusedWithinIdleTTL(t *testing.T) {
	withArenaIdleTTL(t, time.Hour)
	a := GetArena(ArenaLarge)
	PutArena(a)
	require.Same(t, a, GetArena(ArenaLarge), "a returned arena is reused warm")
	PutArena(a)
}

// Parked arenas are released after the idle TTL even when nothing else
// happens: the pool's off-heap memory goes back to where it started.
func TestArenaFreedAfterIdleTTL(t *testing.T) {
	withArenaIdleTTL(t, 50*time.Millisecond)
	base := arenaMPool.CurrNB()
	baseSerial := testutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge)

	// Stay within the pool's capacity (4 slots at GOMAXPROCS <= 2) so every
	// returned arena is parked; freeing past the cap is TestArenaPoolCountCap.
	n := min(5, arenaPools[ArenaLarge].maxCount)
	arenas := make([]*WriteArena, n)
	for i := range arenas {
		arenas[i] = GetArena(ArenaLarge)
		arenas[i].serialBuf.Grow(1024)
	}
	var serialCapacity int
	for _, a := range arenas {
		serialCapacity += a.serialBuf.Cap()
		PutArena(a)
	}
	require.Equal(t, n, parkedCount(ArenaLarge))
	require.Greater(t, arenaMPool.CurrNB(), base)
	require.Equal(t, baseSerial+float64(serialCapacity),
		testutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge))

	require.Eventually(t, func() bool {
		return parkedCount(ArenaLarge) == 0 && arenaMPool.CurrNB() == base &&
			testutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge) == baseSerial
	}, 5*time.Second, 10*time.Millisecond)
}

// After a burst, a workload that keeps reusing one arena keeps exactly that
// one warm; the rest of the burst ages out although the pool stays busy.
// The global idle timer this replaces never fired while anything was active.
func TestArenaBurstReleasedWhileStillActive(t *testing.T) {
	ttl := 150 * time.Millisecond
	withArenaIdleTTL(t, ttl)
	base := arenaMPool.CurrNB()

	// Within capacity, so perArena below divides by what is actually parked.
	burst := make([]*WriteArena, min(8, arenaPools[ArenaLarge].maxCount))
	for i := range burst {
		burst[i] = GetArena(ArenaLarge)
	}
	for _, a := range burst {
		PutArena(a)
	}
	perArena := (arenaMPool.CurrNB() - base) / int64(len(burst))

	deadline := time.Now().Add(4 * ttl)
	var steady *WriteArena
	for time.Now().Before(deadline) {
		a := GetArena(ArenaLarge)
		if steady == nil {
			steady = a
		}
		require.Same(t, steady, a, "the working arena stays on top of the stack")
		PutArena(a)
		time.Sleep(5 * time.Millisecond)
	}
	require.Eventually(t, func() bool {
		return parkedCount(ArenaLarge) == 1
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, base+perArena, arenaMPool.CurrNB(), "only the working arena's memory is kept")
}

func TestArenaPoolCountCap(t *testing.T) {
	withArenaIdleTTL(t, time.Hour)
	base := arenaMPool.CurrNB()
	baseSerial := testutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge)
	max := arenaPools[ArenaLarge].maxCount

	arenas := make([]*WriteArena, max+3)
	for i := range arenas {
		arenas[i] = GetArena(ArenaLarge)
		arenas[i].serialBuf.Grow(1024)
	}
	serialCapacity := arenas[0].serialBuf.Cap()
	perArena := (arenaMPool.CurrNB() - base) / int64(len(arenas))
	for _, a := range arenas {
		PutArena(a)
	}
	require.Equal(t, max, parkedCount(ArenaLarge))
	require.Equal(t, base+perArena*int64(max), arenaMPool.CurrNB(), "arenas beyond the cap are freed at once")
	require.Equal(t, baseSerial+float64(max*serialCapacity),
		testutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge))
}

func TestArenaPoolSerialBufferAdmission(t *testing.T) {
	for _, tier := range []int{ArenaSmall, ArenaLarge} {
		t.Run(map[int]string{ArenaSmall: "small", ArenaLarge: "large"}[tier], func(t *testing.T) {
			emptyArenaPool(tier)
			a := GetArena(tier)
			t.Cleanup(func() {
				emptyArenaPool(tier)
				a.FreeBuffers() // also covers an assertion failure while borrowed
			})
			require.NoError(t, a.serialBuf.WriteByte(1))
			a.serialBuf.Grow(16 << 20)
			require.Greater(t, a.serialBuf.Cap(), 16<<20)
			PutArena(a) // callers need not Reset before admission

			got := GetArena(tier)
			require.Same(t, a, got)
			require.Zero(t, got.serialBuf.Len())
			require.LessOrEqual(t, got.serialBuf.Cap(), 16<<20)

			got.serialBuf.Grow(9 << 20)
			require.Greater(t, got.serialBuf.Cap(), 8<<20)
			require.LessOrEqual(t, got.serialBuf.Cap(), 16<<20)
			got.serialPeak = 9 << 20
			got.Reset()
			require.NoError(t, got.serialBuf.WriteByte(2))
			backing := &got.serialBuf.Bytes()[0]
			PutArena(got)
			got = GetArena(tier)
			got.Reset() // real callers reset the borrowed arena before writing
			require.NoError(t, got.serialBuf.WriteByte(3))
			require.Same(t, backing, &got.serialBuf.Bytes()[0])
			PutArena(got)
		})
	}
}

func TestArenaPooledSerialGaugeAcrossTiers(t *testing.T) {
	emptyArenaPool(ArenaSmall)
	emptyArenaPool(ArenaLarge)
	var small, large *WriteArena
	t.Cleanup(func() {
		emptyArenaPool(ArenaSmall)
		emptyArenaPool(ArenaLarge)
		if small != nil {
			small.FreeBuffers()
		}
		if large != nil {
			large.FreeBuffers()
		}
	})
	base := testutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge)
	small, large = GetArena(ArenaSmall), GetArena(ArenaLarge)
	small.serialBuf.Grow(1024)
	large.serialBuf.Grow(2048)
	PutArena(small)
	PutArena(large)
	require.Equal(t, base+float64(small.serialBuf.Cap()+large.serialBuf.Cap()),
		testutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge))
	require.Same(t, small, GetArena(ArenaSmall))
	require.Equal(t, base+float64(large.serialBuf.Cap()),
		testutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge))
	PutArena(small)
	emptyArenaPool(ArenaLarge)
	require.Equal(t, base+float64(small.serialBuf.Cap()),
		testutil.ToFloat64(metric.MemObjectIOPooledSerialBytesGauge))
}
