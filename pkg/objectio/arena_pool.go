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

package objectio

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/KimMachineGun/automemlimit/memlimit"
	metric "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

// Arena size tiers.  Small arenas serve flush tasks and sinkers that
// process modest data volumes.  Large arenas serve merge/compaction
// tasks that may aggregate up to arenaMaxSize (128 MB).  Keeping the
// tiers separate prevents small callers from inflating arena sizes and
// saves ~50% of permanent RSS on large-core machines.
const (
	ArenaSmall = 0 // flush tasks (single-block writers)
	ArenaLarge = 1 // merge, compaction, and sinker tasks
)

const arenaSmallMax = 16 * 1024 * 1024 // small arena cap

// Pre-warm sizes for freshly created arenas.  Starting at a reasonable
// initial capacity avoids multiple geometric growth steps (0→1→2→…→N)
// that each trigger a Reset allocation.
const (
	arenaSmallInit = 4 * 1024 * 1024  // 4 MB
	arenaLargeInit = 32 * 1024 * 1024 // 32 MB
)

// A warm arena is reused by the next writer. Arenas left unused after a
// burst are returned individually instead of waiting for a global drain.
const arenaIdleTTL = time.Minute

type parkedArena struct {
	arena    *WriteArena
	parkedAt time.Time
}

// arenaFreeList keeps arenas ordered by return time. The most recently
// returned arena is checked out first, leaving burst surplus to expire.
type arenaFreeList struct {
	mu               sync.Mutex
	parked           []parkedArena
	reaper           *time.Timer
	reaperGeneration uint64
	idleTTL          time.Duration // zero uses arenaIdleTTL
	count            atomic.Int32
	maxCount         int32
	bytes            atomic.Int64
	maxBytes         int64
	budget           *arenaByteBudget
}

type arenaByteBudget struct {
	used  atomic.Int64
	limit int64
}

func (b *arenaByteBudget) reserve(size int64) bool {
	for {
		used := b.used.Load()
		if size > b.limit-used {
			return false
		}
		if b.used.CompareAndSwap(used, used+size) {
			return true
		}
	}
}

var arenaPools [2]arenaFreeList

func init() {
	procs := int32(runtime.GOMAXPROCS(0))
	half := procs / 2
	if half < 1 {
		half = 1
	}
	// ArenaSmall: serves flush workers only (GOMAXPROCS/2 goroutines).
	arenaPools[ArenaSmall].maxCount = half
	// ArenaLarge: serves TN merge workers, CN S3 writers, and sinker tasks.
	// Use 2×GOMAXPROCS slots to absorb bursty concurrent demand.
	arenaPools[ArenaLarge].maxCount = procs * 2
	if arenaPools[ArenaLarge].maxCount < 4 {
		arenaPools[ArenaLarge].maxCount = 4
	}
	limit := TotalMem()
	if cgroup, err := memlimit.FromCgroup(); err == nil && cgroup > 0 && (limit == 0 || cgroup < limit) {
		limit = cgroup
	}
	budget := int64(min(limit/16, 4<<30))
	shared := &arenaByteBudget{limit: budget}
	for i := range arenaPools {
		arenaPools[i].maxBytes = budget
		arenaPools[i].budget = shared
	}
}

// GetArena pops a WriteArena from the requested tier's free list, or
// creates a pre-warmed arena when the list is empty.
func GetArena(tier int) *WriteArena {
	return getArena(tier, 0, false)
}

// GetArenaWithin checks out only buffers that fit the caller's workspace.
// Oversized cached storage is retired before any replacement allocation. The
// data buffer's adaptive growth is capped as well; encoding/compression scratch
// is separately included in the caller's complete IO envelope.
func GetArenaWithin(tier int, maxRetainedBytes int) *WriteArena {
	if maxRetainedBytes < 0 {
		panic("negative arena workspace")
	}
	return getArena(tier, maxRetainedBytes, true)
}

func getArena(tier, maxRetainedBytes int, bounded bool) *WriteArena {
	pool := &arenaPools[tier]
	var initSize, limit int
	if tier == ArenaSmall {
		initSize, limit = arenaSmallInit, arenaSmallMax
	} else {
		initSize, limit = arenaLargeInit, arenaMaxSize
	}
	if bounded {
		initSize = min(initSize, maxRetainedBytes)
		limit = min(limit, maxRetainedBytes)
		if limit == 0 {
			limit = -1
		} // explicit zero growth, unlike the default zero value
	}
	if a := pool.get(); a != nil {
		if !bounded || a.Allocated() <= maxRetainedBytes {
			a.sizeLimit = limit
			return a
		}
		a.FreeBuffers()
	}
	a := NewArena(initSize)
	a.sizeLimit = limit
	return a
}

func (pool *arenaFreeList) get() *WriteArena {
	pool.mu.Lock()
	n := len(pool.parked)
	if n == 0 {
		pool.mu.Unlock()
		return nil
	}
	a := pool.parked[n-1].arena
	pool.parked[n-1] = parkedArena{}
	pool.parked = pool.parked[:n-1]
	pool.count.Add(-1)
	metric.MemObjectIOPooledSerialBytesGauge.Sub(float64(a.serialBuf.Cap()))
	size := int64(a.Allocated())
	pool.bytes.Add(-size)
	if pool.budget != nil {
		pool.budget.used.Add(-size)
	}
	pool.mu.Unlock()
	return a
}

// PutArena parks a WriteArena in its tier's free list,
// auto-routing based on sizeLimit.
func PutArena(a *WriteArena) {
	if a == nil {
		return
	}
	tier := ArenaSmall
	if a.sizeLimit > arenaSmallMax {
		tier = ArenaLarge
	}
	pool := &arenaPools[tier]
	pool.put(a)
}

func (pool *arenaFreeList) put(a *WriteArena) {
	// Trim scratch while the caller still owns the arena, before byte accounting.
	a.resetSerialBuf(a.serialBuf.Cap() > arenaSerialRetainLimit)
	size := int64(a.Allocated())
	pool.mu.Lock()
	if pool.count.Load() >= pool.maxCount || size > pool.maxBytes-pool.bytes.Load() ||
		(pool.budget != nil && !pool.budget.reserve(size)) {
		pool.mu.Unlock()
		a.FreeBuffers()
		return
	}
	// Keep detached buffers charged until returned to the allocator, so a
	// concurrent reaper or pressure drain cannot over-admit the pool budget.
	pool.count.Add(1)
	pool.bytes.Add(size)
	metric.MemObjectIOPooledSerialBytesGauge.Add(float64(a.serialBuf.Cap()))
	pool.parked = append(pool.parked, parkedArena{arena: a, parkedAt: time.Now()})
	if pool.reaper == nil {
		pool.armReaperLocked(pool.ttl())
	}
	pool.mu.Unlock()
}

func (pool *arenaFreeList) ttl() time.Duration {
	if pool.idleTTL > 0 {
		return pool.idleTTL
	}
	return arenaIdleTTL
}

// armReaperLocked must hold mu. Generation prevents a stopped callback from
// draining a new set of parked arenas after ReleaseIdleArenas races with Put.
func (pool *arenaFreeList) armReaperLocked(delay time.Duration) {
	pool.reaperGeneration++
	generation := pool.reaperGeneration
	pool.reaper = time.AfterFunc(delay, func() { pool.reap(generation) })
}

// detachLocked removes the oldest parked arenas, retaining their budget until
// freeDetached has returned the backing buffers to the allocator outside the lock.
func (pool *arenaFreeList) detachLocked(count int) []parkedArena {
	if count == 0 {
		return nil
	}
	detached := append([]parkedArena(nil), pool.parked[:count]...)
	for _, p := range detached {
		metric.MemObjectIOPooledSerialBytesGauge.Sub(float64(p.arena.serialBuf.Cap()))
	}
	n := copy(pool.parked, pool.parked[count:])
	clear(pool.parked[n:])
	pool.parked = pool.parked[:n]
	return detached
}

func (pool *arenaFreeList) freeDetached(detached []parkedArena) {
	var size int64
	for _, p := range detached {
		size += int64(p.arena.Allocated())
		p.arena.FreeBuffers()
	}
	pool.count.Add(-int32(len(detached)))
	pool.bytes.Add(-size)
	if pool.budget != nil {
		pool.budget.used.Add(-size)
	}
}

// reap retires only arenas that have remained parked for a full idle TTL.
func (pool *arenaFreeList) reap(generation uint64) {
	pool.mu.Lock()
	if generation != pool.reaperGeneration {
		pool.mu.Unlock()
		return
	}
	pool.reaper = nil
	now := time.Now()
	idleTTL := pool.ttl()
	expired := 0
	for expired < len(pool.parked) && now.Sub(pool.parked[expired].parkedAt) >= idleTTL {
		expired++
	}
	detached := pool.detachLocked(expired)
	if len(pool.parked) != 0 {
		pool.armReaperLocked(idleTTL - now.Sub(pool.parked[0].parkedAt))
	}
	pool.mu.Unlock()
	pool.freeDetached(detached)
}

// arenaDrainDelay preserves the legacy global drain deadline. Individual
// arenas can expire earlier according to arenaIdleTTL.
const arenaDrainDelay = 10 * time.Minute

var (
	arenaDrainMu    sync.Mutex
	arenaDrainTimer *time.Timer
)

// ScheduleArenaDrain retains the legacy global drain for callers that use it.
// Individual arenas also expire after arenaIdleTTL while activity continues.
// Each call resets the timer to arenaDrainDelay from now.  During steady-state checkpointing,
// global timer is repeatedly postponed. Once activity ceases, it returns
// any remaining idle buffers to the allocator.
func ScheduleArenaDrain() {
	arenaDrainMu.Lock()
	defer arenaDrainMu.Unlock()
	if arenaDrainTimer != nil {
		arenaDrainTimer.Stop()
	}
	arenaDrainTimer = time.AfterFunc(arenaDrainDelay, drainArenaPools)
}

// drainArenaPools empties both arena free lists, freeing the off-heap
// backing buffers.  Called by the debounce timer when merge and flush
// have been idle for arenaDrainDelay.
func drainArenaPools() {
	for i := range arenaPools {
		arenaPools[i].drain()
	}
}

// ReleaseIdleArenas returns unused writer scratch under memory pressure.
// Checked-out arenas remain owned by their writers and are never touched.
func ReleaseIdleArenas() {
	drainArenaPools()
}

func (pool *arenaFreeList) drain() {
	pool.mu.Lock()
	pool.reaperGeneration++
	if pool.reaper != nil {
		pool.reaper.Stop()
		pool.reaper = nil
	}
	detached := pool.detachLocked(len(pool.parked))
	pool.mu.Unlock()
	pool.freeDetached(detached)
}
