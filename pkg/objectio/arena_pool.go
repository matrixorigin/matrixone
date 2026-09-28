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
	"time"
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

// arenaIdleTTL is how long a returned arena may stay parked unused before its
// off-heap buffers are freed.  A steady workload takes arenas back well within
// it, so the arenas it cycles stay warm; the extra arenas a burst leaves behind
// (e.g. one per writer of a wide parallel load, each grown up to arenaMaxSize)
// are released once the burst is over, whether or not other work continues.
// A variable so tests can shorten it.
var arenaIdleTTL = time.Minute

type parkedArena struct {
	arena    *WriteArena
	parkedAt time.Time
}

// arenaFreeList holds returned arenas for reuse.  It is a stack: GetArena
// takes the most recently parked arena, so the arenas a workload keeps using
// stay on top and the idle ones sink to the bottom, oldest first, where the
// reaper frees them after arenaIdleTTL.  Get and Put run once per object write
// cycle, not per row, so a mutex is cheap here.
type arenaFreeList struct {
	mu       sync.Mutex
	parked   []parkedArena // ordered by parkedAt, oldest first
	maxCount int
	// reaper is the armed reap timer, or nil.  It is armed only while
	// arenas are parked, so an empty pool costs nothing.
	reaper *time.Timer
}

var arenaPools [2]arenaFreeList

func init() {
	procs := runtime.GOMAXPROCS(0)
	half := procs / 2
	if half < 1 {
		half = 1
	}
	// ArenaSmall: serves flush workers only (GOMAXPROCS/2 goroutines).
	arenaPools[ArenaSmall].maxCount = half
	// ArenaLarge: serves TN merge workers, CN S3 writers, and sinker tasks.
	// Use 2×GOMAXPROCS slots to absorb bursty concurrent demand; slots that
	// the burst leaves idle are released after arenaIdleTTL.
	arenaPools[ArenaLarge].maxCount = procs * 2
	if arenaPools[ArenaLarge].maxCount < 4 {
		arenaPools[ArenaLarge].maxCount = 4
	}
}

// GetArena takes the most recently parked arena of the requested tier, or
// creates a pre-warmed arena when none is parked.
func GetArena(tier int) *WriteArena {
	pool := &arenaPools[tier]
	pool.mu.Lock()
	if n := len(pool.parked); n > 0 {
		a := pool.parked[n-1].arena
		pool.parked[n-1] = parkedArena{}
		pool.parked = pool.parked[:n-1]
		pool.mu.Unlock()
		return a
	}
	pool.mu.Unlock()

	var initSize, limit int
	if tier == ArenaSmall {
		initSize = arenaSmallInit
		limit = arenaSmallMax
	} else {
		initSize = arenaLargeInit
		limit = arenaMaxSize
	}
	a := NewArena(initSize)
	a.sizeLimit = limit
	return a
}

// PutArena parks a WriteArena in its tier's free list, auto-routing based on
// sizeLimit.  A full list frees the arena instead.
func PutArena(a *WriteArena) {
	if a == nil {
		return
	}
	tier := ArenaSmall
	if a.sizeLimit > arenaSmallMax {
		tier = ArenaLarge
	}
	pool := &arenaPools[tier]
	pool.mu.Lock()
	if len(pool.parked) >= pool.maxCount {
		pool.mu.Unlock()
		a.FreeBuffers()
		return
	}
	pool.parked = append(pool.parked, parkedArena{arena: a, parkedAt: time.Now()})
	if pool.reaper == nil {
		pool.reaper = time.AfterFunc(arenaIdleTTL, pool.reap)
	}
	pool.mu.Unlock()
}

// reap frees the arenas that have been parked for arenaIdleTTL and re-arms
// itself for the oldest remaining one, if any.
func (pool *arenaFreeList) reap() {
	now := time.Now()
	pool.mu.Lock()
	expired := 0
	for expired < len(pool.parked) && now.Sub(pool.parked[expired].parkedAt) >= arenaIdleTTL {
		expired++
	}
	var freed []parkedArena
	if expired > 0 {
		freed = append(freed, pool.parked[:expired]...)
		n := copy(pool.parked, pool.parked[expired:])
		clear(pool.parked[n:]) // drop references past the new length
		pool.parked = pool.parked[:n]
	}
	if len(pool.parked) > 0 {
		pool.reaper = time.AfterFunc(arenaIdleTTL-now.Sub(pool.parked[0].parkedAt), pool.reap)
	} else {
		pool.reaper = nil
	}
	pool.mu.Unlock()

	for _, p := range freed {
		p.arena.FreeBuffers()
	}
}
