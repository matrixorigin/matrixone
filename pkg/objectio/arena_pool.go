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

// arenaNode is a linked-list node for the GC-immune WriteArena free list.
type arenaNode struct {
	arena *WriteArena
	next  *arenaNode
}

// arenaFreeList is a lock-free stack of WriteArena instances.
type arenaFreeList struct {
	head     atomic.Pointer[arenaNode]
	count    atomic.Int32
	maxCount int32
	bytes    atomic.Int64
	maxBytes int64
	budget   *arenaByteBudget
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
	for {
		head := pool.head.Load()
		if head == nil {
			return nil
		}
		if pool.head.CompareAndSwap(head, head.next) {
			pool.count.Add(-1)
			size := int64(head.arena.Allocated())
			pool.bytes.Add(-size)
			if pool.budget != nil {
				pool.budget.used.Add(-size)
			}
			return head.arena
		}
	}
}

// PutArena pushes a WriteArena back into its tier's free list,
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
	// Count limits alone allow a few wide-column arenas to pin gigabytes.
	// Reserve all three buffers before publishing; pooled arenas are immutable.
	size := int64(a.Allocated())
	if pool.budget != nil && !pool.budget.reserve(size) {
		a.FreeBuffers()
		return
	}
	for {
		used := pool.bytes.Load()
		if size > pool.maxBytes-used {
			if pool.budget != nil {
				pool.budget.used.Add(-size)
			}
			a.FreeBuffers()
			return
		}
		if pool.bytes.CompareAndSwap(used, used+size) {
			break
		}
	}
	// Optimistically claim a slot before pushing: increment first, then
	// check.  This avoids the TOCTOU race where N goroutines each read
	// count < maxCount and all push, exceeding the soft cap.
	if pool.count.Add(1) > pool.maxCount {
		pool.count.Add(-1)
		pool.bytes.Add(-size)
		if pool.budget != nil {
			pool.budget.used.Add(-size)
		}
		a.FreeBuffers()
		return
	}
	node := &arenaNode{arena: a}
	for {
		oldHead := pool.head.Load()
		node.next = oldHead
		if pool.head.CompareAndSwap(oldHead, node) {
			return
		}
	}
}

// DefaultArenaDrainDelay is the objectio-local debounce used by callers
// without a workload-specific idle horizon.
const DefaultArenaDrainDelay = 2 * time.Minute

var (
	arenaDrainMu    sync.Mutex
	arenaDrainTimer *time.Timer
)

// ScheduleArenaDrain debounces arena pool draining.  Each call resets the
// timer to delay from now.  Once activity ceases, the timer fires and RSS
// is reclaimed.
func ScheduleArenaDrain(delay time.Duration) {
	if delay <= 0 {
		delay = DefaultArenaDrainDelay
	}
	arenaDrainMu.Lock()
	defer arenaDrainMu.Unlock()
	if arenaDrainTimer != nil {
		arenaDrainTimer.Stop()
	}
	arenaDrainTimer = time.AfterFunc(delay, drainArenaPools)
}

// drainArenaPools empties both arena free lists, freeing the off-heap
// backing buffers.  Called by the debounce timer when merge and flush
// have been idle for the caller-provided delay.
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
	for {
		head := pool.head.Load()
		if head == nil {
			return
		}
		if pool.head.CompareAndSwap(head, nil) {
			// Count and free exactly the nodes we grabbed.
			// Use Add(-n) rather than Store(0) so concurrent
			// PutArena calls that incremented count after the CAS
			// are not silently erased.
			var n int32
			var size int64
			for node := head; node != nil; node = node.next {
				size += int64(node.arena.Allocated())
				node.arena.FreeBuffers()
				n++
			}
			pool.count.Add(-n)
			pool.bytes.Add(-size)
			if pool.budget != nil {
				pool.budget.used.Add(-size)
			}
			return
		}
	}
}
