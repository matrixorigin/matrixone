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
	"sync/atomic"
	"unsafe"
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
	next  unsafe.Pointer // *arenaNode
}

// arenaFreeList is a lock-free stack of WriteArena instances.
type arenaFreeList struct {
	head     unsafe.Pointer // *arenaNode
	count    atomic.Int32
	maxCount int32
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
}

// GetArena pops a WriteArena from the requested tier's free list, or
// creates a pre-warmed arena when the list is empty.
func GetArena(tier int) *WriteArena {
	pool := &arenaPools[tier]
	for {
		head := atomic.LoadPointer(&pool.head)
		if head == nil {
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
		node := (*arenaNode)(head)
		next := atomic.LoadPointer(&node.next)
		if atomic.CompareAndSwapPointer(&pool.head, head, next) {
			pool.count.Add(-1)
			return node.arena
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
	if pool.count.Load() >= pool.maxCount {
		return
	}
	node := &arenaNode{arena: a}
	for {
		oldHead := atomic.LoadPointer(&pool.head)
		node.next = oldHead
		if atomic.CompareAndSwapPointer(&pool.head, oldHead, unsafe.Pointer(node)) {
			pool.count.Add(1)
			return
		}
	}
}

// DrainArenaPools empties both arena free lists, allowing the GC to
// reclaim the backing memory.  Call this when merge and flush are idle
// (e.g. after a checkpoint completes) to reduce RSS between active
// periods.  The next GetArena call will create a fresh pre-warmed arena.
func DrainArenaPools() {
	for i := range arenaPools {
		pool := &arenaPools[i]
		for {
			head := atomic.LoadPointer(&pool.head)
			if head == nil {
				break
			}
			if atomic.CompareAndSwapPointer(&pool.head, head, nil) {
				pool.count.Store(0)
				break
			}
		}
	}
}
