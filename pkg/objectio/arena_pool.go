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
	"sync/atomic"
	"unsafe"
)

// arenaNode is a linked-list node for the GC-immune WriteArena free list.
// Using a plain linked list (instead of sync.Pool) prevents GC from
// evicting warmed arenas. sync.Pool is cleared every GC cycle, causing
// 128 MB arenas to be reallocated from scratch repeatedly.
type arenaNode struct {
	arena *WriteArena
	next  unsafe.Pointer // *arenaNode
}

// arenaFreeList is a lock-free stack of WriteArena instances.
type arenaFreeList struct {
	head  unsafe.Pointer // *arenaNode
	count atomic.Int32
}

const arenaMaxPooled = 16 // max arenas to keep in the free list

var arenaPool arenaFreeList

// GetArena pops a WriteArena from the GC-immune free list, or creates
// a fresh zero-sized arena when the list is empty.
func GetArena() *WriteArena {
	for {
		head := atomic.LoadPointer(&arenaPool.head)
		if head == nil {
			return NewArena(0)
		}
		node := (*arenaNode)(head)
		next := atomic.LoadPointer(&node.next)
		if atomic.CompareAndSwapPointer(&arenaPool.head, head, next) {
			arenaPool.count.Add(-1)
			return node.arena
		}
	}
}

// PutArena pushes a WriteArena back into the GC-immune free list.
// Arenas exceeding the pool capacity are silently dropped.
func PutArena(a *WriteArena) {
	if a == nil {
		return
	}
	if int(arenaPool.count.Load()) >= arenaMaxPooled {
		return
	}
	node := &arenaNode{arena: a}
	for {
		oldHead := atomic.LoadPointer(&arenaPool.head)
		node.next = oldHead
		if atomic.CompareAndSwapPointer(&arenaPool.head, oldHead, unsafe.Pointer(node)) {
			arenaPool.count.Add(1)
			return
		}
	}
}
