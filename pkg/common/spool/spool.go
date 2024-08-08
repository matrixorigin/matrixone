// Copyright 2024 Matrix Origin
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

package spool

import (
	"math"
	"sync"
	"sync/atomic"
)

// Spool is a single producer multiple consumer queue
type Spool[T Element] struct {
	mu            sync.Mutex
	cond          *sync.Cond
	capacity      int64
	inuse         int64
	tail          *node[T]
	head          *node[T]
	nodePool      sync.Pool
	closed        bool
	activeCursors atomic.Int32
}

type Element interface {
	// SizeInSpool returns the size to occupy in the pool's capacity
	SizeInSpool() int64
	// SpoolFree will be called if no further cursor can see the value
	SpoolFree()
}

func New[T Element](capacity int64, numCursors int) (*Spool[T], []*Cursor[T]) {

	spool := &Spool[T]{
		capacity: capacity,
		nodePool: sync.Pool{
			New: func() any {
				ret := new(node[T])
				ret.cond = sync.NewCond(&ret.mu)
				return ret
			},
		},
	}
	spool.tail = spool.newNode()
	spool.head = spool.tail
	spool.cond = sync.NewCond(&spool.mu)
	spool.activeCursors.Store(int32(numCursors))

	cursors := make([]*Cursor[T], 0, numCursors)
	for range numCursors {
		cursor := &Cursor[T]{
			spool: spool,
			next:  spool.tail,
		}
		cursor.next.numCursors++
		cursors = append(cursors, cursor)
	}

	return spool, cursors
}

func (s *Spool[T]) send(value T, maxConsumer int64, targetCursor *Cursor[T], stop bool) {
	if !stop {
		size := value.SizeInSpool()
		s.mu.Lock()
		if s.closed {
			panic("send to closed spool")
		}
		for s.inuse+size > s.capacity {
			s.cond.Wait()
		}
		s.inuse += size
		s.mu.Unlock()
	} else {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
	}

	node := s.tail
	newTail := s.newNode()
	node.mu.Lock()
	node.next = newTail
	node.valueOK = true
	if maxConsumer > 0 {
		node.maxConsumer.Store(maxConsumer)
	} else {
		node.maxConsumer.Store(math.MaxInt64)
	}
	node.value = value
	node.target = targetCursor
	node.stop = stop
	node.mu.Unlock()
	node.cond.Broadcast()

	s.tail = newTail

	if s.activeCursors.Load() <= 0 {
		s.checkFree()
	}
}

// Send enqueues a value
func (s *Spool[T]) Send(value T) {
	s.send(value, -1, nil, false)
}

// SendAny enqueues a value that at most one cursor can read
func (s *Spool[T]) SendAny(value T) {
	s.send(value, 1, nil, false)
}

// SendTo enqueues a value that only the specified cursor can read
func (s *Spool[T]) SendTo(target *Cursor[T], value T) {
	s.send(value, -1, target, false)
}

// Close closes the spool
// queued values can still be read
func (s *Spool[T]) Close() {
	var zero T
	s.send(zero, -1, nil, true)
}

func (s *Spool[T]) checkFree() {
	s.mu.Lock()
	defer func() {
		s.mu.Unlock()
		s.cond.Broadcast()
	}()
	for {
		s.head.mu.Lock()
		if !s.head.valueOK {
			s.head.mu.Unlock()
			return
		}
		if s.head.numCursors > 0 {
			s.head.mu.Unlock()
			return
		}
		if !s.head.stop {
			s.head.value.SpoolFree()
		}
		s.inuse -= s.head.value.SizeInSpool()
		s.head.mu.Unlock()
		next := s.head.next
		s.recycleNode(s.head)
		s.head = next
	}
}
