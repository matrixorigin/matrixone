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
)

// Spool is a single producer multiple consumer queue
type Spool[T Element] struct {
	mu        sync.Mutex
	cond      *sync.Cond
	capacity  int64
	inuse     int64
	head      *node[T]
	tail      *node[T]
	nodePool  sync.Pool
	closed    bool
	checkLock sync.Mutex
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
	spool.head = spool.newNode()
	spool.tail = spool.head
	spool.cond = sync.NewCond(&spool.mu)

	cursors := make([]*Cursor[T], 0, numCursors)
	for range numCursors {
		cursor := &Cursor[T]{
			spool: spool,
			next:  spool.head,
		}
		cursor.next.numCursors.Add(1)
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

	node := s.head
	newHead := s.newNode()
	node.mu.Lock()
	node.next = newHead
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

	s.head = newHead

	s.checkFree()
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
	s.checkLock.Lock()
	defer s.checkLock.Unlock()

	for {
		tail := s.tail

		if tail.numCursors.Load() > 0 {
			// still reachable
			return
		}

		tail.mu.Lock()
		if !tail.valueOK {
			// not valid value
			tail.mu.Unlock()
			return
		}
		// tail is not reachable now, so safe to unlock
		tail.mu.Unlock()

		s.tail = tail.next

		// update inuse
		s.mu.Lock()
		s.inuse -= tail.value.SizeInSpool()
		s.mu.Unlock()
		s.cond.Broadcast()

		// free
		if !tail.stop {
			tail.value.SpoolFree()
		}
		s.recycleNode(tail)

	}
}
