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

package fifocache

import "sync"

type Queue[T any] struct {
	mu       sync.Mutex // Mutex to protect queue operations
	head     *queuePart[T]
	tail     *queuePart[T]
	partPool sync.Pool
	size     int
}

type queuePart[T any] struct {
	values []T
	begin  int
	next   *queuePart[T]
}

const maxQueuePartCapacity = 256

func NewQueue[T any]() *Queue[T] {
	queue := &Queue[T]{
		partPool: sync.Pool{
			New: func() any {
				return &queuePart[T]{
					values: make([]T, 0, maxQueuePartCapacity),
				}
			},
		},
	}
	part := queue.partPool.Get().(*queuePart[T])
	queue.head = part
	queue.tail = part
	return queue
}

// empty is an internal helper, assumes lock is held
func (p *Queue[T]) empty() bool {
	return p.head == p.tail && len(p.head.values) == p.head.begin
}

func (p *queuePart[T]) reset() {
	p.values = p.values[:0]
	p.begin = 0
	p.next = nil
}

func (p *Queue[T]) enqueue(v T) {
	p.mu.Lock()         // Acquire lock
	defer p.mu.Unlock() // Ensure lock is released

	if len(p.head.values) >= maxQueuePartCapacity {
		// extend
		newPart := p.partPool.Get().(*queuePart[T])
		newPart.reset()
		p.head.next = newPart
		p.head = newPart
	}
	p.head.values = append(p.head.values, v)
	p.size++
}

func (p *Queue[T]) dequeue() (ret T, ok bool) {
	p.mu.Lock()         // Acquire lock
	defer p.mu.Unlock() // Ensure lock is released

	if p.empty() {
		return
	}

	if p.tail.begin >= len(p.tail.values) {
		// shrink
		if p.tail.next == nil {
			// This should ideally not happen if empty() check passes,
			// but adding a safeguard.
			// Consider logging an error here if it does.
			return
		}
		part := p.tail
		p.tail = p.tail.next
		p.partPool.Put(part) // Return the old part to the pool
	}

	ret = p.tail.values[p.tail.begin]
	var zero T
	p.tail.values[p.tail.begin] = zero
	p.tail.begin++
	p.size--
	ok = true
	return
}

func (p *Queue[T]) Len() int {
	p.mu.Lock()         // Acquire lock
	defer p.mu.Unlock() // Ensure lock is released
	return p.size
}
