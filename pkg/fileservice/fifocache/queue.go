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
	head     *queuePart[T]
	tail     *queuePart[T]
	partPool sync.Pool
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

func (p *Queue[T]) empty() bool {
	return p.head == p.tail &&
		p.head.begin == len(p.head.values)
}

func (p *queuePart[T]) reset() {
	p.values = p.values[:0]
	p.begin = 0
	p.next = nil
}

func (p *Queue[T]) enqueue(v T) {
	if len(p.head.values) >= maxQueuePartCapacity {
		// extend
		newPart := p.partPool.Get().(*queuePart[T])
		newPart.reset()
		p.head.next = newPart
		p.head = newPart
	}
	p.head.values = append(p.head.values, v)
}

func (p *Queue[T]) dequeue() (ret T, ok bool) {
	if p.empty() {
		return
	}

	if p.tail.begin >= len(p.tail.values) {
		// shrink
		if p.tail.next == nil {
			panic("impossible")
		}
		part := p.tail
		p.tail = p.tail.next
		p.partPool.Put(part)
	}

	ret = p.tail.values[p.tail.begin]
	p.tail.begin++
	ok = true
	return
}
