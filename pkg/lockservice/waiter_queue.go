// Copyright 2023 Matrix Origin
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

package lockservice

import (
	"sync"
)

type waiterQueue interface {
	len() int
	reset()
	pop() (*waiter, []*waiter)
	put(...*waiter)
	iter(func([]byte) bool)
}

func newWaiterQueue() *sliceBasedWaiterQueue {
	return &sliceBasedWaiterQueue{}
}

type sliceBasedWaiterQueue struct {
	sync.RWMutex
	offset  int
	waiters []*waiter
}

func (q *sliceBasedWaiterQueue) len() int {
	q.RLock()
	defer q.RUnlock()
	return len(q.waiters) - q.offset
}

func (q *sliceBasedWaiterQueue) pop() (*waiter, []*waiter) {
	if q.len() == 0 {
		panic("BUG: no waiter in queue")
	}

	q.Lock()
	defer q.Unlock()
	v := q.waiters[q.offset]
	q.waiters[q.offset] = nil
	q.offset++
	return v, q.waiters[q.offset:]
}

func (q *sliceBasedWaiterQueue) put(w ...*waiter) {
	q.Lock()
	defer q.Unlock()
	if len(q.waiters) == 0 {
		q.waiters = w
	} else {
		q.waiters = append(q.waiters, w...)
	}
}

func (q *sliceBasedWaiterQueue) iter(fn func([]byte) bool) {
	q.RLock()
	defer q.RUnlock()
	n := len(q.waiters)
	for i := q.offset; i < n; i++ {
		if !fn(q.waiters[i].txnID) {
			return
		}
	}
}

func (q *sliceBasedWaiterQueue) reset() {
	q.Lock()
	defer q.Unlock()
	q.waiters = q.waiters[:0]
	q.offset = 0
}
