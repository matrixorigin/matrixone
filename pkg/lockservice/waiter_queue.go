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
	Len() uint64
	Reset()
	MustGet() *waiter
	MustGetHeadAndTail() (*waiter, []*waiter)
	Put(...*waiter)
	IterTxns(func([]byte) bool)
}

func newWaiterQueue() *sliceBasedWaiterQueue {
	return &sliceBasedWaiterQueue{}
}

type sliceBasedWaiterQueue struct {
	sync.RWMutex
	offset  uint64
	waiters []*waiter
}

func (q *sliceBasedWaiterQueue) Len() uint64 {
	q.RLock()
	defer q.RUnlock()
	return uint64(len(q.waiters)) - q.offset
}

func (q *sliceBasedWaiterQueue) MustGet() *waiter {
	if q.Len() == 0 {
		panic("BUG: no waiter in queue")
	}

	q.Lock()
	defer q.Unlock()
	v := q.waiters[q.offset]
	q.waiters[q.offset] = nil
	q.offset++
	return v
}

func (q *sliceBasedWaiterQueue) MustGetHeadAndTail() (*waiter, []*waiter) {
	if q.Len() == 0 {
		panic("BUG: no waiter in queue")
	}

	q.Lock()
	defer q.Unlock()
	v := q.waiters[q.offset]
	q.waiters[q.offset] = nil
	q.offset++
	return v, q.waiters[q.offset:]
}

func (q *sliceBasedWaiterQueue) Put(w ...*waiter) {
	q.Lock()
	defer q.Unlock()
	q.waiters = append(q.waiters, w...)
}

func (q *sliceBasedWaiterQueue) IterTxns(fn func([]byte) bool) {
	q.RLock()
	defer q.RUnlock()
	n := uint64(len(q.waiters))
	for i := q.offset; i < n; i++ {
		if !fn(q.waiters[i].txnID) {
			return
		}
	}
}

func (q *sliceBasedWaiterQueue) Reset() {
	q.Lock()
	defer q.Unlock()
	q.waiters = q.waiters[:0]
	q.offset = 0
}
