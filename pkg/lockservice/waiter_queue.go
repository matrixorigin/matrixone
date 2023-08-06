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
	"bytes"
	"sync"
)

type waiterQueue interface {
	reset()
	moveToFirst(serviceID string) *waiter
	put(...*waiter)
	iter(func(*waiter) bool)

	beginChange()
	commitChange()
	rollbackChange()
}

func newWaiterQueue() *sliceBasedWaiterQueue {
	return &sliceBasedWaiterQueue{beginChangeIdx: -1}
}

type sliceBasedWaiterQueue struct {
	sync.RWMutex
	beginChangeIdx int
	waiters        []*waiter
}

func (q *sliceBasedWaiterQueue) moveToFirst(serviceID string) *waiter {
	q.Lock()
	defer q.Unlock()
	if len(q.waiters) == 0 {
		return nil
	}
	v := q.waiters[0]
	v.add(serviceID, false, q.waiters[1:]...)
	q.clearWaitersLocked()
	return v
}

func (q *sliceBasedWaiterQueue) put(ws ...*waiter) {
	q.Lock()
	defer q.Unlock()

	if len(q.waiters) == 0 || len(ws) == 0 {
		q.waiters = ws
		return
	}

	// no new waiter added, moved from other waiter's waiter queue.
	if len(ws) > 1 {
		q.waiters = append(q.waiters, ws...)
		return
	}

	// if a new waiter added, need to check if the current txn has a waiter in the queue.
	for _, w := range q.waiters {
		if bytes.Equal(w.txnID, ws[0].txnID) {
			w.sameTxnWaiters = append(w.sameTxnWaiters, ws[0])
			return
		}
	}
	q.waiters = append(q.waiters, ws[0])
}

func (q *sliceBasedWaiterQueue) iter(fn func(*waiter) bool) {
	q.RLock()
	defer q.RUnlock()
	for _, w := range q.waiters {
		if !fn(w) {
			return
		}
	}
}

func (q *sliceBasedWaiterQueue) reset() {
	q.Lock()
	defer q.Unlock()
	q.clearWaitersLocked()
	q.beginChangeIdx = -1
}

func (q *sliceBasedWaiterQueue) clearWaitersLocked() {
	for i := range q.waiters {
		q.waiters[i] = nil
	}
	q.waiters = q.waiters[:0]
}

func (q *sliceBasedWaiterQueue) beginChange() {
	q.Lock()
	defer q.Unlock()

	if q.beginChangeIdx == -1 {
		q.beginChangeIdx = len(q.waiters)
	}
}

func (q *sliceBasedWaiterQueue) commitChange() {
	q.Lock()
	defer q.Unlock()
	q.beginChangeIdx = -1
}

func (q *sliceBasedWaiterQueue) rollbackChange() {
	q.Lock()
	defer q.Unlock()

	if q.beginChangeIdx > -1 {
		n := len(q.waiters)
		for i := q.beginChangeIdx; i < n; i++ {
			q.waiters[i] = nil
		}
		q.waiters = q.waiters[:q.beginChangeIdx]
		q.beginChangeIdx = -1
	}
}
