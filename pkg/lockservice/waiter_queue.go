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

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
)

type waiterQueue interface {
	close(notify notifyValue)
	reset()

	// writes methods, only can used in local_lock_table locked methods
	resetCommittedAt(timestamp.Timestamp)
	moveTo(to waiterQueue)
	put(...*waiter)
	notify(value notifyValue)
	notifyAll(value notifyValue)
	first() *waiter
	removeByTxnID(txnID []byte)
	beginChange()
	commitChange()
	rollbackChange()

	// read methods, can used any where
	iter(func(*waiter) bool)
	size() int
}

func newWaiterQueue() waiterQueue {
	return &sliceBasedWaiterQueue{beginChangeIdx: -1}
}

type sliceBasedWaiterQueue struct {
	sync.RWMutex
	beginChangeIdx int
	waiters        []*waiter
	keyCommittedAt timestamp.Timestamp
}

func (q *sliceBasedWaiterQueue) moveTo(to waiterQueue) {
	q.iter(func(w *waiter) bool {
		to.put(w)
		return true
	})
}

func (q *sliceBasedWaiterQueue) put(ws ...*waiter) {
	q.Lock()
	defer q.Unlock()
	for _, w := range ws {
		w.ref()
	}
	q.waiters = append(q.waiters, ws...)
	v2.TxnLockWaitersTotalHistogram.Observe(float64(len(q.waiters)))
}

func (q *sliceBasedWaiterQueue) notifyAll(value notifyValue) {
	q.Lock()
	defer q.Unlock()

	// save the max committed ts
	if value.ts.Less(q.keyCommittedAt) {
		value.ts = q.keyCommittedAt
	} else {
		q.keyCommittedAt = value.ts
	}

	if len(q.waiters) == 0 {
		return
	}

	if q.beginChangeIdx != -1 {
		panic("BUG: cannot call notify in changing waiter queue")
	}

	for _, w := range q.waiters {
		w.notify(value)
		w.close()
	}
	q.resetWaitersLocked()
	v2.TxnLockWaitersTotalHistogram.Observe(float64(len(q.waiters)))
}

func (q *sliceBasedWaiterQueue) notify(value notifyValue) {
	q.Lock()
	defer q.Unlock()

	// save the max committed ts
	if value.ts.Less(q.keyCommittedAt) {
		value.ts = q.keyCommittedAt
	} else {
		q.keyCommittedAt = value.ts
	}

	if len(q.waiters) == 0 {
		return
	}

	if q.beginChangeIdx != -1 {
		panic("BUG: cannot call notify in changing waiter queue")
	}

	skipAt := -1
	for i, w := range q.waiters {
		if w.notify(value) {
			break
		}
		// already completed
		w.close()
		skipAt = i
		q.waiters[i] = nil
	}
	q.waiters = append(q.waiters[:0], q.waiters[skipAt+1:]...)
	v2.TxnLockWaitersTotalHistogram.Observe(float64(len(q.waiters)))
}

func (q *sliceBasedWaiterQueue) resetCommittedAt(ts timestamp.Timestamp) {
	q.Lock()
	defer q.Unlock()
	if ts.Greater(q.keyCommittedAt) {
		q.keyCommittedAt = ts
	}
}

func (q *sliceBasedWaiterQueue) removeByTxnID(txnID []byte) {
	q.Lock()
	defer q.Unlock()
	if len(q.waiters) == 0 {
		return
	}
	newWaiters := q.waiters[:0]
	for _, w := range q.waiters {
		if w.isTxn(txnID) {
			w.notify(notifyValue{})
			w.close()
			continue
		}
		newWaiters = append(newWaiters, w)
	}
	q.waiters = newWaiters
	v2.TxnLockWaitersTotalHistogram.Observe(float64(len(q.waiters)))
}

func (q *sliceBasedWaiterQueue) first() *waiter {
	var w *waiter
	q.iter(func(v *waiter) bool {
		w = v
		return false
	})
	return w
}

func (q *sliceBasedWaiterQueue) beginChange() {
	q.Lock()
	defer q.Unlock()

	if q.beginChangeIdx != -1 {
		panic("BUG: begin change multiple times")
	}

	if q.beginChangeIdx == -1 {
		q.beginChangeIdx = len(q.waiters)
	}
}

func (q *sliceBasedWaiterQueue) commitChange() {
	q.Lock()
	defer q.Unlock()
	if q.beginChangeIdx == -1 {
		panic("BUG: invalid beginChangeIdx")
	}

	q.beginChangeIdx = -1
}

func (q *sliceBasedWaiterQueue) rollbackChange() {
	q.Lock()
	defer q.Unlock()

	if q.beginChangeIdx == -1 {
		panic("BUG: invalid beginChangeIdx")
	}

	n := len(q.waiters)
	for i := q.beginChangeIdx; i < n; i++ {
		q.waiters[i].close()
		q.waiters[i] = nil
	}
	q.waiters = q.waiters[:q.beginChangeIdx]
	q.beginChangeIdx = -1
	v2.TxnLockWaitersTotalHistogram.Observe(float64(len(q.waiters)))
}

func (q *sliceBasedWaiterQueue) getCommittedIdx() int {
	i := len(q.waiters)
	if q.beginChangeIdx != -1 {
		i = q.beginChangeIdx
	}
	return i
}

func (q *sliceBasedWaiterQueue) iter(fn func(*waiter) bool) {
	q.RLock()
	defer q.RUnlock()

	idx := q.getCommittedIdx()
	for i := 0; i < idx; i++ {
		if !fn(q.waiters[i]) {
			return
		}
	}
}

func (q *sliceBasedWaiterQueue) size() int {
	q.RLock()
	defer q.RUnlock()
	return q.getCommittedIdx()
}

func (q *sliceBasedWaiterQueue) reset() {
	q.Lock()
	defer q.Unlock()
	q.doResetLocked()
}

func (q *sliceBasedWaiterQueue) close(value notifyValue) {
	q.Lock()
	defer q.Unlock()
	idx := q.getCommittedIdx()
	for i := 0; i < idx; i++ {
		w := q.waiters[i]
		w.notify(value)
		w.close()
	}
	q.doResetLocked()
}

func (q *sliceBasedWaiterQueue) doResetLocked() {
	q.resetWaitersLocked()
	q.beginChangeIdx = -1
	q.keyCommittedAt = timestamp.Timestamp{}
}

func (q *sliceBasedWaiterQueue) resetWaitersLocked() {
	for i := range q.waiters {
		q.waiters[i] = nil
	}
	q.waiters = q.waiters[:0]
}
