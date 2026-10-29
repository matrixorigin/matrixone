// Copyright 2026 Matrix Origin
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

package client

// activeTxnQueue is the FIFO membership owner for max-active admission.
// Its links live in the existing waiter and are protected only by client.mu;
// waiter.mu still owns cancellation and promotion state.
type activeTxnQueue struct {
	head, tail *txnOperator
	size       int
}

func (q *activeTxnQueue) pushBack(op *txnOperator) {
	w := op.reset.waiter
	w.queue = q
	w.prev = q.tail
	w.next = nil
	if q.tail == nil {
		q.head = op
	} else {
		q.tail.reset.waiter.next = op
	}
	q.tail = op
	q.size++
}

func (q *activeTxnQueue) remove(op *txnOperator) bool {
	w := op.reset.waiter
	if w == nil || w.queue != q {
		return false
	}
	if w.prev == nil {
		q.head = w.next
	} else {
		w.prev.reset.waiter.next = w.next
	}
	if w.next == nil {
		q.tail = w.prev
	} else {
		w.next.reset.waiter.prev = w.prev
	}
	w.queue, w.prev, w.next = nil, nil, nil
	q.size--
	return true
}

func (q *activeTxnQueue) appendTo(ops []*txnOperator) []*txnOperator {
	for op := q.head; op != nil; op = op.reset.waiter.next {
		ops = append(ops, op)
	}
	return ops
}

// drain captures the exact gates detached by Close before releasing client.mu.
// An operator may be reused after cancellation; shutdown must not read its new
// generation's waiter when it notifies these old gates outside the lock.
func (q *activeTxnQueue) drain() []*activeTxnWaiter {
	waiters := make([]*activeTxnWaiter, 0, q.size)
	for q.head != nil {
		op := q.head
		waiters = append(waiters, op.reset.waiter)
		q.remove(op)
	}
	return waiters
}
