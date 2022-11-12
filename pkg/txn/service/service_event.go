// Copyright 2021 - 2022 Matrix Origin
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

package service

import (
	"context"
	"runtime"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
)

var (
	waiterPool = sync.Pool{
		New: func() any {
			w := &waiter{c: make(chan txn.TxnStatus, 1)}
			w.setFinalizer()
			return w
		},
	}

	notifierPool = sync.Pool{
		New: func() any {
			n := &notifier{}
			n.waiters = make([][]*waiter, len(txn.TxnStatus_name))
			return n
		},
	}
)

func acquireWaiter() *waiter {
	w := waiterPool.Get().(*waiter)
	w.ref()
	w.reuse++
	return w
}

type waiter struct {
	c     chan txn.TxnStatus
	reuse uint64

	mu struct {
		sync.RWMutex
		closed bool
		// A waiter can only be notified once, regardless of how many transactions it watches.
		notified bool
		// The wait will held by notifier and the place to use waiter. The waiter can only be recycled to
		// sync.Pool if it is no longer referenced by any.
		ref int
	}
}

func (w *waiter) ref() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.mu.ref++
}

func (w *waiter) unref() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.unrefLocked()
	w.maybeReleaseLocked()
}

func (w *waiter) unrefLocked() {
	w.mu.ref--
	if w.mu.ref < 0 {
		panic("invalid ref value")
	}
}

func (w *waiter) notify(status txn.TxnStatus) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.mu.notified {
		panic("already notified")
	}

	w.mu.notified = true
	if w.mu.closed {
		return false
	}
	select {
	case w.c <- status:
	default:
		panic("BUG")
	}
	return true
}

func (w *waiter) close() {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.mu.closed = true
	w.unrefLocked()
	w.maybeReleaseLocked()
}

func (w *waiter) maybeReleaseLocked() {
	if w.mu.ref == 0 {
		w.mu.closed = false
		w.mu.notified = false
		select {
		case <-w.c:
		default:
		}
		waiterPool.Put(w)
	}
}

// wait only 3 results:
// 1. want status, nil
// 2. any, context.Done error
// 3. final status(committed or aborted), nil
func (w *waiter) wait(ctx context.Context) (txn.TxnStatus, error) {
	select {
	case <-ctx.Done():
		return txn.TxnStatus_Aborted, moerr.ConvertGoError(ctx.Err())
	case status := <-w.c:
		return status, nil
	}
}

func (w *waiter) setFinalizer() {
	runtime.SetFinalizer(w, func(w *waiter) {
		close(w.c)
	})
}

func acquireNotifier() *notifier {
	return notifierPool.Get().(*notifier)
}

// notifier a transaction corresponds to a notifier, for other transactions that need to be notified that
// the current transaction has reached the specified status.
type notifier struct {
	sync.Mutex
	waiters [][]*waiter
}

func (s *notifier) close(status txn.TxnStatus) {
	s.Lock()
	defer s.Unlock()

	for i, waiters := range s.waiters {
		for j := range waiters {
			waiters[j].notify(status)
			waiters[j].unref()
			waiters[j] = nil
		}
		s.waiters[i] = waiters[:0]
	}
	notifierPool.Put(s)
}

func (s *notifier) notify(status txn.TxnStatus) int {
	s.Lock()
	defer s.Unlock()

	if !s.hasWaitersLocked(status) {
		return 0
	}

	waiters := s.waiters[status]
	n := 0
	for idx, w := range waiters {
		if w.notify(status) {
			n++
		}
		w.unref()
		waiters[idx] = nil
	}
	s.waiters[status] = waiters[:0]
	return n
}

func (s *notifier) addWaiter(w *waiter, status txn.TxnStatus) {
	s.Lock()
	defer s.Unlock()

	w.ref()
	s.waiters[status] = append(s.waiters[status], w)
}

func (s *notifier) hasWaitersLocked(status txn.TxnStatus) bool {
	return len(s.waiters[status]) > 0
}
