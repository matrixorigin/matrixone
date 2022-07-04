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
	return waiterPool.Get().(*waiter)
}

type waiter struct {
	targets  []txn.TxnStatus
	c        chan txn.TxnStatus
	notified bool
}

func (w *waiter) addWaitStatus(targets []txn.TxnStatus) {
	w.targets = append(w.targets, targets...)
}

func (w *waiter) notify(status txn.TxnStatus) {
	select {
	case w.c <- status:
	default:
		panic("BUG")
	}
	w.notified = true
}

func (w *waiter) close() {
	w.targets = w.targets[:0]
	w.notified = false
	waiterPool.Put(w)
}

func (w *waiter) wait(ctx context.Context) (txn.TxnStatus, bool) {
	select {
	case <-ctx.Done():
		return txn.TxnStatus_Aborted, false
	case status, ok := <-w.c:
		return status, ok
	}
}

func (w *waiter) setFinalizer() {
	runtime.SetFinalizer(w, func(w *waiter) {
		close(w.c)
	})
}

type notifier struct {
	sync.Mutex
	waiters [][]*waiter
}

func (s *notifier) close() {
	s.Lock()
	defer s.Unlock()

	for i, waiters := range s.waiters {
		for j := range waiters {
			waiters[j] = nil
		}
		s.waiters[i] = waiters[:0]
	}
	notifierPool.Put(s)
}

func (s *notifier) notify(status txn.TxnStatus) {
	s.Lock()
	defer s.Unlock()

	if !s.hasWaitersLocked(status) {
		return
	}

	waiters := s.waiters[status]
	for idx, w := range waiters {
		w.notify(status)
		waiters[idx] = nil
	}
	s.waiters[status] = waiters[:0]

	s.removeWaiter(int(status))
}

func (s *notifier) removeWaiter(excludeIndex int) {
	for i, waiters := range s.waiters {
		if i == excludeIndex {
			continue
		}

		newWaiters := waiters[:0]
		for _, w := range waiters {
			if !w.notified {
				newWaiters = append(newWaiters, w)
			}
		}
		s.waiters[i] = newWaiters

		n := len(waiters)
		for j := len(newWaiters); j < n; j++ {
			waiters[j] = nil
		}
	}
}

func (s *notifier) addWaiter(w *waiter) {
	s.Lock()
	defer s.Unlock()

	for _, st := range w.targets {
		s.waiters[st] = append(s.waiters[st], w)
	}
}

func (s *notifier) hasWaitersLocked(status txn.TxnStatus) bool {
	return len(s.waiters[status]) > 0
}
