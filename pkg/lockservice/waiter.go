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
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
)

var (
	waiterPool = sync.Pool{
		New: func() any {
			return newWaiter()
		},
	}
)

func acquireWaiter(txnID []byte) *waiter {
	w := waiterPool.Get().(*waiter)
	w.txnID = txnID
	if w.ref() != 1 {
		panic("BUG: invalid ref count")
	}
	return w
}

func newWaiter() *waiter {
	w := &waiter{
		c:       make(chan error, 1),
		waiters: newWaiterQueue(),
	}
	w.setFinalizer()
	w.status.Store(waiting)
	return w
}

const (
	waiting int32 = iota
	completed
)

// waiter is used to allow locking operations to wait for the previous
// lock to be released if a conflict is encountered.
// Each Lock holds one instance of waiter to hold all waiters. Suppose
// we have 3 transactions A, B and a record k1, the pseudocode of how to
// use waiter is as follows:
// 1. A get LockStorage s1
// 2. s1.Lock()
// 3. use s1.Seek(k1) to check conflict, s1.add(Lock(k1, waiter-k1-A))
// 4. s1.Unlock()
// 5. B get LockStorage s1
// 6. s1.Lock
// 7. use s1.Seek(k1) to check conflict, and found Lock(k1, waiter-k1-A)
// 8. so waiter-k1-A.add(waiter-k1-B)
// 9. s1.Unlock
// 10. waiter-k1-B.wait()
// 11. A completed
// 12. s1.Lock()
// 14. replace Lock(k1, waiter-k1-A) to Lock(k1, waiter-k1-B)
// 15. waiter-k1-A.close(), move all others waiters into waiter-k1-B.
// 16. s1.Unlock()
// 17. waiter-k1-B.wait() returned and get the lock
type waiter struct {
	txnID    []byte
	status   atomic.Int32
	c        chan error
	waiters  waiterQueue
	refCount atomic.Int32

	// just used for testing
	beforeSwapStatusAdjustFunc func()
}

func (w *waiter) setFinalizer() {
	// close the channel if gc
	runtime.SetFinalizer(w, func(w *waiter) {
		close(w.c)
	})
}

func (w *waiter) ref() int32 {
	return w.refCount.Add(1)
}

func (w *waiter) unref() {
	n := w.refCount.Add(-1)
	if n < 0 {
		panic("BUG: invalid ref count")
	}
	if n == 0 {
		w.reset()
	}
}

func (w *waiter) add(waiter ...*waiter) {
	if len(waiter) == 0 {
		return
	}
	w.waiters.put(waiter...)
	for i := range waiter {
		waiter[i].ref()
	}
}

func (w *waiter) setCompleted() {
	w.status.Store(completed)
}

func (w *waiter) setWaiting() {
	w.status.Store(waiting)
}

func (w *waiter) mustGetNotifiedValue() error {
	select {
	case err := <-w.c:
		return err
	default:
	}
	panic("BUG: cannot get result from channel")
}

func (w *waiter) resetState() {
	if !w.status.CompareAndSwap(completed, waiting) {
		panic(fmt.Sprintf("BUG: waiter's status cannot be %d", w.status.Load()))
	}
}

func (w *waiter) wait(ctx context.Context) error {
	status := w.status.Load()
	if status == completed {
		return w.mustGetNotifiedValue()
	}
	if status != waiting {
		panic(fmt.Sprintf("BUG: waiter's status cannot be %d", status))
	}

	select {
	case err := <-w.c:
		w.setCompleted()
		return err
	case <-ctx.Done():
	}

	if w.beforeSwapStatusAdjustFunc != nil {
		w.beforeSwapStatusAdjustFunc()
	}

	// context is timeout, and status not changed, no concurrent happen
	if w.status.CompareAndSwap(status, completed) {
		return ctx.Err()
	}

	// notify and timeout are concurrently issued, we use real result to replace
	// timeout error
	w.setCompleted()
	return w.mustGetNotifiedValue()
}

// notify return false means this waiter is completed, cannot be used to notify
func (w *waiter) notify(value error) bool {
	status := w.status.Load()
	if status == completed {
		return false
	}

	if w.beforeSwapStatusAdjustFunc != nil {
		w.beforeSwapStatusAdjustFunc()
	}

	// if status changed, notify and timeout are concurrently issued, need
	// try.
	if w.status.CompareAndSwap(status, completed) {
		w.c <- value
		return true
	}
	return false
}

// close returns the next waiter to hold the lock, and others waiters will move
// into the next waiter.
func (w *waiter) close() *waiter {
	// no new waiters can be added during close.
	nextHolder := w.fetchNextHolder()
	if nextHolder != nil {
		nextHolder.unref()
	}
	w.unref()
	return nextHolder
}

func (w *waiter) fetchNextHolder() *waiter {
	if w.waiters.len() == 0 {
		return nil
	}
	next := w.awakeNextWaiter()
	for {
		if next.notify(nil) {
			return next
		}
		if next.waiters.len() == 0 {
			return nil
		}
		next = next.awakeNextWaiter()
	}
}

func (w *waiter) awakeNextWaiter() *waiter {
	next, remains := w.waiters.mustGetHeadAndTail()
	next.add(remains...)
	w.waiters.reset()
	return next
}

func (w *waiter) reset() {
	if w.waiters.len() > 0 || len(w.c) > 0 {
		panic("BUG: waiter should be empty.")
	}
	w.beforeSwapStatusAdjustFunc = nil
	w.setWaiting()
	w.waiters.reset()
	waiterPool.Put(w)
}
