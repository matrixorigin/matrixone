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
	"encoding/hex"
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
	w.beforeSwapStatusAdjustFunc = func() {}
	return w
}

func newWaiter() *waiter {
	w := &waiter{
		c:       make(chan error, 1),
		waiters: newWaiterQueue(),
	}
	w.setFinalizer()
	w.setStatus(waiting)
	return w
}

type waiterStatus int32

const (
	waiting waiterStatus = iota
	notified
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

// String implement Stringer
func (w *waiter) String() string {
	return fmt.Sprintf("%s-%p",
		hex.EncodeToString(w.txnID),
		w)
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

func (w *waiter) add(waiters ...*waiter) {
	if len(waiters) == 0 {
		return
	}
	w.waiters.put(waiters...)
	for i := range waiters {
		waiters[i].ref()
	}
	logWaitersAdded(w, waiters...)
}

func (w *waiter) getStatus() waiterStatus {
	return waiterStatus(w.status.Load())
}

func (w *waiter) setStatus(status waiterStatus) {
	w.status.Store(int32(status))
	logWaiterStatusChanged(w, status)
}

func (w *waiter) casStatus(old, new waiterStatus) bool {
	if w.status.CompareAndSwap(int32(old), int32(new)) {
		logWaiterStatusChanged(w, new)
		return true
	}
	return false
}

func (w *waiter) mustRecvNotification() error {
	select {
	case err := <-w.c:
		logWaiterGetNotify(w, err)
		return err
	default:
	}
	panic("BUG: must recv result from channel")
}

func (w *waiter) mustSendNotification(value error) {
	select {
	case w.c <- value:
		logWaiterNotified(w, value)
		return
	default:
	}
	panic("BUG: must send value to channel")
}

func (w *waiter) resetWait() {
	if w.casStatus(completed, waiting) {
		return
	}
	panic("invalid reset wait")
}

func (w *waiter) wait(ctx context.Context) error {
	status := w.getStatus()
	if status == notified {
		w.setStatus(completed)
		return w.mustRecvNotification()
	}
	if status != waiting {
		panic(fmt.Sprintf("BUG: waiter's status cannot be %d", status))
	}

	w.beforeSwapStatusAdjustFunc()

	select {
	case err := <-w.c:
		w.setStatus(completed)
		return err
	case <-ctx.Done():
	}

	w.beforeSwapStatusAdjustFunc()

	// context is timeout, and status not changed, no concurrent happen
	if w.casStatus(status, completed) {
		return ctx.Err()
	}

	// notify and timeout are concurrently issued, we use real result to replace
	// timeout error
	w.setStatus(completed)
	return w.mustRecvNotification()
}

// notify return false means this waiter is completed, cannot be used to notify
func (w *waiter) notify(value error) bool {
	for {
		status := w.getStatus()
		// already notified, no wait on w
		if status == notified {
			return false
		}
		if status == completed {
			// wait already completed, wait timeout or wait a result.
			return false
		}

		w.beforeSwapStatusAdjustFunc()
		// if status changed, notify and timeout are concurrently issued, need
		// retry.
		if w.casStatus(status, notified) {
			w.mustSendNotification(value)
			return true
		}
	}
}

func (w *waiter) clearAllNotify() {
	for {
		select {
		case <-w.c:
		default:
			return
		}
	}
}

// close returns the next waiter to hold the lock, and others waiters will move
// into the next waiter.
func (w *waiter) close(err error) *waiter {
	nextWaiter := w.fetchNextWaiter(err)
	w.unref()
	return nextWaiter
}

func (w *waiter) fetchNextWaiter(err error) *waiter {
	if w.waiters.len() == 0 {
		return nil
	}
	next := w.awakeNextWaiter()
	for {
		if next.notify(err) {
			next.unref()
			return next
		}
		if next.waiters.len() == 0 {
			return nil
		}
		next = next.awakeNextWaiter()
	}
}

func (w *waiter) awakeNextWaiter() *waiter {
	next, remains := w.waiters.pop()
	next.add(remains...)
	w.waiters.reset()
	return next
}

func (w *waiter) reset() {
	if w.waiters.len() > 0 || len(w.c) > 0 {
		panic("BUG: waiter should be empty.")
	}
	w.setStatus(waiting)
	w.waiters.reset()
	waiterPool.Put(w)
}
