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

	"github.com/matrixorigin/matrixone/pkg/util/ring"
)

const (
	defaultMaxWaiters = 4096
)

var (
	waiterPool = sync.Pool{
		New: func() any {
			return newWaiter(0)
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

func newWaiter(maxWaiters uint64) *waiter {
	if maxWaiters == 0 {
		maxWaiters = defaultMaxWaiters
	}
	w := &waiter{
		c:       make(chan error, 1),
		waiters: ring.NewRingBuffer[*waiter](maxWaiters),
	}
	w.setFinalizer()
	return w
}

var (
	waitNotifyStatus    int32 = 0
	notifyAddedStatus   int32 = 1
	waitCompletedStatus int32 = 2
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
	status   int32
	c        chan error
	waiters  *ring.RingBuffer[*waiter]
	refCount int32

	// just used for testing
	beforeSwapStatusAdjustFunc func()
}

func (w *waiter) setFinalizer() {
	// close the channal if gc
	runtime.SetFinalizer(w, func(w *waiter) {
		close(w.c)
	})
}

func (w *waiter) ref() int32 {
	return atomic.AddInt32(&w.refCount, 1)
}

func (w *waiter) unref() {
	n := atomic.AddInt32(&w.refCount, -1)
	if n < 0 {
		panic("BUG: invalid ref count")
	}
	if n == 0 {
		w.reset()
	}
}

func (w *waiter) add(waiter *waiter) error {
	err := w.waiters.Put(waiter)
	if err != nil {
		return err
	}
	waiter.ref()
	return nil
}

func (w *waiter) getStatus() int32 {
	return atomic.LoadInt32(&w.status)
}

func (w *waiter) setCompleted() {
	atomic.StoreInt32(&w.status, waitCompletedStatus)
}

func (w *waiter) mustGetNotifiedValue() error {
	select {
	case err := <-w.c:
		return err
	default:
		panic("BUG: must can get result from channel")
	}
}

func (w *waiter) resetWait() {
	if !atomic.CompareAndSwapInt32(&w.status, waitCompletedStatus, waitNotifyStatus) {
		panic("invalid reset wait")
	}
}

func (w *waiter) wait(ctx context.Context) error {
	status := w.getStatus()
	switch status {
	case notifyAddedStatus:
		w.setCompleted()
		return w.mustGetNotifiedValue()
	case waitNotifyStatus:
	default:
		panic(fmt.Sprintf("BUG: invalid status to call wait, %d", status))
	}

	var err error
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-w.c:
		w.setCompleted()
		return err
	}

	if w.beforeSwapStatusAdjustFunc != nil {
		w.beforeSwapStatusAdjustFunc()
	}

	// context is timeout, and status not changed, no concurrent happen
	if atomic.CompareAndSwapInt32(&w.status, status, waitCompletedStatus) {
		return err
	}

	// notify and timeout are concurrently issued, we use real result to replace
	// timeout error
	w.setCompleted()
	return w.mustGetNotifiedValue()
}

// notify return false means this waiter is completed, can not used to notify
func (w *waiter) notify(value error) bool {
	for {
		status := w.getStatus()
		switch status {
		case waitNotifyStatus:
		case notifyAddedStatus:
			panic("already notified")
		case waitCompletedStatus:
			// wait already completed, wait timeout or wait a result.
			return false
		}

		if w.beforeSwapStatusAdjustFunc != nil {
			w.beforeSwapStatusAdjustFunc()
		}

		// if status changed, notify and timeout are concurrently issued, need
		// try.
		if atomic.CompareAndSwapInt32(&w.status, status, notifyAddedStatus) {
			select {
			case w.c <- value:
			default:
				panic("bug")
			}
			return true
		}
	}
}

// close returns the next waiter to hold the lock, and others waiters will move
// into the next waiter.
func (w *waiter) close() *waiter {
	var nextWaiter *waiter
	// no new waiters can added during close.
	if w.waiters.Len() > 0 {
		nextWaiter = w.mustNotifyFirstWaiter()
		if nextWaiter != nil {
			nextWaiter.unref()
		}
	}
	w.unref()
	return nextWaiter
}

func (w *waiter) mustNotifyFirstWaiter() *waiter {
	prevWaiter := w
	for {
		nextWaiter := prevWaiter.waiters.MustGet()
		prevWaiter.changeWaitersToWaitNew(nextWaiter)
		if nextWaiter.notify(nil) {
			return nextWaiter
		}
		if nextWaiter.waiters.Len() == 0 {
			return nil
		}
		prevWaiter = nextWaiter
	}
}

func (w *waiter) changeWaitersToWaitNew(newWaiter *waiter) {
	// make all waiters to waiting newWaiter
	l := w.waiters.Len()
	for i := uint64(0); i < l; i++ {
		if err := newWaiter.add(w.waiters.MustGet()); err != nil {
			panic(err)
		}
	}
}

func (w *waiter) reset() {
	if w.waiters.Len() > 0 {
		panic("invalid waiters")
	}
	if len(w.c) > 0 {
		panic("invalid notify channel")
	}
	w.beforeSwapStatusAdjustFunc = nil
	atomic.StoreInt32(&w.status, 0)
	w.waiters.Reset()
	waiterPool.Put(w)
}
