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
	"runtime"
	"sync"

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
	txnID   []byte
	c       chan error
	waiters *ring.RingBuffer[*waiter]
}

func (w *waiter) setFinalizer() {
	// close the channal if gc
	runtime.SetFinalizer(w, func(w *waiter) {
		close(w.c)
	})
}

func (w *waiter) add(waiter *waiter) error {
	return w.waiters.Put(waiter)
}

func (w *waiter) wait(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-w.c:
			return err
		}
	}
}

func (w *waiter) notify() {
	select {
	case w.c <- nil:
	default:
	}
}

// close returns the next waiter to hold the lock, and others waiters will move
// into the next waiter.
func (w *waiter) close() *waiter {
	var nextWaiter *waiter
	// no new waiters can added during close.
	if w.waiters.Len() > 0 {
		nextWaiter = w.mustNotifyFirstWaiter()
	}
	w.reset()
	return nextWaiter
}

func (w *waiter) mustNotifyFirstWaiter() *waiter {
	waiter := w.waiters.MustGet()
	w.changeWaitersToWaitNew(waiter)
	waiter.notify()
	return waiter
}

func (w *waiter) changeWaitersToWaitNew(newWaiter *waiter) {
	// make all waiters to waiting newWaiter
	for i := uint64(0); i < w.waiters.Len(); i++ {
		newWaiter.add(w.waiters.MustGet())
	}
}

func (w *waiter) reset() {
	if w.waiters.Len() > 0 {
		panic("invalid waiters")
	}
	if len(w.c) > 0 {
		panic("invalid notify channal")
	}
	w.waiters.Reset()
	waiterPool.Put(w)
}
