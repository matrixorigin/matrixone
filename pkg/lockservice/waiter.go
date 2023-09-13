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

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"go.uber.org/zap"
)

var (
	waiterPool = sync.Pool{
		New: func() any {
			return newWaiter()
		},
	}
)

func acquireWaiter(
	serviceID string,
	txnID []byte) *waiter {
	w := waiterPool.Get().(*waiter)
	logWaiterContactPool(serviceID, w, "get")
	w.txnID = txnID
	if w.ref() != 1 {
		panic("BUG: invalid ref count")
	}
	w.beforeSwapStatusAdjustFunc = func() {}
	return w
}

func newWaiter() *waiter {
	w := &waiter{
		c:       make(chan notifyValue, 1),
		waiters: newWaiterQueue(),
	}
	w.setFinalizer()
	w.setStatus("", ready)
	return w
}

type waiterStatus int32

const (
	ready waiterStatus = iota
	blocking
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
	txnID          []byte
	status         atomic.Int32
	c              chan notifyValue
	waiters        waiterQueue
	sameTxnWaiters []*waiter
	refCount       atomic.Int32
	latestCommitTS timestamp.Timestamp
	waitTxn        pb.WaitTxn
	belongTo       pb.WaitTxn
	event          event

	// just used for testing
	beforeSwapStatusAdjustFunc func()
}

// String implement Stringer
func (w *waiter) String() string {
	if w == nil {
		return "nil"
	}
	return fmt.Sprintf("%s-%p(%d)",
		hex.EncodeToString(w.txnID),
		w,
		w.refCount.Load())
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

func (w *waiter) unref(serviceID string) {
	n := w.refCount.Add(-1)
	if n < 0 {
		panic("BUG: invalid ref count, " + w.String())
	}
	if n == 0 {
		w.reset(serviceID)
	}
}

func (w *waiter) add(
	serviceID string,
	incrRef bool,
	waiters ...*waiter) {
	if len(waiters) == 0 {
		return
	}
	if incrRef {
		for i := range waiters {
			waiters[i].ref()
		}
	}
	w.waiters.put(waiters...)
	logWaitersAdded(serviceID, w, waiters...)
}

func (w *waiter) moveTo(serviceID string, to *waiter) {
	to.waiters.beginChange()
	w.waiters.iter(func(w *waiter) bool {
		to.add(serviceID, false, w)
		return true
	})
}

func (w *waiter) getStatus() waiterStatus {
	return waiterStatus(w.status.Load())
}

func (w *waiter) setStatus(
	serviceID string,
	status waiterStatus) {
	w.status.Store(int32(status))
	logWaiterStatusUpdate(serviceID, w, status)
}

func (w *waiter) casStatus(
	serviceID string,
	old, new waiterStatus) bool {
	if w.status.CompareAndSwap(int32(old), int32(new)) {
		logWaiterStatusChanged(serviceID, w, old, new)
		return true
	}
	return false
}

func (w *waiter) mustRecvNotification(
	ctx context.Context,
	serviceID string) notifyValue {
	select {
	case v := <-w.c:
		logWaiterGetNotify(serviceID, w, v)
		return v
	case <-ctx.Done():
		return notifyValue{err: ctx.Err()}
	}
}

func (w *waiter) notifySameTxn(
	serviceID string,
	value notifyValue) {
	if len(w.sameTxnWaiters) == 0 {
		return
	}
	for _, v := range w.sameTxnWaiters {
		v.notify("", notifyValue{})
	}
	w.sameTxnWaiters = w.sameTxnWaiters[:0]
}

func (w *waiter) mustSendNotification(
	serviceID string,
	value notifyValue) {
	logWaiterNotified(serviceID, w, value)

	// update latest max commit ts in waiter queue
	if w.latestCommitTS.Less(value.ts) {
		w.latestCommitTS = value.ts
	} else {
		value.ts = w.latestCommitTS
	}
	w.event.notified()
	select {
	case w.c <- value:
		return
	default:
	}
	panic("BUG: must send value to channel, " + w.String())
}

func (w *waiter) resetWait(serviceID string) {
	if w.casStatus(serviceID, completed, ready) {
		w.event = event{}
		return
	}
	panic("invalid reset wait")
}

func (w *waiter) wait(
	ctx context.Context,
	serviceID string) notifyValue {
	status := w.getStatus()
	if status != blocking &&
		status != notified {
		panic(fmt.Sprintf("BUG: waiter's status cannot be %d", status))
	}

	w.beforeSwapStatusAdjustFunc()

	apply := func(v notifyValue) {
		logWaiterGetNotify(serviceID, w, v)
		w.setStatus(serviceID, completed)
	}
	select {
	case v := <-w.c:
		apply(v)
		return v
	case <-ctx.Done():
		select {
		case v := <-w.c:
			apply(v)
			return v
		default:
		}
	}

	w.beforeSwapStatusAdjustFunc()

	// context is timeout, and status not changed, no concurrent happen
	if w.casStatus(serviceID, status, completed) {
		return notifyValue{err: ctx.Err()}
	}

	// notify and timeout are concurrently issued, we use real result to replace
	// timeout error
	w.setStatus(serviceID, completed)
	return w.mustRecvNotification(ctx, serviceID)
}

// notify return false means this waiter is completed, cannot be used to notify
func (w *waiter) notify(serviceID string, value notifyValue) bool {
	debug := ""
	if getLogger().Enabled(zap.DebugLevel) {
		debug = w.String()
	}

	for {
		status := w.getStatus()
		// not on wait, no need to notify
		if status != blocking {
			logWaiterNotifySkipped(serviceID, debug, "waiter not in blocking")
			return false
		}

		w.beforeSwapStatusAdjustFunc()
		// if status changed, notify and timeout are concurrently issued, need
		// retry.
		if w.casStatus(serviceID, status, notified) {
			w.mustSendNotification(serviceID, value)
			return true
		}
		logWaiterNotifySkipped(serviceID, debug, "concurrently issued")
	}
}

func (w *waiter) clearAllNotify(
	serviceID string,
	reason string) {
	for {
		select {
		case <-w.c:
		default:
			logWaiterClearNotify(serviceID, w, reason)
			return
		}
	}
}

// close returns the next waiter to hold the lock, and others waiters will move
// into the next waiter.
func (w *waiter) close(
	serviceID string,
	value notifyValue) *waiter {
	if value.ts.Less(w.latestCommitTS) {
		value.ts = w.latestCommitTS
	}
	w.notifySameTxn(serviceID, value)
	nextWaiter := w.fetchNextWaiter(serviceID, value)
	logWaiterClose(serviceID, w, value.err)
	w.unref(serviceID)
	return nextWaiter
}

func (w *waiter) fetchNextWaiter(
	serviceID string,
	value notifyValue) *waiter {
	next := w.awakeNextWaiter(serviceID)
	if next == nil {
		logWaiterFetchNextWaiter(serviceID, w, nil, value)
		return nil
	}

	for {
		if next.notify(serviceID, value) {
			next.unref(serviceID)
			return next
		}
		next = next.awakeNextWaiter(serviceID)
		if next == nil {
			return nil
		}
	}
}

func (w *waiter) awakeNextWaiter(serviceID string) *waiter {
	next := w.waiters.moveToFirst(serviceID)
	w.waiters.reset()
	return next
}

func (w *waiter) reset(serviceID string) {
	waiters := 0
	w.waiters.iter(func(w *waiter) bool {
		waiters++
		return true
	})
	notifies := len(w.c)
	if waiters > 0 || notifies > 0 {
		panic(fmt.Sprintf("BUG: waiter should be empty. %s, waiters %d, notifies %d",
			w.String(),
			waiters,
			notifies))
	}

	logWaiterContactPool(serviceID, w, "put")
	w.txnID = nil
	w.event = event{}
	w.latestCommitTS = timestamp.Timestamp{}
	w.setStatus(serviceID, ready)
	w.waitTxn = pb.WaitTxn{}
	w.belongTo = pb.WaitTxn{}
	w.waiters.reset()
	w.sameTxnWaiters = w.sameTxnWaiters[:0]
	waiterPool.Put(w)
}

type notifyValue struct {
	err        error
	ts         timestamp.Timestamp
	defChanged bool
}

func (v notifyValue) String() string {
	return fmt.Sprintf("ts %s, error %+v", v.ts.DebugString(), v.err)
}
