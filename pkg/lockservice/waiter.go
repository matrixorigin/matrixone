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
	"context"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/reuse"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"go.uber.org/zap"
)

type waiterStatus int32

const (
	ready waiterStatus = iota
	blocking
	notified
	completed
)

func acquireWaiter(txn pb.WaitTxn) *waiter {
	w := reuse.Alloc[waiter](nil)
	w.txn = txn
	if w.ref() != 1 {
		panic("BUG: invalid ref count")
	}
	w.beforeSwapStatusAdjustFunc = func() {}
	return w
}

func newWaiter() *waiter {
	w := &waiter{
		status:   &atomic.Int32{},
		refCount: &atomic.Int32{},
		c:        make(chan notifyValue, 1),
	}
	w.setStatus(ready)
	return w
}

func (w waiter) TypeName() string {
	return "lockservice.wait"
}

// waiter is used to allow locking operations to wait for the previous
// lock to be released if a conflict is encountered.
type waiter struct {
	// belong to which txn
	txn         pb.WaitTxn
	waitFor     [][]byte
	conflictKey atomic.Pointer[[]byte]
	lt          atomic.Pointer[localLockTable]
	status      *atomic.Int32
	refCount    *atomic.Int32
	c           chan notifyValue
	event       event
	waitAt      atomic.Value

	// just used for testing
	beforeSwapStatusAdjustFunc func()
}

// String implement Stringer
func (w *waiter) String() string {
	if w == nil {
		return "nil"
	}
	return fmt.Sprintf("%s-%p(%d)",
		hex.EncodeToString(w.txn.TxnID),
		w,
		w.refCount.Load())
}

func (w *waiter) isTxn(txnID []byte) bool {
	if w == nil {
		return false
	}
	return bytes.Equal(w.txn.TxnID, txnID)
}

func (w *waiter) ref() int32 {
	return w.refCount.Add(1)
}

func (w *waiter) close() {
	n := w.refCount.Add(-1)
	if n < 0 {
		panic("BUG: invalid ref count, " + w.String())
	}
	if n == 0 {
		reuse.Free(w, nil)
	}
}

func (w *waiter) getStatus() waiterStatus {
	return waiterStatus(w.status.Load())
}

func (w *waiter) setStatus(
	status waiterStatus,
) {
	w.status.Store(int32(status))
}

func (w *waiter) casStatus(
	old, new waiterStatus,
	logger *log.MOLogger,
) bool {
	if w.status.CompareAndSwap(int32(old), int32(new)) {
		logWaiterStatusChanged(logger, w, old, new)
		return true
	}
	return false
}

func (w *waiter) mustRecvNotification(
	ctx context.Context,
	logger *log.MOLogger,
) notifyValue {
	select {
	case v := <-w.c:
		logWaiterGetNotify(logger, w, v)
		return v
	case <-ctx.Done():
		return notifyValue{err: ctx.Err()}
	}
}

func (w *waiter) mustSendNotification(
	value notifyValue,
	logger *log.MOLogger,
) {
	logWaiterNotified(logger, w, value)

	w.event.notified()
	select {
	case w.c <- value:
		return
	default:
	}
	panic("BUG: must send value to channel, " + w.String())
}

func (w *waiter) resetWait(
	logger *log.MOLogger,
) {
	if w.casStatus(completed, ready, logger) {
		w.event = event{}
		return
	}
	panic("invalid reset wait")
}

func (w *waiter) wait(
	ctx context.Context,
	logger *log.MOLogger,
) notifyValue {
	status := w.getStatus()
	if status != blocking &&
		status != notified {
		panic(fmt.Sprintf("BUG: waiter's status cannot be %d", status))
	}

	w.beforeSwapStatusAdjustFunc()

	apply := func(v notifyValue) {
		logWaiterGetNotify(logger, w, v)
		w.setStatus(completed)
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
	if w.casStatus(status, completed, logger) {
		return notifyValue{err: ctx.Err()}
	}
	// notify and timeout are concurrently issued, we use real result to replace
	// timeout error
	w.setStatus(completed)
	return w.mustRecvNotification(ctx, logger)
}

func (w *waiter) disableNotify() {
	w.setStatus(completed)
	select {
	case <-w.c:
	default:
	}
}

// notify return false means this waiter is completed, cannot be used to notify
func (w *waiter) notify(
	value notifyValue,
	logger *log.MOLogger,
) bool {
	debug := ""
	if logger != nil && logger.Enabled(zap.DebugLevel) {
		debug = w.String()
	}

	for {
		status := w.getStatus()
		// not on wait, no need to notify
		if status != blocking {
			logWaiterNotifySkipped(logger, debug, "waiter not in blocking")
			return false
		}

		w.beforeSwapStatusAdjustFunc()
		// if status changed, notify and timeout are concurrently issued, need
		// retry.
		if w.casStatus(status, notified, logger) {
			w.mustSendNotification(value, logger)
			return true
		}
		logWaiterNotifySkipped(logger, debug, "concurrently issued")
	}
}

func (w *waiter) startWait() {
	w.waitAt.Store(time.Now())
}

func (w *waiter) reset() {
	notifies := len(w.c)
	if notifies > 0 {
		panic(fmt.Sprintf("BUG: waiter should be empty. %s, notifies %d",
			w.String(),
			notifies))
	}

	w.txn = pb.WaitTxn{}
	w.event = event{}
	w.waitFor = w.waitFor[:0]
	w.conflictKey.Store(nil)
	w.lt.Store(nil)
	w.setStatus(ready)
}

type notifyValue struct {
	err        error
	ts         timestamp.Timestamp
	defChanged bool
}

func (v notifyValue) String() string {
	return fmt.Sprintf("ts %s, error %+v", v.ts.DebugString(), v.err)
}
