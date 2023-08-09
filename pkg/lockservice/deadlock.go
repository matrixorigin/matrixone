// Copyright 2022 Matrix Origin
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
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

var (
	maxWaitingCheckCount         = 10240
	deadlockCheckTaskCount       = 4
	fetchWhoWaitingListTaskCount = 4
)

type detector struct {
	serviceID         string
	c                 chan deadlockTxn
	waitTxnsFetchFunc func(pb.WaitTxn, *waiters) (bool, error)
	waitTxnAbortFunc  func(pb.WaitTxn, error)
	ignoreTxns        sync.Map // txnID -> any
	stopper           *stopper.Stopper
	mu                struct {
		sync.Mutex
		closed         bool
		activeCheckTxn map[string]struct{}
	}
}

// newDeadlockDetector create a deadlock detector, waitTxnsFetchFun is used to get the waiting txns
// for the given txn. Then the detector will recursively check all txns's waiting txns until deadlock
// is found. When a deadlock is found, waitTxnAbortFunc is used to notify the external abort to drop a
// txn.
func newDeadlockDetector(
	serviceID string,
	waitTxnsFetchFunc func(pb.WaitTxn, *waiters) (bool, error),
	waitTxnAbortFunc func(pb.WaitTxn, error)) *detector {
	d := &detector{
		serviceID:         serviceID,
		c:                 make(chan deadlockTxn, maxWaitingCheckCount),
		waitTxnsFetchFunc: waitTxnsFetchFunc,
		waitTxnAbortFunc:  waitTxnAbortFunc,
		stopper: stopper.NewStopper("deadlock-detector",
			stopper.WithLogger(getLogger().RawLogger())),
	}
	d.mu.activeCheckTxn = make(map[string]struct{}, maxWaitingCheckCount)
	for i := 0; i < deadlockCheckTaskCount; i++ {
		err := d.stopper.RunTask(d.doCheck)
		if err != nil {
			panic("impossible")
		}
	}
	return d
}

func (d *detector) close() {
	d.mu.Lock()
	d.mu.closed = true
	d.mu.Unlock()
	d.stopper.Stop()
	close(d.c)
}

func (d *detector) txnClosed(txnID []byte) {
	v := util.UnsafeBytesToString(txnID)
	d.ignoreTxns.Delete(v)
}

func (d *detector) check(
	holdTxnID []byte,
	txn pb.WaitTxn) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.mu.closed {
		return ErrDeadlockDetectorClosed
	}

	key := util.UnsafeBytesToString(txn.TxnID)
	if _, ok := d.mu.activeCheckTxn[key]; ok {
		return nil
	}
	d.mu.activeCheckTxn[key] = struct{}{}

	select {
	case d.c <- deadlockTxn{
		holdTxnID: holdTxnID,
		waitTxn:   txn,
	}:
	default:
		// too many txns waiting for deadlock check, just return error
		return ErrDeadlockDetectorClosed
	}
	return nil
}

func (d *detector) doCheck(ctx context.Context) {
	defer getLogger().InfoAction(
		"dead lock checker",
		serviceIDField(d.serviceID))()

	w := &waiters{ignoreTxns: &d.ignoreTxns, serviceID: d.serviceID}
	for {
		select {
		case <-ctx.Done():
			return
		case txn := <-d.c:
			w.reset(txn)
			v := string(txn.waitTxn.TxnID)
			hasDeadlock, err := d.checkDeadlock(w)
			if hasDeadlock {
				if err == nil {
					err = ErrDeadLockDetected
				}
				d.ignoreTxns.Store(v, struct{}{})
				d.waitTxnAbortFunc(txn.waitTxn, err)
			}
			d.mu.Lock()
			delete(d.mu.activeCheckTxn, util.UnsafeBytesToString(txn.waitTxn.TxnID))
			d.mu.Unlock()
		}
	}
}

func (d *detector) checkDeadlock(w *waiters) (bool, error) {
	waitingTxn := w.getCheckTargetTxn()
	for {
		if w.completed() {
			return false, nil
		}

		// find deadlock
		txn := w.getCheckTargetTxn()
		added, err := d.waitTxnsFetchFunc(txn, w)
		if err != nil {
			logCheckDeadLockFailed(d.serviceID, txn, waitingTxn, err)
			return false, err
		}
		if !added {
			logDeadLockFound(d.serviceID, waitingTxn, w)
			return true, nil
		}
		w.next()
	}
}

type waiters struct {
	serviceID  string
	ignoreTxns *sync.Map
	holdTxnID  []byte
	waitTxns   []pb.WaitTxn
	pos        int
}

func (w *waiters) getCheckTargetTxn() pb.WaitTxn {
	return w.waitTxns[w.pos]
}

func (w *waiters) next() {
	w.pos++
}

func (w *waiters) String() string {
	return fmt.Sprintf("%p", w)
}

func (w *waiters) add(txn pb.WaitTxn) bool {
	if bytes.Equal(w.holdTxnID, txn.TxnID) {
		return false
	}
	for i := 0; i < w.pos; i++ {
		if bytes.Equal(w.waitTxns[i].TxnID, txn.TxnID) {
			w.waitTxns = append(w.waitTxns, txn)
			return false
		}
	}
	v := util.UnsafeBytesToString(txn.TxnID)
	if _, ok := w.ignoreTxns.Load(v); ok {
		return true
	}
	w.waitTxns = append(w.waitTxns, txn)
	return true
}

func (w *waiters) reset(txn deadlockTxn) {
	w.pos = 0
	w.holdTxnID = txn.holdTxnID
	w.waitTxns = w.waitTxns[:0]
	w.waitTxns = append(w.waitTxns, txn.waitTxn)
}

func (w *waiters) completed() bool {
	return w.pos == len(w.waitTxns)
}

type deadlockTxn struct {
	holdTxnID []byte
	waitTxn   pb.WaitTxn
}
