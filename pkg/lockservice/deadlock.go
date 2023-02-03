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
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
)

var (
	// ErrDeadlockDetectorClosed deadlock detector is closed
	ErrDeadlockDetectorClosed = moerr.NewInvalidStateNoCtx("deadlock detector is closed")
)

type detector struct {
	c                 chan []byte
	waitTxnsFetchFunc func([]byte, *waiters) bool
	waitTxnAbortFunc  func([]byte)
	ignoreTxns        sync.Map // txnID -> any
	stopper           *stopper.Stopper
	mu                struct {
		sync.RWMutex
		closed bool
	}
}

// newDeadlockDetector create a deadlock detector, waitTxnsFetchFun is used to get the waiting txns
// for the given txn. Then the detector will recursively check all txns's waiting txns until deadlock
// is found. When a deadlock is found, waitTxnAbortFunc is used to notify the external abort to drop a
// txn.
func newDeadlockDetector(waitTxnsFetchFunc func([]byte, *waiters) bool,
	waitTxnAbortFunc func([]byte)) *detector {
	d := &detector{
		c:                 make(chan []byte, 1024),
		waitTxnsFetchFunc: waitTxnsFetchFunc,
		waitTxnAbortFunc:  waitTxnAbortFunc,
		stopper:           stopper.NewStopper("deadlock-detector"),
	}
	err := d.stopper.RunTask(d.doCheck)
	if err != nil {
		panic("impossible")
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
	v := unsafeByteSliceToString(txnID)
	d.ignoreTxns.Delete(v)
}

func (d *detector) check(txnID []byte) error {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.mu.closed {
		return ErrDeadlockDetectorClosed
	}

	d.c <- txnID
	return nil
}

func (d *detector) doCheck(ctx context.Context) {
	w := &waiters{ignoreTxns: &d.ignoreTxns}
	for {
		select {
		case <-ctx.Done():
			return
		case txnID := <-d.c:
			w.reset(txnID)
			v := string(txnID)
			if !d.checkDeadlock(w) {
				d.ignoreTxns.Store(v, struct{}{})
				d.waitTxnAbortFunc(txnID)
			}
		}
	}
}

func (d *detector) checkDeadlock(w *waiters) bool {
	for {
		if w.completed() {
			return true
		}

		// find deadlock
		txnID := w.getCheckTargetTxn()
		if !d.waitTxnsFetchFunc(txnID, w) {
			return false
		}
		w.next()
	}
}

type waiters struct {
	ignoreTxns *sync.Map
	waitTxns   [][]byte
	pos        int
}

func (w *waiters) getCheckTargetTxn() []byte {
	return w.waitTxns[w.pos]
}

func (w *waiters) next() {
	w.pos++
}

func (w *waiters) add(txnID []byte) bool {
	if bytes.Equal(w.waitTxns[0], txnID) {
		return false
	}
	v := unsafeByteSliceToString(txnID)
	if _, ok := w.ignoreTxns.Load(v); ok {
		return true
	}
	w.waitTxns = append(w.waitTxns, txnID)
	return true
}

func (w *waiters) reset(txnID []byte) {
	w.pos = 0
	w.waitTxns = w.waitTxns[:0]
	w.waitTxns = append(w.waitTxns, txnID)
}

func (w *waiters) completed() bool {
	return w.pos == len(w.waitTxns)
}
