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
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/log"
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
	logger            *log.MOLogger
	c                 chan deadlockTxn
	waitTxnsFetchFunc func(pb.WaitTxn, *waiters) (bool, error)
	waitTxnAbortFunc  func(pb.WaitTxn, error)
	ignoreTxns        sync.Map // txnID -> any
	stopper           *stopper.Stopper
	mu                struct {
		sync.Mutex
		closed         bool
		activeCheckTxn map[string]struct{}
		preCheckFunc   func(holdTxnID []byte, txn pb.WaitTxn) error
	}
}

// newDeadlockDetector create a deadlock detector, waitTxnsFetchFun is used to get the waiting txns
// for the given txn. Then the detector will recursively check all txns's waiting txns until deadlock
// is found. When a deadlock is found, waitTxnAbortFunc is used to notify the external abort to drop a
// txn.
func newDeadlockDetector(
	logger *log.MOLogger,
	waitTxnsFetchFunc func(pb.WaitTxn, *waiters) (bool, error),
	waitTxnAbortFunc func(pb.WaitTxn, error),
) *detector {
	d := &detector{
		logger:            logger,
		c:                 make(chan deadlockTxn, maxWaitingCheckCount),
		waitTxnsFetchFunc: waitTxnsFetchFunc,
		waitTxnAbortFunc:  waitTxnAbortFunc,
		stopper: stopper.NewStopper("deadlock-detector",
			stopper.WithLogger(logger.RawLogger())),
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

	if d.mu.preCheckFunc != nil {
		if err := d.mu.preCheckFunc(holdTxnID, txn); err != nil {
			return err
		}
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
		return ErrDeadlockCheckBusy
	}
	return nil
}

func (d *detector) doCheck(ctx context.Context) {
	defer d.logger.InfoAction("dead lock checker")()

	w := &waiters{ignoreTxns: &d.ignoreTxns}
	for {
		select {
		case <-ctx.Done():
			return
		case txn := <-d.c:
			w.reset(txn)
			hasDeadlock, deadlockTxn, err := d.checkDeadlock(w)
			if hasDeadlock {
				if err == nil {
					err = ErrDeadLockDetected
				}
				d.ignoreTxns.Store(string(deadlockTxn.TxnID), struct{}{})
				d.waitTxnAbortFunc(deadlockTxn, err)
			}
			d.mu.Lock()
			delete(d.mu.activeCheckTxn, util.UnsafeBytesToString(txn.waitTxn.TxnID))
			d.mu.Unlock()
		}
	}
}

func (d *detector) checkDeadlock(w *waiters) (bool, pb.WaitTxn, error) {
	for {
		// find deadlock
		txn := w.getCheckTargetTxn()
		added, err := d.waitTxnsFetchFunc(txn, w)
		if err != nil {
			logCheckDeadLockFailed(d.logger, txn, w.root.startTxn(), err)
			return false, pb.WaitTxn{}, err
		}
		if !added {
			logDeadLockFound(d.logger, w.deadlockNode().txn, printPathFromRoot(w.deadlockNode()))
			return true, w.deadlockNode().txn, nil
		}
		if !w.next() {
			return false, pb.WaitTxn{}, nil
		}
	}
}

type waiters struct {
	ignoreTxns *sync.Map
	root       *lockNode
	waitTxns   []*lockNode
	pos        int
}

func (w *waiters) getCheckTargetTxn() pb.WaitTxn {
	return w.waitTxns[w.pos].txn
}

// there are no next txn, if return false
func (w *waiters) next() bool {
	w.pos++
	return !(w.pos == len(w.waitTxns))
}

func (w *waiters) String() string {
	return fmt.Sprintf("%p", w)
}

func (w *waiters) add(txn pb.WaitTxn, waiterAddress string) bool {
	txn.WaiterAddress = waiterAddress
	if w.hasTxn(txn) {
		node := w.waitTxns[w.pos].addChild(txn)
		w.waitTxns = append(w.waitTxns, node)
		return false
	}

	v := util.UnsafeBytesToString(txn.TxnID)
	if _, ok := w.ignoreTxns.Load(v); ok {
		return true
	}

	node := w.waitTxns[w.pos].addChild(txn)
	w.waitTxns = append(w.waitTxns, node)
	return true
}

func (w *waiters) reset(txn deadlockTxn) {
	w.root = newLockNode(pb.WaitTxn{TxnID: txn.holdTxnID})
	current := w.root.addChild(txn.waitTxn)
	w.waitTxns = w.waitTxns[:0]
	w.waitTxns = append(w.waitTxns, w.root)
	w.waitTxns = append(w.waitTxns, current)
	w.pos = 1
}

func (w *waiters) deadlockNode() *lockNode {
	if len(w.waitTxns) > 0 {
		return w.waitTxns[len(w.waitTxns)-1]
	}
	return nil
}

func (w *waiters) hasTxn(txn pb.WaitTxn) bool {
	for i := 0; i < w.pos; i++ {
		if bytes.Equal(w.waitTxns[i].txn.TxnID, txn.TxnID) {
			return true
		}
	}
	return false
}

type lockNode struct {
	txn      pb.WaitTxn
	children []*lockNode
	parent   *lockNode
}

func newLockNode(txn pb.WaitTxn) *lockNode {
	return &lockNode{
		txn:      txn,
		children: make([]*lockNode, 0),
	}
}

func (n *lockNode) addChild(txn pb.WaitTxn) *lockNode {
	child := newLockNode(txn)
	child.parent = n
	n.children = append(n.children, child)
	return child
}

func (n *lockNode) startTxn() pb.WaitTxn {
	if len(n.children) > 0 {
		return n.children[0].txn
	}
	return pb.WaitTxn{}
}

// printPathFromRoot prints the path from root to the given node
func printPathFromRoot(node *lockNode) string {
	if node == nil {
		return "<nil>"
	}

	// Build path from current node to root
	path := make([]*lockNode, 0)
	current := node
	for current != nil {
		path = append(path, current)
		current = current.parent
	}

	// Build string representation
	var buf bytes.Buffer
	for i := len(path) - 1; i >= 0; i-- {
		if i < len(path)-1 {
			buf.WriteString(" <= ")
		}
		buf.WriteString(hex.EncodeToString(path[i].txn.TxnID))
	}
	return buf.String()
}

type deadlockTxn struct {
	holdTxnID []byte
	waitTxn   pb.WaitTxn
}
