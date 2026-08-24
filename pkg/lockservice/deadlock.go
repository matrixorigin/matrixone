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
	"hash/fnv"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"go.uber.org/zap"
)

var (
	maxWaitingCheckCount         = 10240
	deadlockCheckTaskCount       = 4
	fetchWhoWaitingListTaskCount = 4
)

type detector struct {
	logger            *log.MOLogger
	c                 chan deadlockTxn
	waitTxnsFetchFunc func(context.Context, pb.WaitTxn, *waiters) (bool, error)
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
	waitTxnsFetchFunc func(context.Context, pb.WaitTxn, *waiters) (bool, error),
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
	d.mu.Lock()
	clear(d.mu.activeCheckTxn)
	d.mu.Unlock()
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
		v2.TxnDeadlockDetectorEnqueueCounter.WithLabelValues("closed").Inc()
		v2.TxnDeadlockDetectorQueueDepthGauge.Set(float64(len(d.c)))
		return ErrDeadlockDetectorClosed
	}

	if d.mu.preCheckFunc != nil {
		if err := d.mu.preCheckFunc(holdTxnID, txn); err != nil {
			return err
		}
	}

	key := util.UnsafeBytesToString(txn.TxnID)
	if _, ok := d.mu.activeCheckTxn[key]; ok {
		v2.TxnDeadlockDetectorEnqueueCounter.WithLabelValues("dedup_skipped").Inc()
		v2.TxnDeadlockDetectorQueueDepthGauge.Set(float64(len(d.c)))
		return nil
	}

	select {
	case d.c <- deadlockTxn{
		holdTxnID: holdTxnID,
		waitTxn:   txn,
	}:
		d.mu.activeCheckTxn[key] = struct{}{}
		v2.TxnDeadlockDetectorEnqueueCounter.WithLabelValues("queued").Inc()
	default:
		// too many txns waiting for deadlock check, just return error
		v2.TxnDeadlockDetectorEnqueueCounter.WithLabelValues("busy").Inc()
		v2.TxnDeadlockDetectorQueueDepthGauge.Set(float64(len(d.c)))
		d.logger.Warn("deadlock_detector_enqueue_busy",
			zap.Int("queue-depth", len(d.c)),
			zap.Int("queue-capacity", cap(d.c)),
			zap.String("wait-txn", hex.EncodeToString(txn.TxnID)),
			zap.String("hold-txn", hex.EncodeToString(holdTxnID)))
		return ErrDeadlockCheckBusy
	}
	v2.TxnDeadlockDetectorQueueDepthGauge.Set(float64(len(d.c)))
	return nil
}

func (d *detector) doCheck(ctx context.Context) {
	defer d.logger.InfoAction("dead lock checker")()

	w := &waiters{ignoreTxns: &d.ignoreTxns}
	for {
		if ctx.Err() != nil {
			return
		}
		select {
		case <-ctx.Done():
			return
		case txn := <-d.c:
			v2.TxnDeadlockDetectorQueueDepthGauge.Set(float64(len(d.c)))
			w.reset(txn)
			hasDeadlock, deadlockTxn, err := d.checkDeadlock(ctx, w)
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

func (d *detector) checkDeadlock(ctx context.Context, w *waiters) (bool, pb.WaitTxn, error) {
	for {
		if err := ctx.Err(); err != nil {
			return false, pb.WaitTxn{}, err
		}

		current := w.current()
		if !current.fetched {
			txn := current.txn
			added, err := d.waitTxnsFetchFunc(ctx, txn, w)
			if err != nil {
				logCheckDeadLockFailed(d.logger, txn, w.root.startTxn(), err)
				return false, pb.WaitTxn{}, err
			}
			current.fetched = true
			if !added {
				return d.deadlockFound(w)
			}
		}

		cycle, more := w.advance()
		if cycle {
			return d.deadlockFound(w)
		}
		if !more {
			return false, pb.WaitTxn{}, nil
		}
	}
}

func (d *detector) deadlockFound(w *waiters) (bool, pb.WaitTxn, error) {
	node := w.deadlockNode()
	victim := canonicalDeadlockVictim(node)
	logDeadLockFound(d.logger, victim, printPathFromRoot(node))
	return true, victim, nil
}

// canonicalDeadlockVictim selects the same victim for every traversal of a
// cycle. Ranking a txn's full ID avoids always favoring one CN's ID prefix; the
// byte comparison makes the result deterministic even if two hashes collide.
func canonicalDeadlockVictim(node *lockNode) pb.WaitTxn {
	if node == nil {
		return pb.WaitTxn{}
	}

	victim := node.txn
	victimRank := deadlockVictimRank(victim.TxnID)
	for current := node.parent; current != nil; current = current.parent {
		// The closing node has the waiter address needed to route a remote
		// abort. Keep it when the duplicated txn ID closes the cycle.
		if bytes.Equal(current.txn.TxnID, victim.TxnID) {
			continue
		}

		rank := deadlockVictimRank(current.txn.TxnID)
		if rank > victimRank ||
			(rank == victimRank && bytes.Compare(current.txn.TxnID, victim.TxnID) > 0) {
			victim = current.txn
			victimRank = rank
		}
	}
	return victim
}

func deadlockVictimRank(txnID []byte) uint64 {
	h := fnv.New64a()
	_, _ = h.Write(txnID)
	return h.Sum64()
}

type txnVisitState uint8

const (
	txnWhite txnVisitState = iota
	txnGray
	txnBlack
)

type waiters struct {
	ignoreTxns *sync.Map
	root       *lockNode
	stack      []*lockNode
	states     map[string]txnVisitState
	deadlock   *lockNode
}

func (w *waiters) current() *lockNode {
	return w.stack[len(w.stack)-1]
}

func (w *waiters) state(txn pb.WaitTxn) txnVisitState {
	return w.states[util.UnsafeBytesToString(txn.TxnID)]
}

func (w *waiters) setState(txn pb.WaitTxn, state txnVisitState) {
	w.states[util.UnsafeBytesToString(txn.TxnID)] = state
}

func (w *waiters) String() string {
	return fmt.Sprintf("%p", w)
}

func (w *waiters) add(txn pb.WaitTxn, waiterAddress string) bool {
	txn.WaiterAddress = waiterAddress
	state := w.state(txn)
	if state == txnGray {
		w.setDeadlock(w.current().addChild(txn))
		return false
	}
	if state == txnBlack {
		return true
	}

	v := util.UnsafeBytesToString(txn.TxnID)
	if _, ok := w.ignoreTxns.Load(v); ok {
		return true
	}

	w.current().addChild(txn)
	return true
}

func (w *waiters) reset(txn deadlockTxn) {
	w.root = newLockNode(pb.WaitTxn{TxnID: txn.holdTxnID})
	current := w.root.addChild(txn.waitTxn)
	w.root.fetched = true
	w.root.nextChild = len(w.root.children)
	w.stack = w.stack[:0]
	w.stack = append(w.stack, w.root, current)
	if w.states == nil {
		w.states = make(map[string]txnVisitState)
	} else {
		clear(w.states)
	}
	w.setState(w.root.txn, txnGray)
	w.setState(current.txn, txnGray)
	w.deadlock = nil
}

func (w *waiters) deadlockNode() *lockNode {
	return w.deadlock
}

func (w *waiters) setDeadlock(closing *lockNode) {
	for i, node := range w.stack {
		if !bytes.Equal(node.txn.TxnID, closing.txn.TxnID) {
			continue
		}

		cycle := newLockNode(node.txn)
		for _, node := range w.stack[i+1:] {
			cycle = cycle.addChild(node.txn)
		}
		w.deadlock = cycle.addChild(closing.txn)
		return
	}
	w.deadlock = closing
}

func (w *waiters) advance() (bool, bool) {
	for len(w.stack) > 0 {
		current := w.current()
		for current.nextChild < len(current.children) {
			child := current.children[current.nextChild]
			current.nextChild++
			switch w.state(child.txn) {
			case txnGray:
				w.setDeadlock(child)
				return true, false
			case txnBlack:
				continue
			default:
				w.setState(child.txn, txnGray)
				w.stack = append(w.stack, child)
				return false, true
			}
		}
		w.setState(current.txn, txnBlack)
		w.stack = w.stack[:len(w.stack)-1]
	}
	return false, false
}

type lockNode struct {
	txn       pb.WaitTxn
	children  []*lockNode
	parent    *lockNode
	fetched   bool
	nextChild int
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
