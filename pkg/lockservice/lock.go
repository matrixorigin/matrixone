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
	"fmt"
	"strings"
	"sync"
	"time"

	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
)

var (
	waitQueuePool = sync.Pool{New: func() any {
		return newWaiterQueue()
	}}
	holdersPool = sync.Pool{New: func() any {
		return newHolders()
	}}
)

const (
	flagLockRow byte = 1 << iota
	flagLockRangeStart
	flagLockRangeEnd
	flagLockExclusiveMode
	flagLockSharedMode
	flagLockTableDefChanged
)

func newRangeLock(c *lockContext) (Lock, Lock) {
	l := newLock(c)
	return l.toRangeStartLock(), l.toRangeEndLock()
}

func newRowLock(c *lockContext) Lock {
	l := newLock(c)
	return l.toRowLock()
}

func newLock(c *lockContext) Lock {
	l := Lock{
		createAt: time.Now(),
		holders:  holdersPool.Get().(*holders),
		waiters:  waitQueuePool.Get().(waiterQueue),
	}
	l.holders.add(c.waitTxn)
	if c.opts.Mode == pb.LockMode_Exclusive {
		l.value |= flagLockExclusiveMode
	} else {
		l.value |= flagLockSharedMode
	}
	if c.opts.TableDefChanged {
		l.value |= flagLockTableDefChanged
	}
	logHolderAdded(c, l)
	return l
}

func (l Lock) addWaiter(w *waiter) {
	l.waiters.put(w)
	logWaitersAdded(l.holders, w)
}

func (l Lock) addHolder(c *lockContext) {
	l.holders.add(c.waitTxn)
	l.waiters.removeByTxnID(c.waitTxn.TxnID)
	logHolderAdded(c, l)
}

func (l Lock) isEmpty() bool {
	return l.holders.size() == 0 &&
		(l.waiters == nil || l.waiters.size() == 0)
}

func (l Lock) tryHold(c *lockContext) (bool, bool) {
	if l.isEmpty() {
		panic("BUG: try hold on empty lock")
	}

	// txn already hold the lock
	if l.holders.contains(c.txn.txnID) {
		return true, false
	}

	if l.canHold(c) {
		l.addHolder(c)
		return true, true
	}

	return false, false
}

// (no holders && is first waiter txn) || (both shared lock) can hold lock
func (l Lock) canHold(c *lockContext) bool {
	if c.closed {
		panic("BUG: closed context should not call canHold")
	}

	return (l.holders.size() == 0 && l.waiters.first().isTxn(c.txn.txnID)) ||
		l.isLockModeAllowed(c)
}

func (l Lock) isLockModeAllowed(c *lockContext) bool {
	if l.isShared() {
		return c.opts.Mode == pb.LockMode_Shared
	}
	return false
}

func (l Lock) close(notify notifyValue) {
	l.waiters.close(notify)
	l.release()
}

func (l Lock) release() {
	l.holders.clear()
	l.waiters.reset()
	waitQueuePool.Put(l.waiters)
	holdersPool.Put(l.holders)
}

func (l Lock) closeWaiter(w *waiter) bool {
	canRemove := func() bool {
		if !l.isLockRow() {
			panic("BUG: range lock cannot call closeWaiter")
		}

		if l.holders.size() > 0 {
			return false
		}

		if l.waiters.size() == 0 {
			return true
		}

		if l.waiters.first() != w {
			return false
		}

		if l.waiters.size() == 1 {
			return true
		}

		l.waiters.notify(notifyValue{defChanged: l.isLockTableDefChanged()})
		return l.isEmpty()
	}()

	if canRemove {
		// close all ref in waiter queue
		l.waiters.iter(func(w *waiter) bool {
			w.close()
			return true
		})
	}
	return canRemove
}

func (l Lock) closeTxn(
	txn *activeTxn,
	notify notifyValue) (lockCanRemoved bool) {
	l.holders.remove(txn.txnID)

	// has another holders
	if l.holders.size() > 0 {
		return false
	}

	notify.defChanged = l.isLockTableDefChanged()

	if l.isLockRow() {
		// notify first waiter, skip completed waiters
		l.waiters.notify(notify)
		return l.isEmpty()
	}

	// No holders, and is range lock, the all waiters need to be notified.
	// Because these waiters may be attempting to acquire a non-conflicting
	// row locks or range locks between the current range.
	l.waiters.notifyAll(notify)
	return true
}

func (l Lock) toRowLock() Lock {
	l.value |= flagLockRow
	return l
}

func (l Lock) toRangeStartLock() Lock {
	l.value |= flagLockRangeStart
	return l
}

func (l Lock) toRangeEndLock() Lock {
	l.value |= flagLockRangeEnd
	return l
}

func (l Lock) isLockRow() bool {
	return l.value&flagLockRow != 0
}

func (l Lock) isLockRangeEnd() bool {
	return l.value&flagLockRangeEnd != 0
}

func (l Lock) isLockRangeStart() bool {
	return l.value&flagLockRangeStart != 0
}

func (l Lock) isLockTableDefChanged() bool {
	return l.value&flagLockTableDefChanged != 0
}

func (l Lock) isShared() bool {
	return l.value&flagLockSharedMode != 0
}

// IsRangeLock return true if is range lock
func (l Lock) IsRangeLock() bool {
	return !l.isLockRow()
}

// GetLockMode returns lock mode
func (l Lock) GetLockMode() pb.LockMode {
	if l.value&flagLockExclusiveMode != 0 {
		return pb.LockMode_Exclusive
	}
	return pb.LockMode_Shared
}

// IterHolders iter lock holders, if holders is empty means the last holder is closed
// and the lock is waiting first waiter to get this lock.
func (l Lock) IterHolders(fn func(holder pb.WaitTxn) bool) {
	for _, txn := range l.holders.txns {
		if !fn(txn) {
			return
		}
	}
}

// IterHolders iter which txn is waiting for this lock
func (l Lock) IterWaiters(fn func(waiter pb.WaitTxn) bool) {
	l.waiters.iter(func(w *waiter) bool {
		return !fn(w.txn)
	})
}

// String implement Stringer
func (l Lock) String() string {
	g := "row"
	if !l.isLockRow() {
		g = "range-start"
		if l.isLockRangeEnd() {
			g = "range-end"
		}
	}
	// hold txn: mode-[row|range](waiters)
	var waiters bytes.Buffer
	l.waiters.iter(func(w *waiter) bool {
		waiters.WriteString(fmt.Sprintf("%s ", w))
		return true
	})
	return fmt.Sprintf("%s(%s): holder(%s), waiters(%s)",
		l.GetLockMode().String(),
		g,
		l.holders.String(),
		strings.TrimSpace(waiters.String()))
}

func newHolders() *holders {
	return &holders{}
}

func (h *holders) add(txn pb.WaitTxn) {
	h.txns = append(h.txns, txn)
}

func (h *holders) String() string {
	var buf bytes.Buffer
	for _, txn := range h.txns {
		buf.WriteString(fmt.Sprintf("%x ", txn.TxnID))
	}
	return strings.TrimSpace(buf.String())
}

func (h *holders) size() int {
	if h == nil {
		return 0
	}
	return len(h.txns)
}

func (h *holders) contains(txnID []byte) bool {
	for _, t := range h.txns {
		if bytes.Equal(t.TxnID, txnID) {
			return true
		}
	}
	return false
}

func (h *holders) remove(txnID []byte) {
	newTxns := h.txns[:0]
	for _, t := range h.txns {
		if bytes.Equal(t.TxnID, txnID) {
			continue
		}
		newTxns = append(newTxns, t)
	}
	h.txns = newTxns
}

func (h *holders) replace(
	from []byte,
	to pb.WaitTxn) {
	find := false
	for i := range h.txns {
		if bytes.Equal(h.txns[i].TxnID, from) {
			h.txns[i] = to
			find = true
			continue
		}
	}
	if !find {
		panic("BUG: missing holder")
	}
}

func (h *holders) clear() {
	h.txns = h.txns[:0]
}
