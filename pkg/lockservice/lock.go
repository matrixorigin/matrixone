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
		holders: holdersPool.Get().(*holders),
		waiters: waitQueuePool.Get().(waiterQueue),
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
	return l
}

func (l Lock) addWaiter(w *waiter) {
	l.waiters.put(w)
	logWaitersAdded(l.holders, w)
}

func (l Lock) addHolder(c *lockContext) {
	l.holders.add(c.waitTxn)
	l.waiters.removeByTxnID(c.waitTxn.TxnID)
}

func (l Lock) isEmpty() bool {
	return l.holders.size() == 0 &&
		(l.waiters == nil || l.waiters.size() == 0)
}

func (l Lock) tryHold(c *lockContext) bool {
	if l.isEmpty() {
		panic("BUG: try hold on empty lock")
	}

	// txn already hold the lock
	if l.holders.contains(c.txn.txnID) {
		return true
	}

	if l.canHold(c) {
		l.addHolder(c)
		return true
	}

	return false
}

// (no holders && is first waiter txn) || (both shared lock) can hold lock
func (l Lock) canHold(c *lockContext) bool {
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

func (l Lock) closeTxn(
	txn *activeTxn,
	notify notifyValue) {
	notify.defChanged = l.isLockTableDefChanged()
	l.holders.remove(txn.txnID)
	// notify first waiter, skip completed waiters
	l.waiters.notify(notify)
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

func (l Lock) getLockMode() pb.LockMode {
	if l.value&flagLockExclusiveMode != 0 {
		return pb.LockMode_Exclusive
	}
	return pb.LockMode_Shared
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
		l.getLockMode().String(),
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

func (h *holders) clear() {
	h.txns = h.txns[:0]
}
