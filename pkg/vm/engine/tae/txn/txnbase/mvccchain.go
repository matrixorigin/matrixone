// Copyright 2021 Matrix Origin
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

package txnbase

import (
	"bytes"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type MVCCChain[T txnif.MVCCNode[T]] struct {
	*sync.RWMutex
	MVCC      *common.GenericSortedDList[T]
	comparefn func(T, T) int
	newnodefn func() T
	zero      T
}

func NewMVCCChain[T txnif.MVCCNode[T]](comparefn func(T, T) int, newnodefn func() T, rwlocker *sync.RWMutex) *MVCCChain[T] {
	if rwlocker == nil {
		rwlocker = new(sync.RWMutex)
	}
	return &MVCCChain[T]{
		MVCC:      common.NewGenericSortedDList(comparefn),
		RWMutex:   rwlocker,
		comparefn: comparefn,
		newnodefn: newnodefn,
	}
}
func (be *MVCCChain[T]) Depth() int {
	return be.MVCC.Depth()
}
func (be *MVCCChain[T]) StringLocked() string {
	var w bytes.Buffer
	it := common.NewGenericSortedDListIt(nil, be.MVCC, false)
	for it.Valid() {
		version := it.Get().GetPayload()
		_, _ = w.WriteString(" -> \n")
		_, _ = w.WriteString(version.String())
		it.Next()
	}
	return w.String()
}

// for replay
func (be *MVCCChain[T]) GetPrepareTs() types.TS {
	return be.GetLatestNodeLocked().GetPrepare()
}

func (be *MVCCChain[T]) GetTxn() txnif.TxnReader { return be.GetLatestNodeLocked().GetTxn() }

func (be *MVCCChain[T]) Insert(vun T) (node *common.GenericDLNode[T]) {
	un := vun
	node = be.MVCC.Insert(un)
	return
}

// [start, end]
// Check whether there is any committed node in between [start, end]
// -----+------+-------+--------+----------+--------->
//
//	    |      |       |        |          |      Time
//	    |     start    |       end         |
//	commitTs <----- commitTs <--------- commitTs|uncommitted  <=  MVCCChain Header
//	   (1)            (2)                 (3)
func (be *MVCCChain[T]) HasCommittedNodeInRange(start, end types.TS) (ok bool) {
	be.MVCC.Loop(func(n *common.GenericDLNode[T]) bool {
		un := n.GetPayload()
		in, before := un.CommittedIn(start, end)

		// case (2)
		// A committed node is found. Stop the loop.
		// Found
		if in {
			ok = true
			return false
		}

		// case (3)
		// Committed after the end ts or uncommitted.
		// Go to prev node
		// Not found
		if !before {
			return true
		}

		// case (1)
		// Committed before the start ts. Stop the loop.
		// Not found
		return false
	}, false)
	return
}

func (be *MVCCChain[T]) MustOneNodeLocked() (T, bool) {
	var nilT T
	if be.MVCC.Depth() != 1 {
		return nilT, be.MVCC.Depth() == 0
	}
	return be.MVCC.GetHead().GetPayload(), false
}

// GetLatestNodeLocked gets the latest mvcc node.
// It is useful in making command, apply state(e.g. ApplyCommit),
// check confilct.
func (be *MVCCChain[T]) GetLatestNodeLocked() T {
	head := be.MVCC.GetHead()
	if head == nil {
		return be.zero
	}
	payload := head.GetPayload()
	if payload.IsNil() {
		return be.zero
	}
	entry := payload
	return entry
}

// GetLatestCommittedNode gets the latest committed mvcc node.
// It's useful when check whether the catalog/metadata entry is deleted.
func (be *MVCCChain[T]) GetLatestCommittedNodeLocked() (node T) {
	be.MVCC.Loop(func(n *common.GenericDLNode[T]) bool {
		un := n.GetPayload()
		if !un.IsActive() && !un.IsCommitting() {
			node = un
			return false
		}
		return true
	}, false)
	return
}

// GetVisibleNode gets mvcc node according to the txnReader.
// It returns the mvcc node in the same txn as the read txn
// or returns the latest mvcc node with commitTS less than the timestamp.
func (be *MVCCChain[T]) GetVisibleNodeLocked(txn txnif.TxnReader) (node T) {
	be.MVCC.Loop(func(n *common.GenericDLNode[T]) (goNext bool) {
		un := n.GetPayload()
		var visible bool
		if visible = un.IsVisible(txn); visible {
			node = un
		}
		goNext = !visible
		return
	}, false)
	return
}

// It's only used in replay
func (be *MVCCChain[T]) SearchNodeLocked(o T) (node T) {
	be.MVCC.Loop(func(n *common.GenericDLNode[T]) bool {
		un := n.GetPayload()
		compare := be.comparefn(un, o)
		if compare == 0 {
			node = un
			return false
		}
		// return compare > 0
		return true
	}, false)
	return
}

func (be *MVCCChain[T]) LoopChainLocked(fn func(T) bool) {
	be.MVCC.Loop(func(n *common.GenericDLNode[T]) bool {
		un := n.GetPayload()
		return fn(un)
	}, false)
}

func (be *MVCCChain[T]) NeedWaitCommittingLocked(ts types.TS) (bool, txnif.TxnReader) {
	un := be.GetLatestNodeLocked()
	if un.IsNil() {
		return false, nil
	}
	return un.NeedWaitCommitting(ts)
}

func (be *MVCCChain[T]) HasUncommittedNodeLocked() bool {
	var found bool
	be.LoopChainLocked(func(n T) bool {
		if n.IsCommitted() {
			return false
		} else {
			if !n.IsAborted() {
				found = true
			}
		}
		return !found
	})
	return found
}

func (be *MVCCChain[T]) HasCommittedNodeLocked() bool {
	var found bool
	be.LoopChainLocked(func(n T) bool {
		if n.IsCommitted() {
			found = true
			return false
		}
		return true
	})
	return found
}

func (be *MVCCChain[T]) IsCreatingOrAborted() bool {
	un, empty := be.MustOneNodeLocked()
	if empty {
		return true
	}
	if un.IsNil() {
		return false
	}
	return un.IsActive() || un.IsAborted()
}

func (be *MVCCChain[T]) CheckConflictLocked(txn txnif.TxnReader) (err error) {
	if be.IsEmptyLocked() {
		return
	}
	node := be.GetLatestNodeLocked()
	err = node.CheckConflict(txn)
	return
}

func (be *MVCCChain[T]) IsEmptyLocked() bool {
	head := be.MVCC.GetHead()
	return head == nil
}

func (be *MVCCChain[T]) ApplyRollback() error {
	be.Lock()
	defer be.Unlock()
	return be.GetLatestNodeLocked().ApplyRollback()

}

func (be *MVCCChain[T]) ApplyCommit() error {
	be.Lock()
	defer be.Unlock()
	return be.GetLatestNodeLocked().ApplyCommit()
}

func (be *MVCCChain[T]) Apply1PCCommit() error {
	be.Lock()
	defer be.Unlock()
	return be.GetLatestNodeLocked().ApplyCommit()
}

func (be *MVCCChain[T]) PrepareCommit() error {
	be.Lock()
	defer be.Unlock()
	return be.GetLatestNodeLocked().PrepareCommit()
}

func (be *MVCCChain[T]) PrepareRollback() (bool, error) {
	be.Lock()
	defer be.Unlock()
	node := be.MVCC.GetHead()
	_ = node.GetPayload().PrepareRollback()
	be.MVCC.Delete(node)
	isEmpty := be.IsEmptyLocked()
	return isEmpty, nil
}

func (be *MVCCChain[T]) IsCommittedLocked() bool {
	un := be.GetLatestNodeLocked()
	if un.IsNil() {
		return false
	}
	return un.IsCommitted()
}

func (be *MVCCChain[T]) IsCommitted() bool {
	be.RLock()
	defer be.RUnlock()
	un := be.GetLatestNodeLocked()
	if un.IsNil() {
		return false
	}
	return un.IsCommitted()
}

func (be *MVCCChain[T]) ClonePreparedInRange(start, end types.TS) (ret []T) {
	be.RLock()
	defer be.RUnlock()
	return be.ClonePreparedInRangeLocked(start, end)
}

// ClonePreparedInRange will collect all txn node prepared in the time window.
// Wait txn to complete committing if it didn't.
func (be *MVCCChain[T]) ClonePreparedInRangeLocked(start, end types.TS) (ret []T) {
	needWait, txn := be.NeedWaitCommittingLocked(end.Next())
	if needWait {
		be.RUnlock()
		txn.GetTxnState(true)
		be.RLock()
	}
	be.MVCC.Loop(func(n *common.GenericDLNode[T]) bool {
		un := n.GetPayload()
		in, before := un.PreparedIn(start, end)
		if in {
			if ret == nil {
				ret = make([]T, 0)
			}
			ret = append(ret, un.CloneAll())
		} else if !before {
			return false
		}
		return true
	}, true)
	return
}
