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
	"encoding/binary"
	"io"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type MVCCChain struct {
	*sync.RWMutex
	MVCC      *common.GenericSortedDList[txnif.MVCCNode]
	comparefn func(txnif.MVCCNode, txnif.MVCCNode) int
	newnodefn func() txnif.MVCCNode
}

func NewMVCCChain(comparefn func(txnif.MVCCNode, txnif.MVCCNode) int, newnodefn func() txnif.MVCCNode) *MVCCChain {
	return &MVCCChain{
		MVCC:      common.NewGenericSortedDList(comparefn),
		RWMutex:   &sync.RWMutex{},
		comparefn: comparefn,
		newnodefn: newnodefn,
	}
}
func (be *MVCCChain) Depth() int {
	return be.MVCC.Depth()
}
func (be *MVCCChain) StringLocked() string {
	var w bytes.Buffer
	it := common.NewGenericSortedDListIt(nil, be.MVCC, false)
	for it.Valid() {
		version := it.Get().GetPayload()
		_, _ = w.WriteString(" -> ")
		_, _ = w.WriteString(version.String())
		it.Next()
	}
	return w.String()
}

// for replay
func (be *MVCCChain) GetPrepareTs() types.TS {
	return be.GetLatestNodeLocked().GetPrepare()
}

func (be *MVCCChain) GetTxn() txnif.TxnReader { return be.GetLatestNodeLocked().GetTxn() }

func (be *MVCCChain) GetIndexes() []*wal.Index {
	ret := make([]*wal.Index, 0)
	be.MVCC.Loop(func(n *common.GenericDLNode[txnif.MVCCNode]) bool {
		un := n.GetPayload()
		ret = append(ret, un.GetLogIndex())
		return true
	}, true)
	return ret
}

func (be *MVCCChain) Insert(vun txnif.MVCCNode) (node *common.GenericDLNode[txnif.MVCCNode]) {
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
func (be *MVCCChain) HasCommittedNodeInRange(start, end types.TS) (ok bool) {
	be.MVCC.Loop(func(n *common.GenericDLNode[txnif.MVCCNode]) bool {
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

func (be *MVCCChain) MustOneNodeLocked() (txnif.MVCCNode, bool) {
	if be.MVCC.Depth() != 1 {
		return nil, be.MVCC.Depth() == 0
	}
	return be.MVCC.GetHead().GetPayload(), false
}

// GetLatestNodeLocked gets the latest mvcc node.
// It is useful in making command, apply state(e.g. ApplyCommit),
// check confilct.
func (be *MVCCChain) GetLatestNodeLocked() txnif.MVCCNode {
	head := be.MVCC.GetHead()
	if head == nil {
		return nil
	}
	payload := head.GetPayload()
	if payload == nil {
		return nil
	}
	entry := payload
	return entry
}

// GetLatestCommittedNode gets the latest committed mvcc node.
// It's useful when check whether the catalog/metadata entry is deleted.
func (be *MVCCChain) GetLatestCommittedNode() (node txnif.MVCCNode) {
	be.MVCC.Loop(func(n *common.GenericDLNode[txnif.MVCCNode]) bool {
		un := n.GetPayload()
		if !un.IsActive() && !un.IsCommitting() {
			node = un
			return false
		}
		return true
	}, false)
	return
}

// GetVisibleNode gets mvcc node according to the timestamp.
// It returns the mvcc node in the same txn as the read txn
// or returns the latest mvcc node with commitTS less than the timestamp.
func (be *MVCCChain) GetVisibleNode(ts types.TS) (node txnif.MVCCNode) {
	be.MVCC.Loop(func(n *common.GenericDLNode[txnif.MVCCNode]) (goNext bool) {
		un := n.GetPayload()
		var visible bool
		if visible = un.IsVisible(ts); visible {
			node = un
		}
		goNext = !visible
		return
	}, false)
	return
}

// It's only used in replay
func (be *MVCCChain) SearchNode(o txnif.MVCCNode) (node txnif.MVCCNode) {
	be.MVCC.Loop(func(n *common.GenericDLNode[txnif.MVCCNode]) bool {
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

func (be *MVCCChain) LoopChain(fn func(txnif.MVCCNode) bool) {
	be.MVCC.Loop(func(n *common.GenericDLNode[txnif.MVCCNode]) bool {
		un := n.GetPayload()
		return fn(un)
	}, false)
}

func (be *MVCCChain) NeedWaitCommitting(ts types.TS) (bool, txnif.TxnReader) {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return false, nil
	}
	return un.NeedWaitCommitting(ts)
}

func (be *MVCCChain) HasUncommittedNode() bool {
	var found bool
	be.LoopChain(func(n txnif.MVCCNode) bool {
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

func (be *MVCCChain) HasCommittedNode() bool {
	var found bool
	be.LoopChain(func(n txnif.MVCCNode) bool {
		if n.IsCommitted() {
			found = true
			return false
		}
		return true
	})
	return found
}

func (be *MVCCChain) IsCreatingOrAborted() bool {
	un, empty := be.MustOneNodeLocked()
	if empty {
		return true
	}
	if un == nil {
		return false
	}
	return un.IsActive() || un.IsAborted()
}

func (be *MVCCChain) CheckConflict(txn txnif.TxnReader) (err error) {
	node := be.GetLatestNodeLocked()
	err = node.CheckConflict(txn.GetStartTS())
	return
}

func (be *MVCCChain) WriteOneNodeTo(w io.Writer) (n int64, err error) {
	var n2 int64
	n2, err = be.GetLatestNodeLocked().WriteTo(w)
	if err != nil {
		return
	}
	n += n2
	return
}

func (be *MVCCChain) WriteAllTo(w io.Writer) (n int64, err error) {
	n += 8
	if err = binary.Write(w, binary.BigEndian, uint64(be.MVCC.Depth())); err != nil {
		return
	}
	n += 8
	be.MVCC.Loop(func(node *common.GenericDLNode[txnif.MVCCNode]) bool {
		var n2 int64
		n2, err = node.GetPayload().WriteTo(w)
		if err != nil {
			return false
		}
		n += n2
		return true
	}, true)
	return
}

func (be *MVCCChain) ReadOneNodeFrom(r io.Reader) (n int64, err error) {
	var n2 int64
	un := be.newnodefn()
	n2, err = un.ReadFrom(r)
	if err != nil {
		return
	}
	be.Insert(un)
	n += n2
	return
}

func (be *MVCCChain) ReadAllFrom(r io.Reader) (n int64, err error) {
	var depth uint64
	if err = binary.Read(r, binary.BigEndian, &depth); err != nil {
		return
	}
	n += 8
	for i := 0; i < int(depth); i++ {
		var n2 int64
		un := be.newnodefn()
		n2, err = un.ReadFrom(r)
		if err != nil {
			return
		}
		be.MVCC.Insert(un)
		n += n2
	}
	return
}

func (be *MVCCChain) IsEmpty() bool {
	head := be.MVCC.GetHead()
	return head == nil
}

func (be *MVCCChain) ApplyRollback(index *wal.Index) error {
	be.Lock()
	defer be.Unlock()
	return be.GetLatestNodeLocked().ApplyRollback(index)

}

func (be *MVCCChain) ApplyCommit(index *wal.Index) error {
	be.Lock()
	defer be.Unlock()
	return be.GetLatestNodeLocked().ApplyCommit(index)
}

func (be *MVCCChain) Apply1PCCommit(index *wal.Index) error {
	be.Lock()
	defer be.Unlock()
	return be.GetLatestNodeLocked().ApplyCommit(index)
}

func (be *MVCCChain) GetLogIndex() *wal.Index {
	node := be.GetLatestNodeLocked()
	if node == nil {
		return nil
	}
	return node.GetLogIndex()
}

func (be *MVCCChain) CloneLatestNode() (*MVCCChain, txnif.MVCCNode) {
	cloned := &MVCCChain{
		MVCC:    common.NewGenericSortedDList(be.comparefn),
		RWMutex: &sync.RWMutex{},
	}
	un := be.GetLatestNodeLocked()
	uncloned := un.CloneData()
	cloned.Insert(uncloned)
	return cloned, uncloned
}

// In /Catalog, there're three states: Active, Committing and Committed.
// A txn is Active before its CommitTs is allocated.
// It's Committed when its state will never change, i.e. TxnStateCommitted and  TxnStateRollbacked.
// It's Committing when it's in any other state, including TxnStateCommitting, TxnStateRollbacking, TxnStatePrepared and so on. When read or write an entry, if the last txn of the entry is Committing, we wait for it. When write on an Entry, if there's an Active txn, we report w-w conflict.
func (be *MVCCChain) IsCommitting() bool {
	node := be.GetLatestNodeLocked()
	if node == nil {
		return false
	}
	return node.IsCommitting()
}

func (be *MVCCChain) PrepareCommit() error {
	be.Lock()
	defer be.Unlock()
	return be.GetLatestNodeLocked().PrepareCommit()
}

func (be *MVCCChain) PrepareRollback() (bool, error) {
	be.Lock()
	defer be.Unlock()
	node := be.MVCC.GetHead()
	_ = node.GetPayload().PrepareRollback()
	be.MVCC.Delete(node)
	isEmpty := be.IsEmpty()
	return isEmpty, nil
}

func (be *MVCCChain) IsCommitted() bool {
	un := be.GetLatestNodeLocked()
	if un == nil {
		return false
	}
	return un.IsCommitted()
}

func (be *MVCCChain) CloneCommittedInRange(start, end types.TS) (ret *MVCCChain) {
	needWait, txn := be.NeedWaitCommitting(end.Next())
	if needWait {
		be.RUnlock()
		txn.GetTxnState(true)
		be.RLock()
	}
	be.MVCC.Loop(func(n *common.GenericDLNode[txnif.MVCCNode]) bool {
		un := n.GetPayload()
		in, before := un.CommittedIn(start, end)
		if in {
			if ret == nil {
				ret = NewMVCCChain(be.comparefn, be.newnodefn)
			}
			ret.Insert(un.CloneAll())
		} else if !before {
			return false
		}
		return true
	}, true)
	return
}

// ClonePreparedInRange will collect all txn node prepared in the time window.
// Wait txn to complete committing if it didn't.
func (be *MVCCChain) ClonePreparedInRange(start, end types.TS) (ret []txnif.MVCCNode) {
	needWait, txn := be.NeedWaitCommitting(end.Next())
	if needWait {
		be.RUnlock()
		txn.GetTxnState(true)
		be.RLock()
	}
	be.MVCC.Loop(func(n *common.GenericDLNode[txnif.MVCCNode]) bool {
		un := n.GetPayload()
		in, before := un.PreparedIn(start, end)
		if in {
			if ret == nil {
				ret = make([]txnif.MVCCNode, 0)
			}
			ret = append(ret, un.CloneAll())
		} else if !before {
			return false
		}
		return true
	}, true)
	return
}
