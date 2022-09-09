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
	MVCC      *common.GenericSortedDList[MVCCNode]
	comparefn func(MVCCNode, MVCCNode) int
	newnodefn func() MVCCNode
	length    uint64
}

func NewMVCCChain(comparefn func(MVCCNode, MVCCNode) int, newnodefn func() MVCCNode) *MVCCChain {
	return &MVCCChain{
		MVCC:      common.NewGenericSortedDList(comparefn),
		RWMutex:   &sync.RWMutex{},
		comparefn: comparefn,
		newnodefn: newnodefn,
	}
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
func (be *MVCCChain) GetTs() types.TS {
	return be.GetUpdateNodeLocked().GetEnd()
}
func (be *MVCCChain) GetTxn() txnif.TxnReader { return be.GetUpdateNodeLocked().GetTxn() }

func (be *MVCCChain) GetIndexes() []*wal.Index {
	ret := make([]*wal.Index, 0)
	be.MVCC.Loop(func(n *common.GenericDLNode[MVCCNode]) bool {
		un := n.GetPayload()
		ret = append(ret, un.GetLogIndex()...)
		return true
	}, true)
	return ret
}

func (be *MVCCChain) InsertNode(vun MVCCNode) {
	un := vun
	be.MVCC.Insert(un)
}

func (be *MVCCChain) ExistUpdate(minTs, maxTs types.TS) (exist bool) {
	be.MVCC.Loop(func(n *common.GenericDLNode[MVCCNode]) bool {
		un := n.GetPayload()
		committedIn, commitBeforeMinTS := un.CommittedIn(minTs, maxTs)
		if committedIn {
			exist = true
			return false
		}
		if !commitBeforeMinTS {
			return true
		}
		return false
	}, false)
	return
}

// GetUpdateNode gets the latest UpdateNode.
// It is useful in making command, apply state(e.g. ApplyCommit),
// check confilct.
func (be *MVCCChain) GetUpdateNodeLocked() MVCCNode {
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

// GetCommittedNode gets the latest committed UpdateNode.
// It's useful when check whether the catalog/metadata entry is deleted.
func (be *MVCCChain) GetCommittedNode() (node MVCCNode) {
	be.MVCC.Loop(func(n *common.GenericDLNode[MVCCNode]) bool {
		un := n.GetPayload()
		if !un.IsActive() && !un.IsCommitting() {
			node = un
			return false
		}
		return true
	}, false)
	return
}

// GetNodeToRead gets UpdateNode according to the timestamp.
// It returns the UpdateNode in the same txn as the read txn
// or returns the latest UpdateNode with commitTS less than the timestamp.
func (be *MVCCChain) GetNodeToRead(startts types.TS) (node MVCCNode) {
	be.MVCC.Loop(func(n *common.GenericDLNode[MVCCNode]) (goNext bool) {
		un := n.GetPayload()
		var canRead bool
		canRead, goNext = un.TxnCanRead(startts)
		if canRead {
			node = un
		}
		return
	}, false)
	return
}

// GetExactUpdateNode gets the exact UpdateNode with the startTs.
// It's only used in replay
func (be *MVCCChain) GetExactUpdateNode(startts types.TS) (node MVCCNode) {
	be.MVCC.Loop(func(n *common.GenericDLNode[MVCCNode]) bool {
		un := n.GetPayload()
		if un.GetStart() == startts {
			node = un
			return false
		}
		// return un.Start < startts
		return true
	}, false)
	return
}

func (be *MVCCChain) NeedWaitCommitting(startTS types.TS) (bool, txnif.TxnReader) {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false, nil
	}
	return un.NeedWaitCommitting(startTS)
}

func (be *MVCCChain) IsCreating() bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return true
	}
	return un.IsActive()
}

func (be *MVCCChain) PrepareWrite(txn txnif.TxnReader) (err error) {
	node := be.GetUpdateNodeLocked()
	if node.IsActive() {
		if node.IsSameTxn(txn.GetStartTS()) {
			return
		}
		return txnif.ErrTxnWWConflict
	}

	if node.GetEnd().Greater(txn.GetStartTS()) {
		return txnif.ErrTxnWWConflict
	}
	return
}

func (be *MVCCChain) WriteOneNodeTo(w io.Writer) (n int64, err error) {
	var n2 int64
	n2, err = be.GetUpdateNodeLocked().WriteTo(w)
	if err != nil {
		return
	}
	n += n2
	return
}

func (be *MVCCChain) WriteAllTo(w io.Writer) (n int64, err error) {
	n += 8
	if err = binary.Write(w, binary.BigEndian, be.length); err != nil {
		return
	}
	n += 8
	be.MVCC.Loop(func(node *common.GenericDLNode[MVCCNode]) bool {
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
	be.InsertNode(un)
	n += n2
	return
}

func (be *MVCCChain) ReadAllFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &be.length); err != nil {
		return
	}
	n += 8
	for i := 0; i < int(be.length); i++ {
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
	return be.GetUpdateNodeLocked().ApplyRollback(index)

}

func (be *MVCCChain) ApplyCommit(index *wal.Index) error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().ApplyCommit(index)
}

func (be *MVCCChain) GetLogIndex() []*wal.Index {
	node := be.GetUpdateNodeLocked()
	if node == nil {
		return nil
	}
	return node.GetLogIndex()
}

func (be *MVCCChain) CloneLatestNode() (*MVCCChain, MVCCNode) {
	cloned := &MVCCChain{
		MVCC:    common.NewGenericSortedDList(be.comparefn),
		RWMutex: &sync.RWMutex{},
	}
	un := be.GetUpdateNodeLocked()
	uncloned := un.CloneData()
	cloned.InsertNode(uncloned)
	return cloned, uncloned
}

// In /Catalog, there're three states: Active, Committing and Committed.
// A txn is Active before its CommitTs is allocated.
// It's Committed when its state will never change, i.e. TxnStateCommitted and  TxnStateRollbacked.
// It's Committing when it's in any other state, including TxnStateCommitting, TxnStateRollbacking, TxnStatePrepared and so on. When read or write an entry, if the last txn of the entry is Committing, we wait for it. When write on an Entry, if there's an Active txn, we report w-w conflict.
func (be *MVCCChain) IsCommitting() bool {
	node := be.GetUpdateNodeLocked()
	if node == nil {
		return false
	}
	return node.IsCommitting()
}

func (be *MVCCChain) Prepare2PCPrepare() error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().Prepare2PCPrepare()
}

func (be *MVCCChain) PrepareCommit() error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().PrepareCommit()
}

func (be *MVCCChain) PrepareRollback() (bool, error) {
	be.Lock()
	defer be.Unlock()
	node := be.MVCC.GetHead()
	be.MVCC.Delete(node)
	isEmpty := be.IsEmpty()
	return isEmpty, nil
}

func (be *MVCCChain) IsCommitted() bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false
	}
	return un.GetTxn() == nil
}

func (be *MVCCChain) CloneCommittedInRange(start, end types.TS) (ret *MVCCChain) {
	needWait, txn := be.NeedWaitCommitting(end.Next())
	if needWait {
		be.RUnlock()
		txn.GetTxnState(true)
		be.RLock()
	}
	be.MVCC.Loop(func(n *common.GenericDLNode[MVCCNode]) bool {
		un := n.GetPayload()
		committedIn, commitBeforeMinTs := un.CommittedIn(start, end)
		if committedIn {
			if ret == nil {
				ret = NewMVCCChain(be.comparefn, be.newnodefn)
			}
			ret.InsertNode(un.CloneAll())
			ret.length++
		} else if !commitBeforeMinTs {
			return false
		}
		return true
	}, true)
	return
}
