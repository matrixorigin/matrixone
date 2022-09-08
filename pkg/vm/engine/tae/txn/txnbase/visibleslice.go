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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type VisibleSlice struct {
	*sync.RWMutex
	MVCC []*TxnMVCCNode
}

func NewVisibleSlice() *VisibleSlice {
	return &VisibleSlice{
		MVCC:    make([]*TxnMVCCNode, 0),
		RWMutex: &sync.RWMutex{},
	}
}
func (be *VisibleSlice) StringLocked() string {
	var w bytes.Buffer

	length := len(be.MVCC)
	for i := length - 1; i >= 0; i-- {
		version := be.MVCC[i]
		_, _ = w.WriteString(" -> ")
		_, _ = w.WriteString(version.String())
	}
	return w.String()
}

// for replay
func (be *VisibleSlice) GetTs() types.TS {
	return be.GetUpdateNodeLocked().End
}
func (be *VisibleSlice) GetTxn() txnif.TxnReader { return be.GetUpdateNodeLocked().Txn }

func (be *VisibleSlice) GetIndexes() []*wal.Index {
	ret := make([]*wal.Index, 0)
	for _, un := range be.MVCC {
		ret = append(ret, un.LogIndex...)
	}
	return ret
}

func (be *VisibleSlice) InsertNode(un *TxnMVCCNode) {
	be.MVCC = append(be.MVCC, un)
}

func (be *VisibleSlice) ExistUpdate(minTs, maxTs types.TS) (exist bool) {
	length := len(be.MVCC)
	for i := length - 1; i >= 0; i-- {
		un := be.MVCC[i]
		committedIn, commitBeforeMinTS := un.CommittedIn(minTs, maxTs)
		if committedIn {
			exist = true
			break
		}
		if commitBeforeMinTS {
			break
		}
	}
	return
}

// GetUpdateNode gets the latest UpdateNode.
// It is useful in making command, apply state(e.g. ApplyCommit),
// check confilct.
func (be *VisibleSlice) GetUpdateNodeLocked() *TxnMVCCNode {
	length := len(be.MVCC)
	if length == 0 {
		return nil
	}
	return be.MVCC[length-1]
}

// GetCommittedNode gets the latest committed UpdateNode.
// It's useful when check whether the catalog/metadata entry is deleted.
func (be *VisibleSlice) GetCommittedNode() (node *TxnMVCCNode) {
	length := len(be.MVCC)
	for i := length - 1; i >= 0; i-- {
		un := be.MVCC[i]
		if !un.IsActive() && !un.IsCommitting() {
			node = un
			break
		}
	}
	return
}

// GetNodeToRead gets UpdateNode according to the timestamp.
// It returns the UpdateNode in the same txn as the read txn
// or returns the latest UpdateNode with commitTS less than the timestamp.
func (be *VisibleSlice) GetNodeToRead(startts types.TS) (node *TxnMVCCNode) {
	length := len(be.MVCC)
	for i := length - 1; i >= 0; i-- {
		un := be.MVCC[i]
		var canRead bool
		canRead, goNext := un.TxnCanRead(startts)
		if canRead {
			node = un
		}
		if !goNext {
			break
		}
	}
	return
}

// GetExactUpdateNode gets the exact UpdateNode with the startTs.
// It's only used in replay
func (be *VisibleSlice) GetExactUpdateNode(startts types.TS) (node *TxnMVCCNode) {
	length := len(be.MVCC)
	for i := length - 1; i >= 0; i-- {
		un := be.MVCC[i]
		if un.Start == startts {
			node = un
			break
		}
	}
	return
}

func (be *VisibleSlice) NeedWaitCommitting(startTS types.TS) (bool, txnif.TxnReader) {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false, nil
	}
	return un.NeedWaitCommitting(startTS)
}

func (be *VisibleSlice) IsCreating() bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return true
	}
	return un.IsActive()
}

func (be *VisibleSlice) PrepareWrite(txn txnif.TxnReader) (err error) {
	node := be.GetUpdateNodeLocked()
	if node.IsActive() {
		if node.IsSameTxn(txn.GetStartTS()) {
			return
		}
		return txnif.ErrTxnWWConflict
	}

	if node.End.Greater(txn.GetStartTS()) {
		return txnif.ErrTxnWWConflict
	}
	return
}

func (be *VisibleSlice) WriteOneNodeTo(w io.Writer) (n int64, err error) {
	var n2 int64
	n2, err = be.GetUpdateNodeLocked().WriteTo(w)
	if err != nil {
		return
	}
	n += n2
	return
}

func (be *VisibleSlice) WriteAllTo(w io.Writer) (n int64, err error) {
	length := uint32(len(be.MVCC))
	n += 8
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 8
	for _, node := range be.MVCC {
		var n2 int64
		n2, err = node.WriteTo(w)
		if err != nil {
			return
		}
		n += n2
	}
	return
}

func (be *VisibleSlice) ReadOneNodeFrom(r io.Reader) (n int64, err error) {
	var n2 int64
	un := &TxnMVCCNode{}
	n2, err = un.ReadFrom(r)
	if err != nil {
		return
	}
	be.InsertNode(un)
	n += n2
	return
}

func (be *VisibleSlice) ReadAllFrom(r io.Reader) (n int64, err error) {
	length := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n += 8
	for i := 0; i < int(length); i++ {
		var n2 int64
		un := &TxnMVCCNode{}
		n2, err = un.ReadFrom(r)
		if err != nil {
			return
		}
		be.InsertNode(un)
		n += n2
	}
	return
}

func (be *VisibleSlice) IsEmpty() bool {
	return len(be.MVCC) == 0
}

func (be *VisibleSlice) ApplyRollback(index *wal.Index) error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().ApplyRollback(index)

}

func (be *VisibleSlice) ApplyCommit(index *wal.Index) error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().ApplyCommit(index)
}

func (be *VisibleSlice) GetLogIndex() []*wal.Index {
	node := be.GetUpdateNodeLocked()
	if node == nil {
		return nil
	}
	return node.LogIndex
}

func (be *VisibleSlice) CloneLatestNode() (*VisibleSlice, *TxnMVCCNode) {
	cloned := &VisibleSlice{
		MVCC:    make([]*TxnMVCCNode, 0),
		RWMutex: &sync.RWMutex{},
	}
	un := be.GetUpdateNodeLocked()
	uncloned := &TxnMVCCNode{}
	uncloned.CloneData(un)
	cloned.InsertNode(uncloned)
	return cloned, uncloned
}

// In /Catalog, there're three states: Active, Committing and Committed.
// A txn is Active before its CommitTs is allocated.
// It's Committed when its state will never change, i.e. TxnStateCommitted and  TxnStateRollbacked.
// It's Committing when it's in any other state, including TxnStateCommitting, TxnStateRollbacking, TxnStatePrepared and so on. When read or write an entry, if the last txn of the entry is Committing, we wait for it. When write on an Entry, if there's an Active txn, we report w-w conflict.
func (be *VisibleSlice) IsCommitting() bool {
	node := be.GetUpdateNodeLocked()
	if node == nil {
		return false
	}
	return node.IsCommitting()
}

func (be *VisibleSlice) Prepare2PCPrepare() error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().Prepare2PCPrepare()
}

func (be *VisibleSlice) PrepareCommit() error {
	be.Lock()
	defer be.Unlock()
	return be.GetUpdateNodeLocked().PrepareCommit()
}

func (be *VisibleSlice) PrepareRollback() (bool, error) {
	be.Lock()
	defer be.Unlock()
	length := len(be.MVCC)
	be.MVCC = be.MVCC[0 : length-1]
	isEmpty := be.IsEmpty()
	return isEmpty, nil
}

func (be *VisibleSlice) IsCommitted() bool {
	un := be.GetUpdateNodeLocked()
	if un == nil {
		return false
	}
	return un.Txn == nil
}

func (be *VisibleSlice) CloneCommittedInRange(start, end types.TS) (ret *VisibleSlice) {
	needWait, txn := be.NeedWaitCommitting(end.Next())
	if needWait {
		be.RUnlock()
		txn.GetTxnState(true)
		be.RLock()
	}
	for _, un := range be.MVCC {
		committedIn, commitBeforeMinTs := un.CommittedIn(start, end)
		if committedIn {
			if ret == nil {
				ret = NewVisibleSlice()
			}
			ret.InsertNode(un.CloneAll())
		} else if !commitBeforeMinTs {
			break
		}
	}
	return
}
