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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type MVCCNode interface {
	String() string

	IsVisible(ts types.TS) (visible bool)
	CheckConflict(ts types.TS) error
	UpdateNode(o MVCCNode)

	CommittedIn(minTS, maxTS types.TS) (committedIn, commitBeforeMinTS bool)
	NeedWaitCommitting(ts types.TS) (bool, txnif.TxnReader)
	IsSameTxn(ts types.TS) bool
	IsActive() bool
	IsCommitting() bool
	IsCommitted() bool

	GetEnd() types.TS
	GetTxn() txnif.TxnReader
	AddLogIndex(idx *wal.Index)
	GetLogIndex() []*wal.Index

	ApplyCommit(index *wal.Index) (err error)
	ApplyRollback(index *wal.Index) (err error)
	Prepare2PCPrepare() (err error)
	PrepareCommit() (err error)

	WriteTo(w io.Writer) (n int64, err error)
	ReadFrom(r io.Reader) (n int64, err error)
	CloneData() MVCCNode
	CloneAll() MVCCNode
}

// TODO prepare ts
type TxnMVCCNode struct {
	Start, End types.TS
	Txn        txnif.TxnReader
	//Aborted bool
	//State txnif.TxnState
	LogIndex []*wal.Index
}

func NewTxnMVCCNodeWithTxn(txn txnif.TxnReader) *TxnMVCCNode {
	var ts types.TS
	if txn != nil {
		ts = txn.GetStartTS()
	}
	return &TxnMVCCNode{
		Start: ts,
		Txn:   txn,
	}
}

// Check w-w confilct
func (un *TxnMVCCNode) CheckConflict(ts types.TS) error {
	if un.IsActive() {
		if un.IsSameTxn(ts) {
			return nil
		}
		return txnif.ErrTxnWWConflict
	}
	if un.End.Greater(ts) {
		return txnif.ErrTxnWWConflict
	}
	return nil
}

func (un *TxnMVCCNode) IsVisible(ts types.TS) (visible bool) {
	if un.IsSameTxn(ts) {
		return true
	}
	if un.IsActive() || un.IsCommitting() {
		return false
	}
	if un.End.LessEq(ts) {
		return true
	}
	return false

}

func (un *TxnMVCCNode) CommittedIn(minTS, maxTS types.TS) (committedIn, commitBeforeMinTS bool) {
	if un.End.IsEmpty() {
		return false, false
	}
	if un.End.Less(minTS) {
		return false, true
	}
	if un.End.GreaterEq(minTS) && un.End.LessEq(maxTS) {
		return true, false
	}
	return false, false
}

func (un *TxnMVCCNode) NeedWaitCommitting(ts types.TS) (bool, txnif.TxnReader) {
	if !un.IsCommitting() {
		return false, nil
	}
	if un.Txn.GetCommitTS().GreaterEq(ts) {
		return false, nil
	}
	return true, un.Txn
}

func (un *TxnMVCCNode) IsSameTxn(ts types.TS) bool {
	if un.Txn == nil {
		return false
	}
	return un.Txn.GetStartTS().Equal(ts)
}

func (un *TxnMVCCNode) IsActive() bool {
	if un.Txn == nil {
		return false
	}
	return un.Txn.GetTxnState(false) == txnif.TxnStateActive
}

func (un *TxnMVCCNode) IsCommitting() bool {
	if un.Txn == nil {
		return false
	}
	return un.Txn.GetTxnState(false) != txnif.TxnStateActive
}

func (un *TxnMVCCNode) IsCommitted() bool {
	return un.Txn == nil
}

func (un *TxnMVCCNode) GetStart() types.TS {
	return un.Start
}

func (un *TxnMVCCNode) GetEnd() types.TS {
	return un.End
}

func (un *TxnMVCCNode) GetTxn() txnif.TxnReader {
	return un.Txn
}

func (un *TxnMVCCNode) Compare(o *TxnMVCCNode) int {
	if un.Start.Less(o.Start) {
		return -1
	}
	if un.Start.Equal(o.Start) {
		return 0
	}
	return 1
}

func (un *TxnMVCCNode) AddLogIndex(idx *wal.Index) {
	if un.LogIndex == nil {
		un.LogIndex = make([]*wal.Index, 0)
	}
	un.LogIndex = append(un.LogIndex, idx)

}
func (un *TxnMVCCNode) GetLogIndex() []*wal.Index {
	return un.LogIndex
}
func (un *TxnMVCCNode) ApplyCommit(index *wal.Index) (err error) {
	un.Txn = nil
	un.AddLogIndex(index)
	return
}

func (un *TxnMVCCNode) ApplyRollback(index *wal.Index) (err error) {
	un.Txn = nil
	un.AddLogIndex(index)
	return
}

func (un *TxnMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, un.Start); err != nil {
		return
	}
	n += 12
	if err = binary.Write(w, binary.BigEndian, un.End); err != nil {
		return
	}
	n += 12
	var sn int64
	length := uint32(len(un.LogIndex))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 4
	for _, idx := range un.LogIndex {
		sn, err = idx.WriteTo(w)
		if err != nil {
			return
		}
		n += sn
	}
	return
}

func (un *TxnMVCCNode) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &un.Start); err != nil {
		return
	}
	n += 12
	if err = binary.Read(r, binary.BigEndian, &un.End); err != nil {
		return
	}
	n += 12

	length := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n += 4
	if length != 0 {
		un.LogIndex = make([]*wal.Index, 0)
	}
	var sn int64
	for i := 0; i < int(length); i++ {
		idx := new(wal.Index)
		sn, err = idx.ReadFrom(r)
		if err != nil {
			return
		}
		n += sn
		un.LogIndex = append(un.LogIndex, idx)
	}
	return
}

func CompareTxnMVCCNode(e, o *TxnMVCCNode) int {
	return e.Compare(o)
}

func (un *TxnMVCCNode) UpdateNode(o *TxnMVCCNode) {
	if !un.Start.Equal(o.Start) {
		panic("logic err")
	}
	if !un.End.Equal(o.End) {
		panic("logic err")
	}
	un.AddLogIndex(o.LogIndex[0])
}

func (un *TxnMVCCNode) CloneAll() *TxnMVCCNode {
	n := &TxnMVCCNode{}
	n.Start = un.Start
	n.End = un.End
	if len(un.LogIndex) != 0 {
		n.LogIndex = make([]*wal.Index, 0)
		for _, idx := range un.LogIndex {
			n.LogIndex = append(n.LogIndex, idx.Clone())
		}
	}
	return n
}

func (un *TxnMVCCNode) String() string {
	return fmt.Sprintf("[%v,%v][logIndex=%v]",
		un.Start,
		un.End,
		un.LogIndex)
}

func (e *TxnMVCCNode) Prepare2PCPrepare() (ts types.TS, err error) {
	e.End = e.Txn.GetPrepareTS()
	ts = e.End
	return
}

func (e *TxnMVCCNode) PrepareCommit() (ts types.TS, err error) {
	e.End = e.Txn.GetCommitTS()
	ts = e.End
	return
}
