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

package catalog

import (
	"encoding/binary"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type TxnMVCCNode struct {
	Start, End types.TS
	Txn        txnif.TxnReader
	//Aborted bool
	//State txnif.TxnState
	LogIndex []*wal.Index
}

func NewTxnMVCCNodeWithTxn(txn txnif.TxnReader) *TxnMVCCNode {
	return &TxnMVCCNode{
		Start: txn.GetStartTS(),
		Txn:   txn,
	}
}

// Check w-w confilct
func (un *TxnMVCCNode) PrepareWrite(startTS types.TS) error {
	if un.IsActive() {
		if un.IsSameTxn(startTS) {
			return nil
		}
		return txnif.ErrTxnWWConflict
	}
	if un.End.Greater(startTS) {
		return txnif.ErrTxnWWConflict
	}
	return nil
}

func (un *TxnMVCCNode) IsSameStartTs(startTS types.TS) bool {
	return un.Start.Equal(startTS)
}

func (un *TxnMVCCNode) TxnCanRead(startTS types.TS) (canRead, goNext bool) {
	if un.IsSameTxn(startTS) {
		return true, false
	}
	if un.IsActive() || un.IsCommitting() {
		return false, true
	}
	if un.End.LessEq(startTS) {
		return true, false
	}
	return false, true

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

func (un *TxnMVCCNode) NeedWaitCommitting(startTS types.TS) (bool, txnif.TxnReader) {
	if !un.IsCommitting() {
		return false, nil
	}
	if un.Txn.GetCommitTS().GreaterEq(startTS) {
		return false, nil
	}
	return true, un.Txn
}

func (un *TxnMVCCNode) IsSameTxn(startTS types.TS) bool {
	if un.Txn == nil {
		return false
	}
	return un.Txn.GetStartTS().Equal(startTS)
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

// func (un *TxnMVCCNode) Clone() *TxnMVCCNode{
// 	return &TxnMVCCNode{
// 		Start: un.Start,
// 		End: un.End,
// 	}
// }

func (un *TxnMVCCNode) OnCommit() {
	un.End = un.Txn.GetCommitTS()
	un.Txn = nil
}

type EntryMVCCNode struct {
	CreatedAt, DeletedAt types.TS
	Deleted              bool
}

func (un *EntryMVCCNode) HasDropped() bool {
	return un.Deleted
}

func (un *EntryMVCCNode) GetCreatedAt() types.TS {
	return un.CreatedAt
}

func (un *EntryMVCCNode) GetDeletedAt() types.TS {
	return un.DeletedAt
}

func (un *EntryMVCCNode) Clone() *EntryMVCCNode {
	return &EntryMVCCNode{
		CreatedAt: un.CreatedAt,
		DeletedAt: un.DeletedAt,
		Deleted:   un.Deleted,
	}
}

func (un *EntryMVCCNode) ApplyDeleteLocked() (err error) {
	if un.Deleted {
		panic("cannot apply delete to deleted node")
	}
	un.Deleted = true
	return
}

func (un *EntryMVCCNode) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &un.CreatedAt); err != nil {
		return
	}
	n += 12
	if err = binary.Read(r, binary.BigEndian, &un.DeletedAt); err != nil {
		return
	}
	n += 12
	if !un.DeletedAt.IsEmpty() {
		un.Deleted = true
	}
	return
}
func (un *EntryMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, un.CreatedAt); err != nil {
		return
	}
	n += 12
	if err = binary.Write(w, binary.BigEndian, un.DeletedAt); err != nil {
		return
	}
	n += 12
	return
}
