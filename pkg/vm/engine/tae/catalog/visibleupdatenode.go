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

type VisibleUpdateNode struct {
	Start, End types.TS
	Txn        txnif.TxnReader
	//Aborted bool
	//State txnif.TxnState
	LogIndex []*wal.Index
}

func (un *VisibleUpdateNode) IsSameTxn(startTS types.TS) bool {
	if un.Txn == nil {
		return false
	}
	return un.Txn.GetStartTS().Equal(startTS)
}

func (un *VisibleUpdateNode) IsActive() bool {
	if un.Txn == nil {
		return false
	}
	return un.Txn.GetTxnState(false) == txnif.TxnStateActive
}

func (un *VisibleUpdateNode) IsCommitting() bool {
	if un.Txn == nil {
		return false
	}
	return un.Txn.GetTxnState(false) != txnif.TxnStateActive
}

func (un *VisibleUpdateNode) GetStart() types.TS {
	return un.Start
}

func (un *VisibleUpdateNode) GetEnd() types.TS {
	return un.End
}

func (un *VisibleUpdateNode) GetTxn() txnif.TxnReader {
	return un.Txn
}

func (un *VisibleUpdateNode) Compare(o *VisibleUpdateNode) int {
	if un.Start.Less(o.Start) {
		return -1
	}
	if un.Start.Equal(o.Start) {
		return 0
	}
	return 1
}

func (un *VisibleUpdateNode) AddLogIndex(idx *wal.Index) {
	if un.LogIndex == nil {
		un.LogIndex = make([]*wal.Index, 0)
	}
	un.LogIndex = append(un.LogIndex, idx)

}
func (e *VisibleUpdateNode) GetLogIndex() []*wal.Index {
	return e.LogIndex
}
func (un *VisibleUpdateNode) ApplyCommit(index *wal.Index) (err error) {
	un.Txn = nil
	un.AddLogIndex(index)
	return
}

func (un *VisibleUpdateNode) ApplyRollback(index *wal.Index) (err error) {
	un.AddLogIndex(index)
	return
}

func (un *VisibleUpdateNode) WriteTo(w io.Writer) (n int64, err error) {
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

func (un *VisibleUpdateNode) ReadFrom(r io.Reader) (n int64, err error) {
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

// func (un *VisibleUpdateNode) Clone() *VisibleUpdateNode{
// 	return &VisibleUpdateNode{
// 		Start: un.Start,
// 		End: un.End,
// 	}
// }

func (un *VisibleUpdateNode) OnCommit() {
	un.End = un.Txn.GetCommitTS()
	un.Txn = nil
}

type EntryUpdateNode struct {
	CreatedAt, DeletedAt types.TS
	Deleted              bool
}

func (un *EntryUpdateNode) HasDropped() bool {
	return un.Deleted
}

func (un *EntryUpdateNode) GetCreatedAt() types.TS {
	return un.CreatedAt
}

func (un *EntryUpdateNode) GetDeletedAt() types.TS {
	return un.DeletedAt
}

func (un *EntryUpdateNode) Clone() *EntryUpdateNode {
	return &EntryUpdateNode{
		CreatedAt: un.CreatedAt,
		DeletedAt: un.DeletedAt,
		Deleted:   un.Deleted,
	}
}

func (un *EntryUpdateNode) ApplyDeleteLocked() (err error) {
	if un.Deleted {
		panic("cannot apply delete to deleted node")
	}
	un.Deleted = true
	return
}

func (un *EntryUpdateNode) ReadFrom(r io.Reader) (n int64, err error) {
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
func (un *EntryUpdateNode) WriteTo(w io.Writer) (n int64, err error) {
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
