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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type TableUpdateNode struct {
	CreatedAt types.TS
	DeletedAt types.TS
	MetaLoc   string
	DeltaLoc  string

	// State      txnif.TxnState
	Start, End types.TS
	Txn        txnif.TxnReader
	LogIndex   []*wal.Index
	Deleted    bool
}

func NewEmptyTableUpdateNode() *TableUpdateNode {
	return &TableUpdateNode{}
}

func (e *TableUpdateNode) GetEnd() types.TS {
	return e.End
}
func (e *TableUpdateNode) GetStart() types.TS {
	return e.Start
}
func (e *TableUpdateNode) GetLogIndex() []*wal.Index {
	return e.LogIndex
}
func (e *TableUpdateNode) CloneAll() UpdateNodeIf {
	n := e.CloneData().(*TableUpdateNode)
	// n.State = e.State
	n.Start = e.Start
	n.End = e.End
	n.Deleted = e.Deleted
	if len(e.LogIndex) != 0 {
		n.LogIndex = make([]*wal.Index, 0)
		for _, idx := range e.LogIndex {
			n.LogIndex = append(n.LogIndex, idx.Clone())
		}
	}
	return n
}

func (e *TableUpdateNode) CloneData() UpdateNodeIf {
	return &TableUpdateNode{
		CreatedAt: e.CreatedAt,
		DeletedAt: e.DeletedAt,
		MetaLoc:   e.MetaLoc,
		DeltaLoc:  e.DeltaLoc,
	}
}

func (e *TableUpdateNode) cloneData() *TableUpdateNode {
	return &TableUpdateNode{
		CreatedAt: e.CreatedAt,
		DeletedAt: e.DeletedAt,
		MetaLoc:   e.MetaLoc,
		DeltaLoc:  e.DeltaLoc,
	}
}

// for create drop in one txn
func (e *TableUpdateNode) UpdateNode(vun UpdateNodeIf) {
	un := vun.(*TableUpdateNode)
	if e.Start != un.Start {
		panic("logic err")
	}
	if e.End != un.End {
		panic("logic err")
	}
	e.DeletedAt = un.DeletedAt
	e.Deleted = true
	e.AddLogIndex(un.LogIndex[0])
}

func (e *TableUpdateNode) HasDropped() bool {
	return e.Deleted
}

func (e *TableUpdateNode) IsSameTxn(startTs types.TS) bool {
	if e.Txn == nil {
		return false
	}
	return e.Txn.GetStartTS().Equal(startTs)
}

func (e *TableUpdateNode) IsActive() bool {
	if e.Txn == nil {
		return false
	}
	return e.Txn.GetTxnState(false) == txnif.TxnStateActive
}

func (e *TableUpdateNode) IsCommitting() bool {
	if e.Txn == nil {
		return false
	}
	return e.Txn.GetTxnState(false) != txnif.TxnStateActive
}

func (e *TableUpdateNode) IsMetaDataCommitting() bool {
	if e.Txn == nil {
		return false
	}
	state := e.Txn.GetTxnState(false)
	return state == txnif.TxnStateCommitting || state == txnif.TxnStatePreparing
}

func (e *TableUpdateNode) GetTxn() txnif.TxnReader {
	return e.Txn
}

func (e *TableUpdateNode) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(
		fmt.Sprintf("[%v,%v][C=%v,D=%v][Loc1=%s,Loc2=%s][Deleted?%v][logIndex=%v]",
			e.Start,
			e.End,
			e.CreatedAt,
			e.DeletedAt,
			// e.State,
			e.MetaLoc,
			e.DeltaLoc,
			e.Deleted,
			e.LogIndex))
	return w.String()
}

func (e *TableUpdateNode) UpdateMetaLoc(loc string) (err error) {
	e.MetaLoc = loc
	return
}

func (e *TableUpdateNode) UpdateDeltaLoc(loc string) (err error) {
	e.DeltaLoc = loc
	return
}

func (e *TableUpdateNode) ApplyUpdate(be *TableUpdateNode) (err error) {
	// if e.Deleted {
	// 	// TODO
	// }
	e.CreatedAt = be.CreatedAt
	e.DeletedAt = be.DeletedAt
	e.MetaLoc = be.MetaLoc
	e.DeltaLoc = be.DeltaLoc
	return
}

func (e *TableUpdateNode) ApplyDeleteLocked() (err error) {
	if e.Deleted {
		panic("cannot apply delete to deleted node")
	}
	e.Deleted = true
	return
}

func (e *TableUpdateNode) ApplyDelete() (err error) {
	err = e.ApplyDeleteLocked()
	return
}

func compareTableUpdateNode(e, o *TableUpdateNode) int {
	if e.Start.Less(o.Start) {
		return -1
	}
	if e.Start.Equal(o.Start) {
		return 0
	}
	return 1

}
func (e *TableUpdateNode) AddLogIndex(idx *wal.Index) {
	if e.LogIndex == nil {
		e.LogIndex = make([]*wal.Index, 0)
	}
	e.LogIndex = append(e.LogIndex, idx)

}
func (e *TableUpdateNode) ApplyCommit(index *wal.Index) (err error) {
	e.Txn = nil
	e.AddLogIndex(index)
	// e.State = txnif.TxnStateCommitted
	return
}

func (e *TableUpdateNode) ApplyRollback(index *wal.Index) (err error) {
	e.AddLogIndex(index)
	return
}

func (e *TableUpdateNode) Prepare2PCPrepare() (err error) {
	if e.CreatedAt.IsEmpty() {
		e.CreatedAt = e.Txn.GetPrepareTS()
	}
	if e.Deleted {
		e.DeletedAt = e.Txn.GetPrepareTS()
	}
	e.End = e.Txn.GetPrepareTS()
	return
}

func (e *TableUpdateNode) PrepareCommit() (err error) {
	if e.CreatedAt.IsEmpty() {
		e.CreatedAt = e.Txn.GetCommitTS()
	}
	if e.Deleted {
		e.DeletedAt = e.Txn.GetCommitTS()
	}
	e.End = e.Txn.GetCommitTS()
	return
}

//	func (e *UpdateNode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
//		return
//	}
func (e *TableUpdateNode) WriteTo(w io.Writer) (n int64, err error) {
	if err = binary.Write(w, binary.BigEndian, e.Start); err != nil {
		return
	}
	n += 8
	if err = binary.Write(w, binary.BigEndian, e.End); err != nil {
		return
	}
	n += 8
	if err = binary.Write(w, binary.BigEndian, e.CreatedAt); err != nil {
		return
	}
	n += 8
	if err = binary.Write(w, binary.BigEndian, e.DeletedAt); err != nil {
		return
	}
	n += 8
	length := uint32(len([]byte(e.MetaLoc)))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 4
	var n2 int
	n2, err = w.Write([]byte(e.MetaLoc))
	if err != nil {
		return
	}
	if n2 != int(length) {
		panic(fmt.Errorf("logic err %d!=%d, %v", n2, length, err))
	}
	n += int64(n2)
	length = uint32(len([]byte(e.DeltaLoc)))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 4
	n2, err = w.Write([]byte(e.DeltaLoc))
	if err != nil {
		return
	}
	if n2 != int(length) {
		panic(fmt.Errorf("logic err %d!=%d, %v", n2, length, err))
	}
	n += int64(n2)
	length = uint32(len(e.LogIndex))
	if err = binary.Write(w, binary.BigEndian, length); err != nil {
		return
	}
	n += 4
	var sn int64
	for _, idx := range e.LogIndex {
		sn, err = idx.WriteTo(w)
		if err != nil {
			return
		}
		n += sn
	}
	return
}

func (e *TableUpdateNode) ReadFrom(r io.Reader) (n int64, err error) {
	if err = binary.Read(r, binary.BigEndian, &e.Start); err != nil {
		return
	}
	n += 8
	if err = binary.Read(r, binary.BigEndian, &e.End); err != nil {
		return
	}
	n += 8
	if err = binary.Read(r, binary.BigEndian, &e.CreatedAt); err != nil {
		return
	}
	n += 8
	if err = binary.Read(r, binary.BigEndian, &e.DeletedAt); err != nil {
		return
	}
	n += 8
	length := uint32(0)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n += 4
	buf := make([]byte, length)
	var n2 int
	n2, err = r.Read(buf)
	if err != nil {
		return
	}
	if n2 != int(length) {
		panic(fmt.Errorf("logic err %d!=%d, %v", n2, length, err))
	}
	e.MetaLoc = string(buf)
	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	buf = make([]byte, length)
	n2, err = r.Read(buf)
	if err != nil {
		return
	}
	if n2 != int(length) {
		panic(fmt.Errorf("logic err %d!=%d, %v", n2, length, err))
	}
	e.DeltaLoc = string(buf)
	if !e.DeletedAt.IsEmpty() {
		e.Deleted = true
	}

	if err = binary.Read(r, binary.BigEndian, &length); err != nil {
		return
	}
	n += 4
	var sn int64
	if length != 0 {
		e.LogIndex = make([]*wal.Index, 0)
	}
	for i := 0; i < int(length); i++ {
		idx := new(wal.Index)
		sn, err = idx.ReadFrom(r)
		if err != nil {
			return
		}
		n += sn
		e.LogIndex = append(e.LogIndex, idx)
	}
	return
}
