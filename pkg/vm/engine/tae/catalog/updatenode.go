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
	"errors"
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

var ErrTxnActive = errors.New("txn is active")

type INode interface {
	txnif.TxnEntry
	ApplyUpdate(*UpdateNode) error
	ApplyDelete() error
	GetUpdateNode() *UpdateNode
	String() string
}

type UpdateNode struct {
	CreatedAt types.TS
	DeletedAt types.TS
	MetaLoc   string
	DeltaLoc  string

	State      txnif.TxnState
	Start, End types.TS
	Txn        txnif.TxnReader
	LogIndex   []*wal.Index
	Deleted    bool
}

func NewEmptyUpdateNode() *UpdateNode {
	return &UpdateNode{}
}

func (e UpdateNode) CloneAll() *UpdateNode {
	n := e.CloneData()
	n.State = e.State
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

func (e *UpdateNode) CloneData() *UpdateNode {
	return &UpdateNode{
		CreatedAt: e.CreatedAt,
		DeletedAt: e.DeletedAt,
		MetaLoc:   e.MetaLoc,
		DeltaLoc:  e.DeltaLoc,
	}
}

// for create drop in one txn
func (e *UpdateNode) UpdateNode(un *UpdateNode) {
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

func (e *UpdateNode) HasDropped() bool {
	return e.Deleted
}

func (e *UpdateNode) IsSameTxn(startTs types.TS) bool {
	if e.Txn == nil {
		return false
	}
	return e.Txn.GetStartTS().Equal(startTs)
}

func (e *UpdateNode) IsActive() bool {
	return e.Txn != nil
}

func (e *UpdateNode) IsCommitting() bool {
	if e.Txn == nil {
		return false
	}
	return e.Txn.GetTxnState(false) == txnif.TxnStateCommitting
}

func (e *UpdateNode) GetTxn() txnif.TxnReader {
	return e.Txn
}

func (e *UpdateNode) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(
		fmt.Sprintf("[%v,%v][C=%v,D=%v][%v][Loc1=%s,Loc2=%s][Deleted?%v][logIndex=%v]",
			e.Start,
			e.End,
			e.CreatedAt,
			e.DeletedAt,
			e.State,
			e.MetaLoc,
			e.DeltaLoc,
			e.Deleted,
			e.LogIndex))
	return w.String()
}

func (e *UpdateNode) UpdateMetaLoc(loc string) (err error) {
	e.MetaLoc = loc
	return
}

func (e *UpdateNode) UpdateDeltaLoc(loc string) (err error) {
	e.DeltaLoc = loc
	return
}

func (e *UpdateNode) ApplyUpdate(be *UpdateNode) (err error) {
	// if e.Deleted {
	// 	// TODO
	// }
	e.CreatedAt = be.CreatedAt
	e.DeletedAt = be.DeletedAt
	e.MetaLoc = be.MetaLoc
	e.DeltaLoc = be.DeltaLoc
	return
}

func (e *UpdateNode) ApplyDeleteLocked() (err error) {
	if e.Deleted {
		panic("cannot apply delete to deleted node")
	}
	e.Deleted = true
	return
}

func (e *UpdateNode) ApplyDelete() (err error) {
	err = e.ApplyDeleteLocked()
	return
}

func compareUpdateNode(e, o *UpdateNode) int {
	if e.Start.Less(o.Start) {
		return -1
	}
	if e.Start.Equal(o.Start) {
		return 0
	}
	return 1

}
func (e *UpdateNode) AddLogIndex(idx *wal.Index) {
	if e.LogIndex == nil {
		e.LogIndex = make([]*wal.Index, 0)
	}
	e.LogIndex = append(e.LogIndex, idx)

}
func (e *UpdateNode) ApplyCommit(index *wal.Index) (err error) {
	e.Txn = nil
	e.AddLogIndex(index)
	e.State = txnif.TxnStateCommitted
	return
}

func (e *UpdateNode) ApplyRollback(index *wal.Index) (err error) {
	e.AddLogIndex(index)
	return
}

func (e *UpdateNode) Prepare2PCPrepare() (err error) {
	if e.CreatedAt.IsEmpty() {
		e.CreatedAt = e.Txn.GetPrepareTS()
	}
	if e.Deleted {
		e.DeletedAt = e.Txn.GetPrepareTS()
	}
	e.End = e.Txn.GetPrepareTS()
	return
}

func (e *UpdateNode) PrepareCommit() (err error) {
	if e.CreatedAt.IsEmpty() {
		e.CreatedAt = e.Txn.GetCommitTS()
	}
	if e.Deleted {
		e.DeletedAt = e.Txn.GetCommitTS()
	}
	e.End = e.Txn.GetCommitTS()
	return
}

func (e *UpdateNode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	return
}
func (e *UpdateNode) WriteTo(w io.Writer) (n int64, err error) {
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

func (e *UpdateNode) ReadFrom(r io.Reader) (n int64, err error) {
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
