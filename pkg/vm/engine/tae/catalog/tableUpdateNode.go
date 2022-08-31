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

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type TableUpdateNode struct {
	*EntryUpdateNode
	MetaLoc  string
	DeltaLoc string

	*VisibleUpdateNode
}

func NewEmptyTableUpdateNode() *TableUpdateNode {
	return &TableUpdateNode{
		VisibleUpdateNode: &VisibleUpdateNode{},
		EntryUpdateNode:   &EntryUpdateNode{},
	}
}

func (e *TableUpdateNode) CloneAll() UpdateNodeIf {
	n := e.cloneData()
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

func (e *TableUpdateNode) cloneData() *TableUpdateNode {
	return &TableUpdateNode{
		EntryUpdateNode:   e.EntryUpdateNode.Clone(),
		VisibleUpdateNode: &VisibleUpdateNode{},
		MetaLoc:           e.MetaLoc,
		DeltaLoc:          e.DeltaLoc,
	}
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

func (e *TableUpdateNode) ApplyUpdate(be *TableUpdateNode) (err error) {
	e.EntryUpdateNode = be.EntryUpdateNode.Clone()
	e.MetaLoc = be.MetaLoc
	e.DeltaLoc = be.DeltaLoc
	return
}

func (e *TableUpdateNode) ApplyDelete() (err error) {
	err = e.ApplyDeleteLocked()
	return
}

func compareTableUpdateNode(e, o *TableUpdateNode) int {
	return e.Compare(o.VisibleUpdateNode)
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

func (e *TableUpdateNode) WriteTo(w io.Writer) (n int64, err error) {
	var sn int64
	sn, err = e.EntryUpdateNode.WriteTo(w)
	if err != nil {
		return
	}
	n += sn
	sn, err = e.VisibleUpdateNode.WriteTo(w)
	if err != nil {
		return
	}
	n += sn

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
	return
}

func (e *TableUpdateNode) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int64
	sn, err = e.EntryUpdateNode.ReadFrom(r)
	if err != nil {
		return
	}
	n += sn
	sn, err = e.VisibleUpdateNode.ReadFrom(r)
	if err != nil {
		return
	}
	n += sn

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
	return
}
