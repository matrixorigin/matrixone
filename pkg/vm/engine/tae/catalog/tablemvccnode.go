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
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type TableMVCCNode struct {
	*EntryMVCCNode
	*txnbase.TxnMVCCNode
}

func NewEmptyTableMVCCNode() *TableMVCCNode {
	return &TableMVCCNode{
		TxnMVCCNode:   &txnbase.TxnMVCCNode{},
		EntryMVCCNode: &EntryMVCCNode{},
	}
}

func (e *TableMVCCNode) CloneAll() MVCCNode {
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

func (e *TableMVCCNode) cloneData() *TableMVCCNode {
	return &TableMVCCNode{
		EntryMVCCNode: e.EntryMVCCNode.Clone(),
		TxnMVCCNode:   &txnbase.TxnMVCCNode{},
	}
}

func (e *TableMVCCNode) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(
		fmt.Sprintf("[%v,%v][C=%v,D=%v][Deleted?%v][logIndex=%v]",
			e.Start,
			e.End,
			e.CreatedAt,
			e.DeletedAt,
			// e.State,
			e.Deleted,
			e.LogIndex))
	return w.String()
}

// for create drop in one txn
func (e *TableMVCCNode) UpdateNode(vun MVCCNode) {
	un := vun.(*TableMVCCNode)
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

func (e *TableMVCCNode) ApplyUpdate(be *TableMVCCNode) (err error) {
	e.EntryMVCCNode = be.EntryMVCCNode.Clone()
	return
}

func (e *TableMVCCNode) ApplyDelete() (err error) {
	err = e.ApplyDeleteLocked()
	return
}

func compareTableMVCCNode(e, o *TableMVCCNode) int {
	return e.Compare(o.TxnMVCCNode)
}

func (e *TableMVCCNode) Prepare2PCPrepare() (err error) {
	if e.CreatedAt.IsEmpty() {
		e.CreatedAt = e.Txn.GetPrepareTS()
	}
	if e.Deleted {
		e.DeletedAt = e.Txn.GetPrepareTS()
	}
	e.End = e.Txn.GetPrepareTS()
	return
}

func (e *TableMVCCNode) PrepareCommit() (err error) {
	if e.CreatedAt.IsEmpty() {
		e.CreatedAt = e.Txn.GetCommitTS()
	}
	if e.Deleted {
		e.DeletedAt = e.Txn.GetCommitTS()
	}
	e.End = e.Txn.GetCommitTS()
	return
}

func (e *TableMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
	var sn int64
	sn, err = e.EntryMVCCNode.WriteTo(w)
	if err != nil {
		return
	}
	n += sn
	sn, err = e.TxnMVCCNode.WriteTo(w)
	if err != nil {
		return
	}
	n += sn
	return
}

func (e *TableMVCCNode) ReadFrom(r io.Reader) (n int64, err error) {
	var sn int64
	sn, err = e.EntryMVCCNode.ReadFrom(r)
	if err != nil {
		return
	}
	n += sn
	sn, err = e.TxnMVCCNode.ReadFrom(r)
	if err != nil {
		return
	}
	n += sn
	return
}
