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
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type DBMVCCNode struct {
	*EntryMVCCNode
	*txnbase.TxnMVCCNode
}

func NewEmptyDBMVCCNode() txnbase.MVCCNode {
	return &DBMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{},
		TxnMVCCNode:   &txnbase.TxnMVCCNode{},
	}
}

func CompareDBBaseNode(e, o txnbase.MVCCNode) int {
	return e.(*DBMVCCNode).Compare(o.(*DBMVCCNode).TxnMVCCNode)
}

func (e *DBMVCCNode) CloneAll() txnbase.MVCCNode {
	node := e.CloneData()
	node.(*DBMVCCNode).TxnMVCCNode = e.TxnMVCCNode.CloneAll()
	return node
}

func (e *DBMVCCNode) CloneData() txnbase.MVCCNode {
	return &DBMVCCNode{
		EntryMVCCNode: e.EntryMVCCNode.Clone(),
		TxnMVCCNode:   &txnbase.TxnMVCCNode{},
	}
}

func (e *DBMVCCNode) String() string {

	return fmt.Sprintf("%s[C=%v,D=%v][Deleted?%v]",
		e.TxnMVCCNode.String(),
		e.CreatedAt,
		e.DeletedAt,
		e.Deleted)
}

// for create drop in one txn
func (e *DBMVCCNode) UpdateNode(vun txnbase.MVCCNode) {
	un := vun.(*DBMVCCNode)
	e.DeletedAt = un.DeletedAt
	e.Deleted = true
}

func (e *DBMVCCNode) ApplyUpdate(be *DBMVCCNode) (err error) {
	// if e.Deleted {
	// 	// TODO
	// }
	e.EntryMVCCNode = be.EntryMVCCNode.Clone()
	return
}

func (e *DBMVCCNode) ApplyDelete() (err error) {
	err = e.ApplyDeleteLocked()
	return
}

func (e *DBMVCCNode) Prepare2PCPrepare() (err error) {
	var ts types.TS
	ts, err = e.TxnMVCCNode.Prepare2PCPrepare()
	if err != nil {
		return
	}
	if e.CreatedAt.IsEmpty() {
		e.CreatedAt = ts
	}
	if e.Deleted {
		e.DeletedAt = ts
	}
	return
}

func (e *DBMVCCNode) PrepareCommit() (err error) {
	var ts types.TS
	ts, err = e.TxnMVCCNode.PrepareCommit()
	if err != nil {
		return
	}
	if e.CreatedAt.IsEmpty() {
		e.CreatedAt = ts
	}
	if e.Deleted {
		e.DeletedAt = ts
	}
	return
}

func (e *DBMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
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

func (e *DBMVCCNode) ReadFrom(r io.Reader) (n int64, err error) {
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
