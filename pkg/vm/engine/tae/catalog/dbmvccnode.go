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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type DBMVCCNode struct {
	*EntryMVCCNode
	*txnbase.TxnMVCCNode
}

func NewEmptyDBMVCCNode() txnif.MVCCNode {
	return &DBMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{},
		TxnMVCCNode:   &txnbase.TxnMVCCNode{},
	}
}

func CompareDBBaseNode(e, o txnif.MVCCNode) int {
	return e.(*DBMVCCNode).Compare(o.(*DBMVCCNode).TxnMVCCNode)
}

func (e *DBMVCCNode) CloneAll() txnif.MVCCNode {
	node := &DBMVCCNode{
		EntryMVCCNode: e.EntryMVCCNode.Clone(),
		TxnMVCCNode:   e.TxnMVCCNode.CloneAll(),
	}
	return node
}

func (e *DBMVCCNode) CloneData() txnif.MVCCNode {
	return &DBMVCCNode{
		EntryMVCCNode: e.EntryMVCCNode.CloneData(),
		TxnMVCCNode:   &txnbase.TxnMVCCNode{},
	}
}

func (e *DBMVCCNode) String() string {

	return fmt.Sprintf("%s%s",
		e.TxnMVCCNode.String(),
		e.EntryMVCCNode.String())
}

// for create drop in one txn
func (e *DBMVCCNode) Update(vun txnif.MVCCNode) {
	un := vun.(*DBMVCCNode)
	e.CreatedAt = un.CreatedAt
	e.DeletedAt = un.DeletedAt
}

func (e *DBMVCCNode) ApplyCommit(index *wal.Index) (err error) {
	var commitTS types.TS
	commitTS, err = e.TxnMVCCNode.ApplyCommit(index)
	if err != nil {
		return
	}
	e.EntryMVCCNode.ApplyCommit(commitTS)
	return nil
}
func (e *DBMVCCNode) ApplyRollback(index *wal.Index) (err error) {
	var commitTS types.TS
	commitTS, err = e.TxnMVCCNode.ApplyRollback(index)
	if err != nil {
		return
	}
	e.EntryMVCCNode.ApplyCommit(commitTS)
	return nil
}

func (e *DBMVCCNode) PrepareCommit() (err error) {
	_, err = e.TxnMVCCNode.PrepareCommit()
	if err != nil {
		return
	}
	err = e.EntryMVCCNode.PrepareCommit()
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
