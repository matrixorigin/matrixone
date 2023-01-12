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
	"fmt"
	"io"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type TableMVCCNode struct {
	*EntryMVCCNode
	*txnbase.TxnMVCCNode
	SchemaConstraints string // store as immutable, bytes actually
}

func NewEmptyTableMVCCNode() txnif.MVCCNode {
	return &TableMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{},
		TxnMVCCNode:   &txnbase.TxnMVCCNode{},
	}
}

func CompareTableBaseNode(e, o txnif.MVCCNode) int {
	return e.(*TableMVCCNode).Compare(o.(*TableMVCCNode).TxnMVCCNode)
}

func (e *TableMVCCNode) CloneAll() txnif.MVCCNode {
	node := &TableMVCCNode{}
	node.EntryMVCCNode = e.EntryMVCCNode.Clone()
	node.TxnMVCCNode = e.TxnMVCCNode.CloneAll()
	node.SchemaConstraints = e.SchemaConstraints
	return node
}

func (e *TableMVCCNode) CloneData() txnif.MVCCNode {
	return &TableMVCCNode{
		EntryMVCCNode:     e.EntryMVCCNode.CloneData(),
		TxnMVCCNode:       &txnbase.TxnMVCCNode{},
		SchemaConstraints: e.SchemaConstraints,
	}
}

func (e *TableMVCCNode) String() string {

	return fmt.Sprintf("%s%scstr[%d]",
		e.TxnMVCCNode.String(),
		e.EntryMVCCNode.String(),
		len(e.SchemaConstraints))
}

// for create drop in one txn
func (e *TableMVCCNode) Update(vun txnif.MVCCNode) {
	un := vun.(*TableMVCCNode)
	e.CreatedAt = un.CreatedAt
	e.DeletedAt = un.DeletedAt
	e.SchemaConstraints = un.SchemaConstraints
}

func (e *TableMVCCNode) ApplyCommit(index *wal.Index) (err error) {
	var commitTS types.TS
	commitTS, err = e.TxnMVCCNode.ApplyCommit(index)
	if err != nil {
		return
	}
	err = e.EntryMVCCNode.ApplyCommit(commitTS)
	return err
}
func (e *TableMVCCNode) ApplyRollback(index *wal.Index) (err error) {
	var commitTS types.TS
	commitTS, err = e.TxnMVCCNode.ApplyRollback(index)
	if err != nil {
		return
	}
	err = e.EntryMVCCNode.ApplyCommit(commitTS)
	return err
}
func (e *TableMVCCNode) PrepareCommit() (err error) {
	_, err = e.TxnMVCCNode.PrepareCommit()
	if err != nil {
		return
	}
	err = e.EntryMVCCNode.PrepareCommit()
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
	condata := []byte(e.SchemaConstraints)
	l := uint32(len(condata))
	if err = binary.Write(w, binary.BigEndian, l); err != nil {
		return
	}
	n += 4
	var n1 int
	if n1, err = w.Write(condata); err != nil {
		return
	}
	n += int64(n1)
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
		panic(moerr.NewInternalErrorNoCtx("logic err %d!=%d, %v", n2, length, err))
	}
	e.SchemaConstraints = string(buf)
	return
}
