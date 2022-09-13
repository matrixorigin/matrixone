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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

type MetadataMVCCNode struct {
	*EntryMVCCNode
	*txnbase.TxnMVCCNode
	MetaLoc  string
	DeltaLoc string
}

func NewEmptyMetadataMVCCNode() txnbase.MVCCNode {
	return &MetadataMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{},
		TxnMVCCNode:   &txnbase.TxnMVCCNode{},
	}
}

func CompareMetaBaseNode(e, o txnbase.MVCCNode) int {
	return e.(*MetadataMVCCNode).Compare(o.(*MetadataMVCCNode).TxnMVCCNode)
}

func (e *MetadataMVCCNode) CloneAll() txnbase.MVCCNode {
	node := e.CloneData()
	node.(*MetadataMVCCNode).TxnMVCCNode = e.TxnMVCCNode.CloneAll()
	return node
}

func (e *MetadataMVCCNode) CloneData() txnbase.MVCCNode {
	return &MetadataMVCCNode{
		EntryMVCCNode: e.EntryMVCCNode.Clone(),
		TxnMVCCNode:   &txnbase.TxnMVCCNode{},
		MetaLoc:       e.MetaLoc,
		DeltaLoc:      e.DeltaLoc,
	}
}

func (e *MetadataMVCCNode) String() string {

	return fmt.Sprintf("%s[C=%v,D=%v][Loc1=%s,Loc2=%s][Deleted?%v]",
		e.TxnMVCCNode.String(),
		e.CreatedAt,
		e.DeletedAt,
		e.MetaLoc,
		e.DeltaLoc,
		e.Deleted)
}

// for create drop in one txn
func (e *MetadataMVCCNode) UpdateNode(vun txnbase.MVCCNode) {
	un := vun.(*MetadataMVCCNode)
	e.DeletedAt = un.DeletedAt
	e.Deleted = true
}

func (e *MetadataMVCCNode) ApplyUpdate(be *MetadataMVCCNode) (err error) {
	// if e.Deleted {
	// 	// TODO
	// }
	e.EntryMVCCNode = be.EntryMVCCNode.Clone()
	e.MetaLoc = be.MetaLoc
	e.DeltaLoc = be.DeltaLoc
	return
}

func (e *MetadataMVCCNode) ApplyDelete() (err error) {
	err = e.ApplyDeleteLocked()
	return
}

func (e *MetadataMVCCNode) Prepare2PCPrepare() (err error) {
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

func (e *MetadataMVCCNode) PrepareCommit() (err error) {
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

func (e *MetadataMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
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

func (e *MetadataMVCCNode) ReadFrom(r io.Reader) (n int64, err error) {
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
