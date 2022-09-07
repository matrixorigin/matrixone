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
)

const (
	AttrMetadata int16 = iota
	AttrTable
	AttrDB
)

func init() {
	txnif.RegisterAttrFactory(AttrMetadata, func() txnif.Attr {
		return NewEmptyMetadataMVCCNode()
	})
	txnif.RegisterAttrFactory(AttrTable, func() txnif.Attr {
		return NewEmptyTableMVCCNode()
	})
	txnif.RegisterAttrFactory(AttrDB, func() txnif.Attr {
		return NewEmptyDBMVCCNode()
	})
}

func (e *TableMVCCNode) GetType() int16 {
	return AttrTable
}
func (e *MetadataMVCCNode) GetType() int16 {
	return AttrMetadata
}
func (e *DBMVCCNode) GetType() int16 {
	return AttrDB
}

type TableMVCCNode struct {
	*EntryMVCCNode
}

func NewEmptyTableMVCCNode() *TableMVCCNode {
	return &TableMVCCNode{
		EntryMVCCNode: &EntryMVCCNode{},
	}
}

func (e *TableMVCCNode) Clone() txnif.Attr {
	return &TableMVCCNode{
		EntryMVCCNode: e.EntryMVCCNode.Clone(),
	}
}

func (e *TableMVCCNode) String() string {
	return fmt.Sprintf("[C=%v,D=%v][Deleted?%v]",
		e.CreatedAt,
		e.DeletedAt,
		e.Deleted,
	)
}

// for create drop in one txn
func (e *TableMVCCNode) UpdateNode(vun txnif.Attr) {
	un := vun.(*TableMVCCNode)
	e.DeletedAt = un.DeletedAt
	e.Deleted = true
}

func (e *TableMVCCNode) ApplyUpdate(be *TableMVCCNode) (err error) {
	e.EntryMVCCNode = be.EntryMVCCNode.Clone()
	return
}

func (e *TableMVCCNode) ApplyDelete() (err error) {
	err = e.ApplyDeleteLocked()
	return
}

func (e *TableMVCCNode) Prepare2PCPrepare(ts types.TS) (err error) {
	if e.CreatedAt.IsEmpty() {
		e.CreatedAt = ts
	}
	if e.Deleted {
		e.DeletedAt = ts
	}
	return
}

func (e *TableMVCCNode) PrepareCommit(ts types.TS) (err error) {
	if e.CreatedAt.IsEmpty() {
		e.CreatedAt = ts
	}
	if e.Deleted {
		e.DeletedAt = ts
	}
	return
}

func (e *TableMVCCNode) WriteTo(w io.Writer) (n int64, err error) {
	var sn int64
	sn, err = e.EntryMVCCNode.WriteTo(w)
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
	return
}
