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

package txnimpl

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

// node corresponds to an un-appendable block which belongs to txn's workspace.
type node struct {
	*baseNode
	uuid string
}

// NewNode creates a InsertNode object with data in S3/FS.
func NewNode(tbl *txnTable, meta *catalog.BlockEntry) *node {
	return nil
}

func (n *node) Close() error {
	for i, index := range n.storage.pnode.indexes {
		index.Close()
		n.storage.pnode.indexes[i] = nil
	}
	n.storage.pnode.indexes = nil
	return nil
}

func (n *node) Append(data *containers.Batch, offset uint32) (appended uint32, err error) {
	panic("not supported")
}

func (n *node) RangeDelete(start uint32, end uint32) error {
	//TODO::
	panic("not implemented yet ")
}

func (n *node) IsRowDeleted(row uint32) bool {
	//TODO::
	panic("not implemented yet ")
}

func (n *node) PrintDeletes() string {
	//TODO::
	panic("not implemented yet ")
}

func (n *node) FillBlockView(
	view *model.BlockView,
	buffers []*bytes.Buffer,
	colIdxes []int) (err error) {
	//TODO::
	panic("not implemented yet ")
}

func (n *node) FillColumnView(*model.ColumnView, *bytes.Buffer) error {
	//TODO::
	panic("not implemented yet ")
}

func (n *node) Window(start, end uint32) (*containers.Batch, error) {
	//TODO::
	panic("not implemented yet ")
}

func (n *node) GetValue(col int, row uint32) any {
	//TODO::
	panic("not implemented yet ")
}

func (n *node) AddApplyInfo(
	srcOff,
	srcLen,
	destOff,
	destLen uint32,
	dbid uint64,
	dest *common.ID) *appendInfo {
	panic("not supported ")
}

func (n *node) RowsWithoutDeletes() uint32 {
	panic("not supported ")
}

func (n *node) LengthWithDeletes(appended, toAppend uint32) uint32 {
	panic("not supported ")
}

func (n *node) OffsetWithDeletes(count uint32) uint32 {
	panic("not supported ")
}

func (n *node) GetAppends() []*appendInfo {
	panic("not supported ")
}

func (n *node) MakeCommand(uint32, bool) (txnif.TxnCmd, wal.LogEntry, error) {
	return nil, nil, nil
}
