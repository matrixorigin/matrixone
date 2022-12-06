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

// persistedInsertNode corresponds to an uncommitted-standalone-unappendable block
// whose data resides in FS/S3.
type persistedInsertNode struct {
	*baseNode
	uuid string
}

// NewPersistedInsertNode creates a InsertNode object with data in S3/FS.
func NewPersistedInsertNode(tbl *txnTable, meta *catalog.BlockEntry) *persistedInsertNode {
	return nil
}

func (n *persistedInsertNode) Close() error {
	for i, index := range n.storage.pnode.indexes {
		index.Close()
		n.storage.pnode.indexes[i] = nil
	}
	n.storage.pnode.indexes = nil
	return nil
}

func (n *persistedInsertNode) Append(data *containers.Batch, offset uint32) (appended uint32, err error) {
	panic("not supported")
}

func (n *persistedInsertNode) RangeDelete(start uint32, end uint32) error {
	//TODO::
	panic("not implemented yet ")
}

func (n *persistedInsertNode) IsRowDeleted(row uint32) bool {
	//TODO::
	panic("not implemented yet ")
}

func (n *persistedInsertNode) PrintDeletes() string {
	//TODO::
	panic("not implemented yet ")
}

func (n *persistedInsertNode) FillBlockView(
	view *model.BlockView,
	buffers []*bytes.Buffer,
	colIdxes []int) (err error) {
	//TODO::
	panic("not implemented yet ")
}

func (n *persistedInsertNode) FillColumnView(*model.ColumnView, *bytes.Buffer) error {
	//TODO::
	panic("not implemented yet ")
}

func (n *persistedInsertNode) Window(start, end uint32) (*containers.Batch, error) {
	//TODO::
	panic("not implemented yet ")
}

func (n *persistedInsertNode) GetValue(col int, row uint32) any {
	//TODO::
	panic("not implemented yet ")
}

func (n *persistedInsertNode) AddApplyInfo(
	srcOff,
	srcLen,
	destOff,
	destLen uint32,
	dbid uint64,
	dest *common.ID) *appendInfo {
	panic("not supported ")
}

func (n *persistedInsertNode) RowsWithoutDeletes() uint32 {
	panic("not supported ")
}

func (n *persistedInsertNode) LengthWithDeletes(appended, toAppend uint32) uint32 {
	panic("not supported ")
}

func (n *persistedInsertNode) OffsetWithDeletes(count uint32) uint32 {
	panic("not supported ")
}

func (n *persistedInsertNode) GetAppends() []*appendInfo {
	panic("not supported ")
}

func (n *persistedInsertNode) MakeCommand(uint32, bool) (txnif.TxnCmd, wal.LogEntry, error) {
	return nil, nil, nil
}
