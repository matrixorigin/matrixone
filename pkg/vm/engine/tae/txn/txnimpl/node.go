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
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

// node corresponds to an un-appendable standalone-uncommitted block
// which belongs to txn's workspace.
type node struct {
	*baseNode
	//uuid string
}

// NewNode creates a InsertNode object with data in S3/FS.
func NewNode(
	tbl *txnTable,
	fs *objectio.ObjectFS,
	indexCache model.LRUCache,
	sched tasks.TaskScheduler,
	meta *catalog.BlockEntry,
) *node {
	impl := new(node)
	impl.baseNode = newBaseNode(tbl, fs, indexCache, sched, meta)
	// impl.storage.pnode = newPersistedNode(impl.baseNode)
	// impl.storage.pnode.Ref()
	return impl
}

func (n *node) Close() error {
	// n.storage.pnode.close()
	return nil
}

func (n *node) IsPersisted() bool {
	return true
}

func (n *node) Append(data *containers.Batch, offset uint32) (appended uint32, err error) {
	panic("not supported")
}

func (n *node) RangeDelete(start uint32, end uint32) (err error) {
	if n.storage.pnode.deletes == nil {
		n.storage.pnode.deletes = roaring.New()
	}
	n.storage.pnode.deletes.AddRange(uint64(start), uint64(end))
	return
}

func (n *node) IsRowDeleted(row uint32) bool {
	return n.storage.pnode.deletes != nil &&
		n.storage.pnode.deletes.ContainsInt(int(row))
}

func (n *node) PrintDeletes() string {
	if n.storage.pnode.deletes == nil {
		return "NoDeletes"
	}
	return n.storage.pnode.deletes.String()

}

func (n *node) GetSpace() uint32 {
	panic("not supported ")
}

func (n *node) FillBlockView(
	view *model.BlockView,
	colIdxes []int) (err error) {
	//TODO::
	panic("not implemented yet ")
}

func (n *node) FillColumnView(*model.ColumnView) error {
	//TODO::
	panic("not implemented yet ")
}

func (n *node) Window(start, end uint32) (*containers.Batch, error) {
	//TODO::
	panic("not implemented yet ")
}

func (n *node) GetValue(col int, row uint32) (any, bool, error) {
	//TODO::
	panic("not implemented yet ")
}

func (n *node) AddApplyInfo(
	srcOff,
	srcLen,
	destOff,
	destLen uint32,
	dest *common.ID) *appendInfo {
	panic("not supported ")
}

func (n *node) RowsWithoutDeletes() uint32 {
	panic("not implemented yet ")
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

func (n *node) MakeCommand(_ uint32) (txnif.TxnCmd, error) {
	return nil, nil
}

func (n *node) GetColumnDataByIds([]int) (*model.BlockView, error) {
	//TODO::
	panic("not implemented yet ")
}

func (n *node) GetColumnDataById(idx int) (view *model.ColumnView, err error) {
	view = model.NewColumnView(idx)
	vec, err := n.LoadPersistedColumnData(idx)
	if err != nil {
		return
	}
	view.SetData(vec)
	return
}

func (n *node) Prefetch(idxes []uint16) error {
	key := n.meta.GetMetaLoc()
	return blockio.Prefetch(idxes, []uint16{key.ID()}, n.fs.Service, key)
}
