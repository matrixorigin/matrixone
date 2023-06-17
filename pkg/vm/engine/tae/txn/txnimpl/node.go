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
	"context"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

// pnode corresponds to an un-appendable standalone-uncommitted block
// which belongs to txn's workspace.
type pnode struct {
	*baseNode
}

// newPNode creates a InsertNode object with data in S3/FS.
func newPNode(
	tbl *txnTable,
	meta *catalog.BlockEntry,
) *pnode {
	impl := new(pnode)
	impl.baseNode = newBaseNode(tbl, meta)
	return impl
}

func (n *pnode) Close() error {
	return nil
}

func (n *pnode) IsPersisted() bool {
	return true
}

func (n *pnode) Append(data *containers.Batch, offset uint32) (appended uint32, err error) {
	panic("not supported")
}

func (n *pnode) RangeDelete(start uint32, end uint32) (err error) {
	panic("not supported")
}

func (n *pnode) IsRowDeleted(row uint32) bool {
	panic("not supported")
}

func (n *pnode) PrintDeletes() string {
	panic("not supported")
}

func (n *pnode) GetSpace() uint32 {
	panic("not supported ")
}

func (n *pnode) FillBlockView(
	view *containers.BlockView,
	colIdxes []int) (err error) {
	//TODO::
	panic("not implemented yet ")
}

func (n *pnode) FillColumnView(*containers.ColumnView) error {
	//TODO::
	panic("not implemented yet ")
}

func (n *pnode) WindowColumn(start, end uint32, pos int) (containers.Vector, error) {
	//TODO::
	panic("not implemented yet ")
}

func (n *pnode) Window(start, end uint32) (*containers.Batch, error) {
	//TODO::
	panic("not implemented yet ")
}

func (n *pnode) GetValue(col int, row uint32) (any, bool, error) {
	//TODO::
	panic("not implemented yet ")
}

func (n *pnode) AddApplyInfo(
	srcOff,
	srcLen,
	destOff,
	destLen uint32,
	dest *common.ID) *appendInfo {
	panic("not supported ")
}

func (n *pnode) GetAppends() []*appendInfo {
	panic("not supported ")
}

func (n *pnode) MakeCommand(_ uint32) (txnif.TxnCmd, error) {
	return nil, nil
}

func (n *pnode) GetColumnDataByIds([]int) (*containers.BlockView, error) {
	//TODO::
	panic("not implemented yet ")
}

func (n *pnode) GetColumnDataById(ctx context.Context, idx int) (view *containers.ColumnView, err error) {
	view = containers.NewColumnView(idx)
	vec, err := n.LoadPersistedColumnData(ctx, idx)
	if err != nil {
		return
	}
	view.SetData(vec)
	return
}

func (n *pnode) Prefetch(idxes []uint16) error {
	key := n.meta.FastGetMetaLoc()
	return blockio.Prefetch(idxes, []uint16{key.ID()}, n.table.store.dataFactory.GetRuntime().Fs.Service, key)
}
