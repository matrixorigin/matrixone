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

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

// anode corresponds to an appendable standalone-uncommitted block
// which belongs to txn's workspace and can be appended data into.
type anode struct {
	*baseNode
	data        *containers.Batch
	rows        uint32
	appends     []*appendInfo
	isTombstone bool

	isMergeCompact bool
}

// NewANode creates a InsertNode with data in memory.
func NewANode(
	tbl *txnTable,
	meta *catalog.ObjectEntry,
	isTombstone bool,
) *anode {
	impl := new(anode)
	impl.baseNode = newBaseNode(tbl, meta)
	impl.appends = make([]*appendInfo, 0)
	impl.isTombstone = isTombstone
	return impl
}

func (n *anode) Rows() uint32 {
	return n.rows
}

func (n *anode) GetAppends() []*appendInfo {
	return n.appends
}
func (n *anode) AddApplyInfo(srcOff, srcLen, destOff, destLen uint32, dest *common.ID) *appendInfo {
	seq := len(n.appends)
	info := &appendInfo{
		dest:    *dest,
		destOff: destOff,
		destLen: destLen,
		srcOff:  srcOff,
		srcLen:  srcLen,
		seq:     uint32(seq),
	}
	n.appends = append(n.appends, info)
	return info
}

func (n *anode) IsPersisted() bool {
	return false
}

func (n *anode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	if n.data == nil {
		return
	}
	composedCmd := NewAppendCmd(id, n, n.data, n.isTombstone)
	return composedCmd, nil
}

func (n *anode) Close() (err error) {
	if n.data != nil {
		n.data.Close()
		n.data = nil
	}
	return
}

func (n *anode) PrepareAppend(data *containers.Batch, offset uint32) uint32 {
	return uint32(data.Length()) - offset
}

func (n *anode) Append(data *containers.Batch, offset uint32) (an uint32, err error) {
	schema := n.table.GetLocalSchema(n.isTombstone)
	if n.data == nil {
		capacity := data.Length() - int(offset)
		n.data = containers.BuildBatchWithPool(
			schema.AllNames(),
			schema.AllTypes(),
			capacity,
			n.table.store.rt.VectorPool.Small,
		)

		// n.data = containers.BuildBatch(
		// 	schema.AllNames(),
		// 	schema.AllTypes(),
		// 	containers.Options{
		// 		Capacity: capacity,
		// 	},
		// )
	}

	from := uint32(n.data.Length())
	an = n.PrepareAppend(data, offset)
	for _, attr := range data.Attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		// if n.isTombstone {
		// 	if attr == catalog.AttrCommitTs || attr == catalog.AttrAborted {
		// 		continue
		// 	}
		// }
		def := schema.ColDefs[schema.GetColIdx(attr)]
		destVec := n.data.Vecs[def.Idx]
		// logutil.Infof("destVec: %s, %d, %d", destVec.String(), cnt, data.Length())
		destVec.ExtendWithOffset(data.Vecs[def.Idx], int(offset), int(an))
	}
	n.rows = uint32(n.data.Length())
	err = n.FillPhyAddrColumn(from, an)
	return
}

func (n *anode) FillPhyAddrColumn(startRow, length uint32) (err error) {
	if n.isTombstone {
		return
	}
	col := n.table.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	blkID := objectio.NewBlockidWithObjectID(n.meta.ID(), 0)
	if err = objectio.ConstructRowidColumnTo(
		col.GetDownstreamVector(),
		blkID, startRow, length,
		col.GetAllocator(),
	); err != nil {
		col.Close()
		return
	}
	err = n.data.Vecs[n.table.GetLocalSchema(n.isTombstone).PhyAddrKey.Idx].ExtendVec(col.GetDownstreamVector())
	col.Close()
	return
}

func (n *anode) FillBlockView(
	view *containers.Batch, colIdxes []int, mp *mpool.MPool,
) (err error) {
	for _, colIdx := range colIdxes {
		orig := n.data.Vecs[colIdx]
		view.AddVector(n.data.Attrs[colIdx], orig.CloneWindow(0, orig.Length(), mp))
	}
	view.Deletes = n.data.Deletes
	return
}

// func (n *anode) FillColumnView(view *containers.Batch, idx int, mp *mpool.MPool) (err error) {
// 	orig := n.data.GetVectorByName(n.table.schema.ColDefs[idx].Name)
// 	view.AddVector(n.table.schema.ColDefs[idx].Name, orig.CloneWindow(0, orig.Length(), mp))
// 	view.Deletes = n.data.Deletes
// 	return
// }

func (n *anode) Compact() {
	if n.data == nil {
		return
	}
	n.data.Compact()
	n.rows = uint32(n.data.Length())
}

func (n *anode) GetValue(col int, row uint32) (any, bool, error) {
	vec := n.data.Vecs[col]
	return vec.Get(int(row)), vec.IsNull(int(row)), nil
}

func (n *anode) RangeDelete(start, end uint32) error {
	n.data.RangeDelete(int(start), int(end+1))
	return nil
}

func (n *anode) IsRowDeleted(row uint32) bool {
	return n.data.IsDeleted(int(row))
}

func (n *anode) PrintDeletes() string {
	if !n.data.HasDelete() {
		return "NoDeletes"
	}
	return nulls.String(n.data.Deletes)
}

func (n *anode) WindowColumn(start, end uint32, pos int) (vec containers.Vector, err error) {
	data := n.data
	deletes := data.WindowDeletes(int(start), int(end-start), false)
	if deletes != nil {
		vec = data.Vecs[pos].CloneWindow(int(start), int(end-start))
		vec.CompactByBitmap(deletes)
	} else {
		vec = data.Vecs[pos].Window(int(start), int(end-start))
	}
	return
}

func (n *anode) Window(start, end uint32) (bat *containers.Batch, err error) {
	data := n.data
	if data.HasDelete() {
		bat = data.CloneWindow(int(start), int(end-start))
		bat.Compact()
	} else {
		bat = data.Window(int(start), int(end-start))
	}
	return
}

func (n *anode) Scan(ctx context.Context, bat **containers.Batch, colIdxes []int, mp *mpool.MPool) {
	if *bat == nil {
		*bat = containers.NewBatch()
		for _, colIdx := range colIdxes {
			orig := n.data.Vecs[colIdx]
			attr := n.data.Attrs[colIdx]
			(*bat).AddVector(attr, orig.CloneWindow(0, orig.Length(), mp))
		}
		return
	}
	for _, colIdx := range colIdxes {
		orig := n.data.Vecs[colIdx]
		attr := n.data.Attrs[colIdx]
		(*bat).GetVectorByName(attr).Extend(orig)
	}
}

func (n *anode) Prefetch(idxes []uint16) error {
	return nil
}
