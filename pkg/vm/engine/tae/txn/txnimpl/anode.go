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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

// anode corresponds to an appendable standalone-uncommitted block
// which belongs to txn's workspace and can be appended data into.
type anode struct {
	*baseNode
}

// NewANode creates a InsertNode with data in memory.
func NewANode(
	tbl *txnTable,
	meta *catalog.BlockEntry,
) *anode {
	impl := new(anode)
	impl.baseNode = newBaseNode(tbl, meta)
	impl.mnode = newMemoryNode(impl.baseNode)
	impl.mnode.Ref()
	return impl
}

func (n *anode) GetAppends() []*appendInfo {
	return n.mnode.appends
}
func (n *anode) AddApplyInfo(srcOff, srcLen, destOff, destLen uint32, dest *common.ID) *appendInfo {
	seq := len(n.mnode.appends)
	info := &appendInfo{
		dest:    *dest,
		destOff: destOff,
		destLen: destLen,
		srcOff:  srcOff,
		srcLen:  srcLen,
		seq:     uint32(seq),
	}
	n.mnode.appends = append(n.mnode.appends, info)
	return info
}

func (n *anode) IsPersisted() bool {
	return false
}

func (n *anode) MakeCommand(id uint32) (cmd txnif.TxnCmd, err error) {
	if n.IsPersisted() {
		return nil, nil
	}
	if n.mnode.data == nil {
		return
	}
	composedCmd := NewAppendCmd(id, n, n.mnode.data)
	return composedCmd, nil
}

func (n *anode) Close() (err error) {
	if n.mnode.data != nil {
		n.mnode.data.Close()
	}
	return
}

func (n *anode) Append(data *containers.Batch, offset uint32) (an uint32, err error) {
	schema := n.table.GetLocalSchema()
	if n.mnode.data == nil {
		opts := containers.Options{}
		opts.Capacity = data.Length() - int(offset)
		if opts.Capacity > int(MaxNodeRows) {
			opts.Capacity = int(MaxNodeRows)
		}
		n.mnode.data = containers.BuildBatch(schema.AllNames(), schema.AllTypes(), opts)
	}

	from := uint32(n.mnode.data.Length())
	an = n.mnode.PrepareAppend(data, offset)
	for _, attr := range data.Attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		def := schema.ColDefs[schema.GetColIdx(attr)]
		destVec := n.mnode.data.Vecs[def.Idx]
		// logutil.Infof("destVec: %s, %d, %d", destVec.String(), cnt, data.Length())
		destVec.ExtendWithOffset(data.Vecs[def.Idx], int(offset), int(an))
	}
	n.mnode.rows = uint32(n.mnode.data.Length())
	err = n.mnode.FillPhyAddrColumn(from, an)
	return
}

func (n *anode) FillBlockView(view *model.BlockView, colIdxes []int) (err error) {
	for _, colIdx := range colIdxes {
		orig := n.mnode.data.Vecs[colIdx]
		view.SetData(colIdx, orig.CloneWindow(0, orig.Length()))
	}
	view.DeleteMask = n.mnode.data.Deletes
	return
}
func (n *anode) FillColumnView(view *model.ColumnView) (err error) {
	orig := n.mnode.data.Vecs[view.ColIdx]
	view.SetData(orig.CloneWindow(0, orig.Length()))
	view.DeleteMask = n.mnode.data.Deletes
	return
}

func (n *anode) GetSpace() uint32 {
	return MaxNodeRows - n.mnode.rows
}

func (n *anode) RowsWithoutDeletes() uint32 {
	deletes := uint32(0)
	if n.mnode.data != nil && n.mnode.data.Deletes != nil {
		deletes = uint32(n.mnode.data.DeleteCnt())
	}
	return uint32(n.mnode.data.Length()) - deletes
}

func (n *anode) LengthWithDeletes(appended, toAppend uint32) uint32 {
	if !n.mnode.data.HasDelete() {
		return toAppend
	}
	appendedOffset := n.OffsetWithDeletes(appended)
	toAppendOffset := n.OffsetWithDeletes(toAppend + appended)
	// logutil.Infof("appened:%d, toAppend:%d, off1=%d, off2=%d", appended, toAppend, appendedOffset, toAppendOffset)
	return toAppendOffset - appendedOffset
}

func (n *anode) OffsetWithDeletes(count uint32) uint32 {
	if !n.mnode.data.HasDelete() {
		return count
	}
	offset := count
	for offset < n.mnode.rows {
		deletes := n.mnode.data.Deletes.Rank(offset)
		if offset == count+uint32(deletes) {
			break
		}
		offset = count + uint32(deletes)
	}
	return offset
}

func (n *anode) GetValue(col int, row uint32) (any, bool, error) {
	if !n.IsPersisted() {
		vec := n.mnode.data.Vecs[col]
		return vec.Get(int(row)), vec.IsNull(int(row)), nil
	}
	//TODO:: get value from S3/FS
	panic("not implemented yet :GetValue from FS/S3 ")
}

func (n *anode) RangeDelete(start, end uint32) error {
	n.mnode.data.RangeDelete(int(start), int(end+1))
	return nil
}

func (n *anode) IsRowDeleted(row uint32) bool {
	return n.mnode.data.IsDeleted(int(row))
}

func (n *anode) PrintDeletes() string {
	if !n.mnode.data.HasDelete() {
		return "NoDeletes"
	}
	return n.mnode.data.Deletes.String()
}

func (n *anode) WindowColumn(start, end uint32, pos int) (vec containers.Vector, err error) {
	data := n.mnode.data
	deletes := data.WindowDeletes(int(start), int(end-start))
	if deletes != nil {
		vec = data.Vecs[pos].CloneWindow(int(start), int(end-start))
		vec.Compact(deletes)
	} else {
		vec = data.Vecs[pos].Window(int(start), int(end-start))
	}
	return
}

func (n *anode) Window(start, end uint32) (bat *containers.Batch, err error) {
	data := n.mnode.data
	if data.HasDelete() {
		bat = data.CloneWindow(int(start), int(end-start))
		bat.Compact()
	} else {
		bat = data.Window(int(start), int(end-start))
	}
	return
}

func (n *anode) GetColumnDataByIds(
	colIdxes []int,
) (view *model.BlockView, err error) {
	if !n.IsPersisted() {
		view = model.NewBlockView()
		err = n.FillBlockView(view, colIdxes)
		return
	}
	panic("Not Implemented yet : GetColumnDataByIds from S3/FS ")
}

func (n *anode) GetColumnDataById(colIdx int) (view *model.ColumnView, err error) {
	if !n.IsPersisted() {
		view = model.NewColumnView(colIdx)
		err = n.FillColumnView(view)
		return
	}
	panic("Not Implemented yet : GetColumnDataByIds from S3/FS ")
}

func (n *anode) Prefetch(idxes []uint16) error {
	return nil
}
