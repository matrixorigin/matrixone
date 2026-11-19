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
	cnvector "github.com/matrixorigin/matrixone/pkg/container/vector"
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
	data         *containers.Batch
	appendSchema *catalog.Schema
	rows         uint32
	appends      []*appendInfo
	isTombstone  bool

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

func (n *anode) appendSourceSchema() *catalog.Schema {
	if n.appendSchema != nil {
		return n.appendSchema
	}
	return n.table.GetLocalSchema(n.isTombstone)
}

// bindAppendSchema gives the WAL payload the names visible at PrepareTS.
// Freeze can already have applied these rows using the local names. The
// vectors and reservations stay valid because both schemas use the same
// physical sequence numbers, in the same order and with the same types.
func (n *anode) bindAppendSchema(target *catalog.Schema) {
	source := n.appendSourceSchema()
	if target == nil || source == target || !sameAppendColumnLayout(source, target) ||
		walColumnsInSchemaOrder(n.data.Attrs, target) || !walColumnsInSchemaOrder(n.data.Attrs, source) {
		return
	}
	attrs := make([]string, len(source.ColDefs))
	nameIdx := make(map[string]int, len(attrs))
	for i, col := range source.ColDefs {
		name := target.ColDefs[target.SeqnumMap[col.SeqNum]].Name
		attrs[i] = name
		nameIdx[name] = i
	}
	n.data.Attrs, n.data.Nameidx = attrs, nameIdx
	n.appendSchema = target
}

func sameAppendColumnLayout(a, b *catalog.Schema) bool {
	if !a.IsSameColumns(b) {
		return false
	}
	for i, col := range a.ColDefs {
		other := b.ColDefs[i]
		if col.SeqNum != other.SeqNum || !col.Type.Eq(other.Type) {
			return false
		}
	}
	return true
}

// renameColumn keeps the workspace batch aligned with its transaction's schema.
// A rename preserves vector positions and the keys in the workspace index.
func (n *anode) renameColumn(oldName, newName string, pos int) {
	if n.data == nil || pos >= len(n.data.Attrs) || n.data.Attrs[pos] != oldName {
		return
	}
	if oldPos, exists := n.data.Nameidx[oldName]; !exists || oldPos != pos {
		return
	}
	if _, exists := n.data.Nameidx[newName]; exists {
		return
	}
	delete(n.data.Nameidx, oldName)
	n.data.Attrs[pos] = newName
	n.data.Nameidx[newName] = pos
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
	fakePKProvided := false
	for _, attr := range data.Attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		// if n.isTombstone {
		// 	if attr == objectio.TombstoneAttr_CommitTs_Attr || attr == catalog.AttrAborted {
		// 		continue
		// 	}
		// }
		def := schema.ColDefs[schema.GetColIdx(attr)]
		fakePKProvided = fakePKProvided || def.FakePK
		destVec := n.data.Vecs[def.Idx]
		// logutil.Infof("destVec: %s, %d, %d", destVec.String(), cnt, data.Length())
		if an > 0 {
			if err = destVec.UnionBatch(data.Vecs[def.Idx], int64(offset), int(an), nil, destVec.MPool()); err != nil {
				return
			}
		}
	}
	if schema.HasFakePK() && !fakePKProvided {
		fakePK := n.data.Vecs[schema.GetPrimaryKey().Idx]
		if err = cnvector.AppendMultiFixed(
			fakePK,
			uint64(0),
			false,
			int(an),
			fakePK.MPool(),
		); err != nil {
			return
		}
	}
	if err = n.FillPhyAddrColumn(from, an); err != nil {
		return
	}
	n.rows = uint32(n.data.Length())
	return
}

func (n *anode) FillPhyAddrColumn(startRow, length uint32) (err error) {
	if n.isTombstone {
		return
	}
	col := n.table.store.rt.VectorPool.Small.GetVector(&objectio.RowidType)
	blkID := objectio.NewBlockidWithObjectID(n.meta.ID(), 0)
	if err = objectio.ConstructRowidColumnTo(
		col,
		&blkID, startRow, length,
		col.MPool(),
	); err != nil {
		containers.ReleaseVector(col)
		return
	}
	dst := n.data.Vecs[n.table.GetLocalSchema(n.isTombstone).PhyAddrKey.Idx]
	if col.Length() > 0 {
		err = dst.UnionBatch(col, 0, col.Length(), nil, dst.MPool())
	}
	containers.ReleaseVector(col)
	return
}

func (n *anode) Compact() {
	if n.data == nil {
		return
	}
	n.data.Compact()
	n.rows = uint32(n.data.Length())
}

func (n *anode) GetValue(col int, row uint32) (any, bool, error) {
	vec := n.data.Vecs[col]
	return vec.GetValue(int(row)), vec.IsNull(uint64(int(row))), nil
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
		defer deletes.Free()
		src := data.Vecs[pos]
		if vec, err = src.CloneWindow(int(start), int(end), src.MPool()); err != nil {
			return
		}
		dels := make([]int64, 0, deletes.Count())
		deletes.Foreach(func(i uint64) bool {
			dels = append(dels, int64(i))
			return true
		})
		if err = vec.Shrink(dels, true, vec.MPool()); err != nil {
			vec.Free(vec.MPool())
			vec = nil
		}
		return
	}
	return data.Vecs[pos].Window(int(start), int(end))
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

func (n *anode) Scan(ctx context.Context, bat **containers.Batch, colIdxes []int, mp *mpool.MPool) error {
	if *bat == nil {
		newBat := containers.NewBatch()
		for _, colIdx := range colIdxes {
			orig := n.data.Vecs[colIdx]
			attr := n.data.Attrs[colIdx]
			vec, err := orig.CloneWindow(0, orig.Length(), mp)
			if err != nil {
				newBat.Close()
				return err
			}
			newBat.AddVector(attr, vec)
		}
		*bat = newBat
		return nil
	}
	for _, colIdx := range colIdxes {
		orig := n.data.Vecs[colIdx]
		if orig.Length() == 0 {
			continue
		}
		dst := (*bat).GetVectorByName(n.data.Attrs[colIdx])
		if err := dst.UnionBatch(orig, 0, orig.Length(), nil, dst.MPool()); err != nil {
			return err
		}
	}
	return nil
}

func (n *anode) Prefetch() error {
	return nil
}
