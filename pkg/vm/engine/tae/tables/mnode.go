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

package tables

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
)

var _ NodeT = (*memoryNode)(nil)

type memoryNode struct {
	common.RefHelper
	block  *baseBlock
	data   *containers.Batch
	prefix []byte

	pkIndex indexwrapper.Index
	indexes map[int]indexwrapper.Index
}

func newMemoryNode(block *baseBlock) *memoryNode {
	impl := new(memoryNode)
	impl.block = block
	impl.prefix = block.meta.MakeKey()

	schema := block.meta.GetSchema()
	opts := new(containers.Options)
	opts.Allocator = common.MutMemAllocator
	impl.data = containers.BuildBatch(
		schema.AllNames(),
		schema.AllTypes(),
		schema.AllNullables(),
		opts)
	impl.initIndexes(schema)
	impl.OnZeroCB = impl.close
	return impl
}

func (node *memoryNode) initIndexes(schema *catalog.Schema) {
	node.indexes = make(map[int]indexwrapper.Index)
	for _, def := range schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		if def.IsPrimary() {
			node.indexes[def.Idx] = indexwrapper.NewPkMutableIndex(def.Type)
			node.pkIndex = node.indexes[def.Idx]
		} else {
			node.indexes[def.Idx] = indexwrapper.NewMutableIndex(def.Type)
		}
	}
}

func (node *memoryNode) close() {
	logutil.Infof("Releasing Memorynode BLK-%d", node.block.meta.ID)
	node.data.Close()
	node.data = nil
	for i, index := range node.indexes {
		index.Close()
		node.indexes[i] = nil
	}
	node.indexes = nil
	node.pkIndex = nil
	node.block = nil
}

func (node *memoryNode) IsPersisted() bool { return false }

func (node *memoryNode) BatchDedup(
	keys containers.Vector,
	skipFn func(row uint32) error) (sels *roaring.Bitmap, err error) {
	return node.pkIndex.BatchDedup(keys, skipFn)
}

func (node *memoryNode) ContainsKey(key any) (ok bool, err error) {
	if err = node.pkIndex.Dedup(key, nil); err != nil {
		return
	}
	if !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
		return
	}
	ok = true
	err = nil
	return
}

func (node *memoryNode) GetValueByRow(row, col int) (v any) {
	return node.data.Vecs[col].Get(row)
}

func (node *memoryNode) GetRowsByKey(key any) (rows []uint32, err error) {
	return node.pkIndex.GetActiveRow(key)
}

func (node *memoryNode) Rows() uint32 {
	return uint32(node.data.Length())
}

func (node *memoryNode) GetColumnDataWindow(
	from uint32,
	to uint32,
	colIdx int,
	buffer *bytes.Buffer,
) (vec containers.Vector, err error) {
	data := node.data.Vecs[colIdx]
	if buffer != nil {
		data = data.Window(int(from), int(to-from))
		vec = containers.CloneWithBuffer(data, buffer, common.DefaultAllocator)
	} else {
		vec = data.CloneWindow(int(from), int(to-from), common.DefaultAllocator)
	}
	return
}

func (node *memoryNode) GetDataWindow(
	from, to uint32) (bat *containers.Batch, err error) {
	bat = node.data.CloneWindow(
		int(from),
		int(to-from),
		common.DefaultAllocator)
	return
}

func (node *memoryNode) PrepareAppend(rows uint32) (n uint32, err error) {
	left := node.block.meta.GetSchema().BlockMaxRows - uint32(node.data.Length())
	if left == 0 {
		err = moerr.NewInternalError("not appendable")
		return
	}
	if rows > left {
		n = left
	} else {
		n = rows
	}
	return
}

func (node *memoryNode) FillPhyAddrColumn(startRow, length uint32) (err error) {
	col, err := model.PreparePhyAddrData(
		catalog.PhyAddrColumnType,
		node.prefix,
		startRow,
		length)
	if err != nil {
		return
	}
	defer col.Close()
	vec := node.data.Vecs[node.block.meta.GetSchema().PhyAddrKey.Idx]
	vec.Extend(col)
	return
}

func (node *memoryNode) ApplyAppend(
	bat *containers.Batch,
	txn txnif.AsyncTxn) (from int, err error) {
	schema := node.block.meta.GetSchema()
	from = int(node.data.Length())
	for srcPos, attr := range bat.Attrs {
		def := schema.ColDefs[schema.GetColIdx(attr)]
		if def.IsPhyAddr() {
			continue
		}
		destVec := node.data.Vecs[def.Idx]
		destVec.Extend(bat.Vecs[srcPos])
	}
	if err = node.FillPhyAddrColumn(
		uint32(from),
		uint32(bat.Length())); err != nil {
		return
	}
	return
}
