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
	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
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
	block       *baseBlock
	writeSchema *catalog.Schema
	data        *containers.Batch
	prefix      []byte

	//index for primary key : Art tree + ZoneMap.
	pkIndex indexwrapper.Index
	//index for non-primary key : ZoneMap.
	indexes map[uint16]indexwrapper.Index
}

func newMemoryNode(block *baseBlock) *memoryNode {
	impl := new(memoryNode)
	impl.block = block
	impl.prefix = block.meta.MakeKey()

	// Get the lastest schema, it will not be modified, so just keep the pointer
	schema := block.meta.GetSchema()
	impl.writeSchema = schema
	opts := containers.Options{}
	opts.Allocator = common.MutMemAllocator
	impl.data = containers.BuildBatch(schema.AllNames(), schema.AllTypes(), opts)
	impl.initIndexes(schema)
	impl.OnZeroCB = impl.close
	return impl
}

func (node *memoryNode) initIndexes(schema *catalog.Schema) {
	node.indexes = make(map[uint16]indexwrapper.Index)
	for _, def := range schema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		if def.IsRealPrimary() {
			node.pkIndex = indexwrapper.NewPkMutableIndex(def.Type)
			node.indexes[def.SeqNum] = node.pkIndex
		} else {
			node.indexes[def.SeqNum] = indexwrapper.NewMutableIndex(def.Type)
		}
	}
}

func (node *memoryNode) close() {
	logutil.Infof("Releasing Memorynode BLK-%s", node.block.meta.ID.String())
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
	skipFn func(row uint32) error,
	zm []byte,
	bf objectio.BloomFilter,
) (sels *roaring.Bitmap, err error) {
	return node.pkIndex.BatchDedup(keys, skipFn, zm, bf)
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

func (node *memoryNode) GetValueByRow(readSchema *catalog.Schema, row, col int) (v any, isNull bool) {
	idx, ok := node.writeSchema.SeqnumMap[readSchema.ColDefs[col].SeqNum]
	if !ok {
		// TODO(aptend): use default value
		return nil, true
	}
	vec := node.data.Vecs[idx]
	return vec.Get(row), vec.IsNull(row)
}

func (node *memoryNode) Foreach(colIdx int, op func(v any, isNull bool, row int) error, sels *roaring.Bitmap) error {
	return node.data.Vecs[colIdx].Foreach(op, sels)
}

func (node *memoryNode) GetRowsByKey(key any) (rows []uint32, err error) {
	return node.pkIndex.GetActiveRow(key)
}

func (node *memoryNode) Rows() uint32 {
	return uint32(node.data.Length())
}

func (node *memoryNode) GetColumnDataWindow(
	readSchema *catalog.Schema,
	from uint32,
	to uint32,
	col int,
) (vec containers.Vector, err error) {
	idx, ok := node.writeSchema.SeqnumMap[readSchema.ColDefs[col].SeqNum]
	if !ok {
		return containers.FillConstVector(int(to-from), readSchema.ColDefs[col].Type, nil), nil
	}
	data := node.data.Vecs[idx]
	vec = data.CloneWindow(int(from), int(to-from), common.DefaultAllocator)
	return
}

func (node *memoryNode) GetDataWindowOnWriteSchema(
	from, to uint32) (bat *containers.BatchWithVersion, err error) {
	inner := node.data.CloneWindow(int(from), int(to-from), common.DefaultAllocator)
	bat = &containers.BatchWithVersion{
		Version:    node.writeSchema.Version,
		NextSeqnum: uint16(node.writeSchema.Extra.NextColSeqnum),
		Seqnums:    node.writeSchema.AllSeqnums(),
		Batch:      inner,
	}
	return
}

func (node *memoryNode) GetDataWindow(
	readSchema *catalog.Schema,
	from, to uint32) (bat *containers.Batch, err error) {
	// manually clone data
	bat = containers.NewBatchWithCapacity(len(readSchema.ColDefs))
	if node.data.Deletes != nil {
		bat.Deletes = common.BM32Window(bat.Deletes, int(from), int(to))
	}
	for _, col := range readSchema.ColDefs {
		idx, ok := node.writeSchema.SeqnumMap[col.SeqNum]
		var vec containers.Vector
		if !ok {
			vec = containers.FillConstVector(int(from-to), col.Type, nil)
		} else {
			vec = node.data.Vecs[idx].CloneWindow(int(from), int(to-from), common.DefaultAllocator)
		}
		bat.AddVector(col.Name, vec)
	}
	return
}

func (node *memoryNode) PrepareAppend(rows uint32) (n uint32, err error) {
	left := node.writeSchema.BlockMaxRows - uint32(node.data.Length())
	if left == 0 {
		err = moerr.NewInternalErrorNoCtx("not appendable")
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
	vec := node.data.Vecs[node.writeSchema.PhyAddrKey.Idx]
	vec.Extend(col)
	return
}

func (node *memoryNode) ApplyAppend(
	bat *containers.Batch,
	txn txnif.AsyncTxn) (from int, err error) {
	schema := node.writeSchema
	from = int(node.data.Length())
	for srcPos, attr := range bat.Attrs {
		def := schema.ColDefs[schema.GetColIdx(attr)]
		destVec := node.data.Vecs[def.Idx]
		destVec.Extend(bat.Vecs[srcPos])
	}
	return
}
