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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type appendableNode struct {
	block  *dataBlock
	data   *containers.Batch
	rows   uint32
	prefix []byte
}

func newNode(block *dataBlock) *appendableNode {
	impl := new(appendableNode)
	impl.block = block
	impl.rows = 0
	impl.prefix = block.meta.MakeKey()

	schema := block.meta.GetSchema()
	opts := new(containers.Options)
	// opts.Capacity = int(schema.BlockMaxRows)
	opts.Allocator = common.MutMemAllocator
	impl.data = containers.BuildBatch(
		schema.AllNames(),
		schema.AllTypes(),
		schema.AllNullables(),
		opts)
	return impl
}

func (node *appendableNode) Rows() uint32 {
	node.block.mvcc.RLock()
	defer node.block.mvcc.RUnlock()
	return node.rows
}

func (node *appendableNode) getMemoryDataLocked(minRow, maxRow uint32) (bat *containers.Batch, err error) {
	bat = node.data.CloneWindow(int(minRow),
		int(maxRow-minRow),
		common.DefaultAllocator)
	return
}

func (node *appendableNode) getPersistedData(minRow, maxRow uint32) (bat *containers.Batch, err error) {
	schema := node.block.meta.GetSchema()
	opts := new(containers.Options)
	opts.Capacity = int(schema.BlockMaxRows)
	data := containers.NewBatch()
	var vec containers.Vector
	for i, col := range schema.ColDefs {
		vec, err = node.block.LoadColumnData(i, nil)
		if err != nil {
			return nil, err
		}
		data.AddVector(col.Name, vec)
	}
	if maxRow-minRow == uint32(data.Length()) {
		bat = data
	} else {
		bat = data.CloneWindow(int(minRow), int(maxRow-minRow), common.DefaultAllocator)
		data.Close()
	}
	return
}

func (node *appendableNode) getPersistedColumnData(
	minRow,
	maxRow uint32,
	colIdx int,
	buffer *bytes.Buffer,
) (vec containers.Vector, err error) {
	data, err := node.block.LoadColumnData(colIdx, buffer)
	if err != nil {
		return
	}
	if maxRow-minRow == uint32(data.Length()) {
		vec = data
	} else {
		vec = data.CloneWindow(int(minRow), int(maxRow-minRow), common.DefaultAllocator)
		data.Close()
	}
	return
}

func (node *appendableNode) getMemoryColumnDataLocked(
	minRow,
	maxRow uint32,
	colIdx int,
	buffer *bytes.Buffer,
) (vec containers.Vector, err error) {
	data := node.data.Vecs[colIdx]
	if buffer != nil {
		data = data.Window(int(minRow), int(maxRow-minRow))
		vec = containers.CloneWithBuffer(data, buffer, common.DefaultAllocator)
	} else {
		vec = data.CloneWindow(int(minRow), int(maxRow-minRow), common.DefaultAllocator)
	}
	return
}

func (node *appendableNode) GetData(minRow, maxRow uint32) (bat *containers.Batch, err error) {
	node.block.RLock()
	if node.data != nil {
		bat, err = node.getMemoryDataLocked(minRow, maxRow)
		node.block.RUnlock()
		return
	}
	node.block.RUnlock()
	bat, err = node.getPersistedData(minRow, maxRow)
	return
}

func (node *appendableNode) GetColumnData(
	minRow uint32,
	maxRow uint32,
	colIdx int,
	buffer *bytes.Buffer) (vec containers.Vector, err error) {
	node.block.RLock()
	if node.data != nil {
		vec, err = node.getMemoryColumnDataLocked(minRow, maxRow, colIdx, buffer)
		node.block.RUnlock()
		return
	}
	node.block.RUnlock()
	return node.getPersistedColumnData(minRow, maxRow, colIdx, buffer)
}

func (node *appendableNode) Close() (err error) {
	if node.data != nil {
		node.data.Close()
		node.data = nil
	}
	return
}

func (node *appendableNode) PrepareAppend(rows uint32) (n uint32, err error) {
	left := node.block.meta.GetSchema().BlockMaxRows - node.rows
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

func (node *appendableNode) FillPhyAddrColumn(startRow, length uint32) (err error) {
	col, err := model.PreparePhyAddrData(catalog.PhyAddrColumnType, node.prefix, startRow, length)
	if err != nil {
		return
	}
	defer col.Close()
	var vec containers.Vector
	node.block.Lock()
	if node.data == nil {
		node.block.Unlock()
		vec, err = node.block.LoadColumnData(node.block.meta.GetSchema().PhyAddrKey.Idx, nil)
		if err != nil {
			return
		}
	} else {
		vec = node.data.Vecs[node.block.meta.GetSchema().PhyAddrKey.Idx]
		node.block.Unlock()
	}
	vec.Extend(col)
	return
}

func (node *appendableNode) ApplyAppend(bat *containers.Batch, txn txnif.AsyncTxn) (from int, err error) {
	schema := node.block.meta.GetSchema()
	from = int(node.rows)
	for srcPos, attr := range bat.Attrs {
		def := schema.ColDefs[schema.GetColIdx(attr)]
		if def.IsPhyAddr() {
			continue
		}
		destVec := node.data.Vecs[def.Idx]
		node.block.Lock()
		destVec.Extend(bat.Vecs[srcPos])
		node.block.Unlock()
	}
	if err = node.FillPhyAddrColumn(uint32(from), uint32(bat.Length())); err != nil {
		return
	}
	node.rows += uint32(bat.Length())
	return
}
