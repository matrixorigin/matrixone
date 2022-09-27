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
	"sync/atomic"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
)

type appendableNode struct {
	block     *dataBlock
	data      *containers.Batch
	rows      uint32
	exception *atomic.Value
}

func newNode(mgr base.INodeManager, block *dataBlock, file file.Block) *appendableNode {
	impl := new(appendableNode)
	impl.exception = new(atomic.Value)
	impl.block = block
	//impl.rows = file.ReadRows(block.meta.GetMetaLoc())
	impl.rows = 0
	var err error
	schema := block.meta.GetSchema()
	opts := new(containers.Options)
	opts.Capacity = int(schema.BlockMaxRows)
	opts.Allocator = ImmutMemAllocator
	if impl.data, err = file.LoadBatch(
		schema.AllTypes(),
		schema.AllNames(),
		schema.AllNullables(),
		opts); err != nil {
		panic(err)
	}
	return impl
}

func (node *appendableNode) Rows(txn txnif.AsyncTxn, coarse bool) uint32 {
	if coarse {
		node.block.mvcc.RLock()
		defer node.block.mvcc.RUnlock()
		return node.rows
	}
	// TODO: fine row count
	// 1. Load txn ts zonemap
	// 2. Calculate fine row count
	return 0
}

func (node *appendableNode) CheckUnloadable() bool {
	return !node.block.mvcc.HasActiveAppendNode()
}

func (node *appendableNode) GetDataCopy(minRow, maxRow uint32) (columns *containers.Batch, err error) {
	if exception := node.exception.Load(); exception != nil {
		err = exception.(error)
		return
	}
	node.block.RLock()
	columns = node.data.CloneWindow(int(minRow), int(maxRow-minRow), containers.DefaultAllocator)
	node.block.RUnlock()
	return
}

func (node *appendableNode) GetColumnDataCopy(
	minRow uint32,
	maxRow uint32,
	colIdx int,
	buffer *bytes.Buffer) (vec containers.Vector, err error) {
	if exception := node.exception.Load(); exception != nil {
		err = exception.(error)
		return
	}
	node.block.RLock()
	if buffer != nil {
		win := node.data.Vecs[colIdx]
		if maxRow < uint32(node.data.Vecs[colIdx].Length()) {
			win = win.Window(int(minRow), int(maxRow))
		}
		vec = containers.CloneWithBuffer(win, buffer, containers.DefaultAllocator)
	} else {
		vec = node.data.Vecs[colIdx].CloneWindow(int(minRow), int(maxRow-minRow), containers.DefaultAllocator)
	}
	node.block.RUnlock()
	return
}

func (node *appendableNode) Close() (err error) {
	if node.data != nil {
		node.data.Close()
		node.data = nil
	}
	return
}

func (node *appendableNode) PrepareAppend(rows uint32) (n uint32, err error) {
	if exception := node.exception.Load(); exception != nil {
		logutil.Errorf("%v", exception)
		err = exception.(error)
		return
	}
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
	col, err := model.PreparePhyAddrData(catalog.PhyAddrColumnType, node.block.prefix, startRow, length)
	if err != nil {
		return
	}
	defer col.Close()
	vec := node.data.Vecs[node.block.meta.GetSchema().PhyAddrKey.Idx]
	node.block.Lock()
	vec.Extend(col)
	node.block.Unlock()
	return
}

func (node *appendableNode) ApplyAppend(bat *containers.Batch, txn txnif.AsyncTxn) (from int, err error) {
	if exception := node.exception.Load(); exception != nil {
		logutil.Errorf("%v", exception)
		err = exception.(error)
		return
	}
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
