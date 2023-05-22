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
	"context"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

var _ NodeT = (*persistedNode)(nil)

type persistedNode struct {
	common.RefHelper
	block *baseBlock
}

func newPersistedNode(block *baseBlock) *persistedNode {
	node := &persistedNode{
		block: block,
	}
	node.OnZeroCB = node.close
	return node
}

func (node *persistedNode) close() {}

func (node *persistedNode) Rows() uint32 {
	location := node.block.meta.GetMetaLoc()
	return uint32(ReadPersistedBlockRow(location))
}

func (node *persistedNode) BatchDedup(
	keys containers.Vector,
	keysZM index.ZM,
	skipFn func(row uint32) error,
	bf objectio.BloomFilter,
) (sels *roaring.Bitmap, err error) {
	panic("should not be called")
}

func (node *persistedNode) ContainsKey(key any) (ok bool, err error) {
	ctx := context.TODO()
	pkIndex, err := MakeImmuIndex(ctx, node.block.meta, nil, node.block.indexCache, node.block.fs.Service)
	if err != nil {
		return
	}
	if err = pkIndex.Dedup(ctx, key); err == nil {
		return
	}
	if !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
		return
	}
	ok = true
	err = nil
	return
}

func (node *persistedNode) GetColumnDataWindow(
	readSchema *catalog.Schema,
	from uint32,
	to uint32,
	col int,
) (vec containers.Vector, err error) {
	var data containers.Vector
	if data, err = node.block.LoadPersistedColumnData(readSchema, col); err != nil {
		return
	}
	if to-from == uint32(data.Length()) {
		vec = data
	} else {
		vec = data.CloneWindow(int(from), int(to-from), common.DefaultAllocator)
		data.Close()
	}
	return
}

func (node *persistedNode) Foreach(
	readSchema *catalog.Schema,
	colIdx int, op func(v any, isNull bool, row int) error, sel *roaring.Bitmap) (err error) {
	var data containers.Vector
	if data, err = node.block.LoadPersistedColumnData(
		readSchema,
		colIdx,
	); err != nil {
		return
	}
	return data.Foreach(op, sel)
}

func (node *persistedNode) GetDataWindow(
	readSchema *catalog.Schema, from, to uint32) (bat *containers.Batch, err error) {
	panic("to be implemented")
	// data, err := node.block.LoadPersistedData()
	// if err != nil {
	// 	return
	// }

	// if to-from == uint32(data.Length()) {
	// 	bat = data
	// } else {
	// 	bat = data.CloneWindow(int(from), int(to-from), common.DefaultAllocator)
	// 	data.Close()
	// }
	// return
}

func (node *persistedNode) IsPersisted() bool { return true }

func (node *persistedNode) PrepareAppend(rows uint32) (n uint32, err error) {
	panic(moerr.NewInternalErrorNoCtx("not supported"))
}

func (node *persistedNode) ApplyAppend(
	_ *containers.Batch,
	_ txnif.AsyncTxn,
) (from int, err error) {
	panic(moerr.NewInternalErrorNoCtx("not supported"))
}

func (node *persistedNode) GetValueByRow(_ *catalog.Schema, _, _ int) (v any, isNull bool) {
	panic(moerr.NewInternalErrorNoCtx("todo"))
}

func (node *persistedNode) GetRowsByKey(key any) ([]uint32, error) {
	panic(moerr.NewInternalErrorNoCtx("todo"))
}
