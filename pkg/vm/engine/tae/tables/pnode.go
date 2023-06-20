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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
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
	ctx context.Context,
	txn txnif.TxnReader,
	precommit bool,
	keys containers.Vector,
	keysZM index.ZM,
	rowmask *roaring.Bitmap,
	bf objectio.BloomFilter,
) (err error) {
	panic("should not be called")
}

func (node *persistedNode) ContainsKey(ctx context.Context, key any) (ok bool, err error) {
	pkIndex, err := MakeImmuIndex(ctx, node.block.meta, nil, node.block.rt)
	if err != nil {
		return
	}
	if err = pkIndex.Dedup(ctx, key, node.block.rt); err == nil {
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
	if data, err = node.block.LoadPersistedColumnData(context.Background(), readSchema, col); err != nil {
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
	ctx context.Context,
	readSchema *catalog.Schema,
	colIdx int, op func(v any, isNull bool, row int) error, sel *nulls.Bitmap) (err error) {
	var data containers.Vector
	if data, err = node.block.LoadPersistedColumnData(
		ctx,
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

// only used by tae only
// not to optimize it
func (node *persistedNode) GetRowByFilter(
	ctx context.Context,
	txn txnif.TxnReader,
	filter *handle.Filter,
) (row uint32, err error) {
	ok, err := node.ContainsKey(ctx, filter.Val)
	if err != nil {
		return
	}
	if !ok {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	// Note: sort key do not change
	schema := node.block.meta.GetSchema()
	sortKey, err := node.block.LoadPersistedColumnData(ctx, schema, schema.GetSingleSortKeyIdx())
	if err != nil {
		return
	}
	defer sortKey.Close()
	rows := make([]uint32, 0)
	err = sortKey.Foreach(func(v any, _ bool, offset int) error {
		if compute.CompareGeneric(v, filter.Val, sortKey.GetType().Oid) == 0 {
			row := uint32(offset)
			rows = append(rows, row)
			return nil
		}
		return nil
	}, nil)
	if err != nil && !moerr.IsMoErrCode(err, moerr.OkExpectedDup) {
		return
	}
	if len(rows) == 0 {
		err = moerr.NewNotFoundNoCtx()
		return
	}

	// Load persisted commit ts
	commitTSVec, err := node.block.LoadPersistedCommitTS()
	if err != nil {
		return
	}
	defer commitTSVec.Close()

	// Load persisted deletes
	view := containers.NewColumnView(0)
	if err = node.block.FillPersistedDeletes(ctx, txn, view.BaseView); err != nil {
		return
	}

	exist := false
	var deleted bool
	for _, offset := range rows {
		commitTS := commitTSVec.Get(int(offset)).(types.TS)
		if commitTS.Greater(txn.GetStartTS()) {
			break
		}
		deleted = view.IsDeleted(int(offset))
		if !deleted {
			exist = true
			row = offset
			break
		}
	}
	if !exist {
		err = moerr.NewNotFoundNoCtx()
	}
	return
}

func (node *persistedNode) CollectAppendInRange(
	start, end types.TS, withAborted bool,
) (bat *containers.BatchWithVersion, err error) {
	// logtail should have sent metaloc
	return nil, nil
}
