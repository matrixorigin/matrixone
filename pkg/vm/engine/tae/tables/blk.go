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
	"math/rand"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
)

type block struct {
	*baseBlock
}

func newBlock(
	meta *catalog.BlockEntry,
	rt *dbutils.Runtime,
) *block {
	blk := &block{}
	blk.baseBlock = newBaseBlock(blk, meta, rt)
	blk.mvcc.SetDeletesListener(blk.OnApplyDelete)
	pnode := newPersistedNode(blk.baseBlock)
	node := NewNode(pnode)
	node.Ref()
	blk.node.Store(node)
	return blk
}

func (blk *block) Init() (err error) {
	return
}

func (blk *block) OnApplyDelete(
	deleted uint64,
	gen common.RowGen,
	ts types.TS) (err error) {
	blk.meta.GetSegment().GetTable().RemoveRows(deleted)
	return
}

func (blk *block) PrepareCompact() bool {
	return blk.meta.PrepareCompact()
}

func (blk *block) FreezeAppend() {}

func (blk *block) Pin() *common.PinnedItem[*block] {
	blk.Ref()
	return &common.PinnedItem[*block]{
		Val: blk,
	}
}

func (blk *block) GetColumnDataByIds(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	colIdxes []int,
) (view *containers.BlockView, err error) {
	node := blk.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	return blk.ResolvePersistedColumnDatas(
		ctx, txn, schema, colIdxes, false,
	)
}

// GetColumnDataById Get the snapshot at txn's start timestamp of column data.
// Notice that for non-appendable block, if it is visible to txn,
// then all the block data pointed by meta location also be visible to txn;
func (blk *block) GetColumnDataById(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	col int,
) (view *containers.ColumnView, err error) {
	schema := readSchema.(*catalog.Schema)
	return blk.ResolvePersistedColumnData(
		ctx,
		txn,
		schema,
		col,
		false)
}
func (blk *block) CoarseCheckAllRowsCommittedBefore(ts types.TS) bool {
	blk.meta.RLock()
	defer blk.meta.RUnlock()
	return blk.meta.GetCreatedAt().Less(ts)
}

func (blk *block) BatchDedup(
	ctx context.Context,
	txn txnif.AsyncTxn,
	keys containers.Vector,
	keysZM index.ZM,
	rowmask *roaring.Bitmap,
	precommit bool,
	bf objectio.BloomFilter,
) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Infof("BatchDedup BLK-%s: %v", blk.meta.ID.String(), err)
		}
	}()
	return blk.PersistedBatchDedup(
		ctx,
		txn,
		precommit,
		keys,
		keysZM,
		rowmask,
		false,
		bf,
	)
}

func (blk *block) GetValue(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	row, col int) (v any, isNull bool, err error) {
	node := blk.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	return blk.getPersistedValue(
		ctx, txn, schema, row, col, false,
	)
}

func (blk *block) RunCalibration() (score int) {
	score, _ = blk.estimateRawScore()
	return
}

func (blk *block) estimateRawScore() (score int, dropped bool) {
	if blk.meta.HasDropCommitted() {
		dropped = true
		return
	}
	if blk.mvcc.GetChangeNodeCnt() == 0 {
		// No deletes found
		score = 0
	} else {
		// Any delete
		score = 1
	}

	// If any delete found and the table or database of the block had
	// been deleted. Force checkpoint the block
	if score > 0 {
		if _, terminated := blk.meta.GetTerminationTS(); terminated {
			score = 100
		}
	}
	return
}

func (blk *block) EstimateScore(ttl time.Duration, force bool) int {
	ttl = time.Duration(float64(ttl) * float64(rand.Intn(5)+10) / float64(10))
	return blk.adjustScore(blk.estimateRawScore, ttl, force)
}

func (blk *block) GetByFilter(
	ctx context.Context,
	txn txnif.AsyncTxn,
	filter *handle.Filter) (offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if blk.meta.GetSchema().SortKey == nil {
		rid := filter.Val.(types.Rowid)
		offset = rid.GetRowOffset()
		return
	}

	node := blk.PinNode()
	defer node.Unref()
	return blk.getPersistedRowByFilter(ctx, node.MustPNode(), txn, filter)
}

func (blk *block) getPersistedRowByFilter(
	ctx context.Context,
	pnode *persistedNode,
	txn txnif.TxnReader,
	filter *handle.Filter) (offset uint32, err error) {
	ok, err := pnode.ContainsKey(ctx, filter.Val)
	if err != nil {
		return
	}
	if !ok {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	var sortKey containers.Vector
	schema := blk.meta.GetSchema()
	idx := schema.GetSingleSortKeyIdx()
	if sortKey, err = blk.LoadPersistedColumnData(ctx, schema, idx); err != nil {
		return
	}
	defer sortKey.Close()
	off, existed := compute.GetOffsetByVal(sortKey, filter.Val, nil)
	if !existed {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	offset = uint32(off)

	blk.mvcc.RLock()
	defer blk.mvcc.RUnlock()
	deleted, err := blk.mvcc.IsDeletedLocked(offset, txn, blk.mvcc.RWMutex)
	if err != nil {
		return
	}
	if deleted {
		err = moerr.NewNotFoundNoCtx()
	}
	return
}
