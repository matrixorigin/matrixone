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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
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
	*baseObject
}

func newBlock(
	meta *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) *block {
	blk := &block{}
	blk.baseObject = newBaseBlock(blk, meta, rt)
	pnode := newPersistedNode(blk.baseObject)
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
	ts types.TS) (err error) {
	blk.meta.GetTable().RemoveRows(deleted)
	return
}

func (blk *block) PrepareCompact() bool {
	return blk.meta.PrepareCompact()
}

func (blk *block) PrepareCompactInfo() (result bool, reason string) {
	return blk.meta.PrepareCompact(), ""
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
	blkID uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (view *containers.BlockView, err error) {
	node := blk.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	return blk.ResolvePersistedColumnDatas(
		ctx, txn, schema, blkID, colIdxes, false, mp,
	)
}

// GetColumnDataById Get the snapshot at txn's start timestamp of column data.
// Notice that for non-appendable block, if it is visible to txn,
// then all the block data pointed by meta location also be visible to txn;
func (blk *block) GetColumnDataById(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	blkID uint16,
	col int,
	mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	schema := readSchema.(*catalog.Schema)
	return blk.ResolvePersistedColumnData(
		ctx,
		txn,
		schema,
		blkID,
		col,
		false,
		mp,
	)
}
func (blk *block) CoarseCheckAllRowsCommittedBefore(ts types.TS) bool {
	blk.meta.RLock()
	defer blk.meta.RUnlock()
	creatTS := blk.meta.GetCreatedAtLocked()
	return creatTS.Less(&ts)
}

func (blk *block) BatchDedup(
	ctx context.Context,
	txn txnif.AsyncTxn,
	keys containers.Vector,
	keysZM index.ZM,
	rowmask *roaring.Bitmap,
	precommit bool,
	bf objectio.BloomFilter,
	mp *mpool.MPool,
) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Infof("BatchDedup %s (%v)BLK-%s: %v",
				blk.meta.GetTable().GetLastestSchemaLocked().Name,
				blk.IsAppendable(),
				blk.meta.ID.String(),
				err)
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
		mp,
	)
}

func (blk *block) GetValue(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	blkID uint16,
	row, col int,
	mp *mpool.MPool,
) (v any, isNull bool, err error) {
	node := blk.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	return blk.getPersistedValue(
		ctx, txn, schema, blkID, row, col, false, mp,
	)
}

func (blk *block) RunCalibration() (score int, err error) {
	score, _ = blk.estimateRawScore()
	return
}

func (blk *block) estimateRawScore() (score int, dropped bool) {
	if blk.meta.HasDropCommitted() {
		dropped = true
		return
	}
	changeCnt := uint32(0)
	blk.RLock()
	objectMVCC := blk.tryGetMVCC()
	if objectMVCC != nil {
		changeCnt = objectMVCC.GetChangeIntentionCnt()
	}
	blk.RUnlock()
	if changeCnt == 0 {
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

func (blk *block) GetByFilter(
	ctx context.Context,
	txn txnif.AsyncTxn,
	filter *handle.Filter,
	mp *mpool.MPool,
) (blkID uint16, offset uint32, err error) {
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
	return blk.getPersistedRowByFilter(ctx, node.MustPNode(), txn, filter, mp)
}

func (blk *block) getPersistedRowByFilter(
	ctx context.Context,
	pnode *persistedNode,
	txn txnif.TxnReader,
	filter *handle.Filter,
	mp *mpool.MPool,
) (blkID uint16, offset uint32, err error) {
	var sortKey containers.Vector
	schema := blk.meta.GetSchema()
	idx := schema.GetSingleSortKeyIdx()
	objMVCC := blk.tryGetMVCC()
	for blkID = uint16(0); blkID < uint16(blk.meta.BlockCnt()); blkID++ {
		var ok bool
		ok, err = pnode.ContainsKey(ctx, filter.Val, uint32(blkID))
		if err != nil {
			return
		}
		if !ok {
			continue
		}
		if sortKey, err = blk.LoadPersistedColumnData(ctx, schema, idx, mp, blkID); err != nil {
			continue
		}
		defer sortKey.Close()
		off, existed := compute.GetOffsetByVal(sortKey, filter.Val, nil)
		if !existed {
			continue
		}
		offset = uint32(off)

		if objMVCC == nil {
			return
		}
		objMVCC.RLock()
		defer objMVCC.RUnlock()
		var deleted bool
		deleted, err = objMVCC.IsDeletedLocked(offset, txn, blkID)
		if err != nil {
			return
		}
		if deleted {
			continue
		}
		var deletes *nulls.Nulls
		deletes, err = blk.persistedCollectDeleteMaskInRange(ctx, blkID, types.TS{}, txn.GetStartTS(), mp)
		if err != nil {
			return
		}
		if !deletes.Contains(uint64(offset)) {
			return
		}

	}
	err = moerr.NewNotFoundNoCtx()
	return
}

func (blk *block) EstimateMemSize() (int, int) {
	node := blk.PinNode()
	defer node.Unref()
	blk.RLock()
	defer blk.RUnlock()
	objMVCC := blk.tryGetMVCC()
	if objMVCC == nil {
		return 0, 0
	}
	dsize := objMVCC.EstimateMemSizeLocked()
	return 0, dsize
}

func (blk *block) GetRowsOnReplay() uint64 {
	fileRows := uint64(blk.meta.GetLatestCommittedNode().
		BaseNode.ObjectStats.Rows())
	return fileRows
}
