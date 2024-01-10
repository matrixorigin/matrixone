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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/dbutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
)

type AblkTempFilter struct {
	sync.RWMutex
	m map[types.Blockid]bool
}

func (f *AblkTempFilter) Add(id types.Blockid) {
	f.Lock()
	defer f.Unlock()
	f.m[id] = true
}

func (f *AblkTempFilter) Check(id types.Blockid) (skip bool) {
	f.Lock()
	defer f.Unlock()
	if _, ok := f.m[id]; ok {
		delete(f.m, id)
		return true
	}
	return false
}

var AblkTempF *AblkTempFilter

func init() {
	AblkTempF = &AblkTempFilter{
		m: make(map[types.Blockid]bool),
	}
}

type ablock struct {
	*baseBlock
	frozen atomic.Bool
}

func newABlock(
	meta *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) *ablock {
	blk := &ablock{}
	blk.baseBlock = newBaseBlock(blk, meta, rt)
	blk.getMVCC().SetDeletesListener(blk.OnApplyDelete)
	if blk.meta.HasDropCommitted() {
		pnode := newPersistedNode(blk.baseBlock)
		node := NewNode(pnode)
		node.Ref()
		blk.node.Store(node)
		blk.FreezeAppend()
	} else {
		mnode := newMemoryNode(blk.baseBlock)
		node := NewNode(mnode)
		node.Ref()
		blk.node.Store(node)
	}
	return blk
}

func (blk *ablock) OnApplyDelete(
	deleted uint64,
	ts types.TS) (err error) {
	blk.meta.GetTable().RemoveRows(deleted)
	return
}

func (blk *ablock) FreezeAppend() {
	blk.frozen.Store(true)
}

func (blk *ablock) IsAppendFrozen() bool {
	return blk.frozen.Load()
}

func (blk *ablock) IsAppendable() bool {
	if blk.IsAppendFrozen() {
		return false
	}
	node := blk.PinNode()
	defer node.Unref()
	if node.IsPersisted() {
		return false
	}
	return node.Rows() < blk.meta.GetSchema().BlockMaxRows
}

func (blk *ablock) PrepareCompactInfo() (result bool, reason string) {
	if n := blk.RefCount(); n > 0 {
		reason = fmt.Sprintf("entering refcount %d", n)
		return
	}
	blk.FreezeAppend()
	if !blk.meta.PrepareCompact() || !blk.appendMVCC.PrepareCompact() {
		if !blk.meta.PrepareCompact() {
			reason = "meta preparecomp false"
		} else {
			reason = "mvcc preparecomp false"
		}
		return
	}

	if n := blk.RefCount(); n != 0 {
		reason = fmt.Sprintf("ending refcount %d", n)
		return
	}
	return blk.RefCount() == 0, reason
}

func (blk *ablock) PrepareCompact() bool {
	// in aobj, there's only one blk.
	if AblkTempF.Check(*objectio.NewBlockidWithObjectID(&blk.meta.ID, 0)) {
		logutil.Infof("temp ablk filter skip blk %v", blk.meta.ID.String())
		return true
	}
	if blk.RefCount() > 0 {
		return false
	}
	blk.FreezeAppend()
	if !blk.meta.PrepareCompact() || !blk.appendMVCC.PrepareCompact() {
		return false
	}
	return blk.RefCount() == 0
}

func (blk *ablock) Pin() *common.PinnedItem[*ablock] {
	blk.Ref()
	return &common.PinnedItem[*ablock]{
		Val: blk,
	}
}

func (blk *ablock) GetColumnDataByIds(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	_ uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (view *containers.BlockView, err error) {
	return blk.resolveColumnDatas(
		ctx,
		txn,
		readSchema.(*catalog.Schema),
		colIdxes,
		false,
		mp,
	)
}

func (blk *ablock) GetColumnDataById(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	_ uint16,
	col int,
	mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	return blk.resolveColumnData(
		ctx,
		txn,
		readSchema.(*catalog.Schema),
		col,
		false,
		mp,
	)
}

func (blk *ablock) resolveColumnDatas(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	colIdxes []int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.BlockView, err error) {
	node := blk.PinNode()
	defer node.Unref()

	if !node.IsPersisted() {
		return node.MustMNode().resolveInMemoryColumnDatas(
			txn, readSchema, colIdxes, skipDeletes, mp,
		)
	} else {
		return blk.ResolvePersistedColumnDatas(
			ctx,
			txn,
			readSchema,
			0,
			colIdxes,
			skipDeletes,
			mp,
		)
	}
}

// check if all rows are committed before the specified ts
// here we assume that the ts is greater equal than the block's
// create ts and less than the block's delete ts
// it is a coarse-grained check
func (blk *ablock) CoarseCheckAllRowsCommittedBefore(ts types.TS) bool {
	// if the block is not frozen, always return false
	if !blk.IsAppendFrozen() {
		return false
	}

	node := blk.PinNode()
	defer node.Unref()

	// if the block is in memory, check with the in-memory node
	// it is a fine-grained check if the block is in memory
	if !node.IsPersisted() {
		return node.MustMNode().allRowsCommittedBefore(ts)
	}

	// always return false for if the block is persisted
	// it is a coarse-grained check
	return false
}

func (blk *ablock) resolveColumnData(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	col int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.ColumnView, err error) {
	node := blk.PinNode()
	defer node.Unref()

	if !node.IsPersisted() {
		return node.MustMNode().resolveInMemoryColumnData(
			txn, readSchema, col, skipDeletes, mp,
		)
	} else {
		return blk.ResolvePersistedColumnData(
			ctx,
			txn,
			readSchema,
			0,
			col,
			skipDeletes,
			mp,
		)
	}
}

func (blk *ablock) GetValue(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	_ uint16,
	row, col int,
	mp *mpool.MPool,
) (v any, isNull bool, err error) {
	node := blk.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	if !node.IsPersisted() {
		return node.MustMNode().getInMemoryValue(txn, schema, row, col, mp)
	} else {
		return blk.getPersistedValue(
			ctx, txn, schema, 0, row, col, true, mp,
		)
	}
}

// GetByFilter will read pk column, which seqnum will not change, no need to pass the read schema.
func (blk *ablock) GetByFilter(
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
	_, offset, err = node.GetRowByFilter(ctx, txn, filter, mp)
	return
}

func (blk *ablock) BatchDedup(
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
			logutil.Debugf("BatchDedup BLK-%s: %v", blk.meta.ID.String(), err)
		}
	}()
	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return node.BatchDedup(
			ctx,
			txn,
			precommit,
			keys,
			keysZM,
			rowmask,
			bf,
		)
	} else {
		return blk.PersistedBatchDedup(
			ctx,
			txn,
			precommit,
			keys,
			keysZM,
			rowmask,
			true,
			bf,
			mp,
		)
	}
}

func (blk *ablock) CollectAppendInRange(
	start, end types.TS,
	withAborted bool,
	mp *mpool.MPool,
) (*containers.BatchWithVersion, error) {
	node := blk.PinNode()
	defer node.Unref()
	return node.CollectAppendInRange(start, end, withAborted, mp)
}

func (blk *ablock) estimateRawScore() (score int, dropped bool) {
	if blk.meta.HasDropCommitted() {
		dropped = true
		return
	}
	blk.meta.RLock()
	atLeastOneCommitted := blk.meta.HasCommittedNode()
	blk.meta.RUnlock()
	if !atLeastOneCommitted {
		score = 1
		return
	}

	rows := blk.Rows()
	if rows == int(blk.meta.GetSchema().BlockMaxRows) {
		score = 100
		return
	}

	if blk.getMVCC().GetChangeIntentionCnt() == 0 && rows == 0 {
		score = 0
	} else {
		score = 1
	}

	if score > 0 {
		if _, terminated := blk.meta.GetTerminationTS(); terminated {
			score = 100
		}
	}
	return
}

func (blk *ablock) RunCalibration() (score int) {
	score, _ = blk.estimateRawScore()
	return
}

func (blk *ablock) OnReplayAppend(node txnif.AppendNode) (err error) {
	an := node.(*updates.AppendNode)
	blk.appendMVCC.OnReplayAppendNode(an)
	return
}

func (blk *ablock) OnReplayAppendPayload(bat *containers.Batch) (err error) {
	appender, err := blk.MakeAppender()
	if err != nil {
		return
	}
	_, err = appender.ReplayAppend(bat, nil)
	return
}

func (blk *ablock) MakeAppender() (appender data.BlockAppender, err error) {
	if blk == nil {
		err = moerr.GetOkExpectedEOB()
		return
	}
	appender = newAppender(blk)
	return
}

func (blk *ablock) Init() (err error) { return }

func (blk *ablock) EstimateMemSize() (int, int) {
	node := blk.PinNode()
	defer node.Unref()
	blk.RLock()
	defer blk.RUnlock()
	dsize := blk.getMVCC().EstimateMemSizeLocked()
	asize := blk.appendMVCC.EstimateMemSizeLocked()
	if !node.IsPersisted() {
		asize += node.MustMNode().EstimateMemSize()
	}
	return asize, dsize
}

func (blk *ablock) GetRowsOnReplay() uint64 {
	rows := uint64(blk.appendMVCC.GetTotalRow())
	fileRows := uint64(blk.meta.GetLatestCommittedNode().
		BaseNode.ObjectStats.Rows())
	if rows > fileRows {
		return rows
	}
	return fileRows
}
