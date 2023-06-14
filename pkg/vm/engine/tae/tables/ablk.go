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
	"sync/atomic"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type ablock struct {
	*baseBlock
	frozen atomic.Bool
}

func newABlock(
	meta *catalog.BlockEntry,
	fs *objectio.ObjectFS,
	indexCache model.LRUCache,
	scheduler tasks.TaskScheduler) *ablock {
	blk := &ablock{}
	blk.baseBlock = newBaseBlock(blk, meta, indexCache, fs, scheduler)
	blk.mvcc.SetAppendListener(blk.OnApplyAppend)
	blk.mvcc.SetDeletesListener(blk.OnApplyDelete)
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

func (blk *ablock) OnApplyAppend(n txnif.AppendNode) (err error) {
	blk.meta.GetSegment().GetTable().AddRows(uint64(n.GetMaxRow() -
		n.GetStartRow()))
	return
}

func (blk *ablock) OnApplyDelete(
	deleted uint64,
	gen common.RowGen,
	ts types.TS) (err error) {
	blk.meta.GetSegment().GetTable().RemoveRows(deleted)
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

func (blk *ablock) PrepareCompact() bool {
	if blk.RefCount() > 0 {
		return false
	}
	blk.FreezeAppend()
	if !blk.meta.PrepareCompact() || !blk.mvcc.PrepareCompact() {
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
	colIdxes []int,
) (view *model.BlockView, err error) {
	return blk.resolveColumnDatas(
		ctx,
		txn,
		readSchema.(*catalog.Schema),
		colIdxes,
		false)
}

func (blk *ablock) GetColumnDataById(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	col int,
) (view *model.ColumnView, err error) {
	return blk.resolveColumnData(
		ctx,
		txn,
		readSchema.(*catalog.Schema),
		col,
		false)
}

func (blk *ablock) resolveColumnDatas(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	colIdxes []int,
	skipDeletes bool) (view *model.BlockView, err error) {
	node := blk.PinNode()
	defer node.Unref()

	if !node.IsPersisted() {
		return blk.resolveInMemoryColumnDatas(
			node.MustMNode(),
			txn,
			readSchema,
			colIdxes,
			skipDeletes)
	} else {
		return blk.ResolvePersistedColumnDatas(
			ctx,
			node.MustPNode(),
			txn,
			readSchema,
			colIdxes,
			skipDeletes,
		)
	}
}

func (blk *ablock) DataCommittedBefore(ts types.TS) bool {
	if !blk.IsAppendFrozen() {
		return false
	}
	blk.RLock()
	defer blk.RUnlock()
	return blk.mvcc.LastAnodeCommittedBeforeLocked(ts)
}

func (blk *ablock) resolveColumnData(
	ctx context.Context,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	col int,
	skipDeletes bool) (view *model.ColumnView, err error) {
	node := blk.PinNode()
	defer node.Unref()

	if !node.IsPersisted() {
		return blk.resolveInMemoryColumnData(
			node.MustMNode(),
			txn,
			readSchema,
			col,
			skipDeletes)
	} else {
		return blk.ResolvePersistedColumnData(
			ctx,
			txn,
			readSchema,
			col,
			skipDeletes,
		)
	}
}

// Note: With PinNode Context
func (blk *ablock) resolveInMemoryColumnDatas(
	mnode *memoryNode,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	colIdxes []int,
	skipDeletes bool) (view *model.BlockView, err error) {
	blk.RLock()
	defer blk.RUnlock()
	maxRow, visible, deSels, err := blk.mvcc.GetVisibleRowLocked(txn)
	if !visible || err != nil {
		// blk.RUnlock()
		return
	}
	data, err := mnode.GetDataWindow(readSchema, 0, maxRow)
	if err != nil {
		return
	}
	view = model.NewBlockView()
	for _, colIdx := range colIdxes {
		view.SetData(colIdx, data.Vecs[colIdx])
	}
	if skipDeletes {
		// blk.RUnlock()
		return
	}

	err = blk.FillInMemoryDeletesLocked(txn, view.BaseView, blk.RWMutex)
	// blk.RUnlock()
	if err != nil {
		return
	}
	if !deSels.IsEmpty() {
		if view.DeleteMask != nil {
			view.DeleteMask.Or(deSels)
		} else {
			view.DeleteMask = deSels
		}
	}
	return
}

// Note: With PinNode Context
func (blk *ablock) resolveInMemoryColumnData(
	mnode *memoryNode,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	col int,
	skipDeletes bool) (view *model.ColumnView, err error) {
	blk.RLock()
	defer blk.RUnlock()
	maxRow, visible, deSels, err := blk.mvcc.GetVisibleRowLocked(txn)
	if !visible || err != nil {
		// blk.RUnlock()
		return
	}

	view = model.NewColumnView(col)
	var data containers.Vector
	data, err = mnode.GetColumnDataWindow(
		readSchema,
		0,
		maxRow,
		col)
	if err != nil {
		// blk.RUnlock()
		return
	}
	view.SetData(data)
	if skipDeletes {
		// blk.RUnlock()
		return
	}

	err = blk.FillInMemoryDeletesLocked(txn, view.BaseView, blk.RWMutex)
	// blk.RUnlock()
	if err != nil {
		return
	}
	if deSels != nil && !deSels.IsEmpty() {
		if view.DeleteMask != nil {
			view.DeleteMask.Or(deSels)
		} else {
			view.DeleteMask = deSels
		}
	}

	return
}

func (blk *ablock) GetValue(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	row, col int) (v any, isNull bool, err error) {
	node := blk.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	if !node.IsPersisted() {
		return blk.getInMemoryValue(node.MustMNode(), txn, schema, row, col)
	} else {
		return blk.getPersistedValue(
			ctx,
			node.MustPNode(),
			txn,
			schema,
			row,
			col,
			true)
	}
}

// With PinNode Context
func (blk *ablock) getInMemoryValue(
	mnode *memoryNode,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	row, col int) (v any, isNull bool, err error) {
	blk.RLock()
	deleted, err := blk.mvcc.IsDeletedLocked(uint32(row), txn, blk.RWMutex)
	blk.RUnlock()
	if err != nil {
		return
	}
	if deleted {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	view, err := blk.resolveInMemoryColumnData(mnode, txn, readSchema, col, true)
	if err != nil {
		return
	}
	defer view.Close()
	v, isNull = view.GetValue(row)
	//switch val := v.(type) {
	//case []byte:
	//	myVal := make([]byte, len(val))
	//	copy(myVal, val)
	//	v = myVal
	//}
	return
}

// GetByFilter will read pk column, which seqnum will not change, no need to pass the read schema.
func (blk *ablock) GetByFilter(
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
	return node.GetRowByFilter(ctx, txn, filter)
}

func (blk *ablock) BatchDedup(
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
		)
	}
}

func (blk *ablock) CollectAppendInRange(
	start, end types.TS,
	withAborted bool) (*containers.BatchWithVersion, error) {
	node := blk.PinNode()
	defer node.Unref()
	return node.CollectAppendInRange(start, end, withAborted)
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

	if blk.mvcc.GetChangeNodeCnt() == 0 && rows == 0 {
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

func (blk *ablock) EstimateScore(ttl time.Duration, force bool) int {
	return blk.adjustScore(blk.estimateRawScore, ttl, force)
}

func (blk *ablock) OnReplayAppend(node txnif.AppendNode) (err error) {
	an := node.(*updates.AppendNode)
	blk.mvcc.OnReplayAppendNode(an)
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
