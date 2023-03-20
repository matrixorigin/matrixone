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
	"time"

	"sync/atomic"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
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
	bufMgr base.INodeManager,
	scheduler tasks.TaskScheduler) *ablock {
	blk := &ablock{}
	blk.baseBlock = newBaseBlock(blk, meta, bufMgr, fs, scheduler)
	blk.mvcc.SetAppendListener(blk.OnApplyAppend)
	blk.mvcc.SetDeletesListener(blk.OnApplyDelete)
	if blk.meta.HasDropCommitted() {
		pnode := newPersistedNode(blk.baseBlock)
		node := NewNode(pnode)
		node.Ref()
		blk.node.Store(node)
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
	if !blk.meta.PrepareCompact() {
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

func (blk *ablock) GetColumnDataByNames(
	txn txnif.AsyncTxn,
	attrs []string,
) (view *model.BlockView, err error) {
	colIdxes := make([]int, len(attrs))
	for i, attr := range attrs {
		colIdxes[i] = blk.meta.GetSchema().GetColIdx(attr)
	}
	return blk.GetColumnDataByIds(txn, colIdxes)
}

func (blk *ablock) GetColumnDataByName(
	txn txnif.AsyncTxn,
	attr string,
) (view *model.ColumnView, err error) {
	colIdx := blk.meta.GetSchema().GetColIdx(attr)
	return blk.GetColumnDataById(txn, colIdx)
}

func (blk *ablock) GetColumnDataByIds(
	txn txnif.AsyncTxn,
	colIdxes []int,
) (view *model.BlockView, err error) {
	return blk.resolveColumnDatas(
		txn.GetStartTS(),
		colIdxes,
		false)
}

func (blk *ablock) GetColumnDataById(
	txn txnif.AsyncTxn,
	colIdx int,
) (view *model.ColumnView, err error) {
	return blk.resolveColumnData(
		txn.GetStartTS(),
		colIdx,
		false)
}

func (blk *ablock) resolveColumnDatas(
	ts types.TS,
	colIdxes []int,
	skipDeletes bool) (view *model.BlockView, err error) {
	node := blk.PinNode()
	defer node.Unref()

	if !node.IsPersisted() {
		return blk.resolveInMemoryColumnDatas(
			node.MustMNode(),
			ts,
			colIdxes,
			skipDeletes)
	} else {
		return blk.ResolvePersistedColumnDatas(
			node.MustPNode(),
			ts,
			colIdxes,
			skipDeletes,
		)
	}
}

func (blk *ablock) resolveColumnData(
	ts types.TS,
	colIdx int,
	skipDeletes bool) (view *model.ColumnView, err error) {
	node := blk.PinNode()
	defer node.Unref()

	if !node.IsPersisted() {
		return blk.resolveInMemoryColumnData(
			node.MustMNode(),
			ts,
			colIdx,
			skipDeletes)
	} else {
		return blk.ResolvePersistedColumnData(
			node.MustPNode(),
			ts,
			colIdx,
			skipDeletes,
		)
	}
}

// Note: With PinNode Context
func (blk *ablock) resolveInMemoryColumnDatas(
	mnode *memoryNode,
	ts types.TS,
	colIdxes []int,
	skipDeletes bool) (view *model.BlockView, err error) {
	blk.RLock()
	defer blk.RUnlock()
	maxRow, visible, deSels, err := blk.mvcc.GetVisibleRowLocked(ts)
	if !visible || err != nil {
		// blk.RUnlock()
		return
	}

	data, err := mnode.GetDataWindow(0, maxRow)
	if err != nil {
		return
	}
	view = model.NewBlockView(ts)
	for _, colIdx := range colIdxes {
		view.SetData(colIdx, data.Vecs[colIdx])
	}
	if skipDeletes {
		// blk.RUnlock()
		return
	}

	err = blk.FillInMemoryDeletesLocked(view.BaseView, blk.RWMutex)
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

// Note: With PinNode Context
func (blk *ablock) resolveInMemoryColumnData(
	mnode *memoryNode,
	ts types.TS,
	colIdx int,
	skipDeletes bool) (view *model.ColumnView, err error) {
	blk.RLock()
	defer blk.RUnlock()
	maxRow, visible, deSels, err := blk.mvcc.GetVisibleRowLocked(ts)
	if !visible || err != nil {
		// blk.RUnlock()
		return
	}

	view = model.NewColumnView(ts, colIdx)
	var data containers.Vector
	data, err = mnode.GetColumnDataWindow(
		0,
		maxRow,
		colIdx)
	if err != nil {
		// blk.RUnlock()
		return
	}
	view.SetData(data)
	if skipDeletes {
		// blk.RUnlock()
		return
	}

	err = blk.FillInMemoryDeletesLocked(view.BaseView, blk.RWMutex)
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
	txn txnif.AsyncTxn,
	row, col int) (v any, err error) {
	ts := txn.GetStartTS()
	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return blk.getInMemoryValue(node.MustMNode(), ts, row, col)
	} else {
		return blk.getPersistedValue(
			node.MustPNode(),
			ts,
			row,
			col,
			true)
	}
}

// With PinNode Context
func (blk *ablock) getInMemoryValue(
	mnode *memoryNode,
	ts types.TS,
	row, col int) (v any, err error) {
	blk.RLock()
	deleted, err := blk.mvcc.IsDeletedLocked(uint32(row), ts, blk.RWMutex)
	blk.RUnlock()
	if err != nil {
		return
	}
	if deleted {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	view, err := blk.resolveInMemoryColumnData(mnode, ts, col, true)
	if err != nil {
		return
	}
	defer view.Close()
	v = view.GetValue(row)
	return
}

func (blk *ablock) GetByFilter(
	txn txnif.AsyncTxn,
	filter *handle.Filter) (offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if blk.meta.GetSchema().SortKey == nil {
		_, _, offset = model.DecodePhyAddrKeyFromValue(filter.Val)
		return
	}
	ts := txn.GetStartTS()

	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return blk.getInMemoryRowByFilter(node.MustMNode(), ts, filter)
	} else {
		return blk.getPersistedRowByFilter(node.MustPNode(), ts, filter)
	}
}

func (blk *ablock) getPersistedRowByFilter(
	pnode *persistedNode,
	ts types.TS,
	filter *handle.Filter) (row uint32, err error) {
	ok, err := pnode.ContainsKey(filter.Val)
	if err != nil {
		return
	}
	if !ok {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	sortKey, err := blk.LoadPersistedColumnData(
		blk.meta.GetSchema().GetSingleSortKeyIdx())
	if err != nil {
		return
	}
	defer sortKey.Close()
	rows := make([]uint32, 0)
	err = sortKey.ForeachShallow(func(v any, offset int) error {
		if compute.CompareGeneric(v, filter.Val, sortKey.GetType()) == 0 {
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
	commitTSVec, err := blk.LoadPersistedCommitTS()
	if err != nil {
		return
	}
	defer commitTSVec.Close()

	// Load persisted deletes
	view := model.NewColumnView(ts, 0)
	if err = blk.FillPersistedDeletes(view.BaseView); err != nil {
		return
	}

	exist := false
	var deleted bool
	for _, offset := range rows {
		commitTS := commitTSVec.Get(int(offset)).(types.TS)
		if commitTS.Greater(ts) {
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

// With PinNode Context
func (blk *ablock) getInMemoryRowByFilter(
	mnode *memoryNode,
	ts types.TS,
	filter *handle.Filter) (row uint32, err error) {
	blk.RLock()
	defer blk.RUnlock()
	rows, err := mnode.GetRowsByKey(filter.Val)
	if err != nil && !moerr.IsMoErrCode(err, moerr.ErrNotFound) {
		return
	}

	waitFn := func(n *updates.AppendNode) {
		txn := n.Txn
		if txn != nil {
			blk.RUnlock()
			txn.GetTxnState(true)
			blk.RLock()
		}
	}
	if anyWaitable := blk.mvcc.CollectUncommittedANodesPreparedBefore(
		ts,
		waitFn); anyWaitable {
		rows, err = mnode.GetRowsByKey(filter.Val)
		if err != nil {
			return
		}
	}

	for i := len(rows) - 1; i >= 0; i-- {
		row = rows[i]
		appendnode := blk.mvcc.GetAppendNodeByRow(row)
		needWait, txn := appendnode.NeedWaitCommitting(ts)
		if needWait {
			blk.RUnlock()
			txn.GetTxnState(true)
			blk.RLock()
		}
		if appendnode.IsAborted() || !appendnode.IsVisible(ts) {
			continue
		}
		var deleted bool
		deleted, err = blk.mvcc.IsDeletedLocked(row, ts, blk.mvcc.RWMutex)
		if err != nil {
			return
		}
		if !deleted {
			return
		}
	}
	return 0, moerr.NewNotFoundNoCtx()
}

func (blk *ablock) checkConflictAndDupClosure(
	dedupTS types.TS,
	conflictTS types.TS,
	dupRow *uint32,
	rowmask *roaring.Bitmap) func(row uint32) error {
	return func(row uint32) (err error) {
		if rowmask != nil && rowmask.Contains(row) {
			return nil
		}
		appendnode := blk.mvcc.GetAppendNodeByRow(row)
		needWait, txn := appendnode.NeedWaitCommitting(dedupTS)
		if needWait {
			blk.mvcc.RUnlock()
			txn.GetTxnState(true)
			blk.mvcc.RLock()
		}
		if err = appendnode.CheckConflict(conflictTS); err != nil {
			return
		}
		if appendnode.IsAborted() || !appendnode.IsVisible(dedupTS) {
			return nil
		}
		deleteNode := blk.mvcc.GetDeleteNodeByRow(row)
		if deleteNode == nil {
			*dupRow = row
			return moerr.GetOkExpectedDup()
		}
		needWait, txn = deleteNode.NeedWaitCommitting(dedupTS)
		if needWait {
			blk.mvcc.RUnlock()
			txn.GetTxnState(true)
			blk.mvcc.RLock()
		}
		if err = deleteNode.CheckConflict(conflictTS); err != nil {
			return
		}
		if deleteNode.IsAborted() || !deleteNode.IsVisible(dedupTS) {
			return moerr.GetOkExpectedDup()
		}
		return nil
	}
}

func (blk *ablock) inMemoryBatchDedup(
	mnode *memoryNode,
	dedupTS types.TS,
	conflictTS types.TS,
	keys containers.Vector,
	rowmask *roaring.Bitmap) (err error) {
	var dupRow uint32
	blk.RLock()
	defer blk.RUnlock()
	_, err = mnode.BatchDedup(
		keys,
		blk.checkConflictAndDupClosure(dedupTS, conflictTS, &dupRow, rowmask))

	// definitely no duplicate
	if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedDup) {
		return
	}

	def := blk.meta.GetSchema().GetSingleSortKey()
	v := mnode.GetValueByRow(int(dupRow), def.Idx)
	entry := common.TypeStringValue(keys.GetType(), v)
	return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
}

func (blk *ablock) dedupClosure(
	vec containers.Vector,
	ts types.TS,
	mask *roaring.Bitmap,
	def *catalog.ColDef) func(any, int) error {
	return func(v1 any, _ int) (err error) {
		return vec.ForeachShallow(func(v2 any, row int) error {
			if mask != nil && mask.ContainsInt(row) {
				return nil
			}
			if compute.CompareGeneric(v1, v2, vec.GetType()) == 0 {
				commitTSVec, err := blk.LoadPersistedCommitTS()
				if err != nil {
					return err
				}
				defer commitTSVec.Close()
				commiTs := commitTSVec.Get(row).(types.TS)
				if commiTs.Greater(ts) {
					return txnif.ErrTxnWWConflict
				}
				entry := common.TypeStringValue(vec.GetType(), v1)
				return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
			}
			return nil
		}, nil)
	}
}

func (blk *ablock) BatchDedup(
	txn txnif.AsyncTxn,
	keys containers.Vector,
	rowmask *roaring.Bitmap,
	precommit bool) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Infof("BatchDedup BLK-%s: %v", blk.meta.ID.String(), err)
		}
	}()
	dedupTS := txn.GetStartTS()
	if precommit {
		dedupTS = txn.GetPrepareTS()
	}
	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return blk.inMemoryBatchDedup(node.MustMNode(), dedupTS, txn.GetStartTS(), keys, rowmask)
	} else {
		return blk.PersistedBatchDedup(
			node.MustPNode(),
			dedupTS,
			keys,
			rowmask,
			blk.dedupClosure)
	}
}

func (blk *ablock) persistedCollectAppendInRange(
	pnode *persistedNode,
	start, end types.TS,
	withAborted bool) (bat *containers.Batch, err error) {
	// FIXME: we'll gc mvcc after being persisted. refactor it later
	blk.RLock()
	minRow, maxRow, commitTSVec, abortVec, abortedMap :=
		blk.mvcc.CollectAppendLocked(start, end)
	blk.RUnlock()
	if bat, err = pnode.GetDataWindow(minRow, maxRow); err != nil {
		return
	}
	bat.AddVector(catalog.AttrCommitTs, commitTSVec)
	if withAborted {
		bat.AddVector(catalog.AttrAborted, abortVec)
	} else {
		bat.Deletes = abortedMap
		bat.Compact()
	}
	return
}

func (blk *ablock) inMemoryCollectAppendInRange(
	mnode *memoryNode,
	start, end types.TS,
	withAborted bool) (bat *containers.Batch, err error) {
	blk.RLock()
	minRow, maxRow, commitTSVec, abortVec, abortedMap :=
		blk.mvcc.CollectAppendLocked(start, end)
	if bat, err = mnode.GetDataWindow(minRow, maxRow); err != nil {
		blk.RUnlock()
		return
	}
	blk.RUnlock()
	bat.AddVector(catalog.AttrCommitTs, commitTSVec)
	if withAborted {
		bat.AddVector(catalog.AttrAborted, abortVec)
	} else {
		bat.Deletes = abortedMap
		bat.Compact()
	}
	return
}

func (blk *ablock) CollectAppendInRange(
	start, end types.TS,
	withAborted bool) (*containers.Batch, error) {
	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return blk.inMemoryCollectAppendInRange(
			node.MustMNode(),
			start,
			end,
			withAborted)
	} else {
		return blk.persistedCollectAppendInRange(
			node.MustPNode(),
			start,
			end,
			withAborted)
	}
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
