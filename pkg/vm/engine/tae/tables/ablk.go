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
	txn txnif.AsyncTxn,
	readSchema any,
	colIdxes []int,
) (view *model.BlockView, err error) {
	return blk.resolveColumnDatas(
		txn,
		readSchema.(*catalog.Schema),
		colIdxes,
		false)
}

func (blk *ablock) GetColumnDataById(
	txn txnif.AsyncTxn,
	readSchema any,
	col int,
) (view *model.ColumnView, err error) {
	return blk.resolveColumnData(
		txn,
		readSchema.(*catalog.Schema),
		col,
		false)
}

func (blk *ablock) resolveColumnDatas(
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
			node.MustPNode(),
			txn,
			readSchema,
			colIdxes,
			skipDeletes,
		)
	}
}

func (blk *ablock) resolveColumnData(
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
			node.MustPNode(),
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
	txn txnif.AsyncTxn,
	filter *handle.Filter) (offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if blk.meta.GetSchema().SortKey == nil {
		_, offset = model.DecodePhyAddrKeyFromValue(filter.Val)
		return
	}

	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return blk.getInMemoryRowByFilter(node.MustMNode(), txn, filter)
	} else {
		return blk.getPersistedRowByFilter(node.MustPNode(), txn, filter)
	}
}

// only used by tae only
// not to optimize it
func (blk *ablock) getPersistedRowByFilter(
	pnode *persistedNode,
	txn txnif.TxnReader,
	filter *handle.Filter) (row uint32, err error) {
	ok, err := pnode.ContainsKey(filter.Val)
	if err != nil {
		return
	}
	if !ok {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	// Note: sort key do not change
	schema := blk.meta.GetSchema()
	sortKey, err := blk.LoadPersistedColumnData(schema, schema.GetSingleSortKeyIdx())
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
	commitTSVec, err := blk.LoadPersistedCommitTS()
	if err != nil {
		return
	}
	defer commitTSVec.Close()

	// Load persisted deletes
	view := model.NewColumnView(0)
	if err = blk.FillPersistedDeletes(txn, view.BaseView); err != nil {
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

// With PinNode Context
func (blk *ablock) getInMemoryRowByFilter(
	mnode *memoryNode,
	txn txnif.TxnReader,
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
		txn.GetStartTS(),
		waitFn); anyWaitable {
		rows, err = mnode.GetRowsByKey(filter.Val)
		if err != nil {
			return
		}
	}

	for i := len(rows) - 1; i >= 0; i-- {
		row = rows[i]
		appendnode := blk.mvcc.GetAppendNodeByRow(row)
		needWait, waitTxn := appendnode.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			blk.RUnlock()
			waitTxn.GetTxnState(true)
			blk.RLock()
		}
		if appendnode.IsAborted() || !appendnode.IsVisible(txn) {
			continue
		}
		var deleted bool
		deleted, err = blk.mvcc.IsDeletedLocked(row, txn, blk.mvcc.RWMutex)
		if err != nil {
			return
		}
		if !deleted {
			return
		}
	}
	return 0, moerr.NewNotFoundNoCtx()
}

func (blk *ablock) checkConflictAandVisibility(
	node txnif.BaseMVCCNode,
	isCommitting bool,
	txn txnif.TxnReader) (visible bool, err error) {
	// if isCommitting check all nodes commit before txn.CommitTS(PrepareTS)
	// if not isCommitting check nodes commit before txn.StartTS
	if isCommitting {
		needWait := node.IsCommitting()
		if needWait {
			txn := node.GetTxn()
			blk.mvcc.RUnlock()
			txn.GetTxnState(true)
			blk.mvcc.RLock()
		}
	} else {
		needWait, txn := node.NeedWaitCommitting(txn.GetStartTS())
		if needWait {
			blk.mvcc.RUnlock()
			txn.GetTxnState(true)
			blk.mvcc.RLock()
		}
	}
	if err = node.CheckConflict(txn); err != nil {
		return
	}
	if isCommitting {
		visible = node.IsCommitted()
	} else {
		visible = node.IsVisible(txn)
	}
	return
}

func (blk *ablock) checkConflictAndDupClosure(
	txn txnif.TxnReader,
	isCommitting bool,
	dupRow *uint32,
	rowmask *roaring.Bitmap) func(row uint32) error {
	return func(row uint32) (err error) {
		if rowmask != nil && rowmask.Contains(row) {
			return nil
		}
		appendnode := blk.mvcc.GetAppendNodeByRow(row)
		var visible bool
		if visible, err = blk.checkConflictAandVisibility(
			appendnode,
			isCommitting,
			txn); err != nil {
			return
		}
		if appendnode.IsAborted() || !visible {
			return nil
		}
		deleteNode := blk.mvcc.GetDeleteNodeByRow(row)
		if deleteNode == nil {
			*dupRow = row
			return moerr.GetOkExpectedDup()
		}

		if visible, err = blk.checkConflictAandVisibility(
			deleteNode,
			isCommitting,
			txn); err != nil {
			return
		}
		if deleteNode.IsAborted() || !visible {
			return moerr.GetOkExpectedDup()
		}
		return nil
	}
}

func (blk *ablock) inMemoryBatchDedup(
	mnode *memoryNode,
	txn txnif.TxnReader,
	isCommitting bool,
	keys containers.Vector,
	rowmask *roaring.Bitmap) (err error) {
	var dupRow uint32
	blk.RLock()
	defer blk.RUnlock()
	_, err = mnode.BatchDedup(
		keys,
		blk.checkConflictAndDupClosure(txn, isCommitting, &dupRow, rowmask))

	// definitely no duplicate
	if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedDup) {
		return
	}
	def := mnode.writeSchema.GetSingleSortKey()
	v, isNull := mnode.GetValueByRow(mnode.writeSchema, int(dupRow), def.Idx)
	entry := common.TypeStringValue(*keys.GetType(), v, isNull)
	return moerr.NewDuplicateEntryNoCtx(entry, def.Name)
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
	node := blk.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return blk.inMemoryBatchDedup(node.MustMNode(), txn, precommit, keys, rowmask)
	} else {
		return blk.PersistedBatchDedup(
			node.MustPNode(),
			txn,
			precommit,
			keys,
			rowmask,
			true)
	}
}

func (blk *ablock) persistedCollectAppendInRange(
	pnode *persistedNode,
	start, end types.TS,
	withAborted bool) (bat *containers.BatchWithVersion, err error) {
	// logtail should have sent metaloc
	return nil, nil
	// blk.RLock()
	// minRow, maxRow, commitTSVec, abortVec, abortedMap :=
	// 	blk.mvcc.CollectAppendLocked(start, end)
	// blk.RUnlock()
	// if bat, err = pnode.GetDataWindow(minRow, maxRow); err != nil {
	// 	return
	// }
	// bat.AddVector(catalog.AttrCommitTs, commitTSVec)
	// if withAborted {
	// 	bat.AddVector(catalog.AttrAborted, abortVec)
	// } else {
	// 	bat.Deletes = abortedMap
	// 	bat.Compact()
	// }
	// return
}

func (blk *ablock) inMemoryCollectAppendInRange(
	mnode *memoryNode,
	start, end types.TS,
	withAborted bool) (batWithVer *containers.BatchWithVersion, err error) {
	blk.RLock()
	minRow, maxRow, commitTSVec, abortVec, abortedMap :=
		blk.mvcc.CollectAppendLocked(start, end)
	batWithVer, err = mnode.GetDataWindowOnWriteSchema(minRow, maxRow)
	if err != nil {
		blk.RUnlock()
		return nil, err
	}
	blk.RUnlock()

	batWithVer.Seqnums = append(batWithVer.Seqnums, objectio.SEQNUM_COMMITTS)
	batWithVer.AddVector(catalog.AttrCommitTs, commitTSVec)
	if withAborted {
		batWithVer.Seqnums = append(batWithVer.Seqnums, objectio.SEQNUM_ABORT)
		batWithVer.AddVector(catalog.AttrAborted, abortVec)
	} else {
		batWithVer.Deletes = abortedMap
		batWithVer.Compact()
	}

	return
}

func (blk *ablock) CollectAppendInRange(
	start, end types.TS,
	withAborted bool) (*containers.BatchWithVersion, error) {
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
