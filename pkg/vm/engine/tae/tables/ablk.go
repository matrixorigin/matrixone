package tables

import (
	"bytes"
	"fmt"
	"time"

	"sync/atomic"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type ablock struct {
	baseBlock
	intents atomic.Int32
}

func (blk *ablock) ApplyAppendQuota() bool {
	for old := blk.intents.Load(); old >= 0; old = blk.intents.Load() {
		if blk.intents.CompareAndSwap(old, old+1) {
			return true
		}
	}
	return false
}

func (blk *ablock) ReturnAppendQuota() {
	blk.intents.Add(-1)
}

func (blk *ablock) TryFreezeAppend() bool {
	return blk.intents.CompareAndSwap(0, -1)
}

func (blk *ablock) IsAppendFrozen() bool {
	return blk.intents.Load() < 0
}

func (blk *ablock) PrepareCompact() bool {
	if blk.intents.Load() > 0 {
		return false
	}
	blk.TryFreezeAppend()
	if !blk.meta.PrepareCompact() {
		return false
	}
	return blk.IsAppendFrozen()
}

func (blk *ablock) Pin() *common.PinnedItem[*ablock] {
	blk.Ref()
	return &common.PinnedItem[*ablock]{
		Val: blk,
	}
}

func (blk *ablock) GetColumnDataById(
	txn txnif.AsyncTxn,
	colIdx int,
	buffer *bytes.Buffer) (view *model.ColumnView, err error) {
	return blk.resolveColumnData(
		txn.GetStartTS(),
		colIdx,
		buffer,
		false)
}

func (blk *ablock) resolveColumnData(
	ts types.TS,
	colIdx int,
	buffer *bytes.Buffer,
	skipDeletes bool) (view *model.ColumnView, err error) {
	mnode, pnode := blk.PinNode()

	if mnode != nil {
		defer mnode.Close()
		return blk.resolveInMemoryColumnData(
			mnode.Item(),
			ts,
			colIdx,
			buffer,
			skipDeletes)
	} else if pnode != nil {
		defer pnode.Close()
		return blk.resolvePersistedColumnData(
			pnode.Item(),
			ts,
			colIdx,
			buffer,
			skipDeletes,
		)
	}
	panic(moerr.NewInternalError(fmt.Sprintf("bad block %s", blk.meta.String())))
}

func (blk *ablock) resolvePersistedColumnData(
	pnode *persistedNode,
	ts types.TS,
	colIdx int,
	buffer *bytes.Buffer,
	skipDeletes bool) (view *model.ColumnView, err error) {
	view = model.NewColumnView(ts, colIdx)
	vec, err := blk.LoadPersistedColumnData(colIdx, buffer)
	if err != nil {
		return
	}
	view.SetData(vec)

	if skipDeletes {
		return
	}

	defer func() {
		if err != nil {
			view.Close()
		}
	}()

	err = blk.FillPersistedDeletes(view)
	return
}

// Note: With PinNode Context
func (blk *ablock) resolveInMemoryColumnData(
	mnode *memoryNode,
	ts types.TS,
	colIdx int,
	buffer *bytes.Buffer,
	skipDeletes bool) (view *model.ColumnView, err error) {
	blk.RLock()
	maxRow, visible, deSels, err := blk.mvcc.GetVisibleRowLocked(ts)
	if !visible || err != nil {
		blk.RUnlock()
		return
	}

	view = model.NewColumnView(ts, colIdx)
	var data containers.Vector
	data, err = mnode.GetColumnDataWindow(
		0,
		maxRow,
		colIdx,
		buffer)
	if err != nil {
		blk.RUnlock()
		return
	}
	view.SetData(data)
	if skipDeletes {
		blk.RUnlock()
		return
	}

	err = blk.FillInMemoryDeletesLocked(view, blk.RWMutex)
	blk.RUnlock()
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
	mnode, pnode := blk.PinNode()
	if mnode != nil {
		defer mnode.Close()
		return blk.getInMemoryValue(mnode.Item(), ts, row, col)
	} else {
		defer pnode.Close()
		return blk.getPersistedValue(pnode.Item(), ts, row, col)
	}
	panic(moerr.NewInternalError(fmt.Sprintf("bad block %s", blk.meta.String())))
}

func (blk *ablock) getPersistedValue(
	pnode *persistedNode,
	ts types.TS,
	row, col int) (v any, err error) {
	view := model.NewColumnView(ts, col)
	if err = blk.FillPersistedDeletes(view); err != nil {
		return
	}
	if view.DeleteMask != nil && view.DeleteMask.ContainsInt(row) {
		err = moerr.NewNotFound()
		return
	}
	view2, err := blk.resolvePersistedColumnData(pnode, ts, col, nil, true)
	if err != nil {
		return
	}
	defer view2.Close()
	v = view2.GetValue(row)
	return
}

// With PinNode Context
func (blk *ablock) getInMemoryValue(
	mnode *memoryNode,
	ts types.TS,
	row, col int) (v any, err error) {
	blk.RLock()
	defer blk.RUnlock()
	deleted, err := blk.mvcc.IsDeletedLocked(uint32(row), ts, blk.RWMutex)
	if err != nil {
		return
	}
	if deleted {
		err = moerr.NewNotFound()
		return
	}
	view, err := blk.resolveInMemoryColumnData(mnode, ts, col, nil, true)
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

	mnode, pnode := blk.PinNode()
	if mnode != nil {
		defer mnode.Close()
		return blk.getInMemoryRowByFilter(mnode.Item(), ts, filter)
	} else if pnode != nil {
		defer pnode.Close()
		return blk.getPersistedRowByFilter(pnode.Item(), ts, filter)
	}

	panic(moerr.NewInternalError(fmt.Sprintf("bad block %s", blk.meta.String())))
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
		err = moerr.NewNotFound()
		return
	}
	sortKey, err := blk.LoadPersistedColumnData(
		blk.meta.GetSchema().GetSingleSortKeyIdx(),
		nil,
	)
	if err != nil {
		return
	}
	defer sortKey.Close()
	rows := make([]uint32, 0)
	err = sortKey.Foreach(func(v any, offset int) error {
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
		err = moerr.NewNotFound()
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
	if err = blk.FillPersistedDeletes(view); err != nil {
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
		err = moerr.NewNotFound()
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
			blk.mvcc.RUnlock()
			txn.GetTxnState(true)
			blk.mvcc.RLock()
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
			blk.mvcc.RUnlock()
			txn.GetTxnState(true)
			blk.mvcc.RLock()
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
	return 0, moerr.NewNotFound()
}

func (blk *ablock) checkConflictAndDupClosure(
	ts types.TS,
	dupRow *uint32,
	rowmask *roaring.Bitmap) func(row uint32) error {
	return func(row uint32) (err error) {
		if rowmask != nil && rowmask.Contains(row) {
			return nil
		}
		appendnode := blk.mvcc.GetAppendNodeByRow(row)
		needWait, txn := appendnode.NeedWaitCommitting(ts)
		if needWait {
			blk.mvcc.RUnlock()
			txn.GetTxnState(true)
			blk.mvcc.RLock()
		}
		if appendnode.IsAborted() || !appendnode.IsVisible(ts) {
			if err = appendnode.CheckConflict(ts); err != nil {
				return
			}
			return nil
		}
		deleteNode := blk.mvcc.GetDeleteNodeByRow(row)
		if deleteNode == nil {
			*dupRow = row
			return moerr.GetOkExpectedDup()
		}
		needWait, txn = deleteNode.NeedWaitCommitting(ts)
		if needWait {
			blk.mvcc.RUnlock()
			txn.GetTxnState(true)
			blk.mvcc.RLock()
		}
		if deleteNode.IsAborted() || !deleteNode.IsVisible(ts) {
			return moerr.GetOkExpectedDup()
		}
		if err = appendnode.CheckConflict(ts); err != nil {
			return
		}
		if err = deleteNode.CheckConflict(ts); err != nil {
			return
		}
		return nil
	}
}

func (blk *ablock) persistedBatchDedup(
	pnode *persistedNode,
	ts types.TS,
	keys containers.Vector,
	rowmask *roaring.Bitmap) (err error) {
	sels, err := pnode.BatchDedup(
		keys,
		nil,
	)
	if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedPossibleDup) {
		return
	}
	def := blk.meta.GetSchema().GetSingleSortKey()
	view, err := blk.resolvePersistedColumnData(
		pnode,
		ts,
		def.Idx,
		nil,
		false)
	if err != nil {
		return
	}
	defer view.Close()
	deduplicate := func(v1 any, _ int) error {
		return view.GetData().Foreach(func(v2 any, row int) error {
			if view.DeleteMask != nil && view.DeleteMask.ContainsInt(row) {
				return nil
			}
			if compute.CompareGeneric(v1, v2, keys.GetType()) == 0 {
				entry := common.TypeStringValue(keys.GetType(), v1)
				return moerr.NewDuplicateEntry(entry, def.Name)
			}
			return nil
		}, nil)
	}
	err = keys.Foreach(deduplicate, sels)
	return
}

func (blk *ablock) inMemoryBatchDedup(
	mnode *memoryNode,
	ts types.TS,
	keys containers.Vector,
	rowmask *roaring.Bitmap) (err error) {
	var dupRow uint32
	blk.RLock()
	defer blk.RUnlock()
	_, err = mnode.BatchDedup(
		keys,
		blk.checkConflictAndDupClosure(ts, &dupRow, rowmask))

	// definitely no duplicate
	if err == nil || !moerr.IsMoErrCode(err, moerr.OkExpectedDup) {
		return
	}

	def := blk.meta.GetSchema().GetSingleSortKey()
	v := mnode.GetValueByRow(int(dupRow), def.Idx)
	entry := common.TypeStringValue(keys.GetType(), v)
	return moerr.NewDuplicateEntry(entry, def.Name)
}

func (blk *ablock) BatchDedup(
	txn txnif.AsyncTxn,
	keys containers.Vector,
	rowmask *roaring.Bitmap) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Infof("BatchDedup BLK-%d: %v", blk.meta.ID, err)
		}
	}()
	mnode, pnode := blk.PinNode()
	if mnode != nil {
		defer mnode.Close()
		return blk.inMemoryBatchDedup(mnode.Item(), txn.GetStartTS(), keys, rowmask)
	} else if pnode != nil {
		defer pnode.Close()
		return blk.persistedBatchDedup(pnode.Item(), txn.GetStartTS(), keys, rowmask)
	}
	panic(moerr.NewInternalError(fmt.Sprintf("bad block %s", blk.meta.String())))
}

func (blk *ablock) inMemoryCollectAppendInRange(
	mnode *memoryNode,
	start, end types.TS,
	withAborted bool) (bat *containers.Batch, err error) {
	blk.RLock()
	defer blk.RUnlock()
	minRow, maxRow, commitTSVec, abortVec, abortedMap :=
		blk.mvcc.CollectAppend(start, end)
	if bat, err = mnode.GetDataWindow(minRow, maxRow); err != nil {
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

func (blk *ablock) CollectAppendInRange(
	start, end types.TS,
	withAborted bool) (*containers.Batch, error) {
	mnode, pnode := blk.PinNode()
	if mnode != nil {
		defer mnode.Close()
		return blk.inMemoryCollectAppendInRange(
			mnode.Item(),
			start,
			end,
			withAborted)
	} else if pnode != nil {
		defer pnode.Close()
		panic("TODO: persistedCollectAppendInRange")
	}
	panic(moerr.NewInternalError(fmt.Sprintf("bad block %s", blk.meta.String())))
}

func (blk *ablock) BuildCompactionTaskFactory() (
	factory tasks.TxnTaskFactory,
	taskType tasks.TaskType,
	scopes []common.ID,
	err error) {

	if !blk.PrepareCompact() {
		return
	}

	factory = jobs.CompactBlockTaskFactory(blk.meta, blk.scheduler)
	taskType = tasks.DataCompactionTask
	scopes = append(scopes, *blk.meta.AsCommonID())
	return
}

func (blk *ablock) RunCalibration() (score int) {
	score, _ = blk.estimateRawScore()
	return
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

func (blk *ablock) EstimateScore(ttl time.Duration, force bool) int {
	return blk.adjustScore(blk.estimateRawScore, ttl, force)
}
