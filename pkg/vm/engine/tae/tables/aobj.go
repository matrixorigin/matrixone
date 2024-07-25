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

type aobject struct {
	*baseObject
	frozen     atomic.Bool
	freezelock sync.Mutex
}

func newAObject(
	meta *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) *aobject {
	obj := &aobject{}
	obj.baseObject = newBaseObject(obj, meta, rt)
	if obj.meta.Load().HasDropCommitted() {
		pnode := newPersistedNode(obj.baseObject)
		node := NewNode(pnode)
		node.Ref()
		obj.node.Store(node)
		obj.FreezeAppend()
	} else {
		mnode := newObjectMemoryNode(obj)
		node := NewNode(mnode)
		node.Ref()
		obj.node.Store(node)
	}
	return obj
}

func (obj *aobject) FreezeAppend() {
	obj.frozen.Store(true)
}

func (obj *aobject) IsAppendFrozen() bool {
	return obj.frozen.Load()
}

func (obj *aobject) CheckFlushTaskRetry(startts types.TS) bool {
	if !obj.meta.Load().IsAppendable() {
		panic("not support")
	}
	if obj.meta.Load().HasDropCommitted() {
		panic("not support")
	}
	obj.RLock()
	defer obj.RUnlock()
	x := obj.getLastAppendMVCC().GetLatestAppendPrepareTSLocked()
	return x.Greater(&startts)
}
func (obj *aobject) PreapreAppend(
	txn txnif.AsyncTxn,
	startRow, maxRow uint32,
	batchSize int,
) (an *updates.AppendNode, created bool) {
	return obj.getLastAppendMVCC().AddAppendNodeLocked(txn, startRow, maxRow)
}
func (obj *aobject) getLastAppendMVCC() *updates.AppendMVCCHandle {
	node := obj.PinNode()
	defer node.Unref()
	return node.MustMNode().getLastNode().appendMVCC
}

// only used in replay
func (obj *aobject) mustGetAppendMVCC(blkID uint16) *updates.AppendMVCCHandle {
	blkCount := obj.node.Load().MustMNode().blockCnt()
	if blkCount < int(blkID) {
		return nil
	}
	return obj.node.Load().MustMNode().getOrCreateNode(blkID).appendMVCC
}
func (obj *aobject) PPString(level common.PPLevel, depth int, prefix string, blkid int) string {
	rows, err := obj.Rows()
	if err != nil {
		logutil.Warnf("get object rows failed, obj: %v, err: %v", obj.meta.Load().ID().String(), err)
	}
	s := fmt.Sprintf("%s | [Rows=%d]", obj.meta.Load().PPString(level, depth, prefix), rows)
	if level >= common.PPL1 {
		obj.RLock()
		var appendstr, deletestr string
		node := obj.node.Load()
		if !node.IsPersisted() {
			appendstr = node.MustMNode().getLastNode().appendMVCC.StringLocked()
		}
		if mvcc := obj.tryGetMVCC(); mvcc != nil {
			if blkid >= 0 {
				deletestr = mvcc.StringBlkLocked(level, 0, "", blkid)
			} else {
				deletestr = mvcc.String(level, 0, "")
			}
		}
		obj.RUnlock()
		if appendstr != "" {
			s = fmt.Sprintf("%s\n Appends: %s", s, appendstr)
		}
		if deletestr != "" {
			s = fmt.Sprintf("%s\n Deletes: %s", s, deletestr)
		}
	}
	return s
}
func (obj *aobject) BlockCnt() int {
	obj.RLock()
	defer obj.RUnlock()
	return obj.node.Load().MustMNode().blockCnt()
}
func (obj *aobject) IsAppendable(checkBlkCount bool) bool {
	if obj.IsAppendFrozen() {
		return false
	}
	node := obj.PinNode()
	defer node.Unref()
	if node.IsPersisted() {
		return false
	}
	if !checkBlkCount {
		return true
	}
	return node.MustMNode().IsAppendable()
}
func (obj *aobject) GetNewBlock() {
	obj.Lock()
	defer obj.Unlock()
	newNode := obj.node.Load().MustMNode().registerNodeLocked()
	if newNode == nil {
		panic("logic error")
	}
}

func (obj *aobject) LastBlockRows() uint32 {
	if obj.BlockCnt() == 0 {
		return 0
	}
	row, err := obj.node.Load().MustMNode().getLastNode().Rows()
	if err != nil {
		panic(err)
	}
	return row
}

func (obj *aobject) PrepareCompactInfo() (result bool, reason string) {
	if n := obj.RefCount(); n > 0 {
		reason = fmt.Sprintf("entering refcount %d", n)
		return
	}
	obj.FreezeAppend()
	if !obj.meta.Load().PrepareCompact() || !obj.getLastAppendMVCC().PrepareCompact() {
		if !obj.meta.Load().PrepareCompact() {
			reason = "meta preparecomp false"
		} else {
			reason = "mvcc preparecomp false"
		}
		return
	}

	if n := obj.RefCount(); n != 0 {
		reason = fmt.Sprintf("ending refcount %d", n)
		return
	}
	return obj.RefCount() == 0, reason
}

func (obj *aobject) PrepareCompact() bool {
	if obj.RefCount() > 0 {
		return false
	}

	// see more notes in flushtabletail.go
	obj.freezelock.Lock()
	obj.FreezeAppend()
	obj.freezelock.Unlock()

	droppedCommitted := obj.GetObjMeta().HasDropCommitted()

	if droppedCommitted {
		if !obj.meta.Load().PrepareCompactLocked() {
			if obj.meta.Load().CheckPrintPrepareCompactLocked() {
				obj.meta.Load().PrintPrepareCompactDebugLog()
			}
			return false
		}
	} else {
		if !obj.meta.Load().PrepareCompactLocked() {
			if obj.meta.Load().CheckPrintPrepareCompactLocked() {
				obj.meta.Load().PrintPrepareCompactDebugLog()
			}
			return false
		}
		if !obj.getLastAppendMVCC().PrepareCompact() /* all appends are committed */ {
			if obj.meta.Load().CheckPrintPrepareCompactLocked() {
				logutil.Infof("obj %v, data prepare compact failed", obj.meta.Load().ID().String())
				if !obj.meta.Load().HasPrintedPrepareComapct.Load() {
					obj.meta.Load().HasPrintedPrepareComapct.Store(true)
					logutil.Infof("append MVCC %v", obj.getLastAppendMVCC().StringLocked())
				}
			}
			return false
		}
	}
	prepareCompact := obj.RefCount() == 0
	if !prepareCompact && obj.meta.Load().CheckPrintPrepareCompactLocked() {
		logutil.Infof("obj %v, data ref count is %d", obj.meta.Load().ID().String(), obj.RefCount())
	}
	return prepareCompact
}

func (obj *aobject) Pin() *common.PinnedItem[*aobject] {
	obj.Ref()
	return &common.PinnedItem[*aobject]{
		Val: obj,
	}
}

func (obj *aobject) GetColumnDataByIds(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	blkOffset uint16,
	colIdxes []int,
	mp *mpool.MPool,
) (view *containers.Batch, err error) {
	return obj.resolveColumnDatas(
		ctx,
		blkOffset,
		txn,
		readSchema.(*catalog.Schema),
		colIdxes,
		false,
		mp,
	)
}

func (obj *aobject) GetColumnDataById(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	blkID uint16,
	col int,
	mp *mpool.MPool,
) (view *containers.Batch, err error) {
	return obj.resolveColumnData(
		ctx,
		blkID,
		txn,
		readSchema.(*catalog.Schema),
		col,
		false,
		mp,
	)
}

func (obj *aobject) resolveColumnDatas(
	ctx context.Context,
	blkOffset uint16,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	colIdxes []int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.Batch, err error) {
	node := obj.PinNode()
	defer node.Unref()

	if !node.IsPersisted() {
		return node.MustMNode().resolveInMemoryColumnDatas(
			ctx, blkOffset,
			txn, readSchema, colIdxes, skipDeletes, mp,
		)
	} else {
		return obj.ResolvePersistedColumnDatas(
			ctx,
			txn,
			readSchema,
			blkOffset,
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
func (obj *aobject) CoarseCheckAllRowsCommittedBefore(ts types.TS) bool {
	// if the block is not frozen, always return false
	if !obj.IsAppendFrozen() {
		return false
	}

	node := obj.PinNode()
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

func (obj *aobject) resolveColumnData(
	ctx context.Context,
	blkID uint16,
	txn txnif.TxnReader,
	readSchema *catalog.Schema,
	col int,
	skipDeletes bool,
	mp *mpool.MPool,
) (view *containers.Batch, err error) {
	node := obj.PinNode()
	defer node.Unref()

	if !node.IsPersisted() {
		return node.MustMNode().resolveInMemoryColumnData(
			blkID, txn, readSchema, col, skipDeletes, mp,
		)
	} else {
		return obj.ResolvePersistedColumnData(
			ctx,
			txn,
			readSchema,
			blkID,
			col,
			skipDeletes,
			mp,
		)
	}
}

func (obj *aobject) GetValue(
	ctx context.Context,
	txn txnif.AsyncTxn,
	readSchema any,
	blkID uint16,
	row, col int,
	mp *mpool.MPool,
) (v any, isNull bool, err error) {
	node := obj.PinNode()
	defer node.Unref()
	schema := readSchema.(*catalog.Schema)
	if !node.IsPersisted() {
		return node.MustMNode().getInMemoryValue(txn, schema, blkID, row, col, mp)
	} else {
		return obj.getPersistedValue(
			ctx, txn, schema, blkID, row, col, true, mp,
		)
	}
}

// GetByFilter will read pk column, which seqnum will not change, no need to pass the read schema.
func (obj *aobject) GetByFilter(
	ctx context.Context,
	txn txnif.AsyncTxn,
	filter *handle.Filter,
	mp *mpool.MPool,
) (blkID uint16, offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if obj.meta.Load().GetSchema().SortKey == nil {
		rid := filter.Val.(types.Rowid)
		offset = rid.GetRowOffset()
		return
	}

	node := obj.PinNode()
	defer node.Unref()
	blkID, offset, err = node.GetRowByFilter(ctx, txn, filter, mp)
	return
}

func (obj *aobject) BatchDedup(
	ctx context.Context,
	txn txnif.AsyncTxn,
	keys containers.Vector,
	keysZM index.ZM,
	rowmask *roaring.Bitmap,
	precommit bool,
	bf objectio.BloomFilter,
	startBlkID uint16,
	mp *mpool.MPool,
) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Debugf("BatchDedup obj-%s: %v", obj.meta.Load().ID().String(), err)
		}
	}()
	node := obj.PinNode()
	defer node.Unref()
	if !node.IsPersisted() {
		return node.BatchDedup(
			ctx,
			txn,
			precommit,
			keys,
			keysZM,
			rowmask,
			startBlkID,
			bf,
		)
	} else {
		return obj.PersistedBatchDedup(
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

func (obj *aobject) CollectAppendInRange(
	start, end types.TS,
	withAborted bool,
	mp *mpool.MPool,
) (*containers.BatchWithVersion, error) {
	node := obj.PinNode()
	defer node.Unref()
	return node.CollectAppendInRange(start, end, withAborted, mp)
}

func (obj *aobject) CollectAppendInRangeWithBlockID(
	blkID uint16,
	start, end types.TS,
	withAborted bool,
	mp *mpool.MPool,
) (*containers.BatchWithVersion, error) {
	node := obj.PinNode()
	defer node.Unref()
	return node.CollectAppendInRangeWithBlockID(blkID, start, end, withAborted, mp)
}

func (obj *aobject) estimateRawScore() (score int, dropped bool, err error) {
	meta := obj.GetObjMeta()
	if meta.HasDropCommitted() && !meta.InMemoryDeletesExisted() {
		dropped = true
		return
	}
	atLeastOneCommitted := meta.ObjectState >= catalog.ObjectState_Create_ApplyCommit
	if !atLeastOneCommitted {
		score = 1
		return
	}

	rows, err := obj.Rows()
	if rows == int(obj.meta.Load().GetSchema().BlockMaxRows) {
		score = 100
		return
	}

	changesCnt := uint32(0)
	obj.RLock()
	objectMVCC := obj.tryGetMVCC()
	if objectMVCC != nil {
		changesCnt = objectMVCC.GetChangeIntentionCntLocked()
	}
	obj.RUnlock()
	if changesCnt == 0 && rows == 0 {
		score = 0
	} else {
		score = 1
	}

	if score > 0 {
		if _, terminated := obj.meta.Load().GetTerminationTS(); terminated {
			score = 100
		}
	}
	return
}

func (obj *aobject) RunCalibration() (score int, err error) {
	score, _, err = obj.estimateRawScore()
	return
}

func (obj *aobject) OnReplayAppend(node txnif.AppendNode) (err error) {
	an := node.(*updates.AppendNode)
	blkID := an.GetID().GetBlockOffset()
	appendMVCC := obj.mustGetAppendMVCC(blkID)
	if appendMVCC == nil {
		return nil
	}
	appendMVCC.OnReplayAppendNode(an)
	return
}

func (obj *aobject) OnReplayAppendPayload(bat *containers.Batch, blkOffset uint16) (err error) {
	appender, err := obj.MakeAppender()
	if err != nil {
		return
	}
	_, err = appender.ReplayAppend(bat, blkOffset, nil)
	return
}

func (obj *aobject) MakeAppender() (appender data.ObjectAppender, err error) {
	if obj == nil || !obj.IsAppendable(false) {
		err = moerr.GetOkExpectedEOB()
		return
	}
	appender = newAppender(obj)
	return
}

func (obj *aobject) Init() (err error) { return }

func (obj *aobject) EstimateMemSize() (int, int) {
	node := obj.PinNode()
	defer node.Unref()
	obj.RLock()
	defer obj.RUnlock()
	dsize := 0
	objMVCC := obj.tryGetMVCC()
	if objMVCC != nil {
		dsize = objMVCC.EstimateMemSizeLocked()
	}
	asize := 0
	if !node.IsPersisted() {
		asize += node.MustMNode().EstimateMemSize()
	}
	return asize, dsize
}

func (obj *aobject) GetRowsOnReplay() uint64 {
	appendRow, err := obj.node.Load().MustMNode().Rows()
	if err != nil {
		panic(err)
	}
	rows := uint64(appendRow)
	stats := obj.meta.Load().GetObjectStats()
	fileRows := uint64(stats.Rows())
	if rows > fileRows {
		return rows
	}
	return fileRows
}
