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

func newABlock(
	meta *catalog.ObjectEntry,
	rt *dbutils.Runtime,
) *aobject {
	blk := &aobject{}
	blk.baseObject = newBaseBlock(blk, meta, rt)
	if blk.meta.HasDropCommitted() {
		pnode := newPersistedNode(blk.baseObject)
		node := NewNode(pnode)
		node.Ref()
		blk.node.Store(node)
		blk.FreezeAppend()
	} else {
		mnode := newMemoryNode(blk.baseObject)
		node := NewNode(mnode)
		node.Ref()
		blk.node.Store(node)
	}
	return blk
}

func (blk *aobject) FreezeAppend() {
	blk.frozen.Store(true)
}

func (blk *aobject) IsAppendFrozen() bool {
	return blk.frozen.Load()
}

func (blk *aobject) IsAppendable() bool {
	if blk.IsAppendFrozen() {
		return false
	}
	node := blk.PinNode()
	defer node.Unref()
	if node.IsPersisted() {
		return false
	}
	rows, _ := node.Rows()
	return rows < blk.meta.GetSchema().BlockMaxRows
}

func (blk *aobject) PrepareCompactInfo() (result bool, reason string) {
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

func (blk *aobject) PrepareCompact() bool {
	if blk.RefCount() > 0 {
		return false
	}

	// see more notes in flushtabletail.go
	blk.freezelock.Lock()
	blk.FreezeAppend()
	blk.freezelock.Unlock()

	if !blk.meta.PrepareCompact() ||
		!blk.appendMVCC.PrepareCompact() /* all appends are committed */ {
		return false
	}
	return blk.RefCount() == 0
}

func (blk *aobject) Pin() *common.PinnedItem[*aobject] {
	blk.Ref()
	return &common.PinnedItem[*aobject]{
		Val: blk,
	}
}

func (blk *aobject) GetColumnDataByIds(
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

func (blk *aobject) GetColumnDataById(
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

func (blk *aobject) resolveColumnDatas(
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
			ctx,
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
func (blk *aobject) CoarseCheckAllRowsCommittedBefore(ts types.TS) bool {
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

func (blk *aobject) resolveColumnData(
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

func (blk *aobject) GetValue(
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
func (blk *aobject) GetByFilter(
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

func (blk *aobject) BatchDedup(
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

func (blk *aobject) CollectAppendInRange(
	start, end types.TS,
	withAborted bool,
	mp *mpool.MPool,
) (*containers.BatchWithVersion, error) {
	node := blk.PinNode()
	defer node.Unref()
	return node.CollectAppendInRange(start, end, withAborted, mp)
}

func (blk *aobject) estimateRawScore() (score int, dropped bool, err error) {
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

	rows, err := blk.Rows()
	if rows == int(blk.meta.GetSchema().BlockMaxRows) {
		score = 100
		return
	}

	changesCnt := uint32(0)
	objectMVCC := blk.tryGetMVCC()
	if objectMVCC != nil {
		changesCnt = objectMVCC.GetChangeIntentionCnt()
	}
	if changesCnt == 0 && rows == 0 {
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

func (blk *aobject) RunCalibration() (score int, err error) {
	score, _, err = blk.estimateRawScore()
	return
}

func (blk *aobject) OnReplayAppend(node txnif.AppendNode) (err error) {
	an := node.(*updates.AppendNode)
	blk.appendMVCC.OnReplayAppendNode(an)
	return
}

func (blk *aobject) OnReplayAppendPayload(bat *containers.Batch) (err error) {
	appender, err := blk.MakeAppender()
	if err != nil {
		return
	}
	_, err = appender.ReplayAppend(bat, nil)
	return
}

func (blk *aobject) MakeAppender() (appender data.ObjectAppender, err error) {
	if blk == nil {
		err = moerr.GetOkExpectedEOB()
		return
	}
	appender = newAppender(blk)
	return
}

func (blk *aobject) Init() (err error) { return }

func (blk *aobject) EstimateMemSize() (int, int) {
	node := blk.PinNode()
	defer node.Unref()
	blk.RLock()
	defer blk.RUnlock()
	dsize := 0
	objMVCC := blk.tryGetMVCC()
	if objMVCC != nil {
		dsize = objMVCC.EstimateMemSizeLocked()
	}
	asize := blk.appendMVCC.EstimateMemSizeLocked()
	if !node.IsPersisted() {
		asize += node.MustMNode().EstimateMemSize()
	}
	return asize, dsize
}

func (blk *aobject) GetRowsOnReplay() uint64 {
	rows := uint64(blk.appendMVCC.GetTotalRow())
	fileRows := uint64(blk.meta.GetLatestCommittedNode().
		BaseNode.ObjectStats.Rows())
	if rows > fileRows {
		return rows
	}
	return fileRows
}
