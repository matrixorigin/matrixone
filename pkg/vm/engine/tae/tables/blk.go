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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type block struct {
	*baseBlock
}

func newBlock(
	meta *catalog.BlockEntry,
	fs *objectio.ObjectFS,
	bufMgr base.INodeManager,
	scheduler tasks.TaskScheduler) *block {
	blk := &block{}
	blk.baseBlock = newBaseBlock(blk, meta, bufMgr, fs, scheduler)
	blk.mvcc.SetDeletesListener(blk.OnApplyDelete)
	pnode := newPersistedNode(blk.baseBlock)
	node := NewNode(pnode)
	node.Ref()
	blk.node.Store(node)
	return blk
}

func (blk *block) Init() (err error) {
	node := blk.PinNode()
	defer node.Unref()
	node.MustPNode().init()
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

func (blk *block) Pin() *common.PinnedItem[*block] {
	blk.Ref()
	return &common.PinnedItem[*block]{
		Val: blk,
	}
}

func (blk *block) GetColumnDataByNames(
	txn txnif.AsyncTxn,
	attrs []string,
) (view *model.BlockView, err error) {
	colIdxes := make([]int, len(attrs))
	schema := blk.meta.GetSchema()
	for i, attr := range attrs {
		colIdxes[i] = schema.GetColIdx(attr)
	}
	return blk.GetColumnDataByIds(txn, colIdxes)
}

func (blk *block) GetColumnDataByName(
	txn txnif.AsyncTxn,
	attr string,
) (view *model.ColumnView, err error) {
	colIdx := blk.meta.GetSchema().GetColIdx(attr)
	return blk.GetColumnDataById(txn, colIdx)
}

func (blk *block) GetColumnDataByIds(
	txn txnif.AsyncTxn,
	colIdxes []int,
) (view *model.BlockView, err error) {
	node := blk.PinNode()
	defer node.Unref()
	return blk.ResolvePersistedColumnDatas(
		node.MustPNode(),
		txn,
		colIdxes,
		false)
}

// GetColumnDataById Get the snapshot at txn's start timestamp of column data.
// Notice that for non-appendable block, if it is visible to txn,
// then all the block data pointed by meta location also be visible to txn;
func (blk *block) GetColumnDataById(
	txn txnif.AsyncTxn,
	colIdx int,
) (view *model.ColumnView, err error) {
	node := blk.PinNode()
	defer node.Unref()
	return blk.ResolvePersistedColumnData(
		node.MustPNode(),
		txn,
		colIdx,
		false)
}

func (blk *block) BatchDedup(
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
	return blk.PersistedBatchDedup(
		node.MustPNode(),
		txn,
		precommit,
		keys,
		rowmask,
		dedupNABlkClosure)
}

func (blk *block) GetValue(
	txn txnif.AsyncTxn,
	row, col int) (v any, err error) {
	node := blk.PinNode()
	defer node.Unref()
	return blk.getPersistedValue(
		node.MustPNode(),
		txn,
		row,
		col,
		false)
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
	return blk.adjustScore(blk.estimateRawScore, ttl, force)
}

func (blk *block) GetByFilter(
	txn txnif.AsyncTxn,
	filter *handle.Filter) (offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if blk.meta.GetSchema().SortKey == nil {
		_, _, offset = model.DecodePhyAddrKeyFromValue(filter.Val)
		return
	}

	node := blk.PinNode()
	defer node.Unref()
	return blk.getPersistedRowByFilter(node.MustPNode(), txn, filter)
}

func (blk *block) getPersistedRowByFilter(
	pnode *persistedNode,
	txn txnif.TxnReader,
	filter *handle.Filter) (offset uint32, err error) {
	ok, err := pnode.ContainsKey(filter.Val)
	if err != nil {
		return
	}
	if !ok {
		err = moerr.NewNotFoundNoCtx()
		return
	}
	var sortKey containers.Vector
	if sortKey, err = blk.LoadPersistedColumnData(
		blk.meta.GetSchema().GetSingleSortKeyIdx(),
	); err != nil {
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
