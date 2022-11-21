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
	"bytes"
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
	node := newPersistedNode(blk.baseBlock)
	pinned := node.Pin()
	blk.storage.pnode = pinned
	return blk
}

func (blk *block) Init() (err error) {
	_, pnode := blk.PinNode()
	defer pnode.Close()
	pnode.Item().init()
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
	buffers []*bytes.Buffer) (view *model.BlockView, err error) {
	colIdxes := make([]int, len(attrs))
	schema := blk.meta.GetSchema()
	for i, attr := range attrs {
		colIdxes[i] = schema.GetColIdx(attr)
	}
	return blk.GetColumnDataByIds(txn, colIdxes, buffers)
}

func (blk *block) GetColumnDataByName(
	txn txnif.AsyncTxn,
	attr string,
	buffer *bytes.Buffer) (view *model.ColumnView, err error) {
	colIdx := blk.meta.GetSchema().GetColIdx(attr)
	return blk.GetColumnDataById(txn, colIdx, buffer)
}

func (blk *block) GetColumnDataByIds(
	txn txnif.AsyncTxn,
	colIdxes []int,
	buffers []*bytes.Buffer) (view *model.BlockView, err error) {
	_, pnode := blk.PinNode()
	return blk.ResolvePersistedColumnDatas(
		pnode.Item(),
		txn.GetStartTS(),
		colIdxes,
		buffers,
		false)
}

func (blk *block) GetColumnDataById(
	txn txnif.AsyncTxn,
	colIdx int,
	buffer *bytes.Buffer) (view *model.ColumnView, err error) {
	_, pnode := blk.PinNode()
	defer pnode.Close()
	return blk.ResolvePersistedColumnData(
		pnode.Item(),
		txn.GetStartTS(),
		colIdx,
		buffer,
		false)
}

func (blk *block) BatchDedup(
	txn txnif.AsyncTxn,
	keys containers.Vector,
	rowmask *roaring.Bitmap,
	precommit bool) (err error) {
	defer func() {
		if moerr.IsMoErrCode(err, moerr.ErrDuplicateEntry) {
			logutil.Infof("BatchDedup BLK-%d: %v", blk.meta.ID, err)
		}
	}()
	ts := txn.GetStartTS()
	if precommit {
		ts = txn.GetPrepareTS()
	}
	_, pnode := blk.PinNode()
	defer pnode.Close()
	return blk.PersistedBatchDedup(
		pnode.Item(),
		ts,
		keys,
		rowmask,
		blk.dedupClosure)
}

func (blk *block) dedupClosure(
	vec containers.Vector,
	mask *roaring.Bitmap,
	def *catalog.ColDef) func(any, int) error {
	return func(v any, _ int) (err error) {
		if _, existed := compute.GetOffsetByVal(vec, v, mask); existed {
			entry := common.TypeStringValue(vec.GetType(), v)
			return moerr.NewDuplicateEntry(entry, def.Name)
		}
		return nil
	}
}

func (blk *block) GetValue(
	txn txnif.AsyncTxn,
	row, col int) (v any, err error) {
	ts := txn.GetStartTS()
	_, pnode := blk.PinNode()
	defer pnode.Close()
	return blk.getPersistedValue(
		pnode.Item(),
		ts,
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
	ts := txn.GetStartTS()

	_, pnode := blk.PinNode()
	defer pnode.Close()
	return blk.getPersistedRowByFilter(pnode.Item(), ts, filter)
}

func (blk *block) getPersistedRowByFilter(
	pnode *persistedNode,
	ts types.TS,
	filter *handle.Filter) (offset uint32, err error) {
	ok, err := pnode.ContainsKey(filter.Val)
	if err != nil {
		return
	}
	if !ok {
		err = moerr.NewNotFound()
		return
	}
	var sortKey containers.Vector
	if sortKey, err = blk.LoadPersistedColumnData(
		blk.meta.GetSchema().GetSingleSortKeyIdx(),
		nil); err != nil {
		return
	}
	defer sortKey.Close()
	off, existed := compute.GetOffsetByVal(sortKey, filter.Val, nil)
	if !existed {
		err = moerr.NewNotFound()
		return
	}
	offset = uint32(off)

	blk.mvcc.RLock()
	defer blk.mvcc.RUnlock()
	deleted, err := blk.mvcc.IsDeletedLocked(offset, ts, blk.mvcc.RWMutex)
	if err != nil {
		return
	}
	if deleted {
		err = moerr.NewNotFound()
	}
	return
}
