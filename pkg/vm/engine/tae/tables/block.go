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
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/stl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/indexwrapper"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

var ImmutMemAllocator stl.MemAllocator

func init() {
	ImmutMemAllocator = stl.NewSimpleAllocator()
}

type dataBlock struct {
	*sync.RWMutex
	common.ClosedState
	meta      *catalog.BlockEntry
	node      *appendableNode
	file      file.Block
	colFiles  map[int]common.IRWFile
	bufMgr    base.INodeManager
	scheduler tasks.TaskScheduler
	index     indexwrapper.Index
	mvcc      *updates.MVCCHandle
	nice      uint32
	ckpTs     uint64
	prefix    []byte
}

func newBlock(meta *catalog.BlockEntry, segFile file.Segment, bufMgr base.INodeManager, scheduler tasks.TaskScheduler) *dataBlock {
	colCnt := len(meta.GetSchema().ColDefs)
	indexCnt := make(map[int]int)
	if meta.GetSchema().HasSortKey() {
		indexCnt[meta.GetSchema().SortKey.Defs[0].Idx] = 2
	}
	file, err := segFile.OpenBlock(meta.GetID(), colCnt, indexCnt)
	if err != nil {
		panic(err)
	}
	colFiles := make(map[int]common.IRWFile)
	for i := 0; i < colCnt; i++ {
		if colBlk, err := file.OpenColumn(i); err != nil {
			panic(err)
		} else {
			colFiles[i], err = colBlk.OpenDataFile()
			if err != nil {
				panic(err)
			}
			colBlk.Close()
		}
	}
	var node *appendableNode
	block := &dataBlock{
		RWMutex:   new(sync.RWMutex),
		meta:      meta,
		file:      file,
		colFiles:  colFiles,
		mvcc:      updates.NewMVCCHandle(meta),
		scheduler: scheduler,
		bufMgr:    bufMgr,
		prefix:    meta.MakeKey(),
	}
	ts, _ := block.file.ReadTS()
	if meta.IsAppendable() {
		block.mvcc.SetDeletesListener(block.ABlkApplyDelete)
		node = newNode(bufMgr, block, file)
		block.node = node
		if meta.GetSchema().HasPK() {
			block.index = indexwrapper.NewMutableIndex(meta.GetSchema().GetSortKeyType())
		}
	} else {
		block.mvcc.SetDeletesListener(block.BlkApplyDelete)
		block.index = indexwrapper.NewImmutableIndex()
	}
	block.mvcc.SetMaxVisible(ts)
	block.ckpTs = ts
	if ts > 0 {
		if err := block.ReplayIndex(); err != nil {
			panic(err)
		}
		if err := block.ReplayDelta(); err != nil {
			panic(err)
		}
	}
	return block
}

func (blk *dataBlock) ReplayDelta() (err error) {
	if !blk.meta.IsAppendable() {
		return
	}
	an := updates.NewCommittedAppendNode(blk.ckpTs, blk.node.rows, blk.mvcc)
	blk.mvcc.OnReplayAppendNode(an)
	masks, vals := blk.file.LoadUpdates()
	if masks != nil {
		for colIdx, mask := range masks {
			logutil.Info("[Start]",
				common.TimestampField(blk.ckpTs),
				common.OperationField("install-update"),
				common.OperandNameSpace(),
				common.AnyField("rows", blk.node.rows),
				common.AnyField("col", colIdx),
				common.CountField(int(mask.GetCardinality())))
			un := updates.NewCommittedColumnUpdateNode(blk.ckpTs, blk.ckpTs, blk.meta.AsCommonID(), nil)
			un.SetMask(mask)
			un.SetValues(vals[colIdx])
			if err = blk.OnReplayUpdate(uint16(colIdx), un); err != nil {
				return
			}
		}
	}
	deletes, err := blk.file.LoadDeletes()
	if err != nil || deletes == nil {
		return
	}
	logutil.Info("[Start]", common.TimestampField(blk.ckpTs),
		common.OperationField("install-del"),
		common.OperandNameSpace(),
		common.AnyField("rows", blk.node.rows),
		common.CountField(int(deletes.GetCardinality())))
	deleteNode := updates.NewMergedNode(blk.ckpTs)
	deleteNode.SetDeletes(deletes)
	err = blk.OnReplayDelete(deleteNode)
	return
}

func (blk *dataBlock) ReplayIndex() (err error) {
	if blk.meta.IsAppendable() {
		if !blk.meta.GetSchema().HasPK() {
			return
		}
		keysCtx := new(index.KeysCtx)
		err = blk.node.DoWithPin(func() (err error) {
			var vec containers.Vector
			if blk.meta.GetSchema().IsSinglePK() {
				// TODO: use mempool
				vec, err = blk.node.GetColumnDataCopy(blk.node.rows, blk.meta.GetSchema().GetSingleSortKeyIdx(), nil)
				if err != nil {
					return
				}
				// TODO: apply deletes
				keysCtx.Keys = vec
			} else {
				sortKeys := blk.meta.GetSchema().SortKey
				vs := make([]containers.Vector, sortKeys.Size())
				for i := range vs {
					vec, err = blk.node.GetColumnDataCopy(blk.node.rows, sortKeys.Defs[i].Idx, nil)
					if err != nil {
						return
					}
					// TODO: apply deletes
					vs[i] = vec
					defer vs[i].Close()
				}
				keysCtx.Keys = model.EncodeCompoundColumn(vs...)
			}
			return
		})
		if err != nil {
			return
		}
		keysCtx.Start = 0
		keysCtx.Count = keysCtx.Keys.Length()
		defer keysCtx.Keys.Close()
		err = blk.index.BatchUpsert(keysCtx, 0, 0)
		return
	}
	if blk.meta.GetSchema().HasSortKey() {
		err = blk.index.ReadFrom(blk)
	}
	return
}

func (blk *dataBlock) GetMeta() any                 { return blk.meta }
func (blk *dataBlock) GetBufMgr() base.INodeManager { return blk.bufMgr }

func (blk *dataBlock) SetMaxCheckpointTS(ts uint64) {
	atomic.StoreUint64(&blk.ckpTs, ts)
}

func (blk *dataBlock) GetMaxCheckpointTS() uint64 {
	return atomic.LoadUint64(&blk.ckpTs)
}

func (blk *dataBlock) GetMaxVisibleTS() uint64 {
	return blk.mvcc.LoadMaxVisible()
}

func (blk *dataBlock) Close() {
	if blk.node != nil {
		_ = blk.node.Close()
		blk.node = nil
	}
	if blk.file != nil {
		_ = blk.file.Close()
		blk.file = nil
	}
}

func (blk *dataBlock) Destroy() (err error) {
	if !blk.TryClose() {
		return
	}
	if blk.node != nil {
		if err = blk.node.Close(); err != nil {
			return
		}
	}
	for _, file := range blk.colFiles {
		file.Unref()
	}
	blk.colFiles = make(map[int]common.IRWFile)
	if blk.index != nil {
		if err = blk.index.Destroy(); err != nil {
			return
		}
	}
	if blk.file != nil {
		if err = blk.file.Close(); err != nil {
			return
		}
		if err = blk.file.Destroy(); err != nil {
			return
		}
	}
	return
}

func (blk *dataBlock) GetBlockFile() file.Block {
	return blk.file
}

func (blk *dataBlock) GetID() *common.ID { return blk.meta.AsCommonID() }

func (blk *dataBlock) RunCalibration() {
	score := blk.estimateRawScore()
	if score == 0 {
		return
	}
	atomic.AddUint32(&blk.nice, uint32(1))
}

func (blk *dataBlock) resetNice() {
	atomic.StoreUint32(&blk.nice, uint32(0))
}

func (blk *dataBlock) estimateRawScore() int {
	if blk.Rows(nil, true) == int(blk.meta.GetSchema().BlockMaxRows) && blk.meta.IsAppendable() {
		return 100
	}

	if blk.mvcc.GetChangeNodeCnt() == 0 && !blk.meta.IsAppendable() {
		return 0
	} else if blk.mvcc.GetChangeNodeCnt() == 0 && blk.meta.IsAppendable() &&
		blk.mvcc.LoadMaxVisible() <= blk.GetMaxCheckpointTS() {
		return 0
	}
	ret := 0
	cols := 0
	rows := blk.Rows(nil, true)
	factor := float64(0)
	for i := range blk.meta.GetSchema().ColDefs {
		cols++
		cnt := blk.mvcc.GetColumnUpdateCnt(uint16(i))
		colFactor := float64(cnt) / float64(rows)
		if colFactor < 0.005 {
			colFactor *= 10
		} else if colFactor >= 0.005 && colFactor < 0.10 {
			colFactor *= 20
		} else if colFactor >= 0.10 {
			colFactor *= 40
		}
		factor += colFactor
	}
	factor = factor / float64(cols)
	deleteCnt := blk.mvcc.GetDeleteCnt()
	factor += float64(deleteCnt) / float64(rows) * 50
	ret += int(factor * 100)
	if ret == 0 {
		ret += 1
	}
	return ret
}

func (blk *dataBlock) MutationInfo() string {
	rows := blk.Rows(nil, true)
	totalChanges := blk.mvcc.GetChangeNodeCnt()
	s := fmt.Sprintf("Block %s Mutation Info: Changes=%d/%d",
		blk.meta.AsCommonID().BlockString(),
		totalChanges,
		rows)
	if totalChanges == 0 {
		return s
	}
	for i := range blk.meta.GetSchema().ColDefs {
		cnt := blk.mvcc.GetColumnUpdateCnt(uint16(i))
		if cnt == 0 {
			continue
		}
		s = fmt.Sprintf("%s, Col[%d]:%d/%d", s, i, cnt, rows)
	}
	deleteCnt := blk.mvcc.GetDeleteCnt()
	if deleteCnt != 0 {
		s = fmt.Sprintf("%s, Del:%d/%d", s, deleteCnt, rows)
	}
	return s
}

func (blk *dataBlock) EstimateScore() int {
	if blk.meta.IsAppendable() && blk.Rows(nil, true) == int(blk.meta.GetSchema().BlockMaxRows) {
		blk.meta.RLock()
		if blk.meta.IsDroppedCommitted() || blk.meta.IsDroppedUncommitted() {
			blk.meta.RUnlock()
			return 0
		}
		blk.meta.RUnlock()
		return 100
	}

	score := blk.estimateRawScore()
	if score == 0 {
		blk.resetNice()
		return 0
	}
	score += int(atomic.LoadUint32(&blk.nice))
	return score
}

func (blk *dataBlock) BuildCompactionTaskFactory() (
	factory tasks.TxnTaskFactory,
	taskType tasks.TaskType,
	scopes []common.ID,
	err error) {
	blk.meta.RLock()
	dropped := blk.meta.IsDroppedCommitted()
	inTxn := blk.meta.HasActiveTxn()
	blk.meta.RUnlock()
	if dropped || inTxn {
		return
	}
	if !blk.meta.IsAppendable() || (blk.meta.IsAppendable() && blk.Rows(nil, true) == int(blk.meta.GetSchema().BlockMaxRows)) {
		factory = jobs.CompactBlockTaskFactory(blk.meta, blk.scheduler)
		taskType = tasks.DataCompactionTask
	} else if blk.meta.IsAppendable() {
		factory = jobs.CompactABlockTaskFactory(blk.meta, blk.scheduler)
		taskType = tasks.DataCompactionTask
	}
	scopes = append(scopes, *blk.meta.AsCommonID())
	return
}

func (blk *dataBlock) IsAppendable() bool {
	if !blk.meta.IsAppendable() {
		return false
	}
	if blk.node.Rows(nil, true) == blk.meta.GetSegment().GetTable().GetSchema().BlockMaxRows {
		return false
	}
	return true
}

func (blk *dataBlock) GetTotalChanges() int {
	return int(blk.mvcc.GetChangeNodeCnt())
}

func (blk *dataBlock) Rows(txn txnif.AsyncTxn, coarse bool) int {
	if blk.meta.IsAppendable() {
		rows := int(blk.node.Rows(txn, coarse))
		return rows
	}
	return int(blk.file.ReadRows())
}

//for replay
func (blk *dataBlock) GetRowsOnReplay() uint64 {
	rows := uint64(blk.mvcc.GetTotalRow())
	fileRows := uint64(blk.file.ReadRows())
	if rows > fileRows {
		return rows
	}
	return fileRows
}

//for test
func (blk *dataBlock) Flush() {
	blk.node.OnUnload()
}
func (blk *dataBlock) PPString(level common.PPLevel, depth int, prefix string) string {
	s := fmt.Sprintf("%s | [Rows=%d]", blk.meta.PPString(level, depth, prefix), blk.Rows(nil, true))
	if level >= common.PPL1 {
		blk.mvcc.RLock()
		s2 := blk.mvcc.StringLocked()
		blk.mvcc.RUnlock()
		if s2 != "" {
			s = fmt.Sprintf("%s\n%s", s, s2)
		}
	}
	return s
}

func (blk *dataBlock) FillColumnUpdates(view *model.ColumnView) (err error) {
	chain := blk.mvcc.GetColumnChain(uint16(view.ColIdx))
	chain.RLock()
	view.UpdateMask, view.UpdateVals, err = chain.CollectUpdatesLocked(view.Ts)
	chain.RUnlock()
	return
}

func (blk *dataBlock) FillColumnDeletes(view *model.ColumnView) (err error) {
	deleteChain := blk.mvcc.GetDeleteChain()
	n, err := deleteChain.CollectDeletesLocked(view.Ts, false)
	if err != nil {
		return
	}
	dnode := n.(*updates.DeleteNode)
	if dnode != nil {
		view.DeleteMask = dnode.GetDeleteMaskLocked()
	}
	return
}

func (blk *dataBlock) FillBlockView(colIdx uint16, view *model.BlockView) (err error) {
	chain := blk.mvcc.GetColumnChain(colIdx)
	chain.RLock()
	updateMask, updateVals, err := chain.CollectUpdatesLocked(view.Ts)
	chain.RUnlock()
	if err != nil {
		return
	}
	if updateMask != nil {
		view.SetUpdates(int(colIdx), updateMask, updateVals)
	}
	return
}

func (blk *dataBlock) MakeAppender() (appender data.BlockAppender, err error) {
	if !blk.meta.IsAppendable() {
		panic("can not create appender on non-appendable block")
	}
	appender = newAppender(blk.node)
	return
}

func (blk *dataBlock) GetPKColumnDataOptimized(ts uint64) (view *model.ColumnView, err error) {
	sortIdx := blk.meta.GetSchema().GetSingleSortKeyIdx()
	col, err := blk.loadColumnData(sortIdx, nil)
	if err != nil {
		return
	}
	view = model.NewColumnView(ts, sortIdx)
	view.SetData(col)
	blk.mvcc.RLock()
	err = blk.FillColumnDeletes(view)
	blk.mvcc.RUnlock()
	if err != nil {
		return
	}
	return
}

func (blk *dataBlock) GetColumnDataByName(
	txn txnif.AsyncTxn,
	attr string,
	compressed, buffer *bytes.Buffer) (view *model.ColumnView, err error) {
	colIdx := blk.meta.GetSchema().GetColIdx(attr)
	return blk.GetColumnDataById(txn, colIdx, compressed, buffer)
}

func (blk *dataBlock) GetColumnDataById(
	txn txnif.AsyncTxn,
	colIdx int,
	compressed, buffer *bytes.Buffer) (view *model.ColumnView, err error) {
	if blk.meta.IsAppendable() {
		return blk.getVectorCopy(txn.GetStartTS(), colIdx, compressed, buffer, false)
	}

	view = model.NewColumnView(txn.GetStartTS(), colIdx)
	vec, err := blk.loadColumnData(colIdx, buffer)
	if err != nil {
		return
	}
	view.SetData(vec)

	blk.mvcc.RLock()
	err = blk.FillColumnUpdates(view)
	if err == nil {
		err = blk.FillColumnDeletes(view)
	}
	blk.mvcc.RUnlock()
	if err != nil {
		return
	}
	err = view.Eval(true)
	return
}

func (blk *dataBlock) getVectorCopy(
	ts uint64,
	colIdx int,
	compressed, buffer *bytes.Buffer,
	raw bool) (view *model.ColumnView, err error) {
	err = blk.node.DoWithPin(func() (err error) {
		maxRow := uint32(0)
		var visible bool
		blk.mvcc.RLock()
		if ts >= blk.GetMaxVisibleTS() {
			maxRow = blk.node.rows
			visible = true
		} else {
			maxRow, visible, err = blk.mvcc.GetMaxVisibleRowLocked(ts)
		}
		blk.mvcc.RUnlock()
		if !visible || err != nil {
			return
		}

		view = model.NewColumnView(ts, colIdx)
		if raw {
			var data containers.Vector
			data, err = blk.node.GetColumnDataCopy(maxRow, colIdx, buffer)
			if err != nil {
				return
			}
			view.SetData(data)
			return
		}

		vec, err := blk.node.GetColumnDataCopy(maxRow, colIdx, buffer)
		if err != nil {
			return
		}
		view.SetData(vec)
		blk.mvcc.RLock()
		err = blk.FillColumnUpdates(view)
		if err == nil {
			err = blk.FillColumnDeletes(view)
		}
		blk.mvcc.RUnlock()
		if err != nil {
			return
		}

		err = view.Eval(true)
		return
	})

	return
}

func (blk *dataBlock) Update(txn txnif.AsyncTxn, row uint32, colIdx uint16, v any) (node txnif.UpdateNode, err error) {
	if blk.meta.GetSchema().HiddenKey.Idx == int(colIdx) {
		err = data.ErrUpdateHiddenKey
		return
	}
	return blk.updateWithFineLock(txn, row, colIdx, v)
}

func (blk *dataBlock) OnReplayUpdate(colIdx uint16, node txnif.UpdateNode) (err error) {
	chain := blk.mvcc.GetColumnChain(colIdx)
	chain.OnReplayUpdateNode(node)
	return
}

func (blk *dataBlock) updateWithCoarseLock(
	txn txnif.AsyncTxn,
	row uint32,
	colIdx uint16,
	v any) (node txnif.UpdateNode, err error) {
	blk.mvcc.Lock()
	defer blk.mvcc.Unlock()
	err = blk.mvcc.CheckNotDeleted(row, row, txn.GetStartTS())
	if err == nil {
		if err = blk.mvcc.CheckNotUpdated(row, row, txn.GetStartTS()); err != nil {
			return
		}
		chain := blk.mvcc.GetColumnChain(colIdx)
		chain.Lock()
		node = chain.AddNodeLocked(txn)
		if err = chain.TryUpdateNodeLocked(row, v, node); err != nil {
			chain.DeleteNodeLocked(node.GetDLNode())
		}
		chain.Unlock()
	}
	return
}

func (blk *dataBlock) updateWithFineLock(
	txn txnif.AsyncTxn,
	row uint32,
	colIdx uint16,
	v any) (node txnif.UpdateNode, err error) {
	blk.mvcc.RLock()
	defer blk.mvcc.RUnlock()
	err = blk.mvcc.CheckNotDeleted(row, row, txn.GetStartTS())
	if err == nil {
		chain := blk.mvcc.GetColumnChain(colIdx)
		chain.Lock()
		node = chain.AddNodeLocked(txn)
		if err = chain.TryUpdateNodeLocked(row, v, node); err != nil {
			chain.DeleteNodeLocked(node.GetDLNode())
		}
		chain.Unlock()
	}
	return
}

func (blk *dataBlock) OnReplayDelete(node txnif.DeleteNode) (err error) {
	blk.mvcc.OnReplayDeleteNode(node)
	err = node.OnApply()
	return
}

func (blk *dataBlock) RangeDelete(
	txn txnif.AsyncTxn,
	start, end uint32) (node txnif.DeleteNode, err error) {
	blk.mvcc.Lock()
	defer blk.mvcc.Unlock()
	err = blk.mvcc.CheckNotDeleted(start, end, txn.GetStartTS())
	if err == nil {
		if err = blk.mvcc.CheckNotUpdated(start, end, txn.GetStartTS()); err == nil {
			node = blk.mvcc.CreateDeleteNode(txn)
			node.RangeDeleteLocked(start, end)
		}
	}
	return
}

func (blk *dataBlock) GetValue(txn txnif.AsyncTxn, row, col int) (v any, err error) {
	ts := txn.GetStartTS()
	blk.mvcc.RLock()
	deleted, err := blk.mvcc.IsDeletedLocked(uint32(row), ts, blk.mvcc.RWMutex)
	if err != nil {
		blk.mvcc.RUnlock()
		return
	}
	if !deleted {
		chain := blk.mvcc.GetColumnChain(uint16(col))
		chain.RLock()
		v, err = chain.GetValueLocked(uint32(row), ts)
		chain.RUnlock()
		if err == txnif.TxnInternalErr {
			blk.mvcc.RUnlock()
			return
		}
		if err != nil {
			v = nil
			err = nil
		}
	} else {
		err = data.ErrNotFound
	}
	blk.mvcc.RUnlock()
	if v != nil || err != nil {
		return
	}
	view := model.NewColumnView(txn.GetStartTS(), int(col))
	if blk.meta.IsAppendable() {
		view, _ = blk.getVectorCopy(txn.GetStartTS(), int(col), nil, nil, true)
	} else {
		vec, _ := blk.loadColumnData(int(col), nil)
		view.SetData(vec)
	}
	v = view.GetValue(row)
	view.Close()
	return
}

func (blk *dataBlock) loadColumnData(
	colIdx int,
	buffer *bytes.Buffer) (vec containers.Vector, err error) {
	dataFile := blk.colFiles[colIdx]
	def := blk.meta.GetSchema().ColDefs[colIdx]
	vec = containers.MakeVector(def.Type, def.Nullable())
	if err = vec.ReadFromFile(dataFile, buffer); err != nil {
		return
	}
	return
}

func (blk *dataBlock) ablkGetByFilter(ts uint64, filter *handle.Filter) (offset uint32, err error) {
	blk.mvcc.RLock()
	defer blk.mvcc.RUnlock()
	offset, err = blk.index.GetActiveRow(filter.Val)
	// Unknow err. return fast
	if err != nil && err != data.ErrNotFound {
		return
	}

	// If found in active map, check visibility first
	if err == nil {
		var visible bool
		visible, err = blk.mvcc.IsVisibleLocked(offset, ts)
		// Unknow err. return fast
		if err != nil {
			return
		}
		// logutil.Infof("ts=%d, maxVisible=%d,visible=%v", ts, blk.mvcc.LoadMaxVisible(), visible)
		// If row is visible to txn
		if visible {
			var deleted bool
			// Check if it was detetd
			deleted, err = blk.mvcc.IsDeletedLocked(offset, ts, blk.mvcc.RWMutex)
			if err != nil {
				return
			}
			if deleted {
				err = data.ErrNotFound
			}
			return
		}
	}
	err = nil

	// Check delete map
	deleted, existed := blk.index.IsKeyDeleted(filter.Val, ts)
	if !existed || deleted {
		err = data.ErrNotFound
		// panic(fmt.Sprintf("%v:%v %v:%s", existed, deleted, filter.Val, blk.index.String()))
	}
	return
}

func (blk *dataBlock) blkGetByFilter(ts uint64, filter *handle.Filter) (offset uint32, err error) {
	err = blk.index.Dedup(filter.Val)
	if err == nil {
		err = data.ErrNotFound
		return
	}
	if err != data.ErrPossibleDuplicate {
		return
	}
	err = nil
	pkColumn, err := blk.loadColumnData(blk.meta.GetSchema().GetSingleSortKeyIdx(), nil)
	if err != nil {
		return
	}
	defer pkColumn.Close()
	off, existed := compute.GetOffsetByVal(pkColumn, filter.Val, nil)
	if !existed {
		err = data.ErrNotFound
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
		err = data.ErrNotFound
	}
	return
}

func (blk *dataBlock) GetByFilter(txn txnif.AsyncTxn, filter *handle.Filter) (offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if blk.meta.GetSchema().SortKey == nil {
		_, _, offset = model.DecodeHiddenKeyFromValue(filter.Val)
		return
	}
	if blk.meta.IsAppendable() {
		return blk.ablkGetByFilter(txn.GetStartTS(), filter)
	}
	return blk.blkGetByFilter(txn.GetStartTS(), filter)
}

func (blk *dataBlock) BlkApplyDelete(deleted uint64, gen common.RowGen, ts uint64) (err error) {
	blk.meta.GetSegment().GetTable().RemoveRows(deleted)
	return
}

func (blk *dataBlock) ABlkApplyDelete(deleted uint64, gen common.RowGen, ts uint64) (err error) {
	// No pk defined
	if !blk.meta.GetSchema().HasPK() {
		blk.meta.GetSegment().GetTable().RemoveRows(deleted)
		return
	}
	// If any pk defined, update index
	if blk.meta.GetSchema().IsSinglePK() {
		var row uint32
		err = blk.node.DoWithPin(func() (err error) {
			blk.mvcc.RLock()
			vecview := blk.node.data.Vecs[blk.meta.GetSchema().GetSingleSortKeyIdx()].GetView()
			blk.mvcc.RUnlock()
			blk.mvcc.Lock()
			defer blk.mvcc.Unlock()
			var currRow uint32
			for gen.HasNext() {
				row = gen.Next()
				v := vecview.Get(int(row))
				currRow, err = blk.index.GetActiveRow(v)
				if err != nil || currRow == row {
					if err = blk.index.Delete(v, ts); err != nil {
						return
					}
				}
			}
			blk.meta.GetSegment().GetTable().RemoveRows(deleted)
			return
		})
	} else {
		var row uint32
		err = blk.node.DoWithPin(func() (err error) {
			var w bytes.Buffer
			sortKeys := blk.meta.GetSchema().SortKey
			vals := make([]any, sortKeys.Size())
			vecs := make([]containers.VectorView, sortKeys.Size())
			blk.mvcc.RLock()
			for i := range vecs {
				vec := blk.node.data.Vecs[sortKeys.Defs[i].Idx].GetView()
				if err != nil {
					blk.mvcc.RUnlock()
					return err
				}
				vecs[i] = vec
			}
			blk.mvcc.RUnlock()
			blk.mvcc.Lock()
			defer blk.mvcc.Unlock()
			var currRow uint32
			for gen.HasNext() {
				row = gen.Next()
				for i := range vals {
					vals[i] = vecs[i].Get(int(row))
				}
				v := model.EncodeTypedVals(&w, vals...)
				currRow, err = blk.index.GetActiveRow(v)
				if err != nil || currRow == row {
					if err = blk.index.Delete(v, ts); err != nil {
						return
					}
				}
			}
			blk.meta.GetSegment().GetTable().RemoveRows(deleted)
			return
		})
	}
	return
}

func (blk *dataBlock) BatchDedup(txn txnif.AsyncTxn, pks containers.Vector, rowmask *roaring.Bitmap) (err error) {
	if blk.meta.IsAppendable() {
		ts := txn.GetStartTS()
		blk.mvcc.RLock()
		defer blk.mvcc.RUnlock()
		keyselects, err := blk.index.BatchDedup(pks, rowmask)
		// If duplicated with active rows
		// TODO: index should store ts to identify w-w
		if err != nil {
			return err
		}
		// Check with deletes map
		// If txn start ts is bigger than deletes max ts, skip scanning deletes
		if ts > blk.index.GetMaxDeleteTS() {
			return err
		}
		it := keyselects.Iterator()
		for it.HasNext() {
			row := it.Next()
			key := pks.Get(int(row))
			if blk.index.HasDeleteFrom(key, ts) {
				err = txnif.TxnWWConflictErr
				break
			}
		}

		return err
	}
	if blk.index == nil {
		panic("index not found")
	}
	keyselects, err := blk.index.BatchDedup(pks, rowmask)
	if err == nil {
		return
	}
	if keyselects == nil {
		panic("unexpected error")
	}
	view, err := blk.GetPKColumnDataOptimized(txn.GetStartTS())
	if err != nil {
		return err
	}
	defer view.Close()
	deduplicate := func(v any, _ int) error {
		if _, existed := compute.GetOffsetByVal(view.GetData(), v, view.DeleteMask); existed {
			return data.ErrDuplicate
		}
		return nil
	}
	err = pks.Foreach(deduplicate, keyselects)
	return
}

func (blk *dataBlock) CollectAppendLogIndexes(startTs, endTs uint64) (indexes []*wal.Index, err error) {
	blk.mvcc.RLock()
	defer blk.mvcc.RUnlock()
	return blk.mvcc.CollectAppendLogIndexesLocked(startTs, endTs)
}

func (blk *dataBlock) CollectChangesInRange(startTs, endTs uint64) (view *model.BlockView, err error) {
	view = model.NewBlockView(endTs)
	blk.mvcc.RLock()

	for i := range blk.meta.GetSchema().ColDefs {
		chain := blk.mvcc.GetColumnChain(uint16(i))
		chain.RLock()
		updateMask, updateVals, indexes, err := chain.CollectCommittedInRangeLocked(startTs, endTs)
		chain.RUnlock()
		if err != nil {
			blk.mvcc.RUnlock()
			return view, err
		}
		if updateMask != nil {
			view.SetUpdates(i, updateMask, updateVals)
		}
		view.SetLogIndexes(i, indexes)
	}
	deleteChain := blk.mvcc.GetDeleteChain()
	view.DeleteMask, view.DeleteLogIndexes, err = deleteChain.CollectDeletesInRange(startTs, endTs)
	blk.mvcc.RUnlock()
	return
}
func (blk *dataBlock) GetSortColumns(schema *catalog.Schema, data *containers.Batch) []containers.Vector {
	vs := make([]containers.Vector, schema.GetSortKeyCnt())
	for i := range vs {
		vs[i] = data.Vecs[schema.SortKey.Defs[i].Idx]
	}
	return vs
}
