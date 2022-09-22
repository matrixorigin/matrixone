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
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
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

type BlockState int8

// BlockState is the state of the data block, which is different from the state of the mate
const (
	BS_Appendable BlockState = iota
	BS_NotAppendable
)

// The initial state of the block when scoring
type statBlock struct {
	rows      uint32
	startTime time.Time
}

type dataBlock struct {
	common.RefHelper
	*sync.RWMutex
	common.ClosedState
	meta      *catalog.BlockEntry
	node      *appendableNode
	file      file.Block
	colFiles  map[int]common.IRWFile
	bufMgr    base.INodeManager
	scheduler tasks.TaskScheduler
	indexes   map[int]indexwrapper.Index
	pkIndex   indexwrapper.Index // a shortcut, nil if no pk column
	mvcc      *updates.MVCCHandle
	score     *statBlock
	ckpTs     atomic.Value
	prefix    []byte
	state     BlockState
}

func newBlock(meta *catalog.BlockEntry, segFile file.Segment, bufMgr base.INodeManager, scheduler tasks.TaskScheduler) *dataBlock {
	colCnt := len(meta.GetSchema().ColDefs)
	indexCnt := make(map[int]int)
	// every column has a zonemap
	schema := meta.GetSchema()
	for _, colDef := range schema.ColDefs {
		if colDef.IsPhyAddr() {
			continue
		}
		indexCnt[colDef.Idx] = 1
	}
	// ATTENTION: COMPOUNDPK
	// pk column has another bloomfilter
	pkIdx := -1024
	if schema.HasPK() {
		pkIdx = schema.GetSingleSortKeyIdx()
		indexCnt[pkIdx] += 1
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
	//var zeroV types.TS
	block := &dataBlock{
		RWMutex:   new(sync.RWMutex),
		meta:      meta,
		file:      file,
		colFiles:  colFiles,
		mvcc:      updates.NewMVCCHandle(meta),
		scheduler: scheduler,
		indexes:   make(map[int]indexwrapper.Index),
		bufMgr:    bufMgr,
		prefix:    meta.MakeKey(),
	}
	ts, _ := block.file.ReadTS()
	block.mvcc.SetAppendListener(block.OnApplyAppend)
	if meta.IsAppendable() {
		block.mvcc.SetDeletesListener(block.ABlkApplyDelete)
		node = newNode(bufMgr, block, file)
		block.node = node
		// if this block is created to receive data, create mutable index first
		for _, colDef := range schema.ColDefs {
			if colDef.IsPhyAddr() {
				continue
			}
			if colDef.Idx == pkIdx {
				block.indexes[colDef.Idx] = indexwrapper.NewPkMutableIndex(colDef.Type)
				block.pkIndex = block.indexes[colDef.Idx]
			} else {
				block.indexes[colDef.Idx] = indexwrapper.NewMutableIndex(colDef.Type)
			}
		}
		block.state = BS_Appendable
	} else {
		block.mvcc.SetDeletesListener(block.BlkApplyDelete)
		// if this block is created to do compact or merge, no need to new index
		// if this block is loaded from storage, ReplayIndex will create index
	}
	block.mvcc.SetMaxVisible(ts)
	block.ckpTs.Store(ts)
	if !ts.IsEmpty() {
		if err := block.ReplayIndex(); err != nil {
			panic(err)
		}
		if err := block.ReplayDelta(); err != nil {
			panic(err)
		}
	}
	return block
}

func (blk *dataBlock) GetMeta() any                 { return blk.meta }
func (blk *dataBlock) GetBufMgr() base.INodeManager { return blk.bufMgr }
func (blk *dataBlock) SetNotAppendable() {
	blk.Lock()
	defer blk.Unlock()
	blk.state = BS_NotAppendable
}

func (blk *dataBlock) GetBlockState() BlockState {
	blk.RLock()
	defer blk.RUnlock()
	return blk.state
}

func (blk *dataBlock) SetMaxCheckpointTS(ts types.TS) {
	blk.ckpTs.Store(ts)
}

func (blk *dataBlock) GetMaxCheckpointTS() types.TS {
	ts := blk.ckpTs.Load().(types.TS)
	return ts
}

func (blk *dataBlock) GetMaxVisibleTS() types.TS {
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
	for _, index := range blk.indexes {
		if err = index.Destroy(); err != nil {
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

func (blk *dataBlock) RunCalibration() int {
	if blk.meta.IsAppendable() && blk.Rows(nil, true) == int(blk.meta.GetSchema().BlockMaxRows) {
		blk.meta.RLock()
		if blk.meta.HasDropped() {
			blk.meta.RUnlock()
			return 0
		}
		blk.meta.RUnlock()
		return 100
	}
	return blk.estimateRawScore()
}

func (blk *dataBlock) estimateRawScore() int {
	if blk.Rows(nil, true) == int(blk.meta.GetSchema().BlockMaxRows) && blk.meta.IsAppendable() {
		return 100
	}

	if blk.mvcc.GetChangeNodeCnt() == 0 && !blk.meta.IsAppendable() {
		return 0
	} else if blk.mvcc.GetChangeNodeCnt() == 0 && blk.meta.IsAppendable() &&
		blk.mvcc.LoadMaxVisible().LessEq(blk.GetMaxCheckpointTS()) {
		return 0
	}
	return 1
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

func (blk *dataBlock) EstimateScore(interval int64) int {
	if blk.meta.IsAppendable() && blk.Rows(nil, true) == int(blk.meta.GetSchema().BlockMaxRows) {
		blk.meta.RLock()
		if blk.meta.HasDropped() {
			blk.meta.RUnlock()
			return 0
		}
		blk.meta.RUnlock()
		return 100
	}

	score := blk.estimateRawScore()
	if score > 1 {
		return score
	}
	rows := uint32(blk.Rows(nil, true)) - blk.mvcc.GetDeleteCnt()
	if blk.score == nil {
		blk.score = &statBlock{
			rows:      rows,
			startTime: time.Now(),
		}
		return 1
	}
	if blk.score.rows != rows {
		// When the rows in the data block are modified, reset the statBlock
		blk.score.rows = rows
		blk.score.startTime = time.Now()
	} else {
		s := time.Since(blk.score.startTime).Milliseconds()
		if s > interval {
			return 100
		}
	}
	return 1
}

func (blk *dataBlock) BuildCompactionTaskFactory() (
	factory tasks.TxnTaskFactory,
	taskType tasks.TaskType,
	scopes []common.ID,
	err error) {
	// If the conditions are met, immediately modify the data block status to NotAppendable
	blk.SetNotAppendable()
	blk.meta.RLock()
	dropped := blk.meta.IsDroppedCommitted()
	inTxn := blk.meta.IsCreating()
	blk.meta.RUnlock()
	if dropped || inTxn {
		return
	}
	// Make sure no appender use this block to compact
	if blk.RefCount() > 0 {
		// logutil.Infof("blk.RefCount() != 0 : %v, rows: %d", blk.meta.String(), blk.node.rows)
		return
	}
	//logutil.Infof("CompactBlockTaskFactory blk: %d, rows: %d", blk.meta.ID, blk.node.rows)
	factory = jobs.CompactBlockTaskFactory(blk.meta, blk.scheduler)
	taskType = tasks.DataCompactionTask
	scopes = append(scopes, *blk.meta.AsCommonID())
	return
}

func (blk *dataBlock) IsAppendable() bool {
	if !blk.meta.IsAppendable() {
		return false
	}

	if blk.GetBlockState() == BS_NotAppendable {
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

// for replay
func (blk *dataBlock) GetRowsOnReplay() uint64 {
	rows := uint64(blk.mvcc.GetTotalRow())
	fileRows := uint64(blk.file.ReadRows())
	if rows > fileRows {
		return rows
	}
	return fileRows
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

func (blk *dataBlock) FillColumnDeletes(view *model.ColumnView, rwlocker *sync.RWMutex) (err error) {
	deleteChain := blk.mvcc.GetDeleteChain()
	n, err := deleteChain.CollectDeletesLocked(view.Ts, false, rwlocker)
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

func (blk *dataBlock) GetColumnDataByName(
	txn txnif.AsyncTxn,
	attr string,
	buffer *bytes.Buffer) (view *model.ColumnView, err error) {
	colIdx := blk.meta.GetSchema().GetColIdx(attr)
	return blk.GetColumnDataById(txn, colIdx, buffer)
}

func (blk *dataBlock) GetColumnDataById(
	txn txnif.AsyncTxn,
	colIdx int,
	buffer *bytes.Buffer) (view *model.ColumnView, err error) {
	if blk.meta.IsAppendable() {
		return blk.ResolveABlkColumnMVCCData(txn.GetStartTS(), colIdx, buffer, false)
	}
	view, err = blk.ResolveColumnMVCCData(txn.GetStartTS(), colIdx, buffer)
	return
}

func (blk *dataBlock) ResolveColumnMVCCData(
	ts types.TS,
	idx int,
	buffer *bytes.Buffer) (view *model.ColumnView, err error) {
	view = model.NewColumnView(ts, idx)
	raw, err := blk.LoadColumnData(idx, buffer)
	if err != nil {
		return
	}
	view.SetData(raw)

	blk.mvcc.RLock()
	err = blk.FillColumnUpdates(view)
	if err == nil {
		err = blk.FillColumnDeletes(view, blk.mvcc.RWMutex)
	}
	blk.mvcc.RUnlock()
	if err != nil {
		return
	}
	err = view.Eval(true)
	return
}

func (blk *dataBlock) ResolveABlkColumnMVCCData(
	ts types.TS,
	colIdx int,
	buffer *bytes.Buffer,
	raw bool) (view *model.ColumnView, err error) {
	var (
		maxRow  uint32
		visible bool
	)
	blk.mvcc.RLock()
	if ts.GreaterEq(blk.GetMaxVisibleTS()) {
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
	var data containers.Vector
	data, err = blk.node.GetColumnDataCopy(0, maxRow, colIdx, buffer)
	if err != nil {
		return
	}
	view.SetData(data)
	blk.mvcc.RLock()
	err = blk.FillColumnUpdates(view)
	if err == nil {
		err = blk.FillColumnDeletes(view, blk.mvcc.RWMutex)
	}
	blk.mvcc.RUnlock()
	if err != nil {
		return
	}

	err = view.Eval(true)

	return
}

func (blk *dataBlock) Update(txn txnif.AsyncTxn, row uint32, colIdx uint16, v any) (node txnif.UpdateNode, err error) {
	if blk.meta.GetSchema().PhyAddrKey.Idx == int(colIdx) {
		err = moerr.NewTAEError("update physical addr key")
		return
	}
	return blk.updateWithFineLock(txn, row, colIdx, v)
}

// updateWithCoarseLock is unused
// func (blk *dataBlock) updateWithCoarseLock(
// 	txn txnif.AsyncTxn,
// 	row uint32,
// 	colIdx uint16,
// 	v any) (node txnif.UpdateNode, err error) {
// 	blk.mvcc.Lock()
// 	defer blk.mvcc.Unlock()
// 	err = blk.mvcc.CheckNotDeleted(row, row, txn.GetStartTS())
// 	if err == nil {
// 		if err = blk.mvcc.CheckNotUpdated(row, row, txn.GetStartTS()); err != nil {
// 			return
// 		}
// 		chain := blk.mvcc.GetColumnChain(colIdx)
// 		chain.Lock()
// 		node = chain.AddNodeLocked(txn)
// 		if err = chain.TryUpdateNodeLocked(row, v, node); err != nil {
// 			chain.DeleteNodeLocked(node.GetDLNode())
// 		}
// 		chain.Unlock()
// 	}
// 	return
// }

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
			chain.DeleteNodeLocked(node.(*updates.ColumnUpdateNode))
		}
		chain.Unlock()
	}
	return
}

func (blk *dataBlock) RangeDelete(
	txn txnif.AsyncTxn,
	start, end uint32, dt handle.DeleteType) (node txnif.DeleteNode, err error) {
	blk.mvcc.Lock()
	defer blk.mvcc.Unlock()
	err = blk.mvcc.CheckNotDeleted(start, end, txn.GetStartTS())
	if err == nil {
		if err = blk.mvcc.CheckNotUpdated(start, end, txn.GetStartTS()); err == nil {
			node = blk.mvcc.CreateDeleteNode(txn, dt)
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
		if moerr.IsMoErrCode(err, moerr.ErrTxnError) {
			blk.mvcc.RUnlock()
			return
		}
		if err != nil {
			v = nil
			err = nil
		}
	} else {
		err = moerr.NewNotFound()
	}
	blk.mvcc.RUnlock()
	if v != nil || err != nil {
		return
	}
	view := model.NewColumnView(txn.GetStartTS(), int(col))
	if blk.meta.IsAppendable() {
		view, _ = blk.ResolveABlkColumnMVCCData(txn.GetStartTS(), int(col), nil, true)
	} else {
		vec, _ := blk.LoadColumnData(int(col), nil)
		view.SetData(vec)
	}
	v = view.GetValue(row)
	view.Close()
	return
}

func (blk *dataBlock) LoadColumnData(
	colIdx int,
	buffer *bytes.Buffer) (vec containers.Vector, err error) {
	def := blk.meta.GetSchema().ColDefs[colIdx]
	vec = containers.MakeVector(def.Type, def.Nullable())
	err = vec.ReadFromFile(blk.colFiles[colIdx], buffer)
	return
}

func (blk *dataBlock) ablkGetByFilter(ts types.TS, filter *handle.Filter) (offset uint32, err error) {
	blk.mvcc.RLock()
	defer blk.mvcc.RUnlock()
	offset, err = blk.pkIndex.GetActiveRow(filter.Val)
	// Unknow err. return fast
	if err != nil && !moerr.IsMoErrCode(err, moerr.ErrNotFound) {
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
				err = moerr.NewNotFound()
			}
			return
		}
	}
	err = nil

	// Check delete map
	deleted, existed := blk.pkIndex.IsKeyDeleted(filter.Val, ts)
	if !existed || deleted {
		err = moerr.NewNotFound()
		// panic(fmt.Sprintf("%v:%v %v:%s", existed, deleted, filter.Val, blk.index.String()))
	}
	return
}

func (blk *dataBlock) blkGetByFilter(ts types.TS, filter *handle.Filter) (offset uint32, err error) {
	err = blk.pkIndex.Dedup(filter.Val)
	if err == nil {
		err = moerr.NewNotFound()
		return
	}
	if !moerr.IsMoErrCode(err, moerr.ErrTAEPossibleDuplicate) {
		return
	}
	err = nil
	var sortKey containers.Vector
	if sortKey, err = blk.LoadColumnData(blk.meta.GetSchema().GetSingleSortKeyIdx(), nil); err != nil {
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

func (blk *dataBlock) GetByFilter(txn txnif.AsyncTxn, filter *handle.Filter) (offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if blk.meta.GetSchema().SortKey == nil {
		_, _, offset = model.DecodePhyAddrKeyFromValue(filter.Val)
		return
	}
	ts := txn.GetStartTS()
	if blk.meta.IsAppendable() {
		return blk.ablkGetByFilter(ts, filter)
	}
	return blk.blkGetByFilter(ts, filter)
}

func (blk *dataBlock) BlkApplyDelete(deleted uint64, gen common.RowGen, ts types.TS) (err error) {
	blk.meta.GetSegment().GetTable().RemoveRows(deleted)
	return
}

func (blk *dataBlock) OnApplyAppend(n txnif.AppendNode) (err error) {
	blk.meta.GetSegment().GetTable().AddRows(uint64(n.GetMaxRow() - n.GetStartRow()))
	return
}

func (blk *dataBlock) ABlkApplyDelete(deleted uint64, gen common.RowGen, ts types.TS) (err error) {
	// No pk defined
	if !blk.meta.GetSchema().HasPK() {
		blk.meta.GetSegment().GetTable().RemoveRows(deleted)
		return
	}
	// If any pk defined, update index
	if blk.meta.GetSchema().IsSinglePK() {
		var row uint32
		blk.mvcc.RLock()
		vecview := blk.node.data.Vecs[blk.meta.GetSchema().GetSingleSortKeyIdx()].GetView()
		blk.mvcc.RUnlock()
		blk.mvcc.Lock()
		defer blk.mvcc.Unlock()
		var currRow uint32
		for gen.HasNext() {
			row = gen.Next()
			v := vecview.Get(int(row))
			currRow, err = blk.pkIndex.GetActiveRow(v)
			if err != nil || currRow == row {
				if err = blk.pkIndex.Delete(v, ts); err != nil {
					return
				}
			}
		}
		blk.meta.GetSegment().GetTable().RemoveRows(deleted)
	} else {
		var row uint32
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
			currRow, err = blk.pkIndex.GetActiveRow(v)
			if err != nil || currRow == row {
				if err = blk.pkIndex.Delete(v, ts); err != nil {
					return
				}
			}
		}
		blk.meta.GetSegment().GetTable().RemoveRows(deleted)
	}
	return
}

func (blk *dataBlock) BatchDedup(txn txnif.AsyncTxn, pks containers.Vector, rowmask *roaring.Bitmap) (err error) {
	if blk.meta.IsAppendable() {
		ts := txn.GetStartTS()
		blk.mvcc.RLock()
		defer blk.mvcc.RUnlock()
		keyselects, err := blk.pkIndex.BatchDedup(pks, rowmask)
		// If duplicated with active rows
		// TODO: index should store ts to identify w-w
		if err != nil {
			return err
		}
		// Check with deletes map
		// If txn start ts is bigger than deletes max ts, skip scanning deletes
		if ts.Greater(blk.pkIndex.GetMaxDeleteTS()) {
			return err
		}
		it := keyselects.Iterator()
		for it.HasNext() {
			row := it.Next()
			key := pks.Get(int(row))
			if blk.pkIndex.HasDeleteFrom(key, ts) {
				err = moerr.NewTxnWWConflict()
				break
			}
		}

		return err
	}
	if blk.indexes == nil {
		panic("index not found")
	}
	keyselects, err := blk.pkIndex.BatchDedup(pks, rowmask)
	if err == nil {
		return
	}
	if keyselects == nil {
		panic("unexpected error")
	}
	sortKey, err := blk.ResolveColumnMVCCData(
		txn.GetStartTS(),
		blk.meta.GetSchema().GetSingleSortKeyIdx(),
		nil)
	if err != nil {
		return
	}
	defer sortKey.Close()
	deduplicate := func(v any, _ int) error {
		if _, existed := compute.GetOffsetByVal(sortKey.GetData(), v, sortKey.DeleteMask); existed {
			return moerr.NewDuplicate()
		}
		return nil
	}
	err = pks.Foreach(deduplicate, keyselects)
	return
}

func (blk *dataBlock) CollectAppendLogIndexes(startTs, endTs types.TS) (indexes []*wal.Index, err error) {
	blk.mvcc.RLock()
	defer blk.mvcc.RUnlock()
	return blk.mvcc.CollectAppendLogIndexesLocked(startTs, endTs)
}

func (blk *dataBlock) CollectChangesInRange(startTs, endTs types.TS) (view *model.BlockView, err error) {
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
			view.SetLogIndexes(i, indexes)
		}
	}
	deleteChain := blk.mvcc.GetDeleteChain()
	view.DeleteMask, view.DeleteLogIndexes, err = deleteChain.CollectDeletesInRange(startTs, endTs, blk.mvcc.RWMutex)
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

func (blk *dataBlock) CollectAppendInRange(start, end types.TS) (*containers.Batch, error) {
	if blk.meta.IsAppendable() {
		return blk.collectAblkAppendInRange(start, end)
	}
	return nil, nil
}

func (blk *dataBlock) collectAblkAppendInRange(start, end types.TS) (*containers.Batch, error) {
	minRow, maxRow, commitTSVec, abortVec := blk.mvcc.CollectAppend(start, end)
	batch, err := blk.node.GetDataCopy(minRow, maxRow)
	if err != nil {
		return nil, err
	}
	batch.AddVector(catalog.AttrCommitTs, commitTSVec)
	batch.AddVector(catalog.AttrAborted, abortVec)
	return batch, nil
}

func (blk *dataBlock) CollectDeleteInRange(start, end types.TS) (*containers.Batch, error) {
	schema := blk.meta.GetSchema()
	var rawPk containers.Vector
	var err error
	maxRow, visible, err := blk.mvcc.GetMaxVisibleRowLocked(end)
	if err != nil || !visible {
		return nil, err
	}
	if schema.IsSinglePK() {
		rawPk, err = blk.node.GetColumnDataCopy(0, maxRow, schema.GetSingleSortKeyIdx(), nil)
		if err != nil {
			return nil, err
		}
	} else if blk.meta.GetSchema().IsCompoundPK() {
		cols := make([]containers.Vector, 0)
		for _, def := range schema.SortKey.Defs {
			col, err := blk.node.GetColumnDataCopy(0, maxRow, def.Idx, nil)
			if err != nil {
				return nil, err
			}
			cols = append(cols, col)
		}
		rawPk = model.EncodeCompoundColumn(cols...)
	}
	pk, rowID, ts, abort := blk.mvcc.CollectDelete(rawPk, start, end)
	batch := containers.NewBatch()
	if schema.HasPK() {
		batch.AddVector(schema.GetSingleSortKey().Name, pk)
	}
	batch.AddVector(catalog.PhyAddrColumnName, rowID)
	batch.AddVector(catalog.AttrCommitTs, ts)
	batch.AddVector(catalog.AttrAborted, abort)
	return batch, nil
}
