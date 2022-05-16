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

	idxCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common/errors"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/model"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/access/acif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/access/impl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"

	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type dataBlock struct {
	*sync.RWMutex
	common.ClosedState
	meta        *catalog.BlockEntry
	node        *appendableNode
	file        file.Block
	colFiles    map[int]common.IRWFile
	bufMgr      base.INodeManager
	scheduler   tasks.TaskScheduler
	indexHolder acif.IBlockIndexHolder
	mvcc        *updates.MVCCHandle
	nice        uint32
	ckpTs       uint64
}

func newBlock(meta *catalog.BlockEntry, segFile file.Segment, bufMgr base.INodeManager, scheduler tasks.TaskScheduler) *dataBlock {
	colCnt := len(meta.GetSchema().ColDefs)
	indexCnt := make(map[int]int)
	indexCnt[int(meta.GetSchema().PrimaryKey)] = 2
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
	}
	if meta.IsAppendable() {
		node = newNode(bufMgr, block, file)
		block.node = node
		schema := meta.GetSchema()
		block.indexHolder = impl.NewAppendableBlockIndexHolder(block, schema)
	} else {
		block.indexHolder = impl.NewEmptyNonAppendableBlockIndexHolder()
	}
	return block
}

func (blk *dataBlock) ReplayData() (err error) {
	if blk.meta.IsAppendable() {
		w, _ := blk.getVectorWrapper(int(blk.meta.GetSchema().PrimaryKey))
		defer common.GPool.Free(w.MNode)
		blk.indexHolder.(acif.IAppendableBlockIndexHolder).BatchInsert(&w.Vector, 0, gvec.Length(&w.Vector), 0, false)
		return
	}
	return blk.indexHolder.(acif.INonAppendableBlockIndexHolder).InitFromHost(blk, blk.meta.GetSchema(), idxCommon.MockIndexBufferManager /* TODO: use dedicated index buffer manager */)
}

func (blk *dataBlock) SetMaxCheckpointTS(ts uint64) {
	atomic.StoreUint64(&blk.ckpTs, ts)
}

func (blk *dataBlock) GetMaxCheckpointTS() uint64 {
	return atomic.LoadUint64(&blk.ckpTs)
}

func (blk *dataBlock) GetMaxVisibleTS() uint64 {
	return blk.mvcc.LoadMaxVisible()
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
	if blk.indexHolder != nil {
		if err = blk.indexHolder.Destroy(); err != nil {
			return err
		}
	}
	if blk.file != nil {
		blk.file.Close()
		blk.file.Destroy()
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
	} else if blk.mvcc.GetChangeNodeCnt() == 0 && blk.meta.IsAppendable() && blk.mvcc.LoadMaxVisible() <= blk.GetMaxCheckpointTS() {
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
	s := fmt.Sprintf("Block %s Mutation Info: Changes=%d/%d", blk.meta.AsCommonID().ToBlockFilePath(), totalChanges, rows)
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

func (blk *dataBlock) BuildCompactionTaskFactory() (factory tasks.TxnTaskFactory, taskType tasks.TaskType, scopes []common.ID, err error) {
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

func (blk *dataBlock) FillColumnUpdates(view *model.ColumnView) {
	chain := blk.mvcc.GetColumnChain(uint16(view.ColIdx))
	chain.RLock()
	view.UpdateMask, view.UpdateVals = chain.CollectUpdatesLocked(view.Ts)
	chain.RUnlock()
}

func (blk *dataBlock) FillColumnDeletes(view *model.ColumnView) {
	deleteChain := blk.mvcc.GetDeleteChain()
	deleteChain.RLock()
	dnode := deleteChain.CollectDeletesLocked(view.Ts, false).(*updates.DeleteNode)
	deleteChain.RUnlock()
	if dnode != nil {
		view.DeleteMask = dnode.GetDeleteMaskLocked()
	}
}

func (blk *dataBlock) FillBlockView(colIdx uint16, view *model.BlockView) (err error) {
	chain := blk.mvcc.GetColumnChain(colIdx)
	chain.RLock()
	updateMask, updateVals := chain.CollectUpdatesLocked(view.Ts)
	chain.RUnlock()
	if updateMask != nil {
		view.UpdateMasks[colIdx] = updateMask
		view.UpdateVals[colIdx] = updateVals
	}
	return
}

func (blk *dataBlock) MakeBlockView() (view *model.BlockView, err error) {
	mvcc := blk.mvcc
	mvcc.RLock()
	ts := mvcc.LoadMaxVisible()
	view = model.NewBlockView(ts)
	for i := range blk.meta.GetSchema().ColDefs {
		blk.FillBlockView(uint16(i), view)
	}
	deleteChain := mvcc.GetDeleteChain()
	dnode := deleteChain.CollectDeletesLocked(ts, true).(*updates.DeleteNode)
	if dnode != nil {
		view.DeleteMask = dnode.GetDeleteMaskLocked()
	}
	maxRow, _ := blk.mvcc.GetMaxVisibleRowLocked(ts)
	if blk.node != nil {
		attrs := make([]int, len(blk.meta.GetSchema().ColDefs))
		vecs := make([]vector.IVector, len(blk.meta.GetSchema().ColDefs))
		for i := range blk.meta.GetSchema().ColDefs {
			attrs[i] = i
			vecs[i], _ = blk.node.GetVectorView(maxRow, i)
		}
		view.Raw, err = batch.NewBatch(attrs, vecs)
	}
	mvcc.RUnlock()
	if blk.node == nil {
		// Load from block file
		view.RawBatch, err = blk.file.LoadBatch(blk.meta.GetSchema().Attrs(), blk.meta.GetSchema().Types())
	}
	return
}

func (blk *dataBlock) MakeAppender() (appender data.BlockAppender, err error) {
	if !blk.meta.IsAppendable() {
		panic("can not create appender on non-appendable block")
	}
	appender = newAppender(blk.node, blk.indexHolder.(acif.IAppendableBlockIndexHolder))
	return
}

func (blk *dataBlock) GetPKColumnDataOptimized(ts uint64) (view *model.ColumnView, err error) {
	pkIdx := int(blk.meta.GetSchema().PrimaryKey)
	wrapper, err := blk.getVectorWrapper(pkIdx)
	if err != nil {
		return view, err
	}
	view = model.NewColumnView(ts, pkIdx)
	view.MemNode = wrapper.MNode
	view.RawVec = &wrapper.Vector
	blk.mvcc.RLock()
	blk.FillColumnDeletes(view)
	blk.mvcc.RUnlock()
	view.AppliedVec = view.RawVec
	return
}

func (blk *dataBlock) GetColumnDataByName(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer) (view *model.ColumnView, err error) {
	colIdx := blk.meta.GetSchema().GetColIdx(attr)
	return blk.GetColumnDataById(txn, colIdx, compressed, decompressed)
}

func (blk *dataBlock) GetColumnDataById(txn txnif.AsyncTxn, colIdx int, compressed, decompressed *bytes.Buffer) (view *model.ColumnView, err error) {
	if blk.meta.IsAppendable() {
		return blk.getVectorCopy(txn.GetStartTS(), colIdx, compressed, decompressed, false)
	}

	view = model.NewColumnView(txn.GetStartTS(), colIdx)
	if view.RawVec, err = blk.getVectorWithBuffer(colIdx, compressed, decompressed); err != nil {
		return
	}

	blk.mvcc.RLock()
	blk.FillColumnUpdates(view)
	blk.FillColumnDeletes(view)
	blk.mvcc.RUnlock()
	view.Eval(true)
	return
}

func (blk *dataBlock) getVectorCopy(ts uint64, colIdx int, compressed, decompressed *bytes.Buffer, raw bool) (view *model.ColumnView, err error) {
	h := blk.node.mgr.Pin(blk.node)
	if h == nil {
		panic("not expected")
	}
	defer h.Close()

	maxRow := uint32(0)
	visible := true
	blk.mvcc.RLock()
	maxRow, visible = blk.mvcc.GetMaxVisibleRowLocked(ts)
	blk.mvcc.RUnlock()
	if !visible {
		return
	}

	view = model.NewColumnView(ts, colIdx)
	if raw {
		view.RawVec, err = blk.node.GetVectorCopy(maxRow, colIdx, compressed, decompressed)
		return
	}

	ivec, err := blk.node.GetVectorView(maxRow, colIdx)
	if err != nil {
		return
	}
	// TODO: performance optimization needed
	var srcvec *gvec.Vector
	if decompressed == nil {
		srcvec, _ = ivec.CopyToVector()
	} else {
		srcvec, _ = ivec.CopyToVectorWithBuffer(compressed, decompressed)
	}
	if maxRow < uint32(gvec.Length(srcvec)) {
		view.RawVec = gvec.New(srcvec.Typ)
		gvec.Window(srcvec, 0, int(maxRow), view.RawVec)
	} else {
		view.RawVec = srcvec
	}

	blk.mvcc.RLock()
	blk.FillColumnUpdates(view)
	blk.FillColumnDeletes(view)
	blk.mvcc.RUnlock()

	view.Eval(true)

	return
}

func (blk *dataBlock) Update(txn txnif.AsyncTxn, row uint32, colIdx uint16, v interface{}) (node txnif.UpdateNode, err error) {
	return blk.updateWithFineLock(txn, row, colIdx, v)
}

func (blk *dataBlock) OnReplayUpdate(row uint32, colIdx uint16, v interface{}) (err error) {
	blk.mvcc.RLock()
	defer blk.mvcc.RUnlock()
	if err == nil {
		chain := blk.mvcc.GetColumnChain(colIdx)
		chain.Lock()
		node := chain.AddNodeLocked(nil)
		if err = chain.TryUpdateNodeLocked(row, v, node); err != nil {
			chain.DeleteNodeLocked(node.GetDLNode())
		}
		chain.Unlock()
	}
	return
}

func (blk *dataBlock) updateWithCoarseLock(txn txnif.AsyncTxn, row uint32, colIdx uint16, v interface{}) (node txnif.UpdateNode, err error) {
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

func (blk *dataBlock) updateWithFineLock(txn txnif.AsyncTxn, row uint32, colIdx uint16, v interface{}) (node txnif.UpdateNode, err error) {
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

func (blk *dataBlock) OnReplayDelete(start, end uint32) (err error) {
	node := blk.mvcc.CreateDeleteNode(nil)
	node.RangeDeleteLocked(start, end)
	return
}

func (blk *dataBlock) RangeDelete(txn txnif.AsyncTxn, start, end uint32) (node txnif.DeleteNode, err error) {
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

func (blk *dataBlock) GetValue(txn txnif.AsyncTxn, row uint32, col uint16) (v interface{}, err error) {
	ts := txn.GetStartTS()
	blk.mvcc.RLock()
	deleteChain := blk.mvcc.GetDeleteChain()
	deleteChain.RLock()
	deleted := deleteChain.IsDeleted(row, ts)
	deleteChain.RUnlock()
	if !deleted {
		chain := blk.mvcc.GetColumnChain(col)
		chain.RLock()
		v, err = chain.GetValueLocked(row, ts)
		chain.RUnlock()
		if err != nil {
			v = nil
			err = nil
		}
	} else {
		err = txnbase.ErrNotFound
	}
	blk.mvcc.RUnlock()
	if v != nil || err != nil {
		return
	}
	view := model.NewColumnView(txn.GetStartTS(), int(col))
	if blk.meta.IsAppendable() {
		view, _ = blk.getVectorCopy(txn.GetStartTS(), int(col), nil, nil, true)
	} else {
		wrapper, _ := blk.getVectorWrapper(int(col))
		// defer common.GPool.Free(wrapper.MNode)
		view.RawVec = &wrapper.Vector
		view.MemNode = wrapper.MNode
		defer view.Free()
	}
	v = compute.GetValue(view.RawVec, row)
	return
}

func (blk *dataBlock) getVectorWithBuffer(colIdx int, compressed, decompressed *bytes.Buffer) (vec *gvec.Vector, err error) {
	dataFile := blk.colFiles[colIdx]

	wrapper := vector.NewEmptyWrapper(blk.meta.GetSchema().ColDefs[colIdx].Type)
	wrapper.File = dataFile
	if compressed == nil || decompressed == nil {
		_, err = wrapper.ReadFrom(dataFile)
	} else {
		_, err = wrapper.ReadWithBuffer(dataFile, compressed, decompressed)
	}
	if err != nil {
		return
	}
	vec = &wrapper.Vector
	return
}

func (blk *dataBlock) getVectorWrapper(colIdx int) (wrapper *vector.VectorWrapper, err error) {
	dataFile := blk.colFiles[colIdx]

	wrapper = vector.NewEmptyWrapper(blk.meta.GetSchema().ColDefs[colIdx].Type)
	wrapper.File = dataFile
	_, err = wrapper.ReadFrom(dataFile)
	if err != nil {
		return
	}

	return
}

func (blk *dataBlock) ablkGetByFilter(ts uint64, filter *handle.Filter) (offset uint32, err error) {
	readLock := blk.mvcc.GetSharedLock()
	defer readLock.Unlock()
	offset, err = blk.indexHolder.(acif.IAppendableBlockIndexHolder).Search(filter.Val)
	if err != nil {
		return
	}
	if blk.mvcc.IsDeletedLocked(offset, ts) || !blk.mvcc.IsVisibleLocked(offset, ts) {
		err = txnbase.ErrNotFound
	}
	return
}

func (blk *dataBlock) blkGetByFilter(ts uint64, filter *handle.Filter) (offset uint32, err error) {
	mayExists := blk.indexHolder.(acif.INonAppendableBlockIndexHolder).MayContainsKey(filter.Val)
	if !mayExists {
		err = txnbase.ErrNotFound
		return
	}
	pkColumn, err := blk.getVectorWrapper(int(blk.meta.GetSchema().PrimaryKey))
	if err != nil {
		return
	}
	defer common.GPool.Free(pkColumn.MNode)
	data := &pkColumn.Vector
	offset, exist := compute.CheckRowExists(data, filter.Val, nil)
	if !exist {
		err = txnbase.ErrNotFound
		return
	}

	readLock := blk.mvcc.GetSharedLock()
	defer readLock.Unlock()
	if blk.mvcc.IsDeletedLocked(offset, ts) {
		err = txnbase.ErrNotFound
	}
	return
}

func (blk *dataBlock) GetByFilter(txn txnif.AsyncTxn, filter *handle.Filter) (offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	if blk.meta.IsAppendable() {
		return blk.ablkGetByFilter(txn.GetStartTS(), filter)
	}
	return blk.blkGetByFilter(txn.GetStartTS(), filter)
}

func (blk *dataBlock) BatchDedup(txn txnif.AsyncTxn, pks *gvec.Vector) (err error) {
	if blk.meta.IsAppendable() {
		readLock := blk.mvcc.GetSharedLock()
		defer readLock.Unlock()
		if err = blk.indexHolder.(acif.IAppendableBlockIndexHolder).BatchDedup(pks); err != nil {
			if err == errors.ErrKeyDuplicate {
				return txnbase.ErrDuplicated
			}
			return err
		}
		return nil
	}
	if blk.indexHolder == nil {
		return nil
	}
	var visibilityMap *roaring.Bitmap
	err, visibilityMap = blk.indexHolder.(acif.INonAppendableBlockIndexHolder).MayContainsAnyKeys(pks)
	if err == nil {
		return nil
	}
	if visibilityMap == nil {
		panic("unexpected error")
	}
	view, err := blk.GetPKColumnDataOptimized(txn.GetStartTS())
	if err != nil {
		return err
	}
	defer view.Free()
	deduplicate := func(v interface{}) error {
		if _, exist := compute.CheckRowExists(view.AppliedVec, v, view.DeleteMask); exist {
			return txnbase.ErrDuplicated
		}
		return nil
	}
	if err = common.ProcessVector(pks, 0, -1, deduplicate, visibilityMap); err != nil {
		return err
	}
	return
}

func (blk *dataBlock) CollectAppendLogIndexes(startTs, endTs uint64) (indexes []*wal.Index) {
	readLock := blk.mvcc.GetSharedLock()
	defer readLock.Unlock()
	return blk.mvcc.CollectAppendLogIndexesLocked(startTs, endTs)
}

func (blk *dataBlock) CollectChangesInRange(startTs, endTs uint64) (view *model.BlockView) {
	view = model.NewBlockView(endTs)
	blk.mvcc.RLock()

	for i := range blk.meta.GetSchema().ColDefs {
		chain := blk.mvcc.GetColumnChain(uint16(i))
		chain.RLock()
		updateMask, updateVals, indexes := chain.CollectCommittedInRangeLocked(startTs, endTs)
		chain.RUnlock()
		if updateMask != nil {
			view.UpdateMasks[uint16(i)] = updateMask
			view.UpdateVals[uint16(i)] = updateVals
		}
		view.ColLogIndexes[uint16(i)] = indexes
	}
	deleteChain := blk.mvcc.GetDeleteChain()
	deleteChain.RLock()
	view.DeleteMask, view.DeleteLogIndexes = deleteChain.CollectDeletesInRange(startTs, endTs)
	deleteChain.RUnlock()
	blk.mvcc.RUnlock()
	return
}
