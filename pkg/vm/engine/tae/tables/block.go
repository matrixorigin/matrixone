package tables

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"

	idxCommon "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/common/errors"

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
	meta        *catalog.BlockEntry
	node        *appendableNode
	file        file.Block
	bufMgr      base.INodeManager
	ioScheduler tasks.Scheduler
	indexHolder acif.IBlockIndexHolder
	controller  *updates.MutationController
	maxCkp      uint64
	nice        uint32
}

func newBlock(meta *catalog.BlockEntry, segFile file.Segment, bufMgr base.INodeManager, ioScheduler tasks.Scheduler) *dataBlock {
	colCnt := len(meta.GetSchema().ColDefs)
	indexCnt := make(map[int]int)
	indexCnt[int(meta.GetSchema().PrimaryKey)] = 2
	file, err := segFile.OpenBlock(meta.GetID(), colCnt, indexCnt)
	if err != nil {
		panic(err)
	}
	var node *appendableNode
	block := &dataBlock{
		RWMutex:     new(sync.RWMutex),
		meta:        meta,
		file:        file,
		controller:  updates.NewMutationNode(meta),
		ioScheduler: ioScheduler,
	}
	if meta.IsAppendable() {
		node = newNode(bufMgr, block, file)
		block.node = node
		schema := meta.GetSchema()
		block.indexHolder = impl.NewAppendableBlockIndexHolder(block, schema)
	} else {
		// Non-appendable index holder would be initialized during compaction
	}
	return block
}

func (blk *dataBlock) GetBlockFile() file.Block {
	return blk.file
}

func (blk *dataBlock) RefreshIndex() error {
	if blk.meta.IsAppendable() {
		panic("unexpected error")
	}
	if blk.indexHolder == nil {
		blk.indexHolder = impl.NewEmptyNonAppendableBlockIndexHolder()
	}
	return blk.indexHolder.(acif.INonAppendableBlockIndexHolder).InitFromHost(blk, blk.meta.GetSchema(), idxCommon.MockIndexBufferManager /* TODO: use dedicated index buffer manager */)
}

func (blk *dataBlock) GetID() uint64 { return blk.meta.ID }

func (blk *dataBlock) RunCalibration() {
	score := blk.estimateRawScore()
	if score == 0 {
		return
	}
	atomic.AddUint32(&blk.nice, uint32(1))
}

func (blk *dataBlock) estimateRawScore() int {
	if blk.Rows(nil, true) == int(blk.meta.GetSchema().BlockMaxRows) && blk.meta.IsAppendable() {
		return 100
	}
	if blk.controller.GetChangeNodeCnt() == 0 {
		return 0
	}
	cols := 0
	factor := float64(0)
	rows := blk.Rows(nil, true)
	for i := range blk.meta.GetSchema().ColDefs {
		cols++
		cnt := blk.controller.GetColumnUpdateCnt(uint16(i))
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
	deleteCnt := blk.controller.GetDeleteCnt()
	factor += float64(deleteCnt) / float64(rows) * 50
	ret := int(factor * 100)
	if ret == 0 {
		ret += 1
	}
	return ret
}

func (blk *dataBlock) MutationInfo() string {
	rows := blk.Rows(nil, true)
	totalChanges := blk.controller.GetChangeNodeCnt()
	s := fmt.Sprintf("Block %s Mutation Info: Changes=%d/%d", blk.meta.AsCommonID().ToBlockFilePath(), totalChanges, rows)
	if totalChanges == 0 {
		return s
	}
	for i := range blk.meta.GetSchema().ColDefs {
		cnt := blk.controller.GetColumnUpdateCnt(uint16(i))
		if cnt == 0 {
			continue
		}
		s = fmt.Sprintf("%s, Col[%d]:%d/%d", s, i, cnt, rows)
	}
	deleteCnt := blk.controller.GetDeleteCnt()
	if deleteCnt != 0 {
		s = fmt.Sprintf("%s, Del:%d/%d", s, deleteCnt, rows)
	}
	return s
}

func (blk *dataBlock) EstimateScore() int {
	score := blk.estimateRawScore()
	score += int(atomic.LoadUint32(&blk.nice))
	return score
}

func (blk *dataBlock) BuildCheckpointTaskFactory() (factory tasks.TxnTaskFactory, err error) {
	blk.meta.RLock()
	dropped := blk.meta.IsDroppedCommitted()
	inTxn := blk.meta.HasActiveTxn()
	blk.meta.RUnlock()
	if dropped || inTxn {
		return
	}
	factory = jobs.CompactBlockTaskFactory(blk.meta, blk.ioScheduler)
	return
	// if !blk.meta.IsAppendable() {
	// }
	// return
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
	return int(blk.controller.GetChangeNodeCnt())
}

func (blk *dataBlock) Rows(txn txnif.AsyncTxn, coarse bool) int {
	if blk.meta.IsAppendable() {
		rows := int(blk.node.Rows(txn, coarse))
		return rows
	}
	return int(blk.file.ReadRows())
}

func (blk *dataBlock) PPString(level common.PPLevel, depth int, prefix string) string {
	blk.RLock()
	defer blk.RUnlock()
	s := fmt.Sprintf("%s | [Rows=%d]", blk.meta.PPString(level, depth, prefix), blk.Rows(nil, true))
	if level >= common.PPL1 {
		readLock := blk.controller.GetSharedLock()
		s2 := blk.controller.StringLocked()
		readLock.Unlock()
		if s2 != "" {
			s = fmt.Sprintf("%s\n%s", s, s2)
		}
	}
	return s
}

func (blk *dataBlock) makeColumnView(colIdx uint16, view *updates.BlockView) (err error) {
	chain := blk.controller.GetColumnChain(colIdx)
	chain.RLock()
	updateMask, updateVals := chain.CollectUpdatesLocked(view.Ts)
	chain.RUnlock()
	if updateMask != nil {
		view.UpdateMasks[colIdx] = updateMask
		view.UpdateVals[colIdx] = updateVals
	}
	return
}

func (blk *dataBlock) MakeBlockView() (view *updates.BlockView, err error) {
	controller := blk.controller
	readLock := controller.GetSharedLock()
	ts := controller.LoadMaxVisible()
	view = updates.NewBlockView(ts)
	for i := range blk.meta.GetSchema().ColDefs {
		blk.makeColumnView(uint16(i), view)
	}
	deleteChain := controller.GetDeleteChain()
	dnode := deleteChain.CollectDeletesLocked(ts).(*updates.DeleteNode)
	if dnode != nil {
		view.DeleteMask = dnode.GetDeleteMaskLocked()
	}
	maxRow, _ := blk.controller.GetMaxVisibleRowLocked(ts)
	if blk.node != nil {
		attrs := make([]int, len(blk.meta.GetSchema().ColDefs))
		vecs := make([]vector.IVector, len(blk.meta.GetSchema().ColDefs))
		for i := range blk.meta.GetSchema().ColDefs {
			attrs[i] = i
			vecs[i], _ = blk.node.GetVectorView(maxRow, i)
		}
		view.Raw, err = batch.NewBatch(attrs, vecs)
	}
	readLock.Unlock()
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

func (blk *dataBlock) GetColumnDataByName(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer) (vec *gvec.Vector, deletes *roaring.Bitmap, err error) {
	colIdx := blk.meta.GetSchema().GetColIdx(attr)
	return blk.GetColumnDataById(txn, colIdx, compressed, decompressed)
}

func (blk *dataBlock) GetColumnDataById(txn txnif.AsyncTxn, colIdx int, compressed, decompressed *bytes.Buffer) (vec *gvec.Vector, deletes *roaring.Bitmap, err error) {
	if blk.meta.IsAppendable() {
		return blk.getVectorCopy(txn.GetStartTS(), colIdx, compressed, decompressed, false)
	}

	if compressed == nil {
		compressed = &bytes.Buffer{}
		decompressed = &bytes.Buffer{}
	}
	vec, err = blk.getVectorWithBuffer(colIdx, compressed, decompressed)

	view := updates.NewBlockView(txn.GetStartTS())
	sharedLock := blk.controller.GetSharedLock()
	err = blk.makeColumnView(uint16(colIdx), view)
	deleteChain := blk.controller.GetDeleteChain()
	dnode := deleteChain.CollectDeletesLocked(txn.GetStartTS()).(*updates.DeleteNode)
	sharedLock.Unlock()
	if dnode != nil {
		view.DeleteMask = dnode.GetDeleteMaskLocked()
	}
	vec = compute.ApplyUpdateToVector(vec, view.UpdateMasks[uint16(colIdx)], view.UpdateVals[uint16(colIdx)])
	deletes = view.DeleteMask
	return
}

func (blk *dataBlock) getVectorCopy(ts uint64, colIdx int, compressed, decompressed *bytes.Buffer, raw bool) (vec *gvec.Vector, deletes *roaring.Bitmap, err error) {
	h := blk.node.mgr.Pin(blk.node)
	if h == nil {
		panic("not expected")
	}
	defer h.Close()

	maxRow := uint32(0)
	visible := true
	readLock := blk.controller.GetSharedLock()
	maxRow, visible = blk.controller.GetMaxVisibleRowLocked(ts)
	readLock.Unlock()
	if !visible {
		return
	}

	if raw {
		vec, err = blk.node.GetVectorCopy(maxRow, colIdx, compressed, decompressed)
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
		vec = gvec.New(srcvec.Typ)
		gvec.Window(srcvec, 0, int(maxRow), vec)
	} else {
		vec = srcvec
	}

	view := updates.NewBlockView(ts)

	sharedLock := blk.controller.GetSharedLock()
	err = blk.makeColumnView(uint16(colIdx), view)
	deleteChain := blk.controller.GetDeleteChain()
	deleteChain.RLock()
	dnode := deleteChain.CollectDeletesLocked(ts).(*updates.DeleteNode)
	deleteChain.RUnlock()
	sharedLock.Unlock()
	if dnode != nil {
		view.DeleteMask = dnode.GetDeleteMaskLocked()
	}

	vec = compute.ApplyUpdateToVector(vec, view.UpdateMasks[uint16(colIdx)], view.UpdateVals[uint16(colIdx)])

	deletes = view.DeleteMask

	return
}

func (blk *dataBlock) Update(txn txnif.AsyncTxn, row uint32, colIdx uint16, v interface{}) (node txnif.UpdateNode, err error) {
	return blk.updateWithFineLock(txn, row, colIdx, v)
}

func (blk *dataBlock) updateWithCoarseLock(txn txnif.AsyncTxn, row uint32, colIdx uint16, v interface{}) (node txnif.UpdateNode, err error) {
	locker := blk.controller.GetExclusiveLock()
	err = blk.controller.CheckNotDeleted(row, row, txn.GetStartTS())
	if err == nil {
		if err = blk.controller.CheckNotUpdated(row, row, txn.GetStartTS()); err != nil {
			locker.Unlock()
			return
		}
		chain := blk.controller.GetColumnChain(colIdx)
		chain.Lock()
		node = chain.AddNodeLocked(txn)
		if err = chain.TryUpdateNodeLocked(row, v, node); err != nil {
			chain.DeleteNodeLocked(node.GetDLNode())
		}
		chain.Unlock()
	}
	locker.Unlock()
	return
}

func (blk *dataBlock) updateWithFineLock(txn txnif.AsyncTxn, row uint32, colIdx uint16, v interface{}) (node txnif.UpdateNode, err error) {
	locker := blk.controller.GetSharedLock()
	err = blk.controller.CheckNotDeleted(row, row, txn.GetStartTS())
	if err == nil {
		chain := blk.controller.GetColumnChain(colIdx)
		chain.Lock()
		node = chain.AddNodeLocked(txn)
		if err = chain.TryUpdateNodeLocked(row, v, node); err != nil {
			chain.DeleteNodeLocked(node.GetDLNode())
		}
		chain.Unlock()
	}
	locker.Unlock()
	return
}

func (blk *dataBlock) RangeDelete(txn txnif.AsyncTxn, start, end uint32) (node txnif.DeleteNode, err error) {
	locker := blk.controller.GetExclusiveLock()
	err = blk.controller.CheckNotDeleted(start, end, txn.GetStartTS())
	if err == nil {
		if err = blk.controller.CheckNotUpdated(start, end, txn.GetStartTS()); err == nil {
			node = blk.controller.CreateDeleteNode(txn)
			node.RangeDeleteLocked(start, end)
		}
	}
	locker.Unlock()
	return
}

// func (blk *dataBlock) GetUpdateChain() txnif.UpdateChain {
// 	blk.RLock()
// 	defer blk.RUnlock()
// 	return blk.GetUpdateChain()
// }

func (blk *dataBlock) GetValue(txn txnif.AsyncTxn, row uint32, col uint16) (v interface{}, err error) {
	sharedLock := blk.controller.GetSharedLock()
	deleteChain := blk.controller.GetDeleteChain()
	deleted := deleteChain.IsDeleted(row, txn.GetStartTS())
	if !deleted {
		chain := blk.controller.GetColumnChain(col)
		chain.RLock()
		v, err = chain.GetValueLocked(row, txn.GetStartTS())
		chain.RUnlock()
		if err != nil {
			v = nil
			err = nil
		}
	} else {
		err = txnbase.ErrNotFound
	}
	sharedLock.Unlock()
	if v != nil || err != nil {
		return
	}
	var raw *gvec.Vector
	if blk.meta.IsAppendable() {
		var comp bytes.Buffer
		var decomp bytes.Buffer
		raw, _, _ = blk.getVectorCopy(txn.GetStartTS(), int(col), &comp, &decomp, true)
	} else {
		wrapper, _ := blk.getVectorWrapper(int(col))
		defer common.GPool.Free(wrapper.MNode)
		raw = &wrapper.Vector
	}
	v = compute.GetValue(raw, row)
	return
}

func (blk *dataBlock) getVectorWithBuffer(colIdx int, compressed, decompressed *bytes.Buffer) (vec *gvec.Vector, err error) {
	colBlk, _ := blk.file.OpenColumn(colIdx)
	vfile, _ := colBlk.OpenDataFile()

	wrapper := vector.NewEmptyWrapper(blk.meta.GetSchema().ColDefs[colIdx].Type)
	wrapper.File = vfile
	_, err = wrapper.ReadWithBuffer(vfile, compressed, decompressed)
	if err != nil {
		return
	}
	vfile.Unref()
	colBlk.Close()
	vec = &wrapper.Vector
	return
}

func (blk *dataBlock) getVectorWrapper(colIdx int) (wrapper *vector.VectorWrapper, err error) {
	colBlk, _ := blk.file.OpenColumn(colIdx)
	vfile, _ := colBlk.OpenDataFile()

	wrapper = vector.NewEmptyWrapper(blk.meta.GetSchema().ColDefs[colIdx].Type)
	wrapper.File = vfile
	_, err = wrapper.ReadFrom(vfile)
	if err != nil {
		return
	}

	vfile.Unref()
	colBlk.Close()
	return
}

func (blk *dataBlock) GetByFilter(txn txnif.AsyncTxn, filter *handle.Filter) (offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	readLock := blk.controller.GetSharedLock()
	defer readLock.Unlock()
	if blk.meta.IsAppendable() {
		offset, err = blk.indexHolder.(acif.IAppendableBlockIndexHolder).Search(filter.Val)
	} else {
		mayExists := blk.indexHolder.(acif.INonAppendableBlockIndexHolder).MayContainsKey(filter.Val)
		if mayExists {
			// TODO: load exact column data from source and get the row offset if exists and visible
			panic("implement me")
		}
	}
	if err == nil {
		if !blk.controller.IsVisibleLocked(offset, txn.GetStartTS()) {
			err = txnbase.ErrNotFound
		}
	}
	return
}

func (blk *dataBlock) BatchDedup(txn txnif.AsyncTxn, pks *gvec.Vector) (err error) {
	if blk.meta.IsAppendable() {
		readLock := blk.controller.GetSharedLock()
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
	pkColumnData, deletes, err := blk.GetColumnDataById(txn, int(blk.meta.GetSchema().PrimaryKey), nil, nil)
	if err != nil {
		return err
	}
	leftData := compute.ApplyDeleteToVector(pkColumnData, deletes)
	if gvec.Length(leftData) == 0 {
		return nil
	}
	deduplicate := func(v interface{}) error {
		if compute.CheckRowExists(leftData, v) {
			return txnbase.ErrDuplicated
		}
		return nil
	}
	if err = common.ProcessVector(pks, 0, -1, deduplicate, visibilityMap); err != nil {
		return err
	}
	return
}

func (blk *dataBlock) CollectChangesInRange(startTs, endTs uint64) (v interface{}) {
	view := updates.NewBlockView(endTs)
	readLock := blk.controller.GetSharedLock()

	for i := range blk.meta.GetSchema().ColDefs {
		chain := blk.controller.GetColumnChain(uint16(i))
		chain.RLock()
		updateMask, updateVals := chain.CollectCommittedInRangeLocked(startTs, endTs)
		chain.RUnlock()
		if updateMask != nil {
			view.UpdateMasks[uint16(i)] = updateMask
			view.UpdateVals[uint16(i)] = updateVals
		}
	}
	deleteChain := blk.controller.GetDeleteChain()
	view.DeleteMask = deleteChain.CollectDeletesInRange(startTs, endTs)
	readLock.Unlock()
	v = view
	return
}
