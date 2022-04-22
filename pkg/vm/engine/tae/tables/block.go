package tables

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/access/acif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/access/impl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"

	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
)

type dataBlock struct {
	*sync.RWMutex
	meta                 *catalog.BlockEntry
	node                 *appendableNode
	file                 dataio.BlockFile
	bufMgr               base.INodeManager
	updatableIndexHolder acif.IAppendableBlockIndexHolder
	controller           *updates.MutationController
	maxCkp               uint64
}

func newBlock(meta *catalog.BlockEntry, segFile dataio.SegmentFile, bufMgr base.INodeManager) *dataBlock {
	file := segFile.GetBlockFile(meta.GetID())
	var node *appendableNode
	block := &dataBlock{
		RWMutex:    new(sync.RWMutex),
		meta:       meta,
		file:       file,
		controller: updates.NewMutationNode(meta),
	}
	if meta.IsAppendable() {
		node = newNode(bufMgr, block, file)
		block.node = node
		pkType := meta.GetSegment().GetTable().GetSchema().GetPKType()
		block.updatableIndexHolder = impl.NewAppendableBlockIndexHolder(pkType)
	}
	return block
}

func (blk *dataBlock) GetID() uint64 { return blk.meta.ID }
func (blk *dataBlock) IsDirty() bool { return true }
func (blk *dataBlock) TryCheckpoint() {

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

func (blk *dataBlock) Rows(txn txnif.AsyncTxn, coarse bool) int {
	if blk.meta.IsAppendable() {
		rows := int(blk.node.Rows(txn, coarse))
		return rows
	}
	return int(blk.file.Rows())
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

func (blk *dataBlock) MakeBlockView() (view *updates.BlockView) {
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
		for i, colDef := range blk.meta.GetSchema().ColDefs {
			attrs[i] = i
			vecs[i], _ = blk.node.GetVectorView(maxRow, colDef.Name)
		}
		view.Raw, _ = batch.NewBatch(attrs, vecs)
	}
	readLock.Unlock()
	if blk.node == nil {
		// Load from block file
		panic("TODO")
	}
	return
}

func (blk *dataBlock) MakeAppender() (appender data.BlockAppender, err error) {
	appender = newAppender(blk.node, blk.updatableIndexHolder)
	return
}

func (blk *dataBlock) GetVectorCopy(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer) (vec *gvec.Vector, deletes *roaring.Bitmap, err error) {
	return blk.getVectorCopy(txn.GetStartTS(), attr, compressed, decompressed, false)
}

func (blk *dataBlock) getVectorCopy(ts uint64, attr string, compressed, decompressed *bytes.Buffer, raw bool) (vec *gvec.Vector, deletes *roaring.Bitmap, err error) {
	h := blk.node.mgr.Pin(blk.node)
	if h == nil {
		panic("not expected")
	}
	defer h.Close()

	maxRow := uint32(0)
	visible := true
	if blk.meta.IsAppendable() {
		readLock := blk.controller.GetSharedLock()
		maxRow, visible = blk.controller.GetMaxVisibleRowLocked(ts)
		readLock.Unlock()
		logutil.Infof("maxrow=%d", maxRow)
	}
	if !visible {
		return
	}

	if raw {
		vec, err = blk.node.GetVectorCopy(maxRow, attr, compressed, decompressed)
		return
	}

	ivec, err := blk.node.GetVectorView(maxRow, attr)
	if err != nil {
		return
	}

	colIdx := blk.meta.GetSchema().GetColIdx(attr)
	view := updates.NewBlockView(ts)

	sharedLock := blk.controller.GetSharedLock()
	err = blk.makeColumnView(uint16(colIdx), view)
	deleteChain := blk.controller.GetDeleteChain()
	dnode := deleteChain.CollectDeletesLocked(ts).(*updates.DeleteNode)
	sharedLock.Unlock()
	if dnode != nil {
		view.DeleteMask = dnode.GetDeleteMaskLocked()
	}

	// TODO: performance optimization needed
	srcvec, _ := ivec.CopyToVectorWithBuffer(compressed, decompressed)
	if maxRow < uint32(gvec.Length(srcvec)) {
		vec = gvec.New(srcvec.Typ)
		gvec.Window(srcvec, 0, int(maxRow), vec)
	} else {
		vec = srcvec
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
	var comp bytes.Buffer
	var decomp bytes.Buffer
	attr := blk.meta.GetSegment().GetTable().GetSchema().ColDefs[col].Name
	raw, _, _ := blk.getVectorCopy(txn.GetStartTS(), attr, &comp, &decomp, true)
	v = compute.GetValue(raw, row)
	return
}

func (blk *dataBlock) GetByFilter(txn txnif.AsyncTxn, filter *handle.Filter) (offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	readLock := blk.controller.GetSharedLock()
	defer readLock.Unlock()
	offset, err = blk.updatableIndexHolder.Search(filter.Val)
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
		// logutil.Infof("BatchDedup %s: PK=%s", txn.String(), pks.String())
		return blk.updatableIndexHolder.BatchDedup(pks)
	}
	// TODO
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
	return v
}
