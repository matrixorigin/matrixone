package tables

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/access/accessif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/access/impl"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/updates"

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
	updatableIndexHolder accessif.IAppendableBlockIndexHolder
	controller           *updates.MutationController
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

func (blk *dataBlock) MakeAppender() (appender data.BlockAppender, err error) {
	if !blk.IsAppendable() {
		err = data.ErrNotAppendable
		return
	}
	appender = newAppender(blk.node, blk.updatableIndexHolder)
	return
}

func (blk *dataBlock) GetVectorCopy(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer) (vec *gvec.Vector, deletes *roaring.Bitmap, err error) {
	return blk.getVectorCopy(txn, attr, compressed, decompressed, false)
}

func (blk *dataBlock) getVectorCopy(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer, raw bool) (vec *gvec.Vector, deletes *roaring.Bitmap, err error) {
	h := blk.node.mgr.Pin(blk.node)
	if h == nil {
		panic("not expected")
	}
	defer h.Close()
	colIdx := blk.meta.GetSchema().GetColIdx(attr)

	readLock := blk.controller.GetSharedLock()
	chain := blk.controller.GetColumnChain(uint16(colIdx))
	chain.RLock()
	updateMask, updateVals := chain.CollectUpdatesLocked(txn.GetStartTS())
	chain.RUnlock()
	deleteChain := blk.controller.GetDeleteChain()
	dnode := deleteChain.CollectDeletesLocked(txn.GetStartTS()).(*updates.DeleteNode)
	readLock.Unlock()

	blk.RLock()
	vec, err = blk.node.GetVectorCopy(txn, attr, compressed, decompressed)
	blk.RUnlock()
	if err != nil || raw {
		return
	}
	vec = compute.ApplyUpdateToVector(vec, updateMask, updateVals)
	if dnode != nil {
		deletes = dnode.GetDeleteMaskLocked()
	}
	// if dnode != nil {
	// 	vec = dnode.ApplyDeletes(vec)
	// }
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
	raw, _, _ := blk.getVectorCopy(txn, attr, &comp, &decomp, true)
	v = compute.GetValue(raw, row)
	return
}

func (blk *dataBlock) GetByFilter(txn txnif.AsyncTxn, filter *handle.Filter) (offset uint32, err error) {
	if filter.Op != handle.FilterEq {
		panic("logic error")
	}
	blk.RLock()
	defer blk.RUnlock()
	return blk.updatableIndexHolder.Search(filter.Val)
}

func (blk *dataBlock) BatchDedup(txn txnif.AsyncTxn, pks *gvec.Vector) (err error) {
	if blk.updatableIndexHolder == nil {
		panic("unexpected error")
	}
	// logutil.Infof("BatchDedup %s: PK=%s", txn.String(), pks.String())
	return blk.updatableIndexHolder.BatchDedup(pks)
}
