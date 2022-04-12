package tables

import (
	"bytes"
	"sync"

	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/updates"
)

type dataBlock struct {
	*sync.RWMutex
	meta   *catalog.BlockEntry
	node   *appendableNode
	file   dataio.BlockFile
	bufMgr base.INodeManager
	chain  *updates.BlockUpdateChain
}

func newBlock(meta *catalog.BlockEntry, segFile dataio.SegmentFile, bufMgr base.INodeManager) *dataBlock {
	file := segFile.GetBlockFile(meta.GetID())
	var node *appendableNode
	if meta.IsAppendable() {
		node = newNode(bufMgr, meta, file)
	}
	return &dataBlock{
		RWMutex: new(sync.RWMutex),
		meta:    meta,
		file:    file,
		node:    node,
	}
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

func (blk *dataBlock) MakeAppender() (appender data.BlockAppender, err error) {
	if !blk.IsAppendable() {
		err = data.ErrNotAppendable
		return
	}
	appender = newAppender(blk.node)
	return
}

func (blk *dataBlock) GetVectorCopy(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer) (vec *gvec.Vector, err error) {
	h := blk.node.mgr.Pin(blk.node)
	if h == nil {
		panic("not expected")
	}
	defer h.Close()
	blk.RLock()
	defer blk.RUnlock()
	return blk.node.GetVectorCopy(txn, attr, compressed, decompressed)
}

func (blk *dataBlock) RangeDelete(txn txnif.AsyncTxn, start, end uint32) (node txnif.UpdateNode, err error) {
	blk.Lock()
	defer blk.Unlock()
	// First update
	if blk.chain == nil {
		blk.chain = updates.NewUpdateChain(blk.RWMutex, blk.meta)
		node = blk.chain.AddNodeLocked(txn)
		return
	}
	err = blk.chain.TryDeleteRowsLocked(start, end, txn)
	if err != nil {
		return
	}
	node = blk.chain.AddNodeLocked(txn)
	node.ApplyDeleteRowsLocked(start, end)
	return
}
