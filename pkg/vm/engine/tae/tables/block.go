package tables

import (
	"bytes"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/compute"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/access/accessif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index/access/impl"

	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/updates"
)

type dataBlock struct {
	*sync.RWMutex
	meta                 *catalog.BlockEntry
	node                 *appendableNode
	file                 dataio.BlockFile
	bufMgr               base.INodeManager
	chain                *updates.BlockUpdateChain
	updatableIndexHolder accessif.IAppendableBlockIndexHolder
}

func newBlock(meta *catalog.BlockEntry, segFile dataio.SegmentFile, bufMgr base.INodeManager) *dataBlock {
	file := segFile.GetBlockFile(meta.GetID())
	var node *appendableNode
	var holder accessif.IAppendableBlockIndexHolder
	if meta.IsAppendable() {
		node = newNode(bufMgr, meta, file)
		schema := meta.GetSegment().GetTable().GetSchema()
		pkIdx := schema.PrimaryKey
		pkType := schema.Types()[pkIdx]
		holder = impl.NewAppendableBlockIndexHolder(pkType)
	}
	return &dataBlock{
		RWMutex:              new(sync.RWMutex),
		meta:                 meta,
		file:                 file,
		node:                 node,
		updatableIndexHolder: holder,
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
	appender = newAppender(blk.node, blk.updatableIndexHolder)
	return
}

func (blk *dataBlock) GetVectorCopy(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer) (vec *gvec.Vector, err error) {
	return blk.getVectorCopy(txn, attr, compressed, decompressed, false)
}

func (blk *dataBlock) getVectorCopy(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer, raw bool) (vec *gvec.Vector, err error) {
	h := blk.node.mgr.Pin(blk.node)
	if h == nil {
		panic("not expected")
	}
	defer h.Close()
	blk.RLock()
	defer blk.RUnlock()
	vec, err = blk.node.GetVectorCopy(txn, attr, compressed, decompressed)
	if err != nil || raw {
		return
	}
	if blk.chain != nil {
		// TODO: Collect by column
		blkUpdates := blk.chain.CollectCommittedUpdatesLocked(txn)
		logutil.Debug(blkUpdates.String())
		if blkUpdates != nil {
			colIdx := blk.meta.GetSegment().GetTable().GetSchema().GetColIdx(attr)
			vec = blkUpdates.ApplyChangesToColumn(uint16(colIdx), vec)
		}
	}
	return
}

func (blk *dataBlock) Update(txn txnif.AsyncTxn, row uint32, colIdx uint16, v interface{}) (node txnif.UpdateNode, err error) {
	blk.Lock()
	defer blk.Unlock()
	if blk.chain == nil {
		blk.chain = updates.NewUpdateChain(blk.RWMutex, blk.meta)
		node = blk.chain.AddNodeLocked(txn)
		node.ApplyUpdateColLocked(row, colIdx, v)
		return
	}
	// logutil.Info(blk.chain.StringLocked())
	// If the specified row was deleted. w-w
	if err = blk.chain.CheckDeletedLocked(row, row, txn); err != nil {
		return
	}
	// If the specified row was updated by another active txn. w-w
	err = blk.chain.CheckColumnUpdatedLocked(row, colIdx, txn)
	if err != nil {
		return
	}
	node = blk.chain.AddNodeLocked(txn)
	node.ApplyUpdateColLocked(row, colIdx, v)
	return
}

func (blk *dataBlock) RangeDelete(txn txnif.AsyncTxn, start, end uint32) (node txnif.UpdateNode, err error) {
	blk.Lock()
	defer blk.Unlock()
	// First update
	if blk.chain == nil {
		blk.chain = updates.NewUpdateChain(blk.RWMutex, blk.meta)
		node = blk.chain.AddNodeLocked(txn)
		node.ApplyDeleteRowsLocked(start, end)
		return
	}
	err = blk.chain.CheckDeletedLocked(start, end, txn)
	if err != nil {
		return
	}

	for col := range blk.meta.GetSegment().GetTable().GetSchema().ColDefs {
		for row := start; row <= end; row++ {
			if err = blk.chain.CheckColumnUpdatedLocked(row, uint16(col), txn); err != nil {
				return
			}
		}
	}
	node = blk.chain.AddNodeLocked(txn)
	node.ApplyDeleteRowsLocked(start, end)
	return
}

func (blk *dataBlock) GetUpdateChain() txnif.UpdateChain {
	blk.RLock()
	defer blk.RUnlock()
	return blk.chain
}

func (blk *dataBlock) GetValue(txn txnif.AsyncTxn, row uint32, col uint16) (v interface{}, err error) {
	blk.RLock()
	defer blk.RUnlock()
	if blk.chain != nil {
		v, err = blk.chain.GetValueLocked(row, col, txn)
		if err == nil {
			return
		}
	}
	var comp bytes.Buffer
	var decomp bytes.Buffer
	attr := blk.meta.GetSegment().GetTable().GetSchema().ColDefs[col].Name
	raw, _ := blk.getVectorCopy(txn, attr, &comp, &decomp, true)
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
	return blk.updatableIndexHolder.BatchDedup(pks)
}
