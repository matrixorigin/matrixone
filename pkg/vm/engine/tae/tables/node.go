package tables

import (
	"bytes"

	"github.com/RoaringBitmap/roaring"
	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/file"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/updates"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type appendableNode struct {
	*buffer.Node
	file  file.Block
	block *dataBlock
	data  batch.IBatch
	rows  uint32
	mgr   base.INodeManager
}

func newNode(mgr base.INodeManager, block *dataBlock, file file.Block) *appendableNode {
	impl := new(appendableNode)
	id := block.meta.AsCommonID()
	impl.Node = buffer.NewNode(impl, mgr, *id, uint64(catalog.EstimateBlockSize(block.meta, block.meta.GetSchema().BlockMaxRows)))
	impl.UnloadFunc = impl.OnUnload
	impl.LoadFunc = impl.OnLoad
	impl.DestroyFunc = impl.OnDestory
	impl.file = file
	impl.mgr = mgr
	impl.block = block
	mgr.RegisterNode(impl)
	return impl
}

func (node *appendableNode) Rows(txn txnif.AsyncTxn, coarse bool) uint32 {
	if coarse {
		readLock := node.block.mvcc.GetSharedLock()
		defer readLock.Unlock()
		return node.rows
	}
	// TODO: fine row count
	// 1. Load txn ts zonemap
	// 2. Calculate fine row count
	return 0
}

func (node *appendableNode) OnDestory() {
	node.file.Unref()
}

func (node *appendableNode) GetVectorView(maxRow uint32, colIdx int) (vec vector.IVector, err error) {
	ivec, err := node.data.GetVectorByAttr(colIdx)
	if err != nil {
		return
	}
	vec = ivec.GetLatestView()
	return
}

// TODO: Apply updates and txn sels
func (node *appendableNode) GetVectorCopy(maxRow uint32, colIdx int, compressed, decompressed *bytes.Buffer) (vec *gvec.Vector, err error) {
	ro, err := node.GetVectorView(maxRow, colIdx)
	if err != nil {
		return
	}
	if decompressed == nil {
		return ro.CopyToVector()
	}
	return ro.CopyToVectorWithBuffer(compressed, decompressed)
}

func (node *appendableNode) OnLoad() {
	var err error
	schema := node.block.meta.GetSchema()
	if node.data, err = node.file.LoadIBatch(schema.Types(), schema.BlockMaxRows); err != nil {
		panic(err)
	}
}

func (node *appendableNode) OnUnload() {
	logutil.Infof("Unloading block %s", node.block.meta.AsCommonID().String())
	masks := make(map[uint16]*roaring.Bitmap)
	vals := make(map[uint16]map[uint32]interface{})
	mvcc := node.block.mvcc
	readLock := mvcc.GetSharedLock()
	ts := mvcc.LoadMaxVisible()
	for i := range node.block.meta.GetSchema().ColDefs {
		chain := mvcc.GetColumnChain(uint16(i))

		chain.RLock()
		updateMask, updateVals := chain.CollectUpdatesLocked(ts)
		chain.RUnlock()
		if updateMask != nil {
			masks[uint16(i)] = updateMask
			vals[uint16(i)] = updateVals
		}
	}
	deleteChain := mvcc.GetDeleteChain()
	dnode := deleteChain.CollectDeletesLocked(ts).(*updates.DeleteNode)
	readLock.Unlock()
	var deletes *roaring.Bitmap
	if dnode != nil {
		deletes = dnode.GetDeleteMaskLocked()
	}
	ctx := tasks.Context{
		Waitable: true,
	}
	task := jobs.NewFlushABlkTask(&ctx, node.file, node.block.meta.AsCommonID(), node.data, ts, masks, vals, deletes)
	if node.block.ioScheduler != nil {
		node.block.ioScheduler.Schedule(task)
		if err := task.WaitDone(); err != nil {
			panic(err)
		}
	} else {
		if err := task.OnExec(); err != nil {
			panic(err)
		}
	}

	node.data.Close()
	node.data = nil
}

func (node *appendableNode) PrepareAppend(rows uint32) (n uint32, err error) {
	left := node.block.meta.GetSchema().BlockMaxRows - node.rows
	if left == 0 {
		err = data.ErrNotAppendable
		return
	}
	if rows > left {
		n = left
	} else {
		n = rows
	}
	return
	// key = txnbase.KeyEncoder.EncodeBlock(
	// 	node.meta.GetSegment().GetTable().GetDB().GetID(),
	// 	node.meta.GetSegment().GetTable().GetID(),
	// 	node.meta.GetSegment().GetID(),
	// 	node.meta.GetID(),
	// )
}

func (node *appendableNode) ApplyAppend(bat *gbat.Batch, offset, length uint32, txn txnif.AsyncTxn) (from uint32, err error) {
	if node.data == nil {
		vecs := make([]vector.IVector, len(bat.Vecs))
		attrs := make([]int, len(bat.Vecs))
		for i, vec := range bat.Vecs {
			attrs[i] = i
			vecs[i] = vector.NewVector(vec.Typ, uint64(node.block.meta.GetSchema().BlockMaxRows))
		}
		node.data, _ = batch.NewBatch(attrs, vecs)
	}
	from = node.rows
	for idx, attr := range node.data.GetAttrs() {
		for i, a := range bat.Attrs {
			if a == node.block.meta.GetSchema().ColDefs[idx].Name {
				vec, err := node.data.GetVectorByAttr(attr)
				if err != nil {
					return 0, err
				}
				if _, err = vec.AppendVector(bat.Vecs[i], int(offset)); err != nil {
					return from, err
				}
			}
		}
	}
	node.rows += length
	return
}
