package tables

import (
	"bytes"

	gbat "github.com/matrixorigin/matrixone/pkg/container/batch"
	gvec "github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/buffer/base"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/batch"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/sirupsen/logrus"
)

type appendableNode struct {
	*buffer.Node
	file dataio.BlockFile
	meta *catalog.BlockEntry
	data batch.IBatch
	rows uint32
	mgr  base.INodeManager
}

func newNode(mgr base.INodeManager, meta *catalog.BlockEntry, file dataio.BlockFile) *appendableNode {
	impl := new(appendableNode)
	id := meta.AsCommonID()
	impl.Node = buffer.NewNode(impl, mgr, *id, uint64(catalog.EstimateBlockSize(meta, meta.GetSegment().GetTable().GetSchema().BlockMaxRows)))
	impl.UnloadFunc = impl.OnUnload
	impl.LoadFunc = impl.OnLoad
	impl.DestroyFunc = impl.OnDestory
	impl.file = file
	impl.mgr = mgr
	impl.meta = meta
	mgr.RegisterNode(impl)
	return impl
}

func (node *appendableNode) Rows(txn txnif.AsyncTxn, coarse bool) uint32 {
	if coarse {
		return node.rows
	}
	// TODO: fine row count
	// 1. Load txn ts zonemap
	// 2. Calculate fine row count
	return 0
}

func (node *appendableNode) OnDestory() {
	if err := node.file.Destory(); err != nil {
		panic(err)
	}
}

// TODO: Apply updates and txn sels
func (node *appendableNode) GetVectorCopy(txn txnif.AsyncTxn, attr string, compressed, decompressed *bytes.Buffer) (vec *gvec.Vector, err error) {
	colIdx := node.meta.GetSegment().GetTable().GetSchema().GetColIdx(attr)
	ivec, err := node.data.GetVectorByAttr(colIdx)
	if err != nil {
		return nil, err
	}
	ro := ivec.GetLatestView()
	return ro.CopyToVectorWithBuffer(compressed, decompressed)
}

func (node *appendableNode) OnLoad() {
	var err error
	if node.data, err = node.file.LoadData(); err != nil {
		panic(err)
	}
}

func (node *appendableNode) OnUnload() {
	logrus.Infof("Unloading block %s", node.meta.AsCommonID().String())
	if err := node.file.WriteData(node.data, nil, nil); err != nil {
		panic(err)
	}
	if err := node.file.Sync(); err != nil {
		panic(err)
	}
}

func (node *appendableNode) PrepareAppend(rows uint32) (n uint32, err error) {
	left := node.meta.GetSegment().GetTable().GetSchema().BlockMaxRows - node.rows
	if left == 0 {
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

func (node *appendableNode) ApplyAppend(bat *gbat.Batch, offset, length uint32, ctx interface{}) (from uint32, err error) {
	if node.data == nil {
		vecs := make([]vector.IVector, len(bat.Vecs))
		attrs := make([]int, len(bat.Vecs))
		for i, vec := range bat.Vecs {
			attrs[i] = i
			vecs[i] = vector.NewVector(vec.Typ, uint64(node.meta.GetSegment().GetTable().GetSchema().BlockMaxRows))
		}
		node.data, _ = batch.NewBatch(attrs, vecs)
	}
	from = node.rows
	for idx, attr := range node.data.GetAttrs() {
		for i, a := range bat.Attrs {
			if a == node.meta.GetSegment().GetTable().GetSchema().ColDefs[idx].Name {
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
