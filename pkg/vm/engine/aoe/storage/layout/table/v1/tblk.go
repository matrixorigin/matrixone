package table

import (
	gvec "matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"matrixone/pkg/vm/process"
	"runtime"
)

type tBlock struct {
	baseBlock
	node    bb.INode
	nodeMgr bb.INodeManager
}

func (blk *tBlock) getHandle() bb.INodeHandle {
	h := blk.nodeMgr.Pin(blk.node)
	for h == nil {
		runtime.Gosched()
		h = blk.nodeMgr.Pin(blk.node)
	}
	return h
}

func (blk *tBlock) ProcessData(fn func(batch.IBatch)) {
	h := blk.getHandle()
	n := h.GetNode()
	data := n.(mb.IMutableBlock).GetData()
	fn(data)
	h.Close()
}

func (blk *tBlock) Size(attr string) uint64 {
	// TODO
	return 0
}

func (blk *tBlock) GetSegmentedIndex() (id uint64, ok bool) {
	// TODO
	return id, ok
}

func (blk *tBlock) CloneWithUpgrade(host iface.ISegment, meta *metadata.Block) (iface.IBlock, error) {
	// TODO
	return nil, nil
}

func (blk *tBlock) String() string {
	// TODO
	return ""
}

func (blk *tBlock) GetVectorWrapper(attrid int) (*vector.VectorWrapper, error) {
	panic("not implemented")
}

func (blk *tBlock) getVectorCopyFactory(attr string, ref uint64, proc *process.Process) func(batch.IBatch) (*gvec.Vector, error) {
	return func(bat batch.IBatch) (*gvec.Vector, error) {
		colIdx := blk.meta.Segment.Table.Schema.GetColIdx(attr)
		raw := bat.GetVectorByAttr(colIdx).GetLatestView()
		return raw.CopyToVectorWithProc(ref, proc)
	}
}

func (blk *tBlock) GetVectorCopy(attr string, ref uint64, proc *process.Process) (*gvec.Vector, error) {
	fn := blk.getVectorCopyFactory(attr, ref, proc)
	h := blk.getHandle()
	n := h.GetNode()
	data := n.(mb.IMutableBlock).GetData()
	v, err := fn(data)
	h.Close()
	return v, err
}

func (blk *tBlock) Prefetch(attr string) error {
	return nil
}

func (blk *tBlock) GetFullBatch() batch.IBatch {
	panic("not supported")
}

func (blk *tBlock) GetBatch(attrids []int) dbi.IBatchReader {
	// TODO
	return nil
}
