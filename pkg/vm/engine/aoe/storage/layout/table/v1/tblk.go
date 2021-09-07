package table

import (
	"fmt"
	gvec "matrixone/pkg/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	fb "matrixone/pkg/vm/engine/aoe/storage/db/factories/base"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	mb "matrixone/pkg/vm/engine/aoe/storage/mutation/base"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"matrixone/pkg/vm/process"
	"runtime"
	// "matrixone/pkg/logutil"
)

type tblock struct {
	baseBlock
	node    bb.INode
	nodeMgr bb.INodeManager
}

func newTBlock(host iface.ISegment, meta *metadata.Block, factory fb.NodeFactory) (*tblock, error) {
	blk := &tblock{
		baseBlock: *newBaseBlock(host, meta),
		node:      factory.CreateNode(host.GetSegmentFile(), meta),
		nodeMgr:   factory.GetManager(),
	}
	blk.OnZeroCB = blk.close
	blk.Ref()
	return blk, nil
}

func (blk *tblock) close() {
	blk.baseBlock.release()
	blk.node.Close()
}

func (blk *tblock) getHandle() bb.INodeHandle {
	h := blk.nodeMgr.Pin(blk.node)
	for h == nil {
		runtime.Gosched()
		h = blk.nodeMgr.Pin(blk.node)
	}
	return h
}

func (blk *tblock) WithPinedContext(fn func(bb.INode) error) error {
	h := blk.getHandle()
	n := h.GetNode()
	err := fn(n)
	h.Close()
	return err
}

func (blk *tblock) ProcessData(fn func(batch.IBatch) error) error {
	h := blk.getHandle()
	n := h.GetNode()
	data := n.(mb.IMutableBlock).GetData()
	err := fn(data)
	h.Close()
	return err
}

func (blk *tblock) Size(attr string) uint64 {
	// TODO
	return 0
}

func (blk *tblock) GetSegmentedIndex() (id uint64, ok bool) {
	return blk.meta.GetAppliedIndex()
}

func (blk *tblock) CloneWithUpgrade(host iface.ISegment, meta *metadata.Block) (iface.IBlock, error) {
	return newBlock(host, meta)
}

func (blk *tblock) String() string {
	s := fmt.Sprintf("<TBlk[%d]>(Refs=%d)", blk.meta.ID, blk.RefCount())
	return s
}

func (blk *tblock) GetVectorWrapper(attrid int) (*vector.VectorWrapper, error) {
	panic("not implemented")
}

func (blk *tblock) getVectorCopyFactory(attr string, ref uint64, proc *process.Process) func(batch.IBatch) (*gvec.Vector, error) {
	return func(bat batch.IBatch) (*gvec.Vector, error) {
		colIdx := blk.meta.Segment.Table.Schema.GetColIdx(attr)
		raw := bat.GetVectorByAttr(colIdx).GetLatestView()
		return raw.CopyToVectorWithProc(ref, proc)
	}
}

func (blk *tblock) GetVectorCopy(attr string, ref uint64, proc *process.Process) (*gvec.Vector, error) {
	fn := blk.getVectorCopyFactory(attr, ref, proc)
	h := blk.getHandle()
	n := h.GetNode()
	data := n.(mb.IMutableBlock).GetData()
	v, err := fn(data)
	h.Close()
	return v, err
}

func (blk *tblock) Prefetch(attr string) error {
	return nil
}

func (blk *tblock) GetFullBatch() batch.IBatch {
	panic("not supported")
}

func (blk *tblock) GetBatch(attrids []int) dbi.IBatchReader {
	// TODO
	return nil
}
