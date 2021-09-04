package table

import (
	ex "matrixone/pkg/container/vector"
	bmgrif "matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"matrixone/pkg/vm/engine/aoe/storage/container/batch"
	"matrixone/pkg/vm/engine/aoe/storage/container/vector"
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	bb "matrixone/pkg/vm/engine/aoe/storage/mutation/buffer/base"
	"matrixone/pkg/vm/process"
	"sync"
)

type tBlock struct {
	meta *metadata.Block
	link struct {
		sync.RWMutex
		next iface.IBlock
	}
	node    bb.INode
	nodeMgr bb.INodeManager
	fsMgr   base.IManager
	segFile base.ISegmentFile
	host    iface.ISegment
	typ     base.BlockType
}

func (blk *tBlock) GetMeta() *metadata.Block          { return blk.meta }
func (blk *tBlock) GetType() base.BlockType           { return blk.typ }
func (blk *tBlock) GetCount() uint64                  { return blk.meta.GetCount() }
func (blk *tBlock) WeakRefSegment() iface.ISegment    { return blk.host }
func (blk *tBlock) GetSegmentFile() base.ISegmentFile { return blk.segFile }
func (blk *tBlock) Size(attr string) uint64 {
	// TODO
	return 0
}

func (blk *tBlock) GetSegmentedIndex() (id uint64, ok bool) {
	// TODO
	return id, ok
}

func (blk *tBlock) GetMTBufMgr() bmgrif.IBufferManager {
	// TODO
	return nil
}
func (blk *tBlock) GetSSTBufMgr() bmgrif.IBufferManager {
	// TODO
	return nil
}
func (blk *tBlock) GetFsManager() base.IManager {
	// TODO
	return nil
}
func (blk *tBlock) GetIndexHolder() *index.BlockHolder {
	// TODO
	return nil
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

func (blk *tBlock) GetVectorCopy(attr string, ref uint64, proc *process.Process) (*ex.Vector, error) {
	// TODO
	return nil, nil
}

func (blk *tBlock) Prefetch(attr string) error {
	return nil
}

func (blk *tBlock) GetFullBatch() batch.IBatch {
	panic("not implemented")
}

func (blk *tBlock) GetBatch(attrids []int) dbi.IBatchReader {
	// TODO
	return nil
}

func (blk *tBlock) SetNext(next iface.IBlock) {
	// TODO
}
