package handle

import (
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/handle/base"
	"sync"
)

var (
	_ base.IBlockIterator = (*BlockIt)(nil)
)

var itAllocCnt = 0
var itReleaseCnt = 0

type itBlkAlloc struct {
	It BlockIt
}

var itBlkAllocPool = sync.Pool{
	New: func() interface{} {
		itAllocCnt++
		log.Infof("Alloc blk it %d", itAllocCnt)
		return &itBlkAlloc{It: BlockIt{Cols: make([]col.IColumnBlock, 0)}}
	},
}

type BlockIt struct {
	Cols  []col.IColumnBlock
	Alloc *itBlkAlloc
}

func (it *BlockIt) Next() {
	for i, colBlk := range it.Cols {
		newBlk := colBlk.GetNext()
		if newBlk == nil {
			it.Cols = it.Cols[:0]
			return
		}
		it.Cols[i] = newBlk
	}
}

func (it *BlockIt) Valid() bool {
	if it == nil {
		return false
	}
	if it.Cols == nil {
		return false
	}
	return len(it.Cols) != 0
}

func (it *BlockIt) GetBlockHandle() base.IBlockHandle {
	blkHandle := blkHandlePool.Get().(*BlockHandle)
	blkHandle.ID = it.Cols[0].GetID()
	blkHandle.Cols = it.Cols
	// h := &BlockHandle{
	// 	ID:   it.Cols[0].GetID(),
	// 	Cols: it.Cols,
	// }
	return blkHandle
}

func (it *BlockIt) Close() error {
	if alloc := it.Alloc; alloc != nil {
		*it = BlockIt{Cols: it.Cols[:0]}
		// itReleaseCnt++
		// log.Infof("Release %d", itReleaseCnt)
		itBlkAllocPool.Put(alloc)
	}
	return nil
}
