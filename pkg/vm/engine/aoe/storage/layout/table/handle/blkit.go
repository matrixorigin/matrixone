package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/handle/base"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

var (
	_ base.IBlockIterator = (*BlockIt)(nil)
)

var itAllocCnt int32 = 0
var itReleaseCnt = 0

type itBlkAlloc struct {
	It BlockIt
}

var itBlkAllocPool = sync.Pool{
	New: func() interface{} {
		cnt := atomic.AddInt32(&itAllocCnt, int32(1))
		log.Infof("Alloc blk it %d", cnt)
		return &itBlkAlloc{It: BlockIt{Cols: make([]col.IColumnBlock, 0)}}
	},
}

var EmptyBlockIt = &BlockIt{
	Cols: make([]col.IColumnBlock, 0),
}

type BlockIt struct {
	Invalid bool
	Cols    []col.IColumnBlock
	Alloc   *itBlkAlloc
}

func (it *BlockIt) Next() {
	for i, colBlk := range it.Cols {
		newBlk := colBlk.GetNext()
		if newBlk == nil {
			it.Invalid = true
			return
		}
		it.Cols[i].UnRef()
		it.Cols[i] = newBlk
	}
}

func (it *BlockIt) Valid() bool {
	if it == nil {
		return false
	}
	if it.Invalid {
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
	for _, col := range it.Cols {
		blkHandle.Cols = append(blkHandle.Cols, col.Ref())
	}
	blkHandle.Cols = it.Cols
	return blkHandle
}

func (it *BlockIt) Close() error {
	if alloc := it.Alloc; alloc != nil {
		for _, col := range it.Cols {
			col.UnRef()
		}
		*it = BlockIt{Cols: it.Cols[:0]}
		// itReleaseCnt++
		// log.Infof("Release %d", itReleaseCnt)
		itBlkAllocPool.Put(alloc)
	}
	return nil
}
