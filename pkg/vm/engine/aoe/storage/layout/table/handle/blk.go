package handle

import (
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/col"
	"matrixone/pkg/vm/engine/aoe/storage/mock/type/chunk"
	"sync"
)

var (
	allocTimes    = 0
	blkHandlePool = sync.Pool{
		New: func() interface{} {
			allocTimes++
			log.Infof("Alloc blk handle: %d", allocTimes)
			h := new(BlockHandle)
			h.Cols = make([]col.IColumnBlock, 0)
			return h
		},
	}
)

type BlockHandle struct {
	ID   common.ID
	Cols []col.IColumnBlock
}

func (bh *BlockHandle) Close() error {
	if bhh := bh; bhh != nil {
		blkHandlePool.Put(bhh)
		bh = nil
	}
	return nil
}

func (bh *BlockHandle) Fetch() *chunk.Chunk {
	// TODO
	return nil
}
