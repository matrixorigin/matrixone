package handle

import (
	// log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/layout/index"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/col"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/handle/base"
	"sync"
)

var (
	// shAllocTimes  = 0
	segHandlePool = sync.Pool{
		New: func() interface{} {
			// shAllocTimes++
			// log.Infof("Alloc seg handle: %d", shAllocTimes)
			h := new(SegmentHandle)
			h.Cols = make([]col.IColumnSegment, 0)
			return h
		},
	}
)

type SegmentHandle struct {
	ID          common.ID
	Cols        []col.IColumnSegment
	IndexHolder *index.SegmentHolder
}

func (sh *SegmentHandle) Close() error {
	if shh := sh; shh != nil {
		for _, col := range shh.Cols {
			col.UnRef()
		}
		shh.Cols = shh.Cols[:0]
		segHandlePool.Put(shh)
		sh = nil
	}
	return nil
}

func (sh *SegmentHandle) NewIterator() base.IBlockIterator {
	buf := itBlkAllocPool.Get().(*itBlkAlloc)
	buf.It.Alloc = buf
	for _, colSeg := range sh.Cols {
		buf.It.Cols = append(buf.It.Cols, colSeg.GetBlockRoot())
	}
	return &buf.It
}
