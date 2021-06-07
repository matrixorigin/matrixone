package col

import (
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"sync/atomic"
	// "runtime"
	// log "github.com/sirupsen/logrus"
)

type StdColumnBlock struct {
	ColumnBlock
	Part IColumnPart
}

func NewStdColumnBlock(seg IColumnSegment, id common.ID, blkType BlockType) IColumnBlock {
	blk := &StdColumnBlock{
		ColumnBlock: ColumnBlock{
			ID:     id,
			Type:   blkType,
			ColIdx: seg.GetColIdx(),
		},
	}
	// runtime.SetFinalizer(blk, func(o IColumnBlock) {
	// 	o.SetNext(nil)
	// 	// id := o.GetID()
	// 	// log.Infof("[GC]: StdColumnBlock %s [%d]", id.BlockString(), o.GetBlockType())
	// 	o.Close()
	// })
	seg.Append(blk.Ref())
	return blk.Ref()
}

func (blk *StdColumnBlock) CloneWithUpgrade(seg IColumnSegment) IColumnBlock {
	if blk.Type == PERSISTENT_SORTED_BLK {
		panic("logic error")
	}
	var newType BlockType
	switch blk.Type {
	case TRANSIENT_BLK:
		newType = PERSISTENT_BLK
	case PERSISTENT_BLK:
		newType = PERSISTENT_SORTED_BLK
	case MOCK_BLK:
		newType = MOCK_PERSISTENT_BLK
	case MOCK_PERSISTENT_BLK:
		newType = MOCK_PERSISTENT_SORTED_BLK
	}
	cloned := &StdColumnBlock{
		ColumnBlock: ColumnBlock{
			ID:     blk.ID,
			Type:   newType,
			ColIdx: seg.GetColIdx(),
		},
	}
	cloned.Ref()
	blk.RLock()
	part := blk.Part.CloneWithUpgrade(cloned.Ref(), seg.GetSSTBufMgr())
	blk.RUnlock()
	if part == nil {
		panic("logic error")
	}
	cloned.Part = part
	// runtime.SetFinalizer(cloned, func(o IColumnBlock) {
	// 	o.SetNext(nil)
	// 	// id := o.GetID()
	// 	// log.Infof("[GC]: StdColumnBlock %s [%d]", id.BlockString(), o.GetBlockType())
	// 	o.Close()
	// })
	seg.UnRef()
	return cloned
}

func (blk *StdColumnBlock) Ref() IColumnBlock {
	atomic.AddInt64(&blk.Refs, int64(1))
	// newRef := atomic.AddInt64(&blk.Refs, int64(1))
	// log.Infof("StdColumnBlock %s Ref to %d, %p", blk.ID.BlockString(), newRef, blk)
	return blk
}

func (blk *StdColumnBlock) UnRef() {
	newRef := atomic.AddInt64(&blk.Refs, int64(-1))
	// log.Infof("StdColumnBlock %s Unref to %d, %p", blk.ID.BlockString(), newRef, blk)
	if newRef == 0 {
		blk.Close()
	}
}

func (blk *StdColumnBlock) GetPartRoot() IColumnPart {
	blk.RLock()
	p := blk.Part
	blk.RUnlock()
	return p
}

func (blk *StdColumnBlock) Append(part IColumnPart) {
	blk.Lock()
	defer blk.Unlock()
	if !blk.ID.IsSameBlock(part.GetID()) || blk.Part != nil {
		panic("logic error")
	}
	blk.Part = part
}

func (blk *StdColumnBlock) Close() error {
	// log.Infof("Close StdBlk %s Refs=%d, %p", blk.ID.BlockString(), blk.Refs, blk)
	if blk.Next != nil {
		blk.Next.UnRef()
		blk.Next = nil
	}
	if blk.Part != nil {
		blk.Part.Close()
	}
	blk.Part = nil
	return nil
}

func (blk *StdColumnBlock) InitScanCursor(cursor *ScanCursor) error {
	blk.RLock()
	defer blk.RUnlock()
	if blk.Part != nil {
		cursor.Current = blk.Part
		// return blk.Part.InitScanCursor(cursor)
	}
	return nil
}

func (blk *StdColumnBlock) String() string {
	partID := blk.Part.GetNodeID()
	s := fmt.Sprintf("Std[%s](T=%d)[Part=%s]", blk.ID.BlockString(), blk.Type, partID.String())
	return s
}
