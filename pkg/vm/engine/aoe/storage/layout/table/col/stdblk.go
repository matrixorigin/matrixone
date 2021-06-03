package col

import (
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"runtime"
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
	seg.Append(blk)
	runtime.SetFinalizer(blk, func(o IColumnBlock) {
		o.SetNext(nil)
		// id := o.GetID()
		// log.Infof("[GC]: StdColumnBlock %s [%d]", id.BlockString(), o.GetBlockType())
		o.Close()
	})
	return blk
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
	blk.RLock()
	part := blk.Part.CloneWithUpgrade(cloned)
	blk.RUnlock()
	if part == nil {
		panic("logic error")
	}
	cloned.Part = part
	runtime.SetFinalizer(cloned, func(o IColumnBlock) {
		o.SetNext(nil)
		// id := o.GetID()
		// log.Infof("[GC]: StdColumnBlock %s [%d]", id.BlockString(), o.GetBlockType())
		o.Close()
	})
	return cloned
}

func (blk *StdColumnBlock) GetPartRoot() IColumnPart {
	blk.RLock()
	defer blk.RUnlock()
	return blk.Part
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
