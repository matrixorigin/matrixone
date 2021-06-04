package col

import (
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"runtime"
)

type StrColumnBlock struct {
	ColumnBlock
	Parts []IColumnPart
}

func NewStrColumnBlock(seg IColumnSegment, id common.ID, blkType BlockType) IColumnBlock {
	blk := &StrColumnBlock{
		ColumnBlock: ColumnBlock{
			ID:     id,
			Type:   blkType,
			ColIdx: seg.GetColIdx(),
		},
		Parts: make([]IColumnPart, 0),
	}
	seg.Append(blk)
	runtime.SetFinalizer(blk, func(o IColumnBlock) {
		id := o.GetID()
		o.SetNext(nil)
		log.Infof("[GC]: StrColumnSegment %s [%d]", id.BlockString(), o.GetBlockType())
		o.Close()
	})
	return blk
}

func (blk *StrColumnBlock) CloneWithUpgrade(seg IColumnSegment) IColumnBlock {
	return nil
}

func (blk *StrColumnBlock) Close() error {
	for _, part := range blk.Parts {
		err := part.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (blk *StrColumnBlock) GetPartRoot() IColumnPart {
	if len(blk.Parts) == 0 {
		return nil
	}
	return blk.Parts[0]
}

func (blk *StrColumnBlock) Append(part IColumnPart) {
	if !blk.ID.IsSameBlock(part.GetID()) {
		panic("logic error")
	}
	if len(blk.Parts) != 0 {
		blk.Parts[len(blk.Parts)-1].SetNext(part)
	}
	blk.Parts = append(blk.Parts, part)
}

func (blk *StrColumnBlock) InitScanCursor(cursor *ScanCursor) error {
	if len(blk.Parts) != 0 {
		return blk.Parts[0].InitScanCursor(cursor)
	}
	return nil
}

func (blk *StrColumnBlock) String() string {
	return ""
}
