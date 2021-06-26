package col

import (
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	// log "github.com/sirupsen/logrus"
)

type StdColumnBlock struct {
	ColumnBlock
	// Part IColumnPart
}

func NewStdColumnBlock(host iface.IBlock, colIdx int) IColumnBlock {
	blk := &StdColumnBlock{
		ColumnBlock: ColumnBlock{
			ColIdx:      colIdx,
			Meta:        host.GetMeta(),
			SegmentFile: host.GetSegmentFile(),
			Type:        host.GetType(),
		},
	}

	blk.Ref()
	return blk
}

func (blk *StdColumnBlock) CloneWithUpgrade(host iface.IBlock) IColumnBlock {
	if blk.Type == base.PERSISTENT_SORTED_BLK {
		panic("logic error")
	}
	if host.GetMeta().DataState != md.FULL {
		panic(fmt.Sprintf("logic error: blk %s DataState=%d", host.GetMeta().AsCommonID().BlockString(), host.GetMeta().DataState))
	}
	cloned := &StdColumnBlock{
		ColumnBlock: ColumnBlock{
			Type:        host.GetType(),
			ColIdx:      blk.ColIdx,
			Meta:        host.GetMeta(),
			IndexHolder: host.GetIndexHolder(),
			SegmentFile: host.GetSegmentFile(),
		},
	}
	blk.RLock()
	// part := blk.Part.CloneWithUpgrade(cloned.Ref(), seg.GetSSTBufMgr(), seg.GetFsManager())
	blk.RUnlock()
	// if part == nil {
	// 	log.Errorf("logic error")
	// 	panic("logic error")
	// }
	// cloned.Part = part
	cloned.OnZeroCB = cloned.close
	cloned.Ref()
	host.Unref()
	return cloned
}

// func (blk *StdColumnBlock) Append(part IColumnPart) {
// 	blk.Lock()
// 	defer blk.Unlock()
// 	if !blk.ID.IsSameBlock(part.GetID()) || blk.Part != nil {
// 		panic("logic error")
// 	}
// 	blk.Part = part
// }

func (blk *StdColumnBlock) close() {
	// log.Infof("Close StdBlk %s Refs=%d, %p", blk.ID.BlockString(), blk.Refs, blk)
	if blk.IndexHolder != nil {
		blk.IndexHolder.Unref()
		blk.IndexHolder = nil
	}
	// if blk.Part != nil {
	// 	blk.Part.Close()
	// }
	// blk.Part = nil
}

func (blk *StdColumnBlock) String() string {
	s := fmt.Sprintf("<Std[%s](T=%s)(Refs=%d)(Size=%d)>", blk.Meta.String(), blk.Type.String(), blk.RefCount(), blk.Meta.Count)
	return s
}
