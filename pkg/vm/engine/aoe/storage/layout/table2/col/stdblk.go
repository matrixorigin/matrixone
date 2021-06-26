package col

import (
	"fmt"
	"matrixone/pkg/vm/engine/aoe/storage/layout/base"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata"
	"time"

	log "github.com/sirupsen/logrus"
)

type StdColumnBlock struct {
	ColumnBlock
	Part IColumnPart
}

func NewStdColumnBlock(host iface.IBlock, colIdx int) IColumnBlock {
	defer host.Unref()
	blk := &StdColumnBlock{
		ColumnBlock: ColumnBlock{
			ColIdx:      colIdx,
			Meta:        host.GetMeta(),
			SegmentFile: host.GetSegmentFile(),
			Type:        host.GetType(),
		},
	}
	capacity := blk.Meta.Segment.Info.Conf.BlockMaxRows * uint64(blk.Meta.Segment.Schema.ColDefs[colIdx].Type.Size)
	host.Ref()
	blk.Ref()
	part := NewColumnPart(host, blk, capacity)
	for part == nil {
		blk.Ref()
		host.Ref()
		part = NewColumnPart(host, blk, capacity)
		time.Sleep(time.Duration(1) * time.Millisecond)
	}
	part.Unref()
	blk.OnZeroCB = blk.close
	blk.Ref()
	return blk
}

func (blk *StdColumnBlock) CloneWithUpgrade(host iface.IBlock) IColumnBlock {
	defer host.Unref()
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
	cloned.Ref()
	blk.RLock()
	part := blk.Part.CloneWithUpgrade(cloned, host.GetSSTBufMgr())
	blk.RUnlock()
	if part == nil {
		log.Errorf("logic error")
		panic("logic error")
	}
	cloned.Part = part
	cloned.OnZeroCB = cloned.close
	cloned.Ref()
	return cloned
}

func (blk *StdColumnBlock) RegisterPart(part IColumnPart) {
	blk.Lock()
	defer blk.Unlock()
	if blk.Meta.ID != part.GetID() || blk.Part != nil {
		panic("logic error")
	}
	blk.Part = part
}

func (blk *StdColumnBlock) close() {
	if blk.IndexHolder != nil {
		blk.IndexHolder.Unref()
		blk.IndexHolder = nil
	}
	if blk.Part != nil {
		blk.Part.Close()
	}
	blk.Part = nil
	// log.Infof("destroy colblk %d, colidx %d", blk.Meta.ID, blk.ColIdx)
}

func (blk *StdColumnBlock) GetBlockHandle() iface.IColBlockHandle {
	h := new(StdColBlockHandle)
	h.Node = blk.Part.GetManagedNode()
	return h
}

func (blk *StdColumnBlock) String() string {
	s := fmt.Sprintf("<Std[%s](T=%s)(Refs=%d)(Size=%d)>", blk.Meta.String(), blk.Type.String(), blk.RefCount(), blk.Meta.Count)
	return s
}
