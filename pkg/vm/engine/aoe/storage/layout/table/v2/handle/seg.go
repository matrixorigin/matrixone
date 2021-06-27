package handle

import (
	"matrixone/pkg/vm/engine/aoe/storage/dbi"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
)

var (
	_ dbi.ISegment = (*Segment)(nil)
)

type Segment struct {
	Data iface.ISegment
	Attr []int
}

func (seg *Segment) BlockIds() []uint64 {
	return seg.Data.BlockIds()
}

func (seg *Segment) GetID() uint64 {
	return seg.Data.GetMeta().ID
}

func (seg *Segment) GetTableID() uint64 {
	return seg.Data.GetMeta().TableID
}

func (seg *Segment) NewIt() dbi.IBlockIt {
	it := &BlockIt{
		Segment: seg,
		Ids:     seg.Data.BlockIds(),
	}
	return it
}

func (seg *Segment) GetBlock(id uint64) dbi.IBlock {
	data := seg.Data.WeakRefBlock(id)
	if data == nil {
		return nil
	}
	blk := &Block{
		Data: data,
		Host: seg,
	}
	return blk
}
