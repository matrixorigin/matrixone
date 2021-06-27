package handle

import (
	hif "matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/handle/iface"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
)

type Segment struct {
	Data iface.ISegment
	Attr []int
}

func (seg *Segment) GetID() uint64 {
	return seg.Data.GetMeta().ID
}

func (seg *Segment) GetTableID() uint64 {
	return seg.Data.GetMeta().TableID
}

func (seg *Segment) NewIt() hif.IBlockIt {
	it := &BlockIt{
		Segment: seg,
		Ids:     seg.Data.BlockIds(),
	}
	return it
}

func (seg *Segment) Close() error {
	return nil
}
