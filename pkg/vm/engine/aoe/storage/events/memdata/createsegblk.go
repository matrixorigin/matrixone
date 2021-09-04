package memdata

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type createSegBlkEvent struct {
	BaseEvent
	TableData  iface.ITableData
	BlkMeta    *md.Block
	NewSegment bool
	Block      iface.IBlock
}

func NewCreateSegBlkEvent(ctx *Context, newSeg bool, meta *md.Block, tableData iface.ITableData) *createSegBlkEvent {
	e := &createSegBlkEvent{TableData: tableData, NewSegment: newSeg, BlkMeta: meta}
	e.BaseEvent = BaseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.MemdataUpdateEvent, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *createSegBlkEvent) Execute() error {
	var err error
	seg := e.TableData.StrongRefSegment(e.BlkMeta.Segment.ID)
	if seg == nil {
		seg, err = e.TableData.RegisterSegment(e.BlkMeta.Segment)
		if err != nil {
			panic("should not happend")
		}
	}
	seg.Unref()
	blk, err := e.TableData.RegisterBlock(e.BlkMeta)
	if err != nil {
		panic(err)
	}
	e.Block = blk

	return nil
}
