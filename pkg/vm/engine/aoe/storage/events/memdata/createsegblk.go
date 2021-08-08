package memdata

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type createSegBlkEvent struct {
	baseEvent
	TableData  iface.ITableData
	BlkMeta    *md.Block
	NewSegment bool
	Block      iface.IBlock
}

func NewCreateSegBlkEvent(ctx *Context, newSeg bool, meta *md.Block, tableData iface.ITableData) *createSegBlkEvent {
	e := &createSegBlkEvent{TableData: tableData, NewSegment: newSeg, BlkMeta: meta}
	e.baseEvent = baseEvent{
		Ctx:       ctx,
		BaseEvent: *sched.NewBaseEvent(e, sched.MemdataUpdateEvent, ctx.DoneCB),
	}
	return e
}

func (e *createSegBlkEvent) Execute() error {
	if e.NewSegment {
		seg, err := e.TableData.RegisterSegment(e.BlkMeta.Segment)
		if err != nil {
			panic("should not happend")
		}
		seg.Unref()
	}
	blk, err := e.TableData.RegisterBlock(e.BlkMeta)
	if err != nil {
		panic(err)
	}
	e.Block = blk

	return nil
}
