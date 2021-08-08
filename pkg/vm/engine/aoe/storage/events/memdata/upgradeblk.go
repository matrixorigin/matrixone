package memdata

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type upgradeBlkEvent struct {
	baseEvent
	TableData     iface.ITableData
	Meta          *md.Block
	Data          iface.IBlock
	SegmentClosed bool
}

func NewUpgradeBlkEvent(ctx *Context, meta *md.Block, td iface.ITableData) *upgradeBlkEvent {
	e := &upgradeBlkEvent{
		TableData: td,
		Meta:      meta,
	}
	e.baseEvent = baseEvent{
		BaseEvent: *sched.NewBaseEvent(e, sched.MetaUpdateEvent, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *upgradeBlkEvent) Execute() error {
	var err error
	e.Data, err = e.TableData.UpgradeBlock(e.Meta)
	if e.Meta.Segment.DataState == md.CLOSED {
		e.SegmentClosed = true
	}
	return err
}
