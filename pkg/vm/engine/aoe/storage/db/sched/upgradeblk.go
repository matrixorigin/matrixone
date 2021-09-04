package sched

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v1/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type upgradeBlkEvent struct {
	BaseEvent
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
	e.BaseEvent = BaseEvent{
		BaseEvent: *sched.NewBaseEvent(e, sched.UpgradeBlkTask, ctx.DoneCB, ctx.Waitable),
		Ctx:       ctx,
	}
	return e
}

func (e *upgradeBlkEvent) Execute() error {
	var err error
	e.Data, err = e.TableData.UpgradeBlock(e.Meta)
	if err != nil {
		return err
	}
	if e.Data.WeakRefSegment().CanUpgrade() {
		e.SegmentClosed = true
	}
	return nil
}
