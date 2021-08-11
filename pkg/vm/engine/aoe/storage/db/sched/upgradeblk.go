package db

import (
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	md "matrixone/pkg/vm/engine/aoe/storage/metadata/v1"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
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
		BaseEvent: *sched.NewBaseEvent(e, sched.UpgradeBlkTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *upgradeBlkEvent) Execute() error {
	var err error
	e.Data, err = e.TableData.UpgradeBlock(e.Meta)
	if err != nil {
		return err
	}
	if e.Meta.Segment.DataState == md.CLOSED {
		log.Infof("kkkkkkkkkkkk %v", e.Data)
		if e.Data.WeakRefSegment().CanUpgrade() {
			e.SegmentClosed = true
		}
	}
	return nil
}
