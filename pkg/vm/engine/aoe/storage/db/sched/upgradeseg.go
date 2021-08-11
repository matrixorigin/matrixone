package db

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type upgradeSegEvent struct {
	baseEvent
	TableData  iface.ITableData
	Segment    iface.ISegment
	OldSegment iface.ISegment
}

func NewUpgradeSegEvent(ctx *Context, old iface.ISegment, td iface.ITableData) *upgradeSegEvent {
	e := &upgradeSegEvent{
		OldSegment: old,
		TableData:  td,
	}
	e.baseEvent = baseEvent{
		BaseEvent: *sched.NewBaseEvent(e, sched.UpgradeSegTask, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *upgradeSegEvent) Execute() error {
	var err error
	sid := e.OldSegment.GetMeta().ID
	e.Segment, err = e.TableData.UpgradeSegment(sid)
	if err == nil {
		e.Segment.GetMeta().TrySorted()
	}
	return err
}
