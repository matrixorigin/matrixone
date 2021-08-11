package db

import (
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type upgradeSegEvent struct {
	baseEvent
	// SegmentID  uint64
	TableData  iface.ITableData
	Segment    iface.ISegment
	OldSegment iface.ISegment
}

func NewUpgradeSegEvent(ctx *Context, old iface.ISegment, td iface.ITableData) *upgradeSegEvent {
	e := &upgradeSegEvent{
		// SegmentID: segID,
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
	log.Infof("Start upgrade segment %d", sid)
	e.Segment, err = e.TableData.UpgradeSegment(sid)
	if err == nil {
		e.Segment.GetMeta().TrySorted()
	}
	log.Infof("segment %d upgraded", sid)
	// e.OldSegment.Unref()
	return err
}
