package memdata

import (
	"matrixone/pkg/vm/engine/aoe/storage/layout/table/v2/iface"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	// log "github.com/sirupsen/logrus"
)

type upgradeSegEvent struct {
	baseEvent
	SegmentID uint64
	TableData iface.ITableData
	Segment   iface.ISegment
}

func NewUpgradeSegEvent(ctx *Context, segID uint64, td iface.ITableData) *upgradeSegEvent {
	e := &upgradeSegEvent{
		SegmentID: segID,
		TableData: td,
	}
	e.baseEvent = baseEvent{
		BaseEvent: *sched.NewBaseEvent(e, sched.MetaUpdateEvent, ctx.DoneCB, ctx.Waitable),
	}
	return e
}

func (e *upgradeSegEvent) Execute() error {
	var err error
	e.Segment, err = e.TableData.UpgradeSegment(e.SegmentID)
	if err == nil {
		e.Segment.GetMeta().TrySorted()
	}
	return err
}
