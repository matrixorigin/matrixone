package sched

import (
	logutil2 "matrixone/pkg/logutil"
	"matrixone/pkg/vm/engine/aoe/storage/ops"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	"sync/atomic"
)

var (
	eventId     uint64 = 0
	EventPrefix string = "Event"
)

type EventType uint16

const (
	EmptyEvent EventType = iota
	MockEvent
	IOBoundEvent
	CpuBoundEvent
	StatelessEvent
	MetaUpdateEvent
	MemdataUpdateEvent
	MergeSortEvent

	PrecommitBlkMetaTask
	FlushMemtableTask
	MetaCreateTableTask
	MetaDropTableTask
	MetaCreateBlkTask
	CommitBlkTask
	FlushTableMetaTask
	FlushInfoMetaTask
	UpgradeBlkTask
	UpgradeSegTask
	FlushSegTask
)

func GetNextEventId() uint64 {
	return atomic.AddUint64(&eventId, uint64(1))
}

type BaseEvent struct {
	ops.Op
	id   uint64
	t    EventType
	exec func(Event) error
}

func NewBaseEvent(impl iops.IOpInternal, t EventType, doneCB ops.OpDoneCB, waitable bool) *BaseEvent {
	e := &BaseEvent{t: t}
	if doneCB == nil && !waitable {
		doneCB = e.onDone
	}
	if impl == nil {
		impl = e
	}
	e.Op = ops.Op{
		Impl:   impl,
		DoneCB: doneCB,
	}
	if doneCB == nil {
		e.Op.ErrorC = make(chan error)
	}
	return e
}

func (e *BaseEvent) AttachID(id uint64) { e.id = id }
func (e *BaseEvent) ID() uint64         { return e.id }
func (e *BaseEvent) Type() EventType    { return e.t }
func (e *BaseEvent) Cancel() error      { return nil }
func (e *BaseEvent) Execute() error {
	if e.exec != nil {
		return e.exec(e)
	}
	logutil2.Debugf("Execute Event Type=%d, ID=%d", e.t, e.id)
	return nil
}
func (e *BaseEvent) onDone(_ iops.IOp) {
	// log.Infof("Event %d is done: %v", e.id, e.Err)
}
