package sched

import (
	log "github.com/sirupsen/logrus"
	"matrixone/pkg/vm/engine/aoe/storage/ops"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	"sync/atomic"
)

var (
	eventId uint64 = 0
)

type EventType uint16

const (
	EmptyEvent EventType = iota
	MockEvent
	StopEvent
	DataIOBoundEvent
	DataCpuBoundEvent
)

func GetNextEventId() uint64 {
	return atomic.AddUint64(&eventId, uint64(1))
}

type Event interface {
	iops.IOp
	AttachID(uint64)
	ID() uint64
	Type() EventType
	Cancel() error
}

type mockEvent struct {
	ops.Op
	id   uint64
	t    EventType
	exec func(Event) error
}

func newMockEvent(t EventType, doneCB func()) *mockEvent {
	e := &mockEvent{t: t}
	if doneCB == nil {
		doneCB = e.onDone
	}
	e.Op = ops.Op{
		Impl:   e,
		DoneCB: doneCB,
	}
	return e
}

func (e *mockEvent) AttachID(id uint64) { e.id = id }
func (e *mockEvent) ID() uint64         { return e.id }
func (e *mockEvent) Type() EventType    { return e.t }
func (e *mockEvent) Cancel() error      { return nil }
func (e *mockEvent) Execute() error {
	if e.exec != nil {
		return e.exec(e)
	}
	log.Infof("Execute Event Type=%d, ID=%d", e.t, e.id)
	return nil
}
func (e *mockEvent) onDone() {
	log.Infof("Event %d is done: %v", e.id, e.Err)
}

type BaseEvent struct {
	ops.Op
	id   uint64
	t    EventType
	exec func(Event) error
}

func NewBaseEvent(impl iops.IOpInternal, t EventType, doneCB func()) *BaseEvent {
	e := &BaseEvent{t: t}
	if doneCB == nil {
		doneCB = e.onDone
	}
	if impl == nil {
		impl = e
	}
	e.Op = ops.Op{
		Impl:   impl,
		DoneCB: doneCB,
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
	log.Infof("Execute Event Type=%d, ID=%d", e.t, e.id)
	return nil
}
func (e *BaseEvent) onDone() {
	log.Infof("Event %d is done: %v", e.id, e.Err)
}
