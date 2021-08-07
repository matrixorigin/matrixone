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
	DQLEvent
	InsertEvent
	DeleteEvent
)

func (et EventType) IsDML() bool {
	switch et {
	case InsertEvent, DeleteEvent:
		return true
	}
	return false
}

func GetNextEventId() uint64 {
	return atomic.AddUint64(&eventId, uint64(1))
}

type Event interface {
	iops.IOp
	AttachID(uint64)
	ID() uint64
	Type() EventType
	WaitDone() error
	Cancel() error
}

type mockEvent struct {
	ops.Op
	id uint64
	t  EventType
}

func newMockEvent(t EventType) *mockEvent {
	e := &mockEvent{t: t}
	e.Op = ops.Op{
		Impl:   e,
		ErrorC: make(chan error),
	}
	return e
}

func (e *mockEvent) AttachID(id uint64) { e.id = id }
func (e *mockEvent) ID() uint64         { return e.id }
func (e *mockEvent) Type() EventType    { return e.t }
func (e *mockEvent) Cancel() error      { return nil }
func (e *mockEvent) Execute() error {
	log.Infof("Execute Event Type=%d, ID=%d", e.t, e.id)
	return nil
}
