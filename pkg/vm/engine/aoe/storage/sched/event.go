package sched

import "sync/atomic"

var (
	eventId uint64 = 0
)

type EventType uint16

const (
	EmptyEvent EventType = iota
	MockEvent
	StopEvent
)

func GetNextEventId() uint64 {
	return atomic.AddUint64(&eventId, uint64(1))
}

type Event interface {
	AttachID(uint64)
	ID() uint64
	Type() EventType
	WaitDone() error
	Cancel() error
}

type mockEvent struct {
	id uint64
}

func (e *mockEvent) AttachID(id uint64) { e.id = id }
func (e *mockEvent) ID() uint64         { return e.id }
func (e *mockEvent) Type() EventType    { return MockEvent }
func (e *mockEvent) WaitDone() error    { return nil }
func (e *mockEvent) Cancel() error      { return nil }
