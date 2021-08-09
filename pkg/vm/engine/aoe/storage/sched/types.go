package sched

import (
	"io"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	// log "github.com/sirupsen/logrus"
)

type IDAllocFunc func() uint64

type Scheduler interface {
	Start()
	Stop()
	Schedule(Event) error
}

type Dispatcher interface {
	io.Closer
	Dispatch(Event)
}

type Event interface {
	iops.IOp
	AttachID(uint64)
	ID() uint64
	Type() EventType
	Cancel() error
}

type EventHandler interface {
	Start()
	io.Closer
	Enqueue(Event)
}

type Resource interface {
	EventHandler
	Type() ResourceType
	Name() string
}
