package sched

import (
	"errors"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	ops "matrixone/pkg/vm/engine/aoe/storage/worker"
	// log "github.com/sirupsen/logrus"
)

var (
	ErrDispatcherNotFound = errors.New("aoe sched: dispatcher not found")
	ErrSchedule           = errors.New("aoe sched: cannot schedule")
)

type BaseScheduler struct {
	ops.OpWorker
	idAlloc     IDAllocFunc
	dispatchers map[EventType]Dispatcher
}

func NewBaseScheduler(name string) *BaseScheduler {
	scheduler := &BaseScheduler{
		OpWorker:    *ops.NewOpWorker(name),
		idAlloc:     GetNextEventId,
		dispatchers: make(map[EventType]Dispatcher),
	}
	scheduler.ExecFunc = scheduler.doDispatch
	return scheduler
}

func (s *BaseScheduler) RegisterDispatcher(t EventType, dispatcher Dispatcher) {
	s.dispatchers[t] = dispatcher
}

func (s *BaseScheduler) Schedule(e Event) error {
	e.AttachID(s.idAlloc())
	if !s.SendOp(e) {
		return ErrSchedule
	}
	return nil
}

func (s *BaseScheduler) doDispatch(op iops.IOp) {
	e := op.(Event)
	dispatcher := s.dispatchers[e.Type()]
	if dispatcher == nil {
		panic(ErrDispatcherNotFound)
	}
	dispatcher.Dispatch(e)
}

func (s *BaseScheduler) Stop() {
	s.OpWorker.Stop()
	for _, d := range s.dispatchers {
		d.Close()
	}
}
