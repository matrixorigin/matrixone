package sched

import (
	"errors"
	"fmt"
	"time"

	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
)

var (
	ErrDispatcherNotFound = errors.New("aoe sched: dispatcher not found")
)

type Scheduler interface {
	Start()
	Stop()
	Schedule(Event) error
	// OnNewEvent(Event)
}

type sequentialScheduler struct {
	pool        *ants.Pool
	idAlloc     IDAllocFunc
	dispatchers map[EventType]Dispatcher
	scheduled   chan Event
}

func NewSequentialScheduler(workerNum int) *sequentialScheduler {
	if workerNum <= 0 {
		panic(fmt.Sprintf("bad workerCnt %d", workerNum))
	}
	pool, err := ants.NewPool(workerNum)
	if err != nil {
		panic(err)
	}
	scheduler := &sequentialScheduler{
		dispatchers: make(map[EventType]Dispatcher),
		scheduled:   make(chan Event, 1000),
		pool:        pool,
		idAlloc:     GetNextEventId,
	}
	go scheduler.waitScheduledLoop()
	return scheduler
}

func (s *sequentialScheduler) RegisterDispatcher(t EventType, dispatcher Dispatcher) {
	s.dispatchers[t] = dispatcher
}

func (s *sequentialScheduler) waitScheduledLoop() {
	log.Infof("scheduler wait loop | START")
	for event := range s.scheduled {
		time.Sleep(time.Duration(100) * time.Millisecond)
		event.WaitDone()
		log.Infof("event %d done", event.ID())
	}
	log.Infof("scheduler wait loop | DONE")
}

func (s *sequentialScheduler) Schedule(e Event) error {
	e.AttachID(s.idAlloc())
	dispatcher := s.dispatchers[e.Type()]
	if dispatcher == nil {
		return ErrDispatcherNotFound
	}
	dispatcher.Dispatch(e)
	s.scheduled <- e
	log.Infof("event %d scheduled", e.ID())
	return nil
}

func (s *sequentialScheduler) Stop() {
	close(s.scheduled)
}
