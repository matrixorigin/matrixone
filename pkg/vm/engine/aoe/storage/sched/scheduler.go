package sched

import (
	"errors"
	"fmt"
	"github.com/panjf2000/ants/v2"
	log "github.com/sirupsen/logrus"
	"sync"
)

var (
	ErrDispatcherNotFound = errors.New("aoe sched: dispatcher not found")
)

type Scheduler interface {
	Start()
	Stop()
	Schedule(Event) error
}

type sequentialScheduler struct {
	wg          *sync.WaitGroup
	idAlloc     IDAllocFunc
	dispatchers map[EventType]Dispatcher
	pendings    chan Event
	pool        *ants.Pool
	stop        chan struct{}
}

func NewSequentialScheduler(num int) *sequentialScheduler {
	if num <= 0 {
		panic(fmt.Sprintf("bad num %d", num))
	}
	pool, err := ants.NewPool(num)
	if err != nil {
		panic(err)
	}
	scheduler := &sequentialScheduler{
		dispatchers: make(map[EventType]Dispatcher),
		pendings:    make(chan Event, 1000),
		stop:        make(chan struct{}),
		wg:          &sync.WaitGroup{},
		idAlloc:     GetNextEventId,
		pool:        pool,
	}
	// go scheduler.waitPendings()
	return scheduler
}

func (s *sequentialScheduler) RegisterDispatcher(t EventType, dispatcher Dispatcher) {
	s.dispatchers[t] = dispatcher
}

func (s *sequentialScheduler) waitPendings() {
	log.Infof("scheduler wait loop | START")
	for event := range s.pendings {
		// time.Sleep(time.Duration(100) * time.Millisecond)
		// event.WaitDone()
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
	s.wg.Add(1)
	select {
	case <-s.stop:
		log.Infof("add event %d into pendings", e.ID())
		s.pendings <- e
	default:
		log.Infof("dispatch event %d", e.ID())
		f := func(event Event, d Dispatcher) func() {
			return func() {
				d.Dispatch(event)
			}
		}
		f(e, dispatcher)
	}
	s.wg.Done()
	return nil
}

func (s *sequentialScheduler) Stop() {
	close(s.stop)
	go func() {
		s.wg.Wait()
		close(s.pendings)
	}()
	for e := range s.pendings {
		log.Infof("cancel event %d", e.ID())
		e.Cancel()
	}
}
