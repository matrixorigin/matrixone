package sched

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMock(t *testing.T) {
	scheduler := NewBaseScheduler("xx")
	assert.NotNil(t, scheduler)
	dis := newMockDispatcher()
	scheduler.RegisterDispatcher(MockEvent, dis)
	for i := 0; i < 4; i++ {
		e := newMockEvent(MockEvent)
		err := scheduler.Schedule(e)
		assert.Nil(t, err)
	}
	time.Sleep(time.Duration(10) * time.Millisecond)
	scheduler.Stop()
	for i := 0; i < 4; i++ {
		e := newMockEvent(MockEvent)
		err := scheduler.Schedule(e)
		assert.Equal(t, ErrSchedule, err)
		e.Cancel()
	}
}

func TestPoolHandler(t *testing.T) {
	// {
	// 	worker := w.NewOpWorker("xx")
	// 	worker.Start()
	// 	e := newMockEvent(MockEvent)
	// 	e.Worker = worker
	// 	err := e.Push()
	// 	t.Log(err)
	// 	err = e.WaitDone()
	// 	t.Log(err)

	// }
	// return
	scheduler := NewBaseScheduler("xx")
	assert.NotNil(t, scheduler)
	dis := newMockDispatcher()
	dis.RegisterHandler(MockEvent, NewPoolHandler(2))
	scheduler.RegisterDispatcher(MockEvent, dis)
	events := make(chan Event, 1000)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for event := range events {
			t.Logf("e %d", event.ID())
			event.WaitDone()
		}
		wg.Done()
	}()
	for i := 0; i < 4; i++ {
		e := newMockEvent(MockEvent)
		err := scheduler.Schedule(e)
		assert.Nil(t, err)
		events <- e
	}
	time.Sleep(time.Duration(10) * time.Millisecond)
	scheduler.Stop()
	close(events)
	wg.Wait()
}
