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
	scheduler := NewBaseScheduler("xx")
	assert.NotNil(t, scheduler)
	dis := newMockDispatcher()
	dis.RegisterHandler(MockEvent, NewPoolHandler(4))
	scheduler.RegisterDispatcher(MockEvent, dis)
	waitings := make(chan Event, 1000)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for event := range waitings {
			event.WaitDone()
		}
		wg.Done()
	}()
	exec := func(e Event) error {
		time.Sleep(time.Duration(10) * time.Millisecond)
		return nil
	}
	now := time.Now()
	for i := 0; i < 8; i++ {
		e := newMockEvent(MockEvent)
		e.exec = exec
		err := scheduler.Schedule(e)
		assert.Nil(t, err)
		waitings <- e
	}
	scheduler.Stop()
	close(waitings)
	wg.Wait()
	t.Logf("Time: %s", time.Since(now))
}
