package sched

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
)

func TestMock(t *testing.T) {
	scheduler := NewBaseScheduler("xx")
	assert.NotNil(t, scheduler)
	scheduler.Start()
	dis := newMockDispatcher()
	scheduler.RegisterDispatcher(MockEvent, dis)
	for i := 0; i < 4; i++ {
		e := NewBaseEvent(nil, MockEvent, nil, false)
		err := scheduler.Schedule(e)
		assert.Nil(t, err)
	}
	time.Sleep(time.Duration(10) * time.Millisecond)
	scheduler.Stop()
	for i := 0; i < 4; i++ {
		e := NewBaseEvent(nil, MockEvent, nil, false)
		err := scheduler.Schedule(e)
		assert.Equal(t, ErrSchedule, err)
		e.Cancel()
	}
}

func TestPoolHandler(t *testing.T) {
	scheduler := NewBaseScheduler("xx")
	assert.NotNil(t, scheduler)
	scheduler.Start()
	dis := newMockDispatcher()
	dis.RegisterHandler(MockEvent, NewPoolHandler(4))
	scheduler.RegisterDispatcher(MockEvent, dis)
	wg := &sync.WaitGroup{}
	exec := func(e Event) error {
		time.Sleep(time.Duration(10) * time.Millisecond)
		return nil
	}
	now := time.Now()
	for i := 0; i < 8; i++ {
		wg.Add(1)
		e := NewBaseEvent(nil, MockEvent, func(iops.IOp) {
			wg.Done()
		}, false)
		e.exec = exec
		err := scheduler.Schedule(e)
		assert.Nil(t, err)
	}
	scheduler.Stop()
	wg.Wait()
	t.Logf("Time: %s", time.Since(now))
}
