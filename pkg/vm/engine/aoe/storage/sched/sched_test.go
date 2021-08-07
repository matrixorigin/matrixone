package sched

import (
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
		e := newMockEvent()
		err := scheduler.Schedule(e)
		assert.Nil(t, err)
	}
	time.Sleep(time.Duration(100) * time.Millisecond)
	scheduler.Stop()
	for i := 0; i < 4; i++ {
		e := newMockEvent()
		err := scheduler.Schedule(e)
		assert.Equal(t, ErrSchedule, err)
		e.Cancel()
	}
}
