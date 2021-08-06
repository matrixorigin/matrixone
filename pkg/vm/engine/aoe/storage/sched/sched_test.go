package sched

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMock(t *testing.T) {
	scheduler := NewSequentialScheduler(10)
	assert.NotNil(t, scheduler)
	dis := &mockDispatcher{}
	scheduler.RegisterDispatcher(MockEvent, dis)
	for i := 0; i < 4; i++ {
		e := &mockEvent{}
		err := scheduler.Schedule(e)
		assert.Nil(t, err)
	}
	scheduler.Stop()
	time.Sleep(time.Duration(800) * time.Millisecond)
}
