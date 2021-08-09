package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

func TestMeta(t *testing.T) {
	diskH := sched.NewPoolHandler(2, nil)
	cpuH := sched.NewPoolHandler(2, nil)
	diskMgr := sched.NewBaseResourceMgr(diskH)
	cpuMgr := sched.NewBaseResourceMgr(cpuH)
	metaMgr := NewMetaResourceMgr(diskMgr, cpuMgr)
	diskMgr.Start()
	cpuMgr.Start()
	metaMgr.Start()

	event := sched.NewBaseEvent(nil, sched.MockEvent, nil, true)
	metaMgr.Enqueue(event)
	err := event.WaitDone()
	assert.Nil(t, err)

	metaMgr.Close()
	cpuMgr.Close()
	diskMgr.Close()
}
