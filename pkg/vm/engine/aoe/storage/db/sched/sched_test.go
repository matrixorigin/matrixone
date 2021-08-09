package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
	e "matrixone/pkg/vm/engine/aoe/storage"
	iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
)

type observer struct {
	t   *testing.T
	num int
}

func (o *observer) OnExecDone(op iops.IOp) {
	o.num = 100
}

func (o *observer) Check() {
	assert.Equal(o.t, 100, o.num)
}

func TestMeta(t *testing.T) {
	opts := &e.Options{}
	diskH := sched.NewPoolHandler(2, nil)
	cpuH := sched.NewPoolHandler(2, nil)
	diskMgr := sched.NewBaseResourceMgr(diskH)
	cpuMgr := sched.NewBaseResourceMgr(cpuH)
	metaMgr := NewMetaResourceMgr(opts, diskMgr, cpuMgr)
	diskMgr.Start()
	cpuMgr.Start()
	metaMgr.Start()

	event := sched.NewBaseEvent(nil, sched.MockEvent, nil, true)

	ob := &observer{t: t, num: 1}

	event.AddObserver(ob)
	metaMgr.Enqueue(event)
	err := event.WaitDone()
	assert.Nil(t, err)
	ob.Check()

	metaMgr.Close()
	cpuMgr.Close()
	diskMgr.Close()
}
