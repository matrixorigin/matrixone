package db

import (
	"sync"
	"testing"

	e "matrixone/pkg/vm/engine/aoe/storage"
	// iops "matrixone/pkg/vm/engine/aoe/storage/ops/base"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/sched"

	"github.com/stretchr/testify/assert"
)

type observer struct {
	t   *testing.T
	num int
	wg  *sync.WaitGroup
}

func (o *observer) OnExecDone(interface{}) {
	o.num = 100
	o.wg.Done()
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

	events := make([]MetaEvent, 0)
	for i := uint64(1); i < uint64(3); i++ {
		scope := common.ID{TableID: 1}
		event := NewMetaEvent(scope, false, sched.MockEvent, true)
		event.AttachID(i)
		events = append(events, event)
	}

	var wg sync.WaitGroup
	ob := &observer{t: t, num: 1, wg: &wg}
	wg.Add(1)

	for i, event := range events {
		if i == 0 {
			event.AddObserver(ob)
		}
		metaMgr.Enqueue(event)
	}
	for _, event := range events {
		err := event.WaitDone()
		assert.Nil(t, err)
	}
	wg.Wait()
	ob.Check()

	t.Log(metaMgr.String())
	metaMgr.Close()
	cpuMgr.Close()
	diskMgr.Close()
}
