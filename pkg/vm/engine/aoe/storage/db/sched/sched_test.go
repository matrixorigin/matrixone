package db

import (
	"github.com/stretchr/testify/assert"
	e "matrixone/pkg/vm/engine/aoe/storage"
	"matrixone/pkg/vm/engine/aoe/storage/common"
	"matrixone/pkg/vm/engine/aoe/storage/sched"
	"sync"
	"testing"
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
	for i := uint64(1); i < uint64(10); i++ {
		tableId := i%4 + 1
		scope := common.ID{TableID: tableId}
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
		go func(ee sched.Event) {
			err := ee.WaitDone()
			assert.Nil(t, err)
		}(event)
	}
	wg.Wait()
	ob.Check()

	t.Log(metaMgr.String())
	metaMgr.Close()
	cpuMgr.Close()
	diskMgr.Close()
}
