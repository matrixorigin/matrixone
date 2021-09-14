// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
	handler := NewPoolHandler(4, nil)
	handler.Start()
	dis.RegisterHandler(MockEvent, handler)
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

func TestResourceMgr(t *testing.T) {
	handler := NewPoolHandler(10, nil)
	mgr := NewBaseResourceMgr(handler)
	res1H := NewSingleWorkerHandler("res1H")
	res1 := NewBaseResource("res1", ResT_IO, res1H)
	res2H := NewSingleWorkerHandler("res1H")
	res2 := NewBaseResource("res2", ResT_IO, res2H)
	res3H := NewPoolHandler(2, nil)
	res3 := NewBaseResource("res3", ResT_CPU, res3H)
	err := mgr.Add(res1)
	assert.Nil(t, err)
	err = mgr.Add(res2)
	assert.Nil(t, err)
	err = mgr.Add(res3)
	assert.Nil(t, err)
	err = mgr.Add(res1)
	assert.Equal(t, ErrDuplicateResource, err)
	assert.Equal(t, 3, mgr.ResourceCount())
	assert.Equal(t, 2, mgr.ResourceCountByType(ResT_IO))
	assert.NotNil(t, mgr.GetResource("res1"))
	assert.NotNil(t, mgr.GetResource("res2"))
	assert.NotNil(t, mgr.GetResource("res3"))
	mgr.Start()

	event := NewBaseEvent(nil, MockEvent, nil, true)
	mgr.Enqueue(event)
	err = event.WaitDone()
	assert.Nil(t, err)

	mgr.Close()
}
