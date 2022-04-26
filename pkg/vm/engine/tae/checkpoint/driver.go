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

package checkpoint

import (
	"sync"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type ckpDriver struct {
	common.ClosedState
	sm.StateMachine
	schedule tasks.TaskScheduler
}

func NewDriver(schedule tasks.TaskScheduler) *ckpDriver {
	f := &ckpDriver{}
	wg := new(sync.WaitGroup)
	rqueue := sm.NewSafeQueue(10000, 100, f.onRequests)
	f.StateMachine = sm.NewStateMachine(wg, f, rqueue, nil)
	// wqueue := sm.NewSafeQueue(20000, 1000, f.onCheckpoint)
	// f.StateMachine = sm.NewStateMachine(wg, f, rqueue, wqueue)
	return f
}

func (f *ckpDriver) String() string {
	return ""
}

func (f *ckpDriver) onCheckpoint(items ...interface{}) {
}

func (f *ckpDriver) onRequests(items ...interface{}) {
	// for _, item := range items {

	// }
	// changes := make(map[uint64]*changeRequests)
	// for _, item := range items {
	// 	switch req := item.(type) {
	// 	case *changeRequest:
	// 		requests := changes[req.dbId]
	// 		if requests == nil {
	// 			requests = newChangeRequests(req.dbId)
	// 			changes[req.dbId] = requests
	// 		}
	// 		requests.addUnit(req.unit)
	// 	}
	// }
	// if len(changes) != 0 {
	// 	for _, req := range changes {
	// 		f.onChangeRequest(req)
	// 	}
	// }
}

func (f *ckpDriver) OnUpdateColumn(unit data.CheckpointUnit) {
	if _, err := f.EnqueueRecevied(unit); err != nil {
		logutil.Warnf("%v", err)
	}
}
