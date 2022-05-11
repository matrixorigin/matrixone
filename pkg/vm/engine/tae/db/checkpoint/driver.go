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
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logstore/sm"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/wal"
)

type ckpDriver struct {
	common.ClosedState
	sm.StateMachine
	scheduler tasks.TaskScheduler
	units     *LeveledUnits
}

func NewDriver(scheduler tasks.TaskScheduler, cfg *PolicyCfg) *ckpDriver {
	policy := newSimpleLeveledPolicy(cfg)
	units := NewLeveledUnits(scheduler, policy)
	f := &ckpDriver{
		scheduler: scheduler,
		units:     units,
	}
	wg := new(sync.WaitGroup)
	rqueue := sm.NewSafeQueue(10000, 100, f.onRequests)
	f.StateMachine = sm.NewStateMachine(wg, f, rqueue, nil)
	wqueue := sm.NewSafeQueue(20000, 1000, f.onCheckpoint)
	f.StateMachine = sm.NewStateMachine(wg, f, rqueue, wqueue)
	return f
}

func (f *ckpDriver) String() string {
	return ""
}

func (f *ckpDriver) onCheckpoint(items ...interface{}) {
	start := time.Now()
	for _, item := range items {
		ckpEntry := item.(wal.LogEntry)
		ckpEntry.WaitDone()
		ckpEntry.Free()
	}
	logutil.Infof("Total [%d] WAL Checkpointed | [%s]", len(items), time.Since(start))
}

func (f *ckpDriver) onRequests(items ...interface{}) {
	for _, item := range items {
		unit := item.(data.CheckpointUnit)
		f.units.AddUnit(unit)
	}
	f.units.Scan()
}

func (f *ckpDriver) OnUpdateColumn(unit data.CheckpointUnit) {
	if _, err := f.EnqueueRecevied(unit); err != nil {
		logutil.Warnf("%v", err)
	}
}

func (f *ckpDriver) EnqueueCheckpointEntry(entry wal.LogEntry) {
	if _, err := f.EnqueueCheckpoint(entry); err != nil {
		logutil.Warnf("%v", err)
	}
}

func (f *ckpDriver) EnqueueCheckpointUnit(unit data.CheckpointUnit) {
	if _, err := f.EnqueueRecevied(unit); err != nil {
		logutil.Warnf("%v", err)
	}
}

func (f *ckpDriver) Stop() {
	f.StateMachine.Stop()
	logutil.Infof("Checkpoint Driver Stopped")
}
