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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/data"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
)

type Units struct {
	sync.RWMutex
	units map[data.CheckpointUnit]bool
	ts    time.Time
}

func NewUnits() *Units {
	return &Units{
		units: make(map[data.CheckpointUnit]bool),
	}
}

func (units *Units) AddUnit(u data.CheckpointUnit) {
	units.Lock()
	units.units[u] = true
	units.Unlock()
}

func (units *Units) AddUnits(us []data.CheckpointUnit) {
	units.Lock()
	for _, u := range us {
		units.units[u] = true
	}
	units.Unlock()
}

func (units *Units) ConsumeAll() map[data.CheckpointUnit]bool {
	units.Lock()
	ret := units.units
	units.units = make(map[data.CheckpointUnit]bool)
	units.Unlock()
	return ret
}

func (units *Units) UpdateTS() {
	units.Lock()
	units.ts = time.Now()
	units.Unlock()
}

func (units *Units) PrepareConsume(maxDuration time.Duration) bool {
	units.RLock()
	duration := time.Since(units.ts)
	units.RUnlock()
	if duration >= maxDuration {
		return true
	}
	return false
}

type LeveledUnits struct {
	levels    []*Units
	policy    LeveledPolicy
	scheduler tasks.TaskScheduler
}

func NewLeveledUnits(scheduler tasks.TaskScheduler, policy LeveledPolicy) *LeveledUnits {
	lunits := &LeveledUnits{
		levels:    make([]*Units, policy.TotalLevels()),
		policy:    policy,
		scheduler: scheduler,
	}
	for i := range lunits.levels {
		lunits.levels[i] = NewUnits()
	}
	return lunits
}

func (lunits *LeveledUnits) AddUnit(unit data.CheckpointUnit) {
	score := unit.EstimateScore()
	if score == 0 {
		return
	}
	level := lunits.policy.DecideLevel(score)
	lunits.levels[level].AddUnit(unit)
}

func (lunits *LeveledUnits) RunCalibration() {
	for i := len(lunits.levels) - 1; i >= 0; i-- {
		level := lunits.levels[i]
		units := level.ConsumeAll()
		for unit, _ := range units {
			// unit.RunCalibration()
			lunits.AddUnit(unit)
		}
	}
}

func (lunits *LeveledUnits) Scan() {
	for i := len(lunits.levels) - 1; i >= 0; i-- {
		level := lunits.levels[i]
		if ok := level.PrepareConsume(lunits.policy.ScanInterval(i)); !ok {
			continue
		}
		units := level.ConsumeAll()
		level.UpdateTS()
		for unit, _ := range units {
			if lunits.policy.DecideLevel(unit.EstimateScore()) < i {
				lunits.AddUnit(unit)
				continue
			}
			taskFactory, taskType, scopes, err := unit.BuildCompactionTaskFactory()
			if err != nil || taskFactory == nil {
				logutil.Warnf("%s: %v", unit.MutationInfo(), err)
				continue
			}
			logutil.Infof("%s", unit.MutationInfo())
			lunits.scheduler.ScheduleMultiScopedTxnTask(nil, taskType, scopes, taskFactory)
		}
	}
}
