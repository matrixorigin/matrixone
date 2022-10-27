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
	"fmt"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type CheckpointEntry struct {
	sync.RWMutex
	start, end types.TS
	state      State
	location   string
}

func NewCheckpointEntry(start, end types.TS) *CheckpointEntry {
	return &CheckpointEntry{
		start: start,
		end:   end,
		state: ST_Running,
	}
}

func (e *CheckpointEntry) GetStart() types.TS { return e.start }
func (e *CheckpointEntry) GetEnd() types.TS   { return e.end }
func (e *CheckpointEntry) GetState() State {
	e.RLock()
	defer e.RUnlock()
	return e.state
}

func (e *CheckpointEntry) SetLocation(location string) {
	e.Lock()
	defer e.Unlock()
	e.location = location
}

func (e *CheckpointEntry) GetLocation() string {
	e.RLock()
	defer e.RUnlock()
	return e.location
}

func (e *CheckpointEntry) SetState(state State) (ok bool) {
	e.Lock()
	defer e.Unlock()
	// entry is already finished
	if e.state == ST_Finished {
		return
	}
	// entry is already running
	if state == ST_Running && e.state == ST_Running {
		return
	}
	e.state = state
	ok = true
	return
}

func (e *CheckpointEntry) IsRunning() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == ST_Running
}

func (e *CheckpointEntry) IsFinished() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == ST_Finished
}

func (e *CheckpointEntry) IsIncremental() bool {
	return !e.start.IsEmpty()
}

func (e *CheckpointEntry) String() string {
	t := "I"
	if !e.IsIncremental() {
		t = "G"
	}
	return fmt.Sprintf("CKP[%s](%s->%s)", t, e.start.ToString(), e.end.ToString())
}

func (e *CheckpointEntry) MakeBatches() *containers.Batch {
	logutil.Infof("make batches")
	return nil
}
