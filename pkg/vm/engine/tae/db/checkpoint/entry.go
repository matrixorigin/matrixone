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
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/dataio/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tasks"
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
		state: ST_Pending,
	}
}

func (e *CheckpointEntry) GetStart() types.TS { return e.start }
func (e *CheckpointEntry) GetEnd() types.TS   { return e.end }
func (e *CheckpointEntry) GetState() State {
	e.RLock()
	defer e.RUnlock()
	return e.state
}
func (e *CheckpointEntry) IsCommitted() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == ST_Finished
}
func (e *CheckpointEntry) HasOverlap(from, to types.TS) bool {
	if e.start.Greater(to) || e.end.Less(from) {
		return false
	}
	return true
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
func (e *CheckpointEntry) IsPendding() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == ST_Pending
}
func (e *CheckpointEntry) IsFinished() bool {
	e.RLock()
	defer e.RUnlock()
	return e.state == ST_Finished
}

func (e *CheckpointEntry) IsIncremental() bool {
	// Currently only incremental is supported
	return true
}

func (e *CheckpointEntry) String() string {
	t := "I"
	if !e.IsIncremental() {
		t = "G"
	}
	state := e.GetState()
	return fmt.Sprintf("CKP[%s][%v](%s->%s)", t, state, e.start.ToString(), e.end.ToString())
}

func (e *CheckpointEntry) Replay(
	ctx context.Context,
	c *catalog.Catalog,
	fs *objectio.ObjectFS,
	dataFactory catalog.DataFactory) (readDuration, applyDuration time.Duration, err error) {
	reader, err := blockio.NewCheckpointReader(ctx, fs.Service, e.location)
	if err != nil {
		return
	}

	data := logtail.NewCheckpointData()
	defer data.Close()
	t0 := time.Now()
	if err = data.ReadFrom(reader, nil, common.DefaultAllocator); err != nil {
		return
	}
	readDuration = time.Since(t0)
	t0 = time.Now()
	err = data.ApplyReplayTo(c, dataFactory)
	applyDuration = time.Since(t0)
	return
}
func (e *CheckpointEntry) Read(
	ctx context.Context,
	scheduler tasks.JobScheduler,
	fs *objectio.ObjectFS,
) (data *logtail.CheckpointData, err error) {
	reader, err := blockio.NewCheckpointReader(ctx, fs.Service, e.location)
	if err != nil {
		return
	}

	data = logtail.NewCheckpointData()
	if err = data.ReadFrom(
		reader,
		scheduler,
		common.DefaultAllocator,
	); err != nil {
		return
	}
	return
}
func (e *CheckpointEntry) GetByTableID(fs *objectio.ObjectFS, tid uint64) (ins, del, cnIns *api.Batch, err error) {
	reader, err := blockio.NewCheckpointReader(context.Background(), fs.Service, e.location)
	if err != nil {
		return
	}
	data := logtail.NewCheckpointData()
	defer data.Close()
	if err = data.ReadFrom(reader, nil, common.DefaultAllocator); err != nil {
		return
	}
	ins, del, cnIns, err = data.GetTableData(tid)
	return
}
