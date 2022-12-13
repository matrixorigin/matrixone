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
	"errors"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
)

func (r *runner) CleanPenddingCheckpoint() {
	r.storage.Lock()
	defer r.storage.Unlock()
	prev := r.maxCheckpointLocked()
	if !prev.IsFinished() {
		r.storage.entries.Delete(prev)
	}
	if prev.IsRunning() {
		logutil.Warnf("Delete a running checkpoint entry")
	}
}

func (r *runner) ForceGlobalCheckpoint(versionInterval time.Duration) error {
	r.storage.Lock()
	defer r.storage.Unlock()
	prev := r.maxCheckpointLocked()
	if prev == nil {
		return errors.New("no incremental checkpoint")
	}
	if !prev.IsFinished() {
		return errors.New("prev checkpoint not finished")
	}
	if !prev.IsIncremental() {
		return errors.New("prev checkpoint is global")
	}
	entry := NewCheckpointEntry(types.TS{}, prev.end.Next())
	r.storage.entries.Set(entry)
	r.doGlobalCheckpoint(entry)
	if err := r.saveCheckpoint(entry.start, entry.end); err != nil {
		return err
	}
	entry.SetState(ST_Finished)
	return nil
}

func (r *runner) ForceIncrementalGlobalCheckpoint(end types.TS, versionInterval time.Duration) error {
	r.storage.Lock()
	defer r.storage.Unlock()
	prev := r.maxCheckpointLocked()
	if !prev.IsFinished() {
		return errors.New("prev checkpoint not finished")
	}
	start := types.TS{}
	if prev != nil {
		start = prev.end.Next()
	}
	entry := NewCheckpointEntry(start, prev.end.Next())
	r.storage.entries.Set(entry)
	r.doIncrementalCheckpoint(entry)
	if err := r.saveCheckpoint(entry.start, entry.end); err != nil {
		return err
	}
	entry.SetState(ST_Finished)
	return nil
}

func (r *runner) IsAllChangesFlushed(start, end types.TS) bool {
	tree := r.source.ScanInRangePruned(start, end)
	tree.GetTree().Compact()
	return tree.IsEmpty()
}
