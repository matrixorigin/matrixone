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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type RunnerReader interface {
	GetAllIncrementalCheckpoints() []*CheckpointEntry
	GetAllGlobalCheckpoints() []*CheckpointEntry
	GetPenddingIncrementalCount() int
	GetGlobalCheckpointCount() int
	CollectCheckpointsInRange(start, end types.TS) (ckpLoc string, lastEnd types.TS)
	ICKPSeekLT(ts types.TS, cnt int) []*CheckpointEntry
	MaxLSN() uint64
}

func (r *runner) collectCheckpointMetadata() *containers.Batch {
	bat := makeRespBatchFromSchema(CheckpointSchema)
	entries := r.GetAllIncrementalCheckpoints()
	for _, entry := range entries {
		if !entry.IsFinished() {
			continue
		}
		bat.GetVectorByName(CheckpointAttr_StartTS).Append(entry.start)
		bat.GetVectorByName(CheckpointAttr_EndTS).Append(entry.end)
		bat.GetVectorByName(CheckpointAttr_MetaLocation).Append([]byte(entry.GetLocation()))
		bat.GetVectorByName(CheckpointAttr_EntryType).Append(true)
	}
	entries = r.GetAllGlobalCheckpoints()
	for _, entry := range entries {
		if !entry.IsFinished() {
			continue
		}
		bat.GetVectorByName(CheckpointAttr_StartTS).Append(entry.start)
		bat.GetVectorByName(CheckpointAttr_EndTS).Append(entry.end)
		bat.GetVectorByName(CheckpointAttr_MetaLocation).Append([]byte(entry.GetLocation()))
		bat.GetVectorByName(CheckpointAttr_EntryType).Append(false)
	}
	return bat
}
func (r *runner) GetAllIncrementalCheckpoints() []*CheckpointEntry {
	r.storage.Lock()
	snapshot := r.storage.entries.Copy()
	r.storage.Unlock()
	return snapshot.Items()
}
func (r *runner) GetAllGlobalCheckpoints() []*CheckpointEntry {
	r.storage.Lock()
	snapshot := r.storage.globals.Copy()
	r.storage.Unlock()
	return snapshot.Items()
}
func (r *runner) MaxLSN() uint64 {
	endTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	return r.source.GetMaxLSN(types.TS{}, endTs)
}
func (r *runner) MaxLSNInRange(end types.TS) uint64 {
	return r.source.GetMaxLSN(types.TS{}, end)
}

func (r *runner) MaxGlobalCheckpoint() *CheckpointEntry {
	r.storage.RLock()
	defer r.storage.RUnlock()
	global, _ := r.storage.globals.Max()
	return global
}

func (r *runner) MaxCheckpoint() *CheckpointEntry {
	r.storage.RLock()
	defer r.storage.RUnlock()
	entry, _ := r.storage.entries.Max()
	global, _ := r.storage.globals.Max()
	if entry == nil || (global != nil && global.end.Equal(entry.end)) {
		return global
	}
	return entry
}

func (r *runner) ICKPSeekLT(ts types.TS, cnt int) []*CheckpointEntry {
	r.storage.Lock()
	tree := r.storage.entries.Copy()
	r.storage.Unlock()
	it := tree.Iter()
	ok := it.Seek(NewCheckpointEntry(ts, ts, ET_Incremental))
	incrementals := make([]*CheckpointEntry, 0)
	if ok {
		for len(incrementals) < cnt {
			e := it.Item()
			if !e.IsFinished() {
				break
			}
			if e.start.Less(ts) {
				if !it.Next() {
					break
				}
				continue
			}
			incrementals = append(incrementals, e)
			if !it.Next() {
				break
			}
		}
	}
	return incrementals
}

func (r *runner) GetPenddingIncrementalCount() int {
	entries := r.GetAllIncrementalCheckpoints()
	global := r.MaxGlobalCheckpoint()

	count := 0
	for i := len(entries) - 1; i >= 0; i-- {
		if global != nil && entries[i].end.LessEq(global.end) {
			break
		}
		count++
	}
	return count
}

func (r *runner) GetGlobalCheckpointCount() int {
	r.storage.RLock()
	defer r.storage.RUnlock()
	return r.storage.globals.Len()
}
