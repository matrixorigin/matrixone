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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type RunnerReader interface {
	GetAllIncrementalCheckpoints() []*CheckpointEntry
	GetAllGlobalCheckpoints() []*CheckpointEntry
	GetPenddingIncrementalCount() int
	GetGlobalCheckpointCount() int
	CollectCheckpointsInRange(ctx context.Context, start, end types.TS) (ckpLoc string, lastEnd types.TS, err error)
	ICKPSeekLT(ts types.TS, cnt int) []*CheckpointEntry
	MaxLSN() uint64
}

func (r *runner) collectCheckpointMetadata(start, end types.TS) *containers.Batch {
	bat := makeRespBatchFromSchema(CheckpointSchema)
	entries := r.GetAllIncrementalCheckpoints()
	for _, entry := range entries {
		if !entry.IsFinished() && !entry.end.Equal(end) {
			continue
		}
		bat.GetVectorByName(CheckpointAttr_StartTS).Append(entry.start, false)
		bat.GetVectorByName(CheckpointAttr_EndTS).Append(entry.end, false)
		bat.GetVectorByName(CheckpointAttr_MetaLocation).Append([]byte(entry.GetLocation()), false)
		bat.GetVectorByName(CheckpointAttr_EntryType).Append(true, false)
	}
	entries = r.GetAllGlobalCheckpoints()
	for _, entry := range entries {
		if !entry.IsFinished() && !entry.end.Equal(end) {
			continue
		}
		bat.GetVectorByName(CheckpointAttr_StartTS).Append(entry.start, false)
		bat.GetVectorByName(CheckpointAttr_EndTS).Append(entry.end, false)
		bat.GetVectorByName(CheckpointAttr_MetaLocation).Append([]byte(entry.GetLocation()), false)
		bat.GetVectorByName(CheckpointAttr_EntryType).Append(false, false)
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
		if !entries[i].IsFinished() {
			continue
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

func (r *runner) GCByTS(ctx context.Context, ts types.TS) error {
	prev := r.gcTS.Load()
	if prev == nil {
		r.gcTS.Store(ts)
	} else {
		prevTS := prev.(types.TS)
		if prevTS.Less(ts) {
			r.gcTS.Store(ts)
		}
	}
	logutil.Infof("GC %v", ts.ToString())
	r.gcCheckpointQueue.Enqueue(struct{}{})
	return nil
}

func (r *runner) getGCTS() types.TS {
	prev := r.gcTS.Load()
	if prev == nil {
		return types.TS{}
	}
	return prev.(types.TS)
}

func (r *runner) getGCedTS() types.TS {
	r.storage.RLock()
	minGlobal, _ := r.storage.globals.Min()
	minIncremental, _ := r.storage.entries.Min()
	r.storage.RUnlock()
	if minGlobal == nil {
		return types.TS{}
	}
	if minIncremental == nil {
		return minGlobal.end
	}
	if minIncremental.start.GreaterEq(minGlobal.end) {
		return minGlobal.end
	}
	return minIncremental.start
}

func (r *runner) getTSToGC() types.TS {
	maxGlobal := r.MaxGlobalCheckpoint()
	if maxGlobal == nil {
		return types.TS{}
	}
	if maxGlobal.IsFinished() {
		return maxGlobal.end.Prev()
	}
	globals := r.GetAllGlobalCheckpoints()
	if len(globals) == 1 {
		return types.TS{}
	}
	maxGlobal = globals[len(globals)-1]
	return maxGlobal.end.Prev()
}

func (r *runner) ExistPendingEntryToGC() bool {
	_, needGC := r.getTSTOGC()
	return needGC
}

func (r *runner) IsTSStale(ts types.TS) bool {
	gcts := r.getGCTS()
	if gcts.IsEmpty() {
		return false
	}
	minValidTS := gcts.Physical() - r.options.globalVersionInterval.Nanoseconds()
	return ts.Physical() < minValidTS
}
