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
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type RunnerReader interface {
	GetAllIncrementalCheckpoints() []*CheckpointEntry
	GetAllGlobalCheckpoints() []*CheckpointEntry
	GetPenddingIncrementalCount() int
	GetGlobalCheckpointCount() int
	CollectCheckpointsInRange(ctx context.Context, start, end types.TS) (ckpLoc string, lastEnd types.TS, err error)
	ICKPSeekLT(ts types.TS, cnt int) []*CheckpointEntry
	MaxGlobalCheckpoint() *CheckpointEntry
	GetLowWaterMark() types.TS
	MaxLSN() uint64
	GetCatalog() *catalog.Catalog
	GetCheckpointMetaFiles() map[string]struct{}
	RemoveCheckpointMetaFile(string)
	AddCheckpointMetaFile(string)
	ICKPRange(start, end *types.TS, cnt int) []*CheckpointEntry
	DeleteCompactedEntry(entry *CheckpointEntry)
	GetCompacted() *CheckpointEntry
	UpdateCompacted(entry *CheckpointEntry)
}

func (r *runner) collectCheckpointMetadata(start, end types.TS, ckpLSN, truncateLSN uint64) *containers.Batch {
	bat := makeRespBatchFromSchema(CheckpointSchema)
	entries := r.GetAllIncrementalCheckpoints()
	for _, entry := range entries {
		if !entry.IsFinished() && !entry.end.Equal(&end) {
			continue
		}
		bat.GetVectorByName(CheckpointAttr_StartTS).Append(entry.start, false)
		bat.GetVectorByName(CheckpointAttr_EndTS).Append(entry.end, false)
		bat.GetVectorByName(CheckpointAttr_MetaLocation).Append([]byte(entry.GetLocation()), false)
		bat.GetVectorByName(CheckpointAttr_EntryType).Append(true, false)
		bat.GetVectorByName(CheckpointAttr_Version).Append(entry.version, false)
		bat.GetVectorByName(CheckpointAttr_AllLocations).Append([]byte(entry.tnLocation), false)
		bat.GetVectorByName(CheckpointAttr_CheckpointLSN).Append(entry.ckpLSN, false)
		bat.GetVectorByName(CheckpointAttr_TruncateLSN).Append(entry.truncateLSN, false)
		bat.GetVectorByName(CheckpointAttr_Type).Append(int8(ET_Incremental), false)
	}
	entries = r.GetAllGlobalCheckpoints()
	for _, entry := range entries {
		if !entry.IsFinished() && !entry.end.Equal(&end) {
			continue
		}
		bat.GetVectorByName(CheckpointAttr_StartTS).Append(entry.start, false)
		bat.GetVectorByName(CheckpointAttr_EndTS).Append(entry.end, false)
		bat.GetVectorByName(CheckpointAttr_MetaLocation).Append([]byte(entry.GetLocation()), false)
		bat.GetVectorByName(CheckpointAttr_EntryType).Append(false, false)
		bat.GetVectorByName(CheckpointAttr_Version).Append(entry.version, false)
		bat.GetVectorByName(CheckpointAttr_AllLocations).Append([]byte(entry.tnLocation), false)
		bat.GetVectorByName(CheckpointAttr_CheckpointLSN).Append(entry.ckpLSN, false)
		bat.GetVectorByName(CheckpointAttr_TruncateLSN).Append(entry.truncateLSN, false)
		bat.GetVectorByName(CheckpointAttr_Type).Append(int8(ET_Global), false)
	}
	return bat
}
func (r *runner) GetAllIncrementalCheckpoints() []*CheckpointEntry {
	r.storage.Lock()
	snapshot := r.storage.incrementals.Copy()
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

func (r *runner) MaxIncrementalCheckpoint() *CheckpointEntry {
	r.storage.RLock()
	defer r.storage.RUnlock()
	entry, _ := r.storage.incrementals.Max()
	return entry
}

func (r *runner) GetCatalog() *catalog.Catalog {
	return r.catalog
}

func (r *runner) ICKPSeekLT(ts types.TS, cnt int) []*CheckpointEntry {
	r.storage.Lock()
	tree := r.storage.incrementals.Copy()
	r.storage.Unlock()
	it := tree.Iter()
	ok := it.Seek(NewCheckpointEntry(r.rt.SID(), ts, ts, ET_Incremental))
	incrementals := make([]*CheckpointEntry, 0)
	if ok {
		for len(incrementals) < cnt {
			e := it.Item()
			if !e.IsFinished() {
				break
			}
			if e.start.LT(&ts) {
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

func (r *runner) ICKPRange(start, end *types.TS, cnt int) []*CheckpointEntry {
	r.storage.Lock()
	tree := r.storage.incrementals.Copy()
	r.storage.Unlock()
	it := tree.Iter()
	ok := it.Seek(NewCheckpointEntry(r.rt.SID(), *start, *start, ET_Incremental))
	incrementals := make([]*CheckpointEntry, 0)
	if ok {
		for len(incrementals) < cnt {
			e := it.Item()
			if !e.IsFinished() {
				break
			}
			if e.start.GE(start) && e.start.LT(end) {
				incrementals = append(incrementals, e)
			}
			if !it.Next() {
				break
			}
		}
	}
	return incrementals
}

func (r *runner) GetCompacted() *CheckpointEntry {
	r.storage.Lock()
	defer r.storage.Unlock()
	return r.storage.compacted.Load()
}

func (r *runner) UpdateCompacted(entry *CheckpointEntry) {
	r.storage.Lock()
	defer r.storage.Unlock()
	r.storage.compacted.Store(entry)
	return
}

// this API returns the min ts of all checkpoints
// the start of global checkpoint is always 0
func (r *runner) GetLowWaterMark() types.TS {
	r.storage.RLock()
	defer r.storage.RUnlock()
	global, okG := r.storage.globals.Min()
	incremental, okI := r.storage.incrementals.Min()
	if !okG && !okI {
		return types.TS{}
	}
	if !okG {
		return incremental.start
	}
	if !okI {
		return global.start
	}
	if global.end.LT(&incremental.start) {
		return global.end
	}
	return incremental.start
}

func (r *runner) GetPenddingIncrementalCount() int {
	entries := r.GetAllIncrementalCheckpoints()
	global := r.MaxGlobalCheckpoint()

	count := 0
	for i := len(entries) - 1; i >= 0; i-- {
		if global != nil && entries[i].end.LE(&global.end) {
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

func (r *runner) getLastFinishedGlobalCheckpointLocked() *CheckpointEntry {
	g, ok := r.storage.globals.Max()
	if !ok {
		return nil
	}
	if g.IsFinished() {
		return g
	}
	it := r.storage.globals.Iter()
	it.Seek(g)
	defer it.Release()
	if !it.Prev() {
		return nil
	}
	return it.Item()
}

func (r *runner) GetAllCheckpoints() []*CheckpointEntry {
	ckps := make([]*CheckpointEntry, 0)
	var ts types.TS
	r.storage.Lock()
	g := r.getLastFinishedGlobalCheckpointLocked()
	tree := r.storage.incrementals.Copy()
	r.storage.Unlock()
	if g != nil {
		ts = g.GetEnd()
		ckps = append(ckps, g)
	}
	pivot := NewCheckpointEntry(r.rt.SID(), ts.Next(), ts.Next(), ET_Incremental)
	iter := tree.Iter()
	defer iter.Release()
	if ok := iter.Seek(pivot); ok {
		for {
			e := iter.Item()
			if !e.IsFinished() {
				break
			}
			ckps = append(ckps, e)
			if !iter.Next() {
				break
			}
		}
	}
	return ckps
}

func (r *runner) GCByTS(ctx context.Context, ts types.TS) error {
	prev := r.gcTS.Load()
	if prev == nil {
		r.gcTS.Store(ts)
	} else {
		prevTS := prev.(types.TS)
		if prevTS.LT(&ts) {
			r.gcTS.Store(ts)
		}
	}
	logutil.Debugf("GC %v", ts.ToString())
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
	minIncremental, _ := r.storage.incrementals.Min()
	r.storage.RUnlock()
	if minGlobal == nil {
		return types.TS{}
	}
	if minIncremental == nil {
		return minGlobal.end
	}
	if minIncremental.start.GE(&minGlobal.end) {
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
