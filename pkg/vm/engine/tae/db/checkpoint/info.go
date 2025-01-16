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

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type RunnerWriter interface {
	RemoveCheckpointMetaFile(string)
	AddCheckpointMetaFile(string)
	UpdateCompacted(entry *CheckpointEntry) bool
}

type RunnerReader interface {
	String() string
	GetAllIncrementalCheckpoints() []*CheckpointEntry
	GetAllGlobalCheckpoints() []*CheckpointEntry
	GetPenddingIncrementalCount() int
	CollectCheckpointsInRange(ctx context.Context, start, end types.TS) (ckpLoc string, lastEnd types.TS, err error)
	ICKPSeekLT(ts types.TS, cnt int) []*CheckpointEntry
	GetLowWaterMark() types.TS
	MaxLSN() uint64
	GetCatalog() *catalog.Catalog
	GetCheckpointMetaFiles() map[string]struct{}
	ICKPRange(start, end *types.TS, cnt int) []*CheckpointEntry
	GetCompacted() *CheckpointEntry

	// for test, delete in next phase
	GetAllCheckpoints() []*CheckpointEntry
	GetAllCheckpointsForBackup(compact *CheckpointEntry) []*CheckpointEntry

	MaxGlobalCheckpoint() *CheckpointEntry
	MaxIncrementalCheckpoint() *CheckpointEntry
	GetDirtyCollector() logtail.Collector
}

func (r *runner) collectCheckpointMetadata(start, end types.TS) *containers.Batch {
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
	return r.store.GetAllIncrementalCheckpoints()
}

func (r *runner) GetAllGlobalCheckpoints() []*CheckpointEntry {
	return r.store.GetAllGlobalCheckpoints()
}

func (r *runner) MaxLSN() uint64 {
	endTs := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	return r.source.GetMaxLSN(types.TS{}, endTs)
}

func (r *runner) MaxLSNInRange(end types.TS) uint64 {
	return r.source.GetMaxLSN(types.TS{}, end)
}

func (r *runner) MaxGlobalCheckpoint() *CheckpointEntry {
	return r.store.MaxGlobalCheckpoint()
}

func (r *runner) MaxIncrementalCheckpoint() *CheckpointEntry {
	return r.store.MaxIncrementalCheckpoint()
}

func (r *runner) GetCatalog() *catalog.Catalog {
	return r.catalog
}

func (r *runner) ICKPSeekLT(ts types.TS, cnt int) []*CheckpointEntry {
	return r.store.ICKPSeekLT(ts, cnt)
}

func (r *runner) ICKPRange(start, end *types.TS, cnt int) []*CheckpointEntry {
	return r.store.ICKPRange(start, end, cnt)
}

func (r *runner) GetCompacted() *CheckpointEntry {
	return r.store.GetCompacted()
}

func (r *runner) UpdateCompacted(entry *CheckpointEntry) (updated bool) {
	if !entry.IsCompact() {
		logutil.Error(
			"Update-CKP-Compact-Error",
			zap.String("entry", entry.String()),
		)
		return false
	}
	return r.store.UpdateCompacted(entry)
}

// this API returns the min ts of all checkpoints
// the start of global checkpoint is always 0
func (r *runner) GetLowWaterMark() types.TS {
	return r.store.GetLowWaterMark()
}

func (r *runner) GetPenddingIncrementalCount() int {
	return r.store.GetPenddingIncrementalCount()
}

func (r *runner) GetAllCheckpoints() []*CheckpointEntry {
	return r.store.GetAllCheckpoints()
}

func (r *runner) GetAllCheckpointsForBackup(compact *CheckpointEntry) []*CheckpointEntry {
	return r.store.GetAllCheckpointsForBackup(compact)
}

func (r *runner) GCByTS(ctx context.Context, ts types.TS) error {
	if _, updated := r.store.UpdateGCIntent(&ts); !updated {
		// TODO
		return nil
	}
	_, err := r.gcCheckpointQueue.Enqueue(struct{}{})
	return err
}

func (r *runner) GCNeeded() bool {
	return r.store.GCNeeded()
}
