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

package gc

import (
	"context"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

const (
	Default_Coarse_EstimateRows = 10000000
	Default_Coarse_Probility    = 0.00001
	Default_CanGC_TailSize      = 64 * malloc.MB
)

type GCJobExecutorOption func(*GCJob)

func WithGCJobCoarseConfig(
	estimateRows int,
	probility float64,
	size int,
) GCJobExecutorOption {
	return func(e *GCJob) {
		e.config.coarseEstimateRows = estimateRows
		e.config.coarseProbility = probility
		e.config.canGCCacheSize = size
	}
}

type FilterProvider interface {
	CoarseFilter(ctx context.Context) (FilterFn, error)
	FineFilter(ctx context.Context) (FilterFn, error)
}

type BaseCheckpointGCJob struct {
	GCExecutor
	config struct {
		coarseEstimateRows int
		coarseProbility    float64
		canGCCacheSize     int
	}
	sourcer          engine.BaseReader
	snapshotMeta     *logtail.SnapshotMeta
	accountSnapshots map[uint32][]types.TS
	pitr             *logtail.PitrInfo
	ts               *types.TS
	result           struct {
		filesToGC  []string
		filesNotGC []objectio.ObjectStats
	}
	transObjects map[string]*ObjectEntry
	buffer       containers.IBatchBuffer

	filterProvider FilterProvider
}

func (e *BaseCheckpointGCJob) fillDefaults() {
	if e.config.coarseEstimateRows <= 0 {
		e.config.coarseEstimateRows = Default_Coarse_EstimateRows
	}
	if e.config.coarseProbility <= 0 {
		e.config.coarseProbility = Default_Coarse_Probility
	}
	if e.config.canGCCacheSize <= 0 {
		e.config.canGCCacheSize = Default_CanGC_TailSize
	}
}

func (e *BaseCheckpointGCJob) Result() ([]string, []objectio.ObjectStats) {
	return e.result.filesToGC, e.result.filesNotGC
}

func (e *BaseCheckpointGCJob) Close() error {
	for name, entry := range e.transObjects {
		entry.Release()
		delete(e.transObjects, name)
	}
	if e.sourcer != nil {
		e.sourcer.Close()
		e.sourcer = nil
	}
	if e.buffer != nil {
		e.buffer.Close(e.mp)
		e.buffer = nil
	}
	e.snapshotMeta = nil
	e.accountSnapshots = nil
	e.pitr = nil
	e.ts = nil
	e.result.filesToGC = nil
	e.result.filesNotGC = nil
	return e.GCExecutor.Close()
}

func (e *BaseCheckpointGCJob) Execute(ctx context.Context) error {
	if e.transObjects == nil {
		e.transObjects = make(map[string]*ObjectEntry, 100)
	}
	if len(e.transObjects) > 0 {
		logutil.Warnf("transObjects is not empty: %d, maybe it is not closed", len(e.transObjects))
	}
	attrs, attrTypes := logtail.GetDataSchema()
	e.buffer = containers.NewOneSchemaBatchBuffer(
		mpool.MB*16,
		attrs,
		attrTypes,
	)
	defer e.buffer.Close(e.mp)
	coarseFilter, err := e.filterProvider.CoarseFilter(ctx)
	if err != nil {
		return err
	}
	fineFilter, err := e.filterProvider.FineFilter(ctx)
	if err != nil {
		return err
	}

	return e.ExecuteTemplate(ctx, coarseFilter, fineFilter)
}

func (e *BaseCheckpointGCJob) ExecuteTemplate(
	ctx context.Context,
	coarseFilter, fineFilter FilterFn,
) error {
	e.result.filesToGC = make([]string, 0, 20)
	finalSinker, err := MakeFinalCanGCSinker(&e.result.filesToGC)
	if err != nil {
		return err
	}

	newFiles, err := e.Run(
		ctx,
		e.sourcer.Read,
		coarseFilter,
		fineFilter,
		finalSinker,
	)
	if err != nil {
		return err
	}

	e.result.filesNotGC = make([]objectio.ObjectStats, 0, len(newFiles))
	e.result.filesNotGC = append(e.result.filesNotGC, newFiles...)
	return nil
}

func (e *BaseCheckpointGCJob) FineFilter(_ context.Context) (FilterFn, error) {
	return MakeSnapshotAndPitrFineFilter(
		e.ts,
		e.accountSnapshots,
		e.pitr,
		e.snapshotMeta,
		e.transObjects,
	)
}

func MakeSnapshotAndPitrFineFilter(
	ts *types.TS,
	accountSnapshots map[uint32][]types.TS,
	pitrs *logtail.PitrInfo,
	snapshotMeta *logtail.SnapshotMeta,
	transObjects map[string]*ObjectEntry,
) (
	filter FilterFn,
	err error,
) {
	tableSnapshots, tablePitrs := snapshotMeta.AccountToTableSnapshots(
		accountSnapshots,
		pitrs,
	)
	return func(
		ctx context.Context,
		bm *bitmap.Bitmap,
		bat *batch.Batch,
		mp *mpool.MPool,
	) error {
		createTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[1])
		deleteTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
		tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[4])
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			buf := bat.Vecs[0].GetRawBytesAt(i)
			stats := (objectio.ObjectStats)(buf)
			name := stats.ObjectName().UnsafeString()
			tableID := tableIDs[i]
			createTS := createTSs[i]
			dropTS := deleteTSs[i]

			snapshots := tableSnapshots[tableID]
			pitr := tablePitrs[tableID]

			if entry := transObjects[name]; entry != nil {
				if !logtail.ObjectIsSnapshotRefers(
					entry.stats, pitr, &entry.createTS, &entry.dropTS, snapshots,
				) {
					bm.Add(uint64(i))
				}
				continue
			}
			if !createTS.LT(ts) || !dropTS.LT(ts) {
				continue
			}
			if dropTS.IsEmpty() {
				panic(fmt.Sprintf("dropTS is empty, name: %s, createTS: %s", name, createTS.ToString()))
			}
			if !logtail.ObjectIsSnapshotRefers(
				&stats, pitr, &createTS, &dropTS, snapshots,
			) {
				bm.Add(uint64(i))
			}
		}
		return nil
	}, nil
}

func MakeFinalCanGCSinker(
	filesToGC *[]string,
) (
	SinkerFn,
	error,
) {
	buffer := make(map[string]struct{}, 100)
	return func(
		ctx context.Context, bat *batch.Batch,
	) error {
		clear(buffer)
		var dropTSs []types.TS
		var tableIDs []uint64
		if bat.Vecs[0].Length() > 0 {
			dropTSs = vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
			tableIDs = vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[4])
		}
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			buf := bat.Vecs[0].GetRawBytesAt(i)
			stats := (objectio.ObjectStats)(buf)
			name := stats.ObjectName().String()
			dropTS := dropTSs[i]
			tableID := tableIDs[i]
			if !dropTS.IsEmpty() {
				buffer[name] = struct{}{}
				continue
			}
			if !logtail.IsMoTable(tableID) {
				buffer[name] = struct{}{}
			}
		}
		for name := range buffer {
			*filesToGC = append(*filesToGC, name)
		}
		return nil
	}, nil
}
