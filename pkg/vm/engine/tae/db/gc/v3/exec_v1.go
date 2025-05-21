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
	"unsafe"

	"github.com/matrixorigin/matrixone/pkg/common/malloc"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/ckputil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
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

type CheckpointBasedGCJob struct {
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
	globalCkpLoc     objectio.Location
	globalCkpVer     uint32

	result struct {
		filesToGC  []string
		filesNotGC []objectio.ObjectStats
	}
}

func NewCheckpointBasedGCJob(
	ts *types.TS,
	globalCkpLoc objectio.Location,
	gckpVersion uint32,
	sourcer engine.BaseReader,
	pitr *logtail.PitrInfo,
	accountSnapshots map[uint32][]types.TS,
	snapshotMeta *logtail.SnapshotMeta,
	buffer *containers.OneSchemaBatchBuffer,
	isOwner bool,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...GCJobExecutorOption,
) *CheckpointBasedGCJob {
	e := &CheckpointBasedGCJob{
		sourcer:          sourcer,
		snapshotMeta:     snapshotMeta,
		accountSnapshots: accountSnapshots,
		pitr:             pitr,
		ts:               ts,
		globalCkpLoc:     globalCkpLoc,
		globalCkpVer:     gckpVersion,
	}
	for _, opt := range opts {
		opt(e)
	}
	e.fillDefaults()
	e.GCExecutor = *NewGCExecutor(buffer, isOwner, e.config.canGCCacheSize, mp, fs)
	return e
}

func (e *CheckpointBasedGCJob) Close() error {
	if e.sourcer != nil {
		e.sourcer.Close()
		e.sourcer = nil
	}
	e.snapshotMeta = nil
	e.accountSnapshots = nil
	e.pitr = nil
	e.ts = nil
	e.globalCkpLoc = nil
	e.globalCkpVer = 0
	e.result.filesToGC = nil
	e.result.filesNotGC = nil
	return e.GCExecutor.Close()
}

func (e *CheckpointBasedGCJob) fillDefaults() {
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

func (e *CheckpointBasedGCJob) Execute(ctx context.Context) error {
	attrs, attrTypes := ckputil.DataScan_TableIDAtrrs, ckputil.DataScan_TableIDTypes
	buffer := containers.NewOneSchemaBatchBuffer(
		mpool.MB*16,
		attrs,
		attrTypes,
	)
	defer buffer.Close(e.mp)
	transObjects := make(map[string]*ObjectEntry, 100)
	coarseFilter, err := MakeBloomfilterCoarseFilter(
		ctx,
		e.config.coarseEstimateRows,
		e.config.coarseProbility,
		buffer,
		e.globalCkpLoc,
		e.globalCkpVer,
		e.ts,
		&transObjects,
		e.mp,
		e.fs,
	)
	if err != nil {
		return err
	}

	fineFilter, err := MakeSnapshotAndPitrFineFilter(
		e.ts,
		e.accountSnapshots,
		e.pitr,
		e.snapshotMeta,
		transObjects,
	)
	if err != nil {
		return err
	}

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
	transObjects = nil

	e.result.filesNotGC = make([]objectio.ObjectStats, 0, len(newFiles))
	e.result.filesNotGC = append(e.result.filesNotGC, newFiles...)
	return nil
}

func (e *CheckpointBasedGCJob) Result() ([]string, []objectio.ObjectStats) {
	return e.result.filesToGC, e.result.filesNotGC
}

func MakeBloomfilterCoarseFilter(
	ctx context.Context,
	rowCount int,
	probability float64,
	buffer containers.IBatchBuffer,
	location objectio.Location,
	ckpVersion uint32,
	ts *types.TS,
	transObjects *map[string]*ObjectEntry,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (
	FilterFn,
	error,
) {
	reader := logtail.NewCKPReader(ckpVersion, location, mp, fs)
	var err error
	if err = reader.ReadMeta(ctx); err != nil {
		return nil, err
	}
	bf, err := BuildBloomfilter(
		ctx,
		rowCount,
		probability,
		ckputil.TableObjectsAttr_ID_Idx,
		reader.LoadBatchData,
		buffer,
		mp,
	)
	if err != nil {
		return nil, err
	}
	return func(
		ctx context.Context,
		bm *bitmap.Bitmap,
		bat *batch.Batch,
		mp *mpool.MPool,
	) (err error) {
		createTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[1])
		dropTSs := vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
		dbs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[3])
		tableIDs := vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[4])
		bf.Test(
			bat.Vecs[0],
			func(exists bool, i int) {
				if exists {
					return
				}

				bm.Add(uint64(i))
				createTS := createTSs[i]
				dropTS := dropTSs[i]
				if !createTS.LT(ts) || !dropTS.LT(ts) {
					return
				}

				buf := bat.Vecs[0].GetRawBytesAt(i)
				stats := (objectio.ObjectStats)(buf)
				name := stats.ObjectName().UnsafeString()

				if dropTS.IsEmpty() && (*transObjects)[name] == nil {
					object := &ObjectEntry{
						stats:    &stats,
						createTS: createTS,
						dropTS:   dropTS,
						db:       dbs[i],
						table:    tableIDs[i],
					}
					(*transObjects)[name] = object
					return
				}
				if (*transObjects)[name] != nil {
					(*transObjects)[name].dropTS = dropTS
					return
				}
			},
		)
		return nil

	}, nil
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
			stats := (*objectio.ObjectStats)(unsafe.Pointer(&buf[0]))
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
