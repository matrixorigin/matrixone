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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/blockio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

const (
	Default_Coarse_EstimateRows = 10000000
	Default_Coarse_Probility    = 0.0001
)

type GCJobExecutorOption func(*GCJob)

func WithGCJobCoarseConfig(
	estimateRows int,
	probility float64,
) GCJobExecutorOption {
	return func(e *GCJob) {
		e.config.coarseEstimateRows = estimateRows
		e.config.coarseProbility = probility
	}
}

type CheckpointBasedGCJob struct {
	GCExecutor
	config struct {
		coarseEstimateRows int
		coarseProbility    float64
	}
	// TODO: use reader to read the data
	gcSourceFiles    []objectio.ObjectStats
	snapshotMeta     *logtail.SnapshotMeta
	accountSnapshots map[uint32][]types.TS
	pitr             *logtail.PitrInfo
	ts               *types.TS
	globalCkpLoc     objectio.Location
	dir              string

	from, to *types.TS

	result struct {
		filesToGC  []string
		filesNotGC []objectio.ObjectStats
	}
}

func NewCheckpointBasedGCJob(
	dir string,
	ts *types.TS,
	from, to *types.TS,
	globalCkpLoc objectio.Location,
	gcSourceFiles []objectio.ObjectStats,
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
		GCExecutor:       *NewGCExecutor(buffer, isOwner, mp, fs),
		gcSourceFiles:    gcSourceFiles,
		snapshotMeta:     snapshotMeta,
		accountSnapshots: accountSnapshots,
		pitr:             pitr,
		ts:               ts,
		dir:              dir,
		globalCkpLoc:     globalCkpLoc,
		from:             from,
		to:               to,
	}
	for _, opt := range opts {
		opt(e)
	}
	e.fillDefaults()
	return e
}

func (e *CheckpointBasedGCJob) Close() error {
	e.gcSourceFiles = nil
	e.snapshotMeta = nil
	e.accountSnapshots = nil
	e.pitr = nil
	e.ts = nil
	e.globalCkpLoc = nil
	e.dir = ""
	e.from = nil
	e.to = nil
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
}

func (e *CheckpointBasedGCJob) getNextCoarseBatch(
	ctx context.Context,
	_ []string,
	_ *plan.Expr,
	mp *mpool.MPool,
	bat *batch.Batch,
) (bool, error) {
	if len(e.gcSourceFiles) == 0 {
		return true, nil
	}
	bat.CleanOnlyData()
	if err := loader(
		ctx, e.fs, &e.gcSourceFiles[0], bat, mp,
	); err != nil {
		return false, err
	}
	e.gcSourceFiles = e.gcSourceFiles[1:]
	return false, nil
}

func (e *CheckpointBasedGCJob) Execute(ctx context.Context) error {
	attrs, attrTypes := logtail.GetDataSchema()
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
		e.getNextCoarseBatch,
		coarseFilter,
		fineFilter,
		finalSinker,
	)
	if err != nil {
		return err
	}
	transObjects = nil
	if err := WriteNewMetaFile(
		ctx,
		e.dir,
		e.from,
		e.to,
		newFiles,
		e.mp,
		e.fs,
	); err != nil {
		return err
	}

	e.result.filesNotGC = make([]objectio.ObjectStats, 0, len(newFiles))
	e.result.filesNotGC = append(e.result.filesNotGC, newFiles...)

	return nil
}

func (e *CheckpointBasedGCJob) Result() ([]string, []objectio.ObjectStats) {
	return e.result.filesToGC, e.result.filesNotGC
}

func WriteNewMetaFile(
	ctx context.Context,
	dir string,
	from, to *types.TS,
	newFiles []objectio.ObjectStats,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (err error) {
	name := blockio.EncodeCheckpointMetadataFileName(
		dir, PrefixGCMeta, *from, *to,
	)
	ret := batch.NewWithSchema(
		false,
		false,
		ObjectTableMetaAttrs,
		ObjectTableMetaTypes,
	)
	defer func() {
		ret.FreeColumns(mp)
	}()
	for i := range newFiles {
		if err = vector.AppendBytes(
			ret.GetVector(0),
			newFiles[i][:],
			false,
			mp,
		); err != nil {
			return
		}
	}

	var writer *objectio.ObjectWriter
	if writer, err = objectio.NewObjectWriterSpecial(
		objectio.WriterGC, name, fs,
	); err != nil {
		return
	}
	if _, err = writer.WriteWithoutSeqnum(ret); err != nil {
		return
	}

	_, err = writer.WriteEnd(ctx)
	return
}

func MakeBloomfilterCoarseFilter(
	ctx context.Context,
	rowCount int,
	probability float64,
	buffer containers.IBatchBuffer,
	location objectio.Location,
	ts *types.TS,
	transObjects *map[string]*ObjectEntry,
	mp *mpool.MPool,
	fs fileservice.FileService,
) (
	FilterFn,
	error,
) {
	reader, err := logtail.MakeGlobalCheckpointDataReader(ctx, "", fs, location, 0)
	if err != nil {
		return nil, err
	}
	bf, err := BuildBloomfilter(
		ctx,
		rowCount,
		probability,
		2,
		reader.LoadBatchData,
		buffer,
		mp,
	)
	if err != nil {
		reader.Close()
		return nil, err
	}
	reader.Close()
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
				if !isSnapshotRefers(
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
			if !isSnapshotRefers(
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
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			buf := bat.Vecs[0].GetRawBytesAt(i)
			stats := (*objectio.ObjectStats)(unsafe.Pointer(&buf[0]))
			name := stats.ObjectName().String()
			buffer[name] = struct{}{}
		}
		for name := range buffer {
			*filesToGC = append(*filesToGC, name)
		}
		return nil
	}, nil
}
