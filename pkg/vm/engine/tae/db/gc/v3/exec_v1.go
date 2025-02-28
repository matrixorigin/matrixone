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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"

	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
)

type CheckpointBasedGCJob struct {
	BaseCheckpointGCJob
	globalCkpLoc objectio.Location
}

func NewCheckpointBasedGCJob(
	ts *types.TS,
	globalCkpLoc objectio.Location,
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
	e := &BaseCheckpointGCJob{
		sourcer:          sourcer,
		snapshotMeta:     snapshotMeta,
		accountSnapshots: accountSnapshots,
		pitr:             pitr,
		ts:               ts,
		transObjects:     make(map[string]*ObjectEntry, 100),
	}

	for _, opt := range opts {
		opt(e)
	}
	e.fillDefaults()
	e.GCExecutor = *NewGCExecutor(buffer, isOwner, e.config.canGCCacheSize, mp, fs)
	job := &CheckpointBasedGCJob{
		BaseCheckpointGCJob: *e,
		globalCkpLoc:        globalCkpLoc,
	}
	job.filterProvider = job
	return job
}

func (e *CheckpointBasedGCJob) Close() error {
	e.globalCkpLoc = nil
	return e.BaseCheckpointGCJob.Close()
}

func (e *CheckpointBasedGCJob) CoarseFilter(ctx context.Context) (FilterFn, error) {
	return MakeBloomfilterCoarseFilter(
		ctx,
		e.config.coarseEstimateRows,
		e.config.coarseProbility,
		e.buffer,
		e.globalCkpLoc,
		e.ts,
		&e.transObjects,
		e.mp,
		e.fs,
	)
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
					entry := NewObjectEntry()
					entry.stats = &stats
					entry.createTS = createTS
					entry.dropTS = dropTS
					entry.db = dbs[i]
					entry.table = tableIDs[i]
					(*transObjects)[name] = entry
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
