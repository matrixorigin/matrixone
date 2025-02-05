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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/bitmap"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type CheckpointFastGCJob struct {
	CheckpointBasedGCJob
}

func NewCheckpointFastGCJob(
	ts *types.TS,
	sourcer engine.BaseReader,
	pitr *logtail.PitrInfo,
	accountSnapshots map[uint32][]types.TS,
	snapshotMeta *logtail.SnapshotMeta,
	buffer *containers.OneSchemaBatchBuffer,
	isOwner bool,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...GCJobExecutorOption,
) *CheckpointFastGCJob {
	e := &CheckpointBasedGCJob{
		sourcer:          sourcer,
		snapshotMeta:     snapshotMeta,
		accountSnapshots: accountSnapshots,
		pitr:             pitr,
		ts:               ts,
	}
	for _, opt := range opts {
		opt(e)
	}
	e.fillDefaults()
	e.GCExecutor = *NewGCExecutor(buffer, isOwner, e.config.canGCCacheSize, mp, fs)
	return &CheckpointFastGCJob{
		CheckpointBasedGCJob: *e,
	}
}

func (e *CheckpointFastGCJob) Execute(ctx context.Context) error {
	attrs, attrTypes := logtail.GetDataSchema()
	buffer := containers.NewOneSchemaBatchBuffer(
		mpool.MB*16,
		attrs,
		attrTypes,
	)
	defer buffer.Close(e.mp)
	transObjects := make(map[string]*ObjectEntry, 100)
	coarseFilter, err := makeSoftDeleteFilterCoarseFilter(
		transObjects,
		e.snapshotMeta,
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

func makeSoftDeleteFilterCoarseFilter(
	transObjects map[string]*ObjectEntry,
	meta *logtail.SnapshotMeta,
) (
	FilterFn,
	error,
) {
	filterTable := meta.GetSnapshotTableIDs()
	tables := meta.GetTableIDs()
	logutil.Infof("GetTableIDs count is %d", len(tables))
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
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			createTS := createTSs[i]
			dropTS := dropTSs[i]
			buf := bat.Vecs[0].GetRawBytesAt(i)
			stats := (objectio.ObjectStats)(buf)
			name := stats.ObjectName().UnsafeString()
			dropTSIsEmpty := dropTS.IsEmpty()
			if dropTSIsEmpty {
				if (transObjects)[name] == nil {
					object := &ObjectEntry{
						stats:    &stats,
						createTS: createTS,
						dropTS:   dropTS,
						db:       dbs[i],
						table:    tableIDs[i],
					}
					(transObjects)[name] = object
				}
				table := tables[tableIDs[i]]
				if table != nil && !table.IsDrop() {
					continue
				}
			}

			if _, ok := filterTable[tableIDs[i]]; ok {
				continue
			}

			if catalog.IsSystemTable(tableIDs[i]) {
				continue
			}
			bm.Add(uint64(i))
			if (transObjects)[name] != nil {
				(transObjects)[name].dropTS = dropTS
				continue
			}
		}
		return nil

	}, nil
}
