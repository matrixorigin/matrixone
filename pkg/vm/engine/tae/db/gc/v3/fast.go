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
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

type CheckpointFastGCJob struct {
	BaseCheckpointGCJob
	catalog *catalog2.Catalog
}

func NewCheckpointFastGCJob(
	ts *types.TS,
	sourcer engine.BaseReader,
	pitr *logtail.PitrInfo,
	accountSnapshots map[uint32][]types.TS,
	snapshotMeta *logtail.SnapshotMeta,
	buffer *containers.OneSchemaBatchBuffer,
	isOwner bool,
	catalog *catalog2.Catalog,
	mp *mpool.MPool,
	fs fileservice.FileService,
	opts ...GCJobExecutorOption,
) *CheckpointFastGCJob {
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
	job := &CheckpointFastGCJob{
		BaseCheckpointGCJob: *e,
		catalog:             catalog,
	}
	job.filterProvider = job
	return job
}

func (e *CheckpointFastGCJob) CoarseFilter(_ context.Context) (FilterFn, error) {
	return makeSoftDeleteFilterCoarseFilter(
		e.transObjects,
		e.snapshotMeta,
		e.catalog,
	)
}

func makeSoftDeleteFilterCoarseFilter(
	transObjects map[string]*ObjectEntry,
	meta *logtail.SnapshotMeta,
	c *catalog2.Catalog,
) (
	FilterFn,
	error,
) {
	filterTable := meta.GetSnapshotTableIDs()
	tables := meta.GetTableIDs()
	tableIsDrop := func(db, tid uint64) bool {
		dbEntry, getErr := c.GetDatabaseByID(db)
		if getErr != nil {
			return true
		}
		dbEntry.GetDeleteAtLocked()
		tableEntry, getErr := dbEntry.GetTableEntryByID(tid)
		if getErr != nil {
			return true
		}
		dropTS := tableEntry.GetDeleteAtLocked()
		if dropTS.IsEmpty() {
			return true
		}
		return false
	}
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
					object := NewObjectEntry()
					object.stats = &stats
					object.createTS = createTS
					object.dropTS = dropTS
					object.db = dbs[i]
					object.table = tableIDs[i]
					(transObjects)[name] = object
				}
				table := tables[tableIDs[i]]
				if table != nil && !table.IsDrop() {
					continue
				}

				if table == nil {
					if !tableIsDrop(dbs[i], tableIDs[i]) {
						continue
					}
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
