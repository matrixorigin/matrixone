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
	"github.com/matrixorigin/matrixone/pkg/common/bloomfilter"
	"github.com/matrixorigin/matrixone/pkg/common/malloc"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"go.uber.org/zap"

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
	iscpTables       map[uint64]types.TS
	pitr             *logtail.PitrInfo
	ts               *types.TS
	globalCkpLoc     objectio.Location
	globalCkpVer     uint32
	checkpointCli    checkpoint.Runner // Added to access catalog

	result struct {
		vecToGC    *vector.Vector
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
	iscpTables map[uint64]types.TS,
	snapshotMeta *logtail.SnapshotMeta,
	checkpointCli checkpoint.Runner,
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
		iscpTables:       iscpTables,
		checkpointCli:    checkpointCli,
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
	e.result.vecToGC = nil
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
		false,
	)
	defer buffer.Close(e.mp)
	transObjects := make(map[string]map[uint64]*ObjectEntry, 100)
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
		e.iscpTables,
		e.checkpointCli,
	)
	if err != nil {
		return err
	}

	e.result.vecToGC = vector.NewVec(types.New(types.T_varchar, types.MaxVarcharLen, 0))
	finalSinker, err := MakeFinalCanGCSinker(e.result.vecToGC, e.mp)
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

func (e *CheckpointBasedGCJob) Result() (*vector.Vector, []objectio.ObjectStats) {
	return e.result.vecToGC, e.result.filesNotGC
}

func dataProcess(b *bloomfilter.BloomFilter, vec *vector.Vector, pool *mpool.MPool) error {
	gcVec := vector.NewVec(types.New(types.T_varchar, types.MaxVarcharLen, 0))
	for i := 0; i < vec.Length(); i++ {
		stats := objectio.ObjectStats(vec.GetBytesAt(i))
		if err := vector.AppendBytes(
			gcVec, []byte(stats.ObjectName().UnsafeString()), false, pool,
		); err != nil {
			return err
		}
	}
	b.Add(gcVec)
	gcVec.Free(pool)
	return nil
}

func MakeBloomfilterCoarseFilter(
	ctx context.Context,
	rowCount int,
	probability float64,
	buffer containers.IBatchBuffer,
	location objectio.Location,
	ckpVersion uint32,
	ts *types.TS,
	transObjects *map[string]map[uint64]*ObjectEntry,
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
		dataProcess,
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
		nameVec := vector.NewVec(types.New(types.T_varchar, types.MaxVarcharLen, 0))
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			stats := objectio.ObjectStats(bat.Vecs[0].GetBytesAt(i))
			if err = vector.AppendBytes(
				nameVec, []byte(stats.ObjectName().UnsafeString()), false, mp,
			); err != nil {
				return err
			}
		}
		defer nameVec.Free(mp)
		bf.Test(
			nameVec,
			func(exists bool, i int) {
				if exists {
					return
				}

				createTS := createTSs[i]
				dropTS := dropTSs[i]
				if !createTS.LT(ts) || !dropTS.LT(ts) {
					return
				}
				bm.Add(uint64(i))
				buf := bat.Vecs[0].GetRawBytesAt(i)
				stats := (objectio.ObjectStats)(buf)
				name := stats.ObjectName().UnsafeString()
				tid := tableIDs[i]
				if (*transObjects)[name] == nil ||
					(*transObjects)[name][tableIDs[i]] == nil {
					if (*transObjects)[name] == nil {
						(*transObjects)[name] = make(map[uint64]*ObjectEntry)
					}
					object := &ObjectEntry{
						stats:    &stats,
						createTS: createTS,
						dropTS:   dropTS,
						db:       dbs[i],
						table:    tid,
					}
					(*transObjects)[name][tid] = object
					return
				}
				if (*transObjects)[name] != nil && (*transObjects)[name][tid] != nil {
					(*transObjects)[name][tid].dropTS = dropTS
					return
				}
			},
		)
		return nil

	}, nil
}

// buildTableExistenceMap creates a combined map of table IDs from both snapshotMeta and catalog
// This avoids the need for locking on every table existence check
func buildTableExistenceMap(snapshotMeta *logtail.SnapshotMeta, checkpointCli checkpoint.Runner) (map[uint64]bool, error) {
	// First, copy all table IDs from snapshotMeta
	tableExistenceMap := snapshotMeta.GetAllTableIDs()
	catalog := checkpointCli.GetCatalog()
	count := len(tableExistenceMap)
	defer func() {
		logutil.Info("GC-TRACE-TABLE-LIST",
			zap.Int("gc-table-count", count),
			zap.Int("all-table-count", len(tableExistenceMap)))
	}()
	if catalog == nil {
		return tableExistenceMap, nil
	}
	it := catalog.MakeDBIt(true)
	for ; it.Valid(); it.Next() {
		db := it.Get().GetPayload()

		itTable := db.MakeTableIt(true)
		for itTable.Valid() {
			table := itTable.Get().GetPayload()
			drop := table.GetDeleteAtLocked()
			if drop.IsEmpty() {
				tableID := table.GetID()
				tableExistenceMap[tableID] = true
			}
			itTable.Next()
		}
	}

	return tableExistenceMap, nil
}

func MakeSnapshotAndPitrFineFilter(
	ts *types.TS,
	accountSnapshots map[uint32][]types.TS,
	pitrs *logtail.PitrInfo,
	snapshotMeta *logtail.SnapshotMeta,
	transObjects map[string]map[uint64]*ObjectEntry,
	iscpTables map[uint64]types.TS,
	checkpointCli checkpoint.Runner,
) (
	filter FilterFn,
	err error,
) {
	// Build combined table existence map from both snapshotMeta and catalog
	tableExistenceMap, err := buildTableExistenceMap(snapshotMeta, checkpointCli)
	if err != nil {
		return nil, err
	}

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
			deleteTS := deleteTSs[i]

			snapshots := tableSnapshots[tableID]
			pitr := tablePitrs[tableID]

			if transObjects[name] != nil {
				tables := transObjects[name]
				if entry := tables[tableID]; entry != nil {
					// Check if the table still exists using the combined map
					ok := tableExistenceMap[tableID]
					// The table has not been dropped, and the dropTS is empty, so it cannot be deleted.
					if entry.dropTS.IsEmpty() && ok {
						continue
					}

					if !logtail.ObjectIsSnapshotRefers(
						entry.stats, pitr, &entry.createTS, &entry.dropTS, snapshots,
					) {
						if iscpTables == nil {
							bm.Add(uint64(i))
							continue
						}
						if iscpTS, ok := iscpTables[entry.table]; ok {
							if entry.stats.GetCNCreated() || entry.stats.GetAppendable() {
								if (!entry.dropTS.IsEmpty() && entry.dropTS.LT(&iscpTS)) ||
									entry.createTS.GT(&iscpTS) {
									continue
								}
							}
						}
						bm.Add(uint64(i))
					}
					continue
				}
			}
			if !createTS.LT(ts) || !deleteTS.LT(ts) {
				continue
			}
			if deleteTS.IsEmpty() {
				logutil.Warn("GC-PANIC-TS-EMPTY",
					zap.String("name", name),
					zap.String("createTS", createTS.ToString()))
				continue
			}
			if !logtail.ObjectIsSnapshotRefers(
				&stats, pitr, &createTS, &deleteTS, snapshots,
			) {
				if iscpTables == nil {
					bm.Add(uint64(i))
					continue
				}
				if iscpTS, ok := iscpTables[tableID]; ok {
					if stats.GetCNCreated() || stats.GetAppendable() {
						if (!deleteTS.IsEmpty() && deleteTS.LT(&iscpTS)) ||
							createTS.GT(&iscpTS) {
							continue
						}
					}
				}
				bm.Add(uint64(i))
			}
		}
		return nil
	}, nil
}

func MakeFinalCanGCSinker(
	vec *vector.Vector,
	mp *mpool.MPool,
) (
	SinkerFn,
	error,
) {
	return func(
		ctx context.Context, bat *batch.Batch,
	) error {
		var dropTSs []types.TS
		var tableIDs []uint64
		if bat.Vecs[0].Length() > 0 {
			dropTSs = vector.MustFixedColNoTypeCheck[types.TS](bat.Vecs[2])
			tableIDs = vector.MustFixedColNoTypeCheck[uint64](bat.Vecs[4])
		}
		for i := 0; i < bat.Vecs[0].Length(); i++ {
			stats := objectio.ObjectStats(bat.Vecs[0].GetBytesAt(i))
			dropTS := dropTSs[i]
			tableID := tableIDs[i]
			if !dropTS.IsEmpty() {
				if err := vector.AppendBytes(
					vec, []byte(stats.ObjectName().UnsafeString()), false, mp,
				); err != nil {
					return err
				}
				continue
			}
			if !logtail.IsMoTable(tableID) {
				if err := vector.AppendBytes(
					vec, []byte(stats.ObjectName().UnsafeString()), false, mp,
				); err != nil {
					return err
				}
			}
		}
		return nil
	}, nil
}
