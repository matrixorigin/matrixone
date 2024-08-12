// Copyright 2022 Matrix Origin
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

package disttae

import (
	"context"
	"strconv"
	"strings"
	"sync"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

// tryAdjustThreeTablesCreatedTime analyzes the mo_tables batch and tries to adjust the created time of the three tables.
func (e *Engine) tryAdjustThreeTablesCreatedTimeWithBatch(b *batch.Batch) {
	if e.timeFixed {
		return
	}

	aidIdx := catalog.MO_TABLES_ACCOUNT_ID_IDX + cache.MO_OFF
	tnameIdx := catalog.MO_TABLES_REL_NAME_IDX + cache.MO_OFF
	createdTsIdx := catalog.MO_TABLES_CREATED_TIME_IDX + cache.MO_OFF
	for i := 0; i < b.RowCount(); i++ {
		aid := vector.GetFixedAt[uint32](b.Vecs[aidIdx], i)
		tname := b.Vecs[tnameIdx].GetStringAt(i)
		if aid == 0 && tname == "mo_user" {
			ts := vector.GetFixedAt[types.Timestamp](b.Vecs[createdTsIdx], i)
			vector.SetFixedAt(e.moDatabaseCreatedTime, 0, ts)
			vector.SetFixedAt(e.moTablesCreatedTime, 0, ts)
			vector.SetFixedAt(e.moColumnsCreatedTime, 0, ts)
			vector.SetFixedAt(e.moCatalogCreatedTime, 0, ts)
			e.timeFixed = true
			return
		}
	}
}

// init is used to insert some data that will not be synchronized by logtail.
func (e *Engine) init(ctx context.Context) (err error) {
	e.Lock()
	defer e.Unlock()

	e.catalog = cache.NewCatalog()
	e.partitions = make(map[[2]uint64]*logtailreplay.Partition)

	e.moCatalogCreatedTime, e.moDatabaseCreatedTime, e.moTablesCreatedTime, e.moColumnsCreatedTime, err = initEngine(ctx, e.service, e.catalog, e.partitions, e.mp, e.packerPool)
	return err
}

func initEngine(
	ctx context.Context,
	service string,
	metaCache *cache.CatalogCache,
	partitions map[[2]uint64]*logtailreplay.Partition,
	mp *mpool.MPool,
	packerPool *fileservice.Pool[*types.Packer],
) (*vector.Vector, *vector.Vector, *vector.Vector, *vector.Vector, error) {
	var packer *types.Packer
	var moCatalogCreatedTime *vector.Vector
	var moDatabaseCreatedTime *vector.Vector
	var moTablesCreatedTime *vector.Vector
	var moColumnsCreatedTime *vector.Vector
	put := packerPool.Get(&packer)
	defer put.Put()

	{
		partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}] = logtailreplay.NewPartition(service, 1)
		partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}] = logtailreplay.NewPartition(service, 2)
		partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}] = logtailreplay.NewPartition(service, 3)
	}

	{ // mo_catalog
		part := partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}]
		bat, err := catalog.GenCreateDatabaseTuple("", 0, 0, 0, catalog.MO_CATALOG, catalog.MO_CATALOG_ID, "", mp, packer)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		moCatalogCreatedTime = bat.Vecs[catalog.MO_DATABASE_CREATED_TIME_IDX]
		ibat, err := fillRandomRowidAndZeroTs(bat, mp)
		if err != nil {
			bat.Clean(mp)
			return nil, nil, nil, nil, err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_DATABASE_CPKEY_IDX, packer, mp)
		done()
		metaCache.InsertDatabase(bat)
	}

	{ // init mo_database table

		// insert into mo_tables partition
		tbl := catalog.Table{
			AccountId:    0,
			UserId:       0,
			RoleId:       0,
			DatabaseId:   catalog.MO_CATALOG_ID,
			DatabaseName: catalog.MO_CATALOG,
			TableId:      catalog.MO_DATABASE_ID,
			TableName:    catalog.MO_DATABASE,
			Kind:         catalog.SystemOrdinaryRel,
		}
		bat, err := catalog.GenCreateTableTuple(tbl, mp, packer)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		moDatabaseCreatedTime = bat.Vecs[catalog.MO_TABLES_CREATED_TIME_IDX]
		ibat, err := fillRandomRowidAndZeroTs(bat, mp)
		if err != nil {
			bat.Clean(mp)
			return nil, nil, nil, nil, err
		}

		part := partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}]
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_TABLES_CPKEY_IDX, packer, mp)
		done()
		metaCache.InsertTable(bat) // cache
		// do not clean the bat because the the partition state will be holding the bat

		// insert into mo_columns partition
		cols, err := catalog.GenColumnsFromDefs(0, catalog.MO_DATABASE, catalog.MO_CATALOG,
			catalog.MO_DATABASE_ID, catalog.MO_CATALOG_ID, catalog.GetDefines(service).MoDatabaseTableDefs)
		if err != nil {
			return nil, nil, nil, nil, err
		}

		part = partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}]
		bat, err = catalog.GenCreateColumnTuples(cols, mp, packer)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		ibat, err = fillRandomRowidAndZeroTs(bat, mp)
		if err != nil {
			bat.Clean(mp)
			return nil, nil, nil, nil, err
		}
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_COLUMNS_ATT_CPKEY_IDX, packer, mp)
		done()
		metaCache.InsertColumns(bat)
	}

	{ // init mo_tables table
		part := partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}]
		cols, err := catalog.GenColumnsFromDefs(0, catalog.MO_TABLES, catalog.MO_CATALOG,
			catalog.MO_TABLES_ID, catalog.MO_CATALOG_ID, catalog.GetDefines(service).MoTablesTableDefs)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		tbl := catalog.Table{
			AccountId:    0,
			UserId:       0,
			RoleId:       0,
			DatabaseId:   catalog.MO_CATALOG_ID,
			DatabaseName: catalog.MO_CATALOG,
			TableId:      catalog.MO_TABLES_ID,
			TableName:    catalog.MO_TABLES,
			Kind:         catalog.SystemOrdinaryRel,
		}
		bat, err := catalog.GenCreateTableTuple(tbl, mp, packer)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		moTablesCreatedTime = bat.Vecs[catalog.MO_TABLES_CREATED_TIME_IDX]
		ibat, err := fillRandomRowidAndZeroTs(bat, mp)
		if err != nil {
			bat.Clean(mp)
			return nil, nil, nil, nil, err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_TABLES_CPKEY_IDX, packer, mp)
		done()
		metaCache.InsertTable(bat)

		part = partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}]
		bat, err = catalog.GenCreateColumnTuples(cols, mp, packer)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		ibat, err = fillRandomRowidAndZeroTs(bat, mp)
		if err != nil {
			bat.Clean(mp)
			return nil, nil, nil, nil, err
		}
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_COLUMNS_ATT_CPKEY_IDX, packer, mp)
		done()
		metaCache.InsertColumns(bat)
	}

	{ // mo_columns
		part := partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}]
		cols, err := catalog.GenColumnsFromDefs(0, catalog.MO_COLUMNS, catalog.MO_CATALOG, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG_ID, catalog.GetDefines(service).MoColumnsTableDefs)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		tbl := catalog.Table{
			AccountId:    0,
			UserId:       0,
			RoleId:       0,
			DatabaseId:   catalog.MO_CATALOG_ID,
			DatabaseName: catalog.MO_CATALOG,
			TableId:      catalog.MO_COLUMNS_ID,
			TableName:    catalog.MO_COLUMNS,
			Kind:         catalog.SystemOrdinaryRel,
		}
		bat, err := catalog.GenCreateTableTuple(tbl, mp, packer)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		moColumnsCreatedTime = bat.Vecs[catalog.MO_TABLES_CREATED_TIME_IDX]
		ibat, err := fillRandomRowidAndZeroTs(bat, mp)
		if err != nil {
			bat.Clean(mp)
			return nil, nil, nil, nil, err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_TABLES_CPKEY_IDX, packer, mp)
		done()
		metaCache.InsertTable(bat)

		part = partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}]
		bat, err = catalog.GenCreateColumnTuples(cols, mp, packer)
		if err != nil {
			return nil, nil, nil, nil, err
		}
		ibat, err = fillRandomRowidAndZeroTs(bat, mp)
		if err != nil {
			bat.Clean(mp)
			return nil, nil, nil, nil, err
		}
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_COLUMNS_ATT_CPKEY_IDX, packer, mp)
		done()
		metaCache.InsertColumns(bat)
	}

	return moCatalogCreatedTime, moDatabaseCreatedTime, moTablesCreatedTime, moColumnsCreatedTime, nil
}

func (e *Engine) GetLatestCatalogCache() *cache.CatalogCache {
	return e.catalog
}

func (e *Engine) getOrCreateSnapPart(
	ctx context.Context,
	tbl *txnTable,
	ts types.TS) (*logtailreplay.PartitionState, error) {

	//check whether the latest partition is available for reuse.
	// if the snapshot-read's ts is too old , subscribing table maybe timeout.
	//if err := tbl.updateLogtail(ctx); err == nil {
	//	if p := e.getOrCreateLatestPart(tbl.db.databaseId, tbl.tableId); p.CanServe(ts) {
	//		return p, nil
	//	}
	//}

	//check whether the snapshot partitions are available for reuse.
	e.mu.Lock()
	tblSnaps, ok := e.mu.snapParts[[2]uint64{tbl.db.databaseId, tbl.tableId}]
	if !ok {
		e.mu.snapParts[[2]uint64{tbl.db.databaseId, tbl.tableId}] = &struct {
			sync.Mutex
			snaps []*logtailreplay.Partition
		}{}
		tblSnaps = e.mu.snapParts[[2]uint64{tbl.db.databaseId, tbl.tableId}]
	}
	e.mu.Unlock()

	tblSnaps.Lock()
	defer tblSnaps.Unlock()
	for _, snap := range tblSnaps.snaps {
		if snap.CanServe(ts) {
			return snap.Snapshot(), nil
		}
	}

	//new snapshot partition and apply checkpoints into it.
	snap := logtailreplay.NewPartition(e.service, tbl.tableId)
	//TODO::if tableId is mo_tables, or mo_colunms, or mo_database,
	//      we should init the partition,ref to engine.init
	ckps, err := checkpoint.ListSnapshotCheckpoint(ctx, e.service, e.fs, ts, tbl.tableId, nil)
	if err != nil {
		return nil, err
	}
	snap.ConsumeSnapCkps(ctx, ckps, func(
		checkpoint *checkpoint.CheckpointEntry,
		state *logtailreplay.PartitionState) error {
		locs := make([]string, 0)
		locs = append(locs, checkpoint.GetLocation().String())
		locs = append(locs, strconv.Itoa(int(checkpoint.GetVersion())))
		locations := strings.Join(locs, ";")
		entries, closeCBs, err := logtail.LoadCheckpointEntries(
			ctx,
			e.service,
			locations,
			tbl.tableId,
			tbl.tableName,
			tbl.db.databaseId,
			tbl.db.databaseName,
			e.mp,
			e.fs)
		if err != nil {
			return err
		}
		defer func() {
			for _, cb := range closeCBs {
				cb()
			}
		}()
		for _, entry := range entries {
			if err = consumeEntry(
				ctx,
				tbl.primarySeqnum,
				e,
				nil,
				state,
				entry); err != nil {
				return err
			}
		}
		return nil
	})
	if snap.CanServe(ts) {
		tblSnaps.snaps = append(tblSnaps.snaps, snap)
		return snap.Snapshot(), nil
	}

	start, end := snap.GetDuration()
	//if has no checkpoints or ts > snap.end, use latest partition.
	if snap.IsEmpty() || ts.Greater(&end) {
		ps, err := tbl.tryToSubscribe(ctx)
		if err != nil {
			return nil, err
		}
		if ps == nil {
			ps = tbl.getTxn().engine.GetOrCreateLatestPart(tbl.db.databaseId, tbl.tableId).Snapshot()
		}
		return ps, nil
	}
	if ts.Less(&start) {
		return nil, moerr.NewInternalErrorNoCtx(
			"No valid checkpoints for snapshot read,maybe snapshot is too old, "+
				"snapshot op:%s, start:%s, end:%s",
			tbl.db.op.Txn().DebugString(),
			start.ToTimestamp().DebugString(),
			end.ToTimestamp().DebugString())
	}
	panic("impossible path")
}

func (e *Engine) GetOrCreateLatestPart(
	databaseId,
	tableId uint64) *logtailreplay.Partition {
	e.Lock()
	defer e.Unlock()
	partition, ok := e.partitions[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		partition = logtailreplay.NewPartition(e.service, tableId)
		e.partitions[[2]uint64{databaseId, tableId}] = partition
	}
	return partition
}

func LazyLoadLatestCkp(
	ctx context.Context,
	engine TempEngine,
	tbl engine.Relation) (*logtailreplay.Partition, error) {

	part := engine.GetOrCreateLatestPart(tbl.GetDBID(ctx), tbl.GetTableID(ctx))
	cache := engine.GetLatestCatalogCache()

	if err := part.ConsumeCheckpoints(
		ctx,
		func(checkpoint string, state *logtailreplay.PartitionState) error {
			entries, closeCBs, err := logtail.LoadCheckpointEntries(
				ctx,
				engine.GetService(),
				checkpoint,
				tbl.GetTableID(ctx),
				tbl.GetTableName(),
				tbl.GetDBID(ctx),
				tbl.GetDBName(),
				engine.GetMPool(),
				engine.GetFS())
			if err != nil {
				return err
			}
			defer func() {
				for _, cb := range closeCBs {
					cb()
				}
			}()
			for _, entry := range entries {
				if err = consumeEntry(ctx, tbl.GetPrimarySeqNum(), engine, cache, state, entry); err != nil {
					return err
				}
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	return part, nil
}

func (e *Engine) UpdateOfPush(
	ctx context.Context,
	databaseId,
	tableId uint64, ts timestamp.Timestamp) error {
	return e.pClient.TryToSubscribeTable(ctx, databaseId, tableId)
}
