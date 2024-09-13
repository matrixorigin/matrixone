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

	"github.com/matrixorigin/matrixone/pkg/logutil"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
)

// tryAdjustThreeTablesCreatedTime analyzes the mo_tables batch and tries to adjust the created time of the three tables.
func (eng *Engine) tryAdjustThreeTablesCreatedTimeWithBatch(b *batch.Batch) {
	if eng.timeFixed {
		return
	}

	aidIdx := catalog.MO_TABLES_ACCOUNT_ID_IDX + cache.MO_OFF
	tnameIdx := catalog.MO_TABLES_REL_NAME_IDX + cache.MO_OFF
	createdTsIdx := catalog.MO_TABLES_CREATED_TIME_IDX + cache.MO_OFF
	for i := 0; i < b.RowCount(); i++ {
		aid := vector.GetFixedAtWithTypeCheck[uint32](b.Vecs[aidIdx], i)
		tname := b.Vecs[tnameIdx].GetStringAt(i)
		if aid == 0 && tname == "mo_user" {
			ts := vector.GetFixedAtWithTypeCheck[types.Timestamp](b.Vecs[createdTsIdx], i)
			vector.SetFixedAtWithTypeCheck(eng.moDatabaseCreatedTime, 0, ts)
			vector.SetFixedAtWithTypeCheck(eng.moTablesCreatedTime, 0, ts)
			vector.SetFixedAtWithTypeCheck(eng.moColumnsCreatedTime, 0, ts)
			vector.SetFixedAtWithTypeCheck(eng.moCatalogCreatedTime, 0, ts)
			eng.timeFixed = true
			return
		}
	}
}

// init is used to insert some data that will not be synchronized by logtail.
func (eng *Engine) init(ctx context.Context) error {
	eng.Lock()
	defer eng.Unlock()
	m := eng.mp

	eng.catalog = cache.NewCatalog()
	eng.partitions = make(map[[2]uint64]*logtailreplay.Partition)

	var packer *types.Packer
	put := eng.packerPool.Get(&packer)
	defer put.Put()

	{
		eng.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}] = logtailreplay.NewPartition(eng.service, 1)
		eng.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}] = logtailreplay.NewPartition(eng.service, 2)
		eng.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}] = logtailreplay.NewPartition(eng.service, 3)
	}

	{ // mo_catalog
		part := eng.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}]
		bat, err := catalog.GenCreateDatabaseTuple("", 0, 0, 0, catalog.MO_CATALOG, catalog.MO_CATALOG_ID, "", m, packer)
		if err != nil {
			return err
		}
		eng.moCatalogCreatedTime = bat.Vecs[catalog.MO_DATABASE_CREATED_TIME_IDX]
		ibat, err := fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_DATABASE_CPKEY_IDX, packer, eng.mp)
		done()
		eng.catalog.InsertDatabase(bat)
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
		bat, err := catalog.GenCreateTableTuple(tbl, m, packer)
		if err != nil {
			return err
		}
		eng.moDatabaseCreatedTime = bat.Vecs[catalog.MO_TABLES_CREATED_TIME_IDX]
		ibat, err := fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}

		part := eng.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}]
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_TABLES_CPKEY_IDX, packer, eng.mp)
		done()
		eng.catalog.InsertTable(bat) // cache
		// do not clean the bat because the the partition state will be holding the bat

		// insert into mo_columns partition
		cols, err := catalog.GenColumnsFromDefs(0, catalog.MO_DATABASE, catalog.MO_CATALOG,
			catalog.MO_DATABASE_ID, catalog.MO_CATALOG_ID, catalog.GetDefines(eng.service).MoDatabaseTableDefs)
		if err != nil {
			return err
		}

		part = eng.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}]
		bat, err = catalog.GenCreateColumnTuples(cols, m, packer)
		if err != nil {
			return err
		}
		ibat, err = fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_COLUMNS_ATT_CPKEY_IDX, packer, eng.mp)
		done()
		eng.catalog.InsertColumns(bat)
	}

	{ // init mo_tables table
		part := eng.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}]
		cols, err := catalog.GenColumnsFromDefs(0, catalog.MO_TABLES, catalog.MO_CATALOG,
			catalog.MO_TABLES_ID, catalog.MO_CATALOG_ID, catalog.GetDefines(eng.service).MoTablesTableDefs)
		if err != nil {
			return err
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
		bat, err := catalog.GenCreateTableTuple(tbl, m, packer)
		if err != nil {
			return err
		}
		eng.moTablesCreatedTime = bat.Vecs[catalog.MO_TABLES_CREATED_TIME_IDX]
		ibat, err := fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_TABLES_CPKEY_IDX, packer, eng.mp)
		done()
		eng.catalog.InsertTable(bat)

		part = eng.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}]
		bat, err = catalog.GenCreateColumnTuples(cols, m, packer)
		if err != nil {
			return err
		}
		ibat, err = fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_COLUMNS_ATT_CPKEY_IDX, packer, eng.mp)
		done()
		eng.catalog.InsertColumns(bat)
	}

	{ // mo_columns
		part := eng.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}]
		cols, err := catalog.GenColumnsFromDefs(0, catalog.MO_COLUMNS, catalog.MO_CATALOG, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG_ID, catalog.GetDefines(eng.service).MoColumnsTableDefs)
		if err != nil {
			return err
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
		bat, err := catalog.GenCreateTableTuple(tbl, m, packer)
		if err != nil {
			return err
		}
		eng.moColumnsCreatedTime = bat.Vecs[catalog.MO_TABLES_CREATED_TIME_IDX]
		ibat, err := fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_TABLES_CPKEY_IDX, packer, eng.mp)
		done()
		eng.catalog.InsertTable(bat)

		part = eng.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}]
		bat, err = catalog.GenCreateColumnTuples(cols, m, packer)
		if err != nil {
			return err
		}
		ibat, err = fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_COLUMNS_ATT_CPKEY_IDX, packer, eng.mp)
		done()
		eng.catalog.InsertColumns(bat)
	}

	// clear all tables in global stats.
	eng.globalStats.clearTables()

	return nil
}

func (eng *Engine) GetLatestCatalogCache() *cache.CatalogCache {
	return eng.catalog
}

func (eng *Engine) getOrCreateSnapPart(
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
	eng.mu.Lock()
	tblSnaps, ok := eng.mu.snapParts[[2]uint64{tbl.db.databaseId, tbl.tableId}]
	if !ok {
		eng.mu.snapParts[[2]uint64{tbl.db.databaseId, tbl.tableId}] = &struct {
			sync.Mutex
			snaps []*logtailreplay.Partition
		}{}
		tblSnaps = eng.mu.snapParts[[2]uint64{tbl.db.databaseId, tbl.tableId}]
	}
	eng.mu.Unlock()

	tblSnaps.Lock()
	defer tblSnaps.Unlock()
	for _, snap := range tblSnaps.snaps {
		if snap.CanServe(ts) {
			return snap.Snapshot(), nil
		}
	}

	//new snapshot partition and apply checkpoints into it.
	snap := logtailreplay.NewPartition(eng.service, tbl.tableId)
	//TODO::if tableId is mo_tables, or mo_colunms, or mo_database,
	//      we should init the partition,ref to engine.init
	ckps, err := checkpoint.ListSnapshotCheckpoint(ctx, eng.service, eng.fs, ts, tbl.tableId, nil)
	if err != nil {
		return nil, err
	}
	err = snap.ConsumeSnapCkps(ctx, ckps, func(
		checkpoint *checkpoint.CheckpointEntry,
		state *logtailreplay.PartitionState) error {
		locs := make([]string, 0)
		locs = append(locs, checkpoint.GetLocation().String())
		locs = append(locs, strconv.Itoa(int(checkpoint.GetVersion())))
		locations := strings.Join(locs, ";")
		entries, closeCBs, err := logtail.LoadCheckpointEntries(
			ctx,
			eng.service,
			locations,
			tbl.tableId,
			tbl.tableName,
			tbl.db.databaseId,
			tbl.db.databaseName,
			eng.mp,
			eng.fs)
		if err != nil {
			return err
		}
		defer func() {
			for _, cb := range closeCBs {
				if cb != nil {
					cb()
				}
			}
		}()
		for _, entry := range entries {
			if err = consumeEntry(
				ctx,
				tbl.primarySeqnum,
				eng,
				nil,
				state,
				entry); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		logutil.Infof("Snapshot consumeSnapCkps failed, err:%v", err)
		return nil, err
	}
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
		return nil, moerr.NewInternalErrorNoCtxf(
			"No valid checkpoints for snapshot read,maybe snapshot is too old, "+
				"snapshot op:%s, start:%s, end:%s",
			tbl.db.op.Txn().DebugString(),
			start.ToTimestamp().DebugString(),
			end.ToTimestamp().DebugString())
	}
	panic("impossible path")
}

func (eng *Engine) GetOrCreateLatestPart(
	databaseId,
	tableId uint64) *logtailreplay.Partition {
	eng.Lock()
	defer eng.Unlock()
	partition, ok := eng.partitions[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		partition = logtailreplay.NewPartition(eng.service, tableId)
		eng.partitions[[2]uint64{databaseId, tableId}] = partition
	}
	return partition
}

func (eng *Engine) LazyLoadLatestCkp(
	ctx context.Context,
	tblHandler engine.Relation) (*logtailreplay.Partition, error) {

	var (
		ok  bool
		tbl *txnTable
	)

	if tbl, ok = tblHandler.(*txnTable); !ok {
		delegate := tblHandler.(*txnTableDelegate)
		tbl = delegate.origin
	}

	part := eng.GetOrCreateLatestPart(tbl.db.databaseId, tbl.tableId)
	cache := eng.GetLatestCatalogCache()

	if err := part.ConsumeCheckpoints(
		ctx,
		func(checkpoint string, state *logtailreplay.PartitionState) error {
			entries, closeCBs, err := logtail.LoadCheckpointEntries(
				ctx,
				eng.service,
				checkpoint,
				tbl.tableId,
				tbl.tableName,
				tbl.db.databaseId,
				tbl.db.databaseName,
				tbl.getTxn().engine.mp,
				tbl.getTxn().engine.fs)
			if err != nil {
				return err
			}
			defer func() {
				for _, cb := range closeCBs {
					if cb != nil {
						cb()
					}
				}
			}()
			for _, entry := range entries {
				if err = consumeEntry(ctx, tbl.primarySeqnum, eng, cache, state, entry); err != nil {
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

func (eng *Engine) UpdateOfPush(
	ctx context.Context,
	databaseId,
	tableId uint64, ts timestamp.Timestamp) error {
	return eng.pClient.TryToSubscribeTable(ctx, databaseId, tableId)
}
