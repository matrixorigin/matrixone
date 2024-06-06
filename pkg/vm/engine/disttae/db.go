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

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
)

// init is used to insert some data that will not be synchronized by logtail.
func (e *Engine) init(ctx context.Context) error {
	e.Lock()
	defer e.Unlock()
	m := e.mp

	e.catalog = cache.NewCatalog()
	e.partitions = make(map[[2]uint64]*logtailreplay.Partition)

	var packer *types.Packer
	put := e.packerPool.Get(&packer)
	defer put.Put()

	{
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}] = logtailreplay.NewPartition()
	}

	{
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}] = logtailreplay.NewPartition()
	}

	{
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}] = logtailreplay.NewPartition()
	}

	{ // mo_catalog
		part := e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}]
		bat, err := genCreateDatabaseTuple("", 0, 0, 0, catalog.MO_CATALOG, catalog.MO_CATALOG_ID, "", m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF, packer)
		done()
		e.catalog.InsertDatabase(bat)
		bat.Clean(m)
	}

	{ // mo_database
		part := e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}]
		cols, err := genColumns(0, catalog.MO_DATABASE, catalog.MO_CATALOG, catalog.MO_DATABASE_ID,
			catalog.MO_CATALOG_ID, catalog.MoDatabaseTableDefs)
		if err != nil {
			return err
		}
		tbl := new(txnTable)
		tbl.relKind = catalog.SystemOrdinaryRel
		bat, err := genCreateTableTuple(tbl, "", 0, 0, 0,
			catalog.MO_DATABASE, catalog.MO_DATABASE_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, types.Rowid{}, false, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_TABLES_REL_ID_IDX, packer)
		done()
		e.catalog.InsertTable(bat)
		bat.Clean(m)

		part = e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}]
		bat = batch.NewWithSize(len(catalog.MoColumnsSchema))
		bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
		bat.SetRowCount(len(cols))
		for _, col := range cols {
			bat0, err := genCreateColumnTuple(col, types.Rowid{}, false, m)
			if err != nil {
				return err
			}
			if bat.Vecs[0] == nil {
				for i, vec := range bat0.Vecs {
					bat.Vecs[i] = vector.NewVec(*vec.GetType())
				}
			}
			for i, vec := range bat0.Vecs {
				if err := bat.Vecs[i].UnionOne(vec, 0, m); err != nil {
					bat.Clean(m)
					bat0.Clean(m)
					return err
				}
			}
			bat0.Clean(m)
		}
		ibat, err = genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX, packer)
		done()
		e.catalog.InsertColumns(bat)
		bat.Clean(m)
	}

	{ // mo_tables
		part := e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}]
		cols, err := genColumns(0, catalog.MO_TABLES, catalog.MO_CATALOG, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG_ID, catalog.MoTablesTableDefs)
		if err != nil {
			return err
		}
		tbl := new(txnTable)
		tbl.relKind = catalog.SystemOrdinaryRel
		bat, err := genCreateTableTuple(tbl, "", 0, 0, 0, catalog.MO_TABLES, catalog.MO_TABLES_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, types.Rowid{}, false, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_TABLES_REL_ID_IDX, packer)
		done()
		e.catalog.InsertTable(bat)
		bat.Clean(m)

		part = e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}]
		bat = batch.NewWithSize(len(catalog.MoColumnsSchema))
		bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
		bat.SetRowCount(len(cols))
		for _, col := range cols {
			bat0, err := genCreateColumnTuple(col, types.Rowid{}, false, m)
			if err != nil {
				return err
			}
			if bat.Vecs[0] == nil {
				for i, vec := range bat0.Vecs {
					bat.Vecs[i] = vector.NewVec(*vec.GetType())
				}
			}
			for i, vec := range bat0.Vecs {
				if err := bat.Vecs[i].UnionOne(vec, 0, m); err != nil {
					bat.Clean(m)
					bat0.Clean(m)
					return err
				}
			}
			bat0.Clean(m)
		}
		ibat, err = genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX, packer)
		done()
		e.catalog.InsertColumns(bat)
		bat.Clean(m)
	}

	{ // mo_columns
		part := e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}]
		cols, err := genColumns(0, catalog.MO_COLUMNS, catalog.MO_CATALOG, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG_ID, catalog.MoColumnsTableDefs)
		if err != nil {
			return err
		}
		tbl := new(txnTable)
		tbl.relKind = catalog.SystemOrdinaryRel
		bat, err := genCreateTableTuple(tbl, "", 0, 0, 0, catalog.MO_COLUMNS, catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG_ID, catalog.MO_CATALOG, types.Rowid{}, false, m)
		if err != nil {
			return err
		}
		ibat, err := genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_TABLES_REL_ID_IDX, packer)
		done()
		e.catalog.InsertTable(bat)
		bat.Clean(m)

		part = e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}]
		bat = batch.NewWithSize(len(catalog.MoColumnsSchema))
		bat.Attrs = append(bat.Attrs, catalog.MoColumnsSchema...)
		bat.SetRowCount(len(cols))
		for _, col := range cols {
			bat0, err := genCreateColumnTuple(col, types.Rowid{}, false, m)
			if err != nil {
				return err
			}
			if bat.Vecs[0] == nil {
				for i, vec := range bat0.Vecs {
					bat.Vecs[i] = vector.NewVec(*vec.GetType())
				}
			}
			for i, vec := range bat0.Vecs {
				if err := bat.Vecs[i].UnionOne(vec, 0, m); err != nil {
					bat.Clean(m)
					bat0.Clean(m)
					return err
				}
			}
			bat0.Clean(m)
		}
		ibat, err = genInsertBatch(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done = part.MutateState()
		state.HandleRowsInsert(ctx, ibat, MO_PRIMARY_OFF+catalog.MO_COLUMNS_ATT_UNIQ_NAME_IDX, packer)
		done()
		e.catalog.InsertColumns(bat)
		bat.Clean(m)
	}

	return nil
}

func (e *Engine) getLatestCatalogCache() *cache.CatalogCache {
	return e.catalog
}

func (e *Engine) loadSnapCkpForTable(
	ctx context.Context,
	snapCatalog *cache.CatalogCache,
	loc string,
	tid uint64,
	tblName string,
	did uint64,
	dbName string,
	pkSeqNum int,
) error {
	entries, closeCBs, err := logtail.LoadCheckpointEntries(
		ctx,
		loc,
		tid,
		tblName,
		did,
		dbName,
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
		if err = consumeEntry(ctx, pkSeqNum, e, snapCatalog, nil, entry); err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) getOrCreateSnapCatalogCache(
	ctx context.Context,
	ts types.TS) (*cache.CatalogCache, error) {
	if e.catalog.CanServe(ts) {
		return e.catalog, nil
	}
	e.snapCatalog.Lock()
	defer e.snapCatalog.Unlock()
	for _, snap := range e.snapCatalog.snaps {
		if snap.CanServe(ts) {
			return snap, nil
		}
	}
	snapCata := cache.NewCatalog()
	//TODO:: insert mo_tables, or mo_colunms, or mo_database, mo_catalog into snapCata.
	//       ref to engine.init.
	ckps, err := checkpoint.ListSnapshotCheckpoint(ctx, e.fs, ts, 0, nil)
	if ckps == nil {
		return nil, moerr.NewInternalErrorNoCtx("No checkpoints for snapshot read")
	}
	if err != nil {
		return nil, err
	}
	//Notice that checkpoints must contain only one or zero global checkpoint
	//followed by zero or multi continuous incremental checkpoints.
	start := types.MaxTs()
	end := types.TS{}
	for _, ckp := range ckps {
		locs := make([]string, 0)
		locs = append(locs, ckp.GetLocation().String())
		locs = append(locs, strconv.Itoa(int(ckp.GetVersion())))
		locations := strings.Join(locs, ";")
		//FIXME::pkSeqNum == 0?
		if err := e.loadSnapCkpForTable(
			ctx,
			snapCata,
			locations,
			catalog.MO_DATABASE_ID,
			catalog.MO_DATABASE,
			catalog.MO_CATALOG_ID,
			catalog.MO_CATALOG,
			0); err != nil {
			return nil, err
		}
		if err := e.loadSnapCkpForTable(
			ctx,
			snapCata,
			locations,
			catalog.MO_TABLES_ID,
			catalog.MO_TABLES,
			catalog.MO_CATALOG_ID,
			catalog.MO_CATALOG, 0); err != nil {
			return nil, err
		}
		if err := e.loadSnapCkpForTable(
			ctx,
			snapCata,
			locations,
			catalog.MO_COLUMNS_ID,
			catalog.MO_COLUMNS,
			catalog.MO_CATALOG_ID,
			catalog.MO_CATALOG,
			0); err != nil {
			return nil, err
		}
		//update start and end of snapCata.
		if ckp.GetType() == checkpoint.ET_Global {
			start = ckp.GetEnd()
		}
		if ckp.GetType() == checkpoint.ET_Incremental {
			ckpstart := ckp.GetStart()
			if ckpstart.Less(&start) {
				start = ckpstart
			}
			ckpend := ckp.GetEnd()
			if ckpend.Greater(&end) {
				end = ckpend
			}
		}
	}
	if end.IsEmpty() {
		//only on global checkpoint.
		end = start
	}
	if ts.Greater(&end) || ts.Less(&start) {
		return nil, moerr.NewInternalErrorNoCtx("Invalid checkpoints for snapshot read")
	}
	snapCata.UpdateDuration(start, end)
	e.snapCatalog.snaps = append(e.snapCatalog.snaps, snapCata)
	return snapCata, nil
}

func (e *Engine) getOrCreateSnapPart(
	ctx context.Context,
	tbl *txnTable,
	ts types.TS) (*logtailreplay.Partition, error) {

	//check whether the latest partition is available for reuse.
	err := tbl.updateLogtail(ctx)
	if err != nil {
		return nil, err
	}
	if p := e.getOrCreateLatestPart(tbl.db.databaseId, tbl.tableId); p.CanServe(ts) {
		return p, nil
	}

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
			return snap, nil
		}
	}

	//new snapshot partition and apply checkpoints into it.
	snap := logtailreplay.NewPartition()
	//TODO::if tableId is mo_tables, or mo_colunms, or mo_database,
	//      we should init the partition,ref to engine.init
	ckps, err := checkpoint.ListSnapshotCheckpoint(ctx, e.fs, ts, tbl.tableId, nil)
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
		return snap, nil
	}

	start, end := snap.GetDuration()
	//if has no checkpoints or ts > snap.end, use latest partition.
	if snap.IsEmpty() || ts.Greater(&end) {
		err := tbl.updateLogtail(ctx)
		if err != nil {
			return nil, err
		}
		return e.getOrCreateLatestPart(tbl.db.databaseId, tbl.tableId), nil
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

func (e *Engine) getOrCreateLatestPart(
	databaseId,
	tableId uint64) *logtailreplay.Partition {
	e.Lock()
	defer e.Unlock()
	partition, ok := e.partitions[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		partition = logtailreplay.NewPartition()
		e.partitions[[2]uint64{databaseId, tableId}] = partition
	}
	return partition
}

func (e *Engine) lazyLoadLatestCkp(
	ctx context.Context,
	tbl *txnTable) (*logtailreplay.Partition, error) {
	part := e.getOrCreateLatestPart(tbl.db.databaseId, tbl.tableId)
	cache := e.getLatestCatalogCache()

	if err := part.ConsumeCheckpoints(
		ctx,
		func(checkpoint string, state *logtailreplay.PartitionState) error {
			entries, closeCBs, err := logtail.LoadCheckpointEntries(
				ctx,
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
					cb()
				}
			}()
			for _, entry := range entries {
				if err = consumeEntry(ctx, tbl.primarySeqnum, e, cache, state, entry); err != nil {
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
