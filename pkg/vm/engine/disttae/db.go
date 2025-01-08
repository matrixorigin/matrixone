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
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
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
		aid := vector.GetFixedAtWithTypeCheck[uint32](b.Vecs[aidIdx], i)
		tname := b.Vecs[tnameIdx].GetStringAt(i)
		if aid == 0 && tname == "mo_user" {
			ts := vector.GetFixedAtWithTypeCheck[types.Timestamp](b.Vecs[createdTsIdx], i)
			vector.SetFixedAtWithTypeCheck(e.moDatabaseCreatedTime, 0, ts)
			vector.SetFixedAtWithTypeCheck(e.moTablesCreatedTime, 0, ts)
			vector.SetFixedAtWithTypeCheck(e.moColumnsCreatedTime, 0, ts)
			vector.SetFixedAtWithTypeCheck(e.moCatalogCreatedTime, 0, ts)
			e.timeFixed = true
			return
		}
	}
}

func initSysTable(
	ctx context.Context,
	//e *Engine,
	service string,
	mp *mpool.MPool,
	packerPool *fileservice.Pool[*types.Packer],
	moCatalogCreatedTime **vector.Vector,
	moDatabaseCreatedTime **vector.Vector,
	moTablesCreatedTime **vector.Vector,
	moColumnsCreatedTime **vector.Vector,
	part *logtailreplay.Partition,
	cc *cache.CatalogCache,
	tid uint64,
	initCatalog bool) error {

	m := mp
	//part := e.partitions[[2]uint64{catalog.MO_CATALOG_ID, tid}]

	var packer *types.Packer
	put := packerPool.Get(&packer)
	defer put.Put()

	switch tid {
	case catalog.MO_DATABASE_ID:
		bat, err := catalog.GenCreateDatabaseTuple(
			"",
			0,
			0,
			0,
			catalog.MO_CATALOG,
			catalog.MO_CATALOG_ID,
			"",
			m,
			packer)
		if err != nil {
			return err
		}
		if moCatalogCreatedTime != nil {
			*moCatalogCreatedTime = bat.Vecs[catalog.MO_DATABASE_CREATED_TIME_IDX]
		}
		ibat, err := fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_DATABASE_CPKEY_IDX, packer, mp)
		done()
		if initCatalog {
			cc.InsertDatabase(bat)
		}
	case catalog.MO_TABLES_ID:
		//put mo_database into mo_tables
		tbl := catalog.Table{
			AccountId:    0,
			UserId:       0,
			RoleId:       0,
			DatabaseId:   catalog.MO_CATALOG_ID,
			DatabaseName: catalog.MO_CATALOG,
			TableId:      catalog.MO_DATABASE_ID,
			TableName:    catalog.MO_DATABASE,
			Constraint:   catalog.GetDefines(service).MoDatabaseConstraint,
			Kind:         catalog.SystemOrdinaryRel,
		}
		bat, err := catalog.GenCreateTableTuple(tbl, m, packer)
		if err != nil {
			return err
		}
		if moDatabaseCreatedTime != nil {
			*moDatabaseCreatedTime = bat.Vecs[catalog.MO_TABLES_CREATED_TIME_IDX]
		}
		ibat, err := fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}

		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_TABLES_CPKEY_IDX, packer, mp)
		if initCatalog {
			cc.InsertTable(bat) // cache
		}
		//put mo_tables into mo_tables.
		tbl = catalog.Table{
			AccountId:    0,
			UserId:       0,
			RoleId:       0,
			DatabaseId:   catalog.MO_CATALOG_ID,
			DatabaseName: catalog.MO_CATALOG,
			TableId:      catalog.MO_TABLES_ID,
			TableName:    catalog.MO_TABLES,
			Constraint:   catalog.GetDefines(service).MoTableConstraint,
			Kind:         catalog.SystemOrdinaryRel,
		}
		bat, err = catalog.GenCreateTableTuple(tbl, m, packer)
		if err != nil {
			return err
		}
		if moTablesCreatedTime != nil {
			*moTablesCreatedTime = bat.Vecs[catalog.MO_TABLES_CREATED_TIME_IDX]
		}
		ibat, err = fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state.HandleRowsInsert(ctx, ibat, catalog.MO_TABLES_CPKEY_IDX, packer, mp)
		if initCatalog {
			cc.InsertTable(bat)
		}

		//put mo_columns into mo_tables.
		tbl = catalog.Table{
			AccountId:    0,
			UserId:       0,
			RoleId:       0,
			DatabaseId:   catalog.MO_CATALOG_ID,
			DatabaseName: catalog.MO_CATALOG,
			TableId:      catalog.MO_COLUMNS_ID,
			TableName:    catalog.MO_COLUMNS,
			Constraint:   catalog.GetDefines(service).MoColumnConstraint,
			Kind:         catalog.SystemOrdinaryRel,
		}
		bat, err = catalog.GenCreateTableTuple(tbl, m, packer)
		if err != nil {
			return err
		}
		if moColumnsCreatedTime != nil {
			*moColumnsCreatedTime = bat.Vecs[catalog.MO_TABLES_CREATED_TIME_IDX]
		}
		ibat, err = fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state.HandleRowsInsert(ctx, ibat, catalog.MO_TABLES_CPKEY_IDX, packer, mp)
		if initCatalog {
			cc.InsertTable(bat)
		}
		done()
	case catalog.MO_COLUMNS_ID:
		//put mo_database into mo_columns
		cols, err := catalog.GenColumnsFromDefs(
			0,
			catalog.MO_DATABASE,
			catalog.MO_CATALOG,
			catalog.MO_DATABASE_ID,
			catalog.MO_CATALOG_ID,
			catalog.GetDefines(service).MoDatabaseTableDefs)
		if err != nil {
			return err
		}

		bat, err := catalog.GenCreateColumnTuples(cols, m, packer)
		if err != nil {
			return err
		}
		ibat, err := fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state, done := part.MutateState()
		state.HandleRowsInsert(ctx, ibat, catalog.MO_COLUMNS_ATT_CPKEY_IDX, packer, mp)
		if initCatalog {
			cc.InsertColumns(bat)
		}

		//put mo_tables into mo_columns
		cols, err = catalog.GenColumnsFromDefs(
			0,
			catalog.MO_TABLES,
			catalog.MO_CATALOG,
			catalog.MO_TABLES_ID,
			catalog.MO_CATALOG_ID,
			catalog.GetDefines(service).MoTablesTableDefs)
		if err != nil {
			return err
		}
		bat, err = catalog.GenCreateColumnTuples(cols, m, packer)
		if err != nil {
			return err
		}
		ibat, err = fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state.HandleRowsInsert(ctx, ibat, catalog.MO_COLUMNS_ATT_CPKEY_IDX, packer, mp)
		if initCatalog {
			cc.InsertColumns(bat)
		}
		// put mo_columns into mo_columns
		cols, err = catalog.GenColumnsFromDefs(
			0,
			catalog.MO_COLUMNS,
			catalog.MO_CATALOG,
			catalog.MO_COLUMNS_ID,
			catalog.MO_CATALOG_ID,
			catalog.GetDefines(service).MoColumnsTableDefs)
		if err != nil {
			return err
		}

		bat, err = catalog.GenCreateColumnTuples(cols, m, packer)
		if err != nil {
			return err
		}
		ibat, err = fillRandomRowidAndZeroTs(bat, m)
		if err != nil {
			bat.Clean(m)
			return err
		}
		state.HandleRowsInsert(ctx, ibat, catalog.MO_COLUMNS_ATT_CPKEY_IDX, packer, mp)
		if initCatalog {
			cc.InsertColumns(bat)
		}
		done()
	}
	return nil
}

// init is used to insert some data that will not be synchronized by logtail.
func (e *Engine) init(ctx context.Context) error {
	e.Lock()
	defer e.Unlock()

	newcache := cache.NewCatalog()
	e.partitions = make(map[[2]uint64]*logtailreplay.Partition)

	{
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}] =
			logtailreplay.NewPartition(e.service, 1)
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}] =
			logtailreplay.NewPartition(e.service, 2)
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}] =
			logtailreplay.NewPartition(e.service, 3)
	}

	err := initSysTable(
		ctx,
		e.service,
		e.mp,
		e.packerPool,
		&e.moCatalogCreatedTime,
		&e.moDatabaseCreatedTime,
		&e.moTablesCreatedTime,
		&e.moColumnsCreatedTime,
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_DATABASE_ID}],
		newcache,
		catalog.MO_DATABASE_ID,
		true)
	if err != nil {
		return err
	}

	err = initSysTable(
		ctx,
		e.service,
		e.mp,
		e.packerPool,
		&e.moCatalogCreatedTime,
		&e.moDatabaseCreatedTime,
		&e.moTablesCreatedTime,
		&e.moColumnsCreatedTime,
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}],
		newcache,
		catalog.MO_TABLES_ID,
		true)
	if err != nil {
		return err

	}

	err = initSysTable(
		ctx,
		e.service,
		e.mp,
		e.packerPool,
		&e.moCatalogCreatedTime,
		&e.moDatabaseCreatedTime,
		&e.moTablesCreatedTime,
		&e.moColumnsCreatedTime,
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}],
		newcache,
		catalog.MO_COLUMNS_ID,
		true)
	if err != nil {
		return err
	}

	e.catalog.Store(newcache)
	// clear all tables in global stats.
	e.globalStats.clearTables()

	return nil
}

func (e *Engine) GetLatestCatalogCache() *cache.CatalogCache {
	return e.catalog.Load()
}

func requestSnapshotRead(ctx context.Context, tbl *txnTable, snapshot *types.TS) (resp any, err error) {
	whichTN := func(string) ([]uint64, error) { return nil, nil }
	payload := func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
		req := cmd_util.SnapshotReadReq{}
		ts := snapshot.ToTimestamp()
		req.Snapshot = &ts

		return req.MarshalBinary()
	}

	responseUnmarshaler := func(payload []byte) (any, error) {
		checkpoints := &cmd_util.SnapshotReadResp{}
		if err = checkpoints.UnmarshalBinary(payload); err != nil {
			return nil, err
		}
		return checkpoints, nil
	}
	tableProc := tbl.proc.Load()
	proc := process.NewTopProcess(ctx, tableProc.GetMPool(),
		tableProc.Base.TxnClient, tableProc.GetTxnOperator(),
		tableProc.Base.FileService, tableProc.Base.LockService,
		tableProc.Base.QueryClient, tableProc.Base.Hakeeper,
		tableProc.Base.UdfService, tableProc.Base.Aicm,
	)
	handler := ctl.GetTNHandlerFunc(api.OpCode_OpSnapshotRead, whichTN, payload, responseUnmarshaler)
	result, err := handler(proc, "DN", "", ctl.MoCtlTNCmdSender)
	sleep, sarg, exist := fault.TriggerFault("snapshot_read_timeout")
	if exist {
		time.Sleep(time.Duration(sleep) * time.Second)
		return nil, moerr.NewInternalError(ctx, sarg)
	}
	if moerr.IsMoErrCode(err, moerr.ErrNotSupported) {
		// try the previous RPC method
		return nil, moerr.NewNotSupportedNoCtx("current tn version not supported `snapshot read`")
	}
	if err != nil {
		return nil, err
	}
	if len(result.Data.([]any)) == 0 {
		return cmd_util.SnapshotReadResp{Succeed: false}, nil
	}
	return result.Data.([]any)[0], nil
}

func (e *Engine) getOrCreateSnapPart(
	ctx context.Context,
	tbl *txnTable,
	ts types.TS) (*logtailreplay.PartitionState, error) {
	response, err := requestSnapshotRead(ctx, tbl, &ts)
	if err != nil {
		return nil, err
	}
	resp, ok := response.(*cmd_util.SnapshotReadResp)
	var checkpointEntries []*checkpoint.CheckpointEntry
	if ok && resp.Succeed && len(resp.Entries) > 0 {
		checkpointEntries = make([]*checkpoint.CheckpointEntry, 0, len(resp.Entries))
		entries := resp.Entries
		for _, entry := range entries {
			start := types.TimestampToTS(*entry.Start)
			end := types.TimestampToTS(*entry.End)
			entryType := entry.EntryType
			checkpointEntry := checkpoint.NewCheckpointEntry("", start, end, checkpoint.EntryType(entryType))
			checkpointEntry.SetLocation(entry.Location1, entry.Location2)
			checkpointEntries = append(checkpointEntries, checkpointEntry)
		}
	}

	ckpsCanServe := func() bool {
		// The checkpoint entry required by SnapshotRead must meet two or more checkpoints,
		// otherwise the latest partition can meet this SnapshotRead request
		if len(checkpointEntries) < 2 {
			return false
		}

		// The end time of the penultimate checkpoint must not be less than the ts of the snapshot,
		// because the data of the snapshot may exist in the wal and be collected to the next checkpoint
		end := checkpointEntries[len(checkpointEntries)-2].GetEnd()
		return !end.LT(&ts)
	}

	if !ckpsCanServe() {
		//check whether the latest partition is available for reuse.
		if ps, err := tbl.tryToSubscribe(ctx); err == nil {
			if ps != nil && ps.CanServe(ts) {
				logutil.Infof("getOrCreateSnapPart:reuse latest partition state:%p, tbl:%p, table name:%s, tid:%v, txn:%s",
					ps,
					tbl,
					tbl.tableName,
					tbl.tableId,
					tbl.db.op.Txn().DebugString())
				return ps, nil
			}
			var start, end types.TS
			if ps != nil {
				start, end = ps.GetDuration()
			}
			logutil.Infof("getOrCreateSnapPart, "+
				"latest partition state:%p, duration:[%s_%s] can't serve snapshot read at ts :%s, tbl:%p, table:%s, relKind:%s, tid:%v, txn:%s",
				ps,
				start.ToString(),
				end.ToString(),
				ts.ToString(),
				tbl,
				tbl.tableName,
				tbl.relKind,
				tbl.tableId,
				tbl.db.op.Txn().DebugString())
		}
	}

	//subscribe failed : 1. network timeout,
	//2. table id is too old ,pls ref to issue:https://github.com/matrixorigin/matrixone/issues/17012

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
		if snap.Snapshot().CanServe(ts) {
			return snap.Snapshot(), nil
		}
	}

	//new snapshot partition and apply checkpoints into it.
	snap := logtailreplay.NewPartition(e.service, tbl.tableId)
	if tbl.tableId == catalog.MO_TABLES_ID ||
		tbl.tableId == catalog.MO_DATABASE_ID ||
		tbl.tableId == catalog.MO_COLUMNS_ID {
		err := initSysTable(
			ctx,
			e.service,
			e.mp,
			e.packerPool,
			nil,
			nil,
			nil,
			nil,
			snap,
			nil,
			tbl.tableId,
			false)
		if err != nil {
			return nil, err
		}

	}
	ckps := checkpointEntries
	err = snap.ConsumeSnapCkps(ctx, ckps, func(
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
				if cb != nil {
					cb()
				}
			}
		}()
		for _, entry := range entries {
			if err = consumeEntry(
				ctx,
				tbl.primarySeqnum,
				e,
				nil,
				state,
				entry,
				false); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		logutil.Infof("Snapshot consumeSnapCkps failed, err:%v", err)
		return nil, err
	}
	if snap.Snapshot().CanServe(ts) {
		tblSnaps.snaps = append(tblSnaps.snaps, snap)
		return snap.Snapshot(), nil
	}

	start, end := snap.Snapshot().GetDuration()
	//if has no checkpoints or ts > snap.end, use latest partition.
	if snap.Snapshot().IsEmpty() || ts.GT(&end) {
		logutil.Infof("getOrCreateSnapPart:Start to resue latest ps for snapshot read at:%s, "+
			"table name:%s, tbl:%p, tid:%v, txn:%s, snapIsEmpty:%v, end:%s",
			ts.ToString(),
			tbl.tableName,
			tbl,
			tbl.tableId,
			tbl.db.op.Txn().DebugString(),
			snap.Snapshot().IsEmpty(),
			end.ToString(),
		)
		ps, err := tbl.tryToSubscribe(ctx)
		if err != nil {
			return nil, err
		}
		if ps != nil && ps.CanServe(ts) {
			logutil.Infof("getOrCreateSnapPart: resue latest partiton state:%p, "+
				"tbl:%p, table name:%s, tid:%v, txn:%s,ckpIsEmpty:%v, ckp-end:%s",
				ps,
				tbl,
				tbl.tableName,
				tbl.tableId,
				tbl.db.op.Txn().DebugString(),
				snap.Snapshot().IsEmpty(),
				end.ToString())
			return ps, nil
		}
		var start, end types.TS
		if ps != nil {
			start, end = ps.GetDuration()
		}
		logutil.Infof("getOrCreateSnapPart: "+
			"latest partition state:%p, duration[%s_%s] can't serve for snapshot read at:%s, tbl:%p, table name:%s, tid:%v, txn:%s",
			ps,
			start.ToString(),
			end.ToString(),
			ts.ToString(),
			tbl,
			tbl.tableName,
			tbl.tableId,
			tbl.db.op.Txn().DebugString())
		return nil, moerr.NewInternalErrorNoCtxf("Latest partition state can't serve for snapshot read at:%s, "+
			"table:%s, tid:%v, txn:%s",
			ts.ToString(),
			tbl.tableName,
			tbl.tableId,
			tbl.db.op.Txn().DebugString())
	}
	if ts.LT(&start) {
		return nil, moerr.NewInternalErrorNoCtxf(
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

func (e *Engine) LazyLoadLatestCkp(
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

	part := e.GetOrCreateLatestPart(tbl.db.databaseId, tbl.tableId)
	cache := e.GetLatestCatalogCache()

	if err := part.ConsumeCheckpoints(
		ctx,
		func(checkpoint string, state *logtailreplay.PartitionState) error {
			entries, closeCBs, err := logtail.LoadCheckpointEntries(
				ctx,
				e.service,
				checkpoint,
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
					if cb != nil {
						cb()
					}
				}
			}()
			for _, entry := range entries {
				if err = consumeEntry(ctx, tbl.primarySeqnum, e, cache, state, entry, false); err != nil {
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
