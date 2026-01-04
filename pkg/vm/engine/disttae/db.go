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
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function/ctl"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/cache"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/checkpoint"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/logtail"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// tryAdjustSysTablesCreatedTimeWithBatch analyzes the mo_tables batch and tries to adjust the created time of the system tables.
func (e *Engine) tryAdjustSysTablesCreatedTimeWithBatch(b *batch.Batch) {
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
			// Update all system tables' created time
			for _, vec := range e.sysTablesCreatedTime {
				vector.SetFixedAtWithTypeCheck(vec, 0, ts)
			}
			e.timeFixed = true
			return
		}
	}
}

// sysTableCreatedTimeIdx defines the index mapping for sysTablesCreatedTime slice
const (
	sysTableCreatedTimeIdxCatalog  = 0 // mo_catalog (database)
	sysTableCreatedTimeIdxDatabase = 1 // mo_database (table in mo_tables)
	sysTableCreatedTimeIdxTables   = 2 // mo_tables (table in mo_tables)
	sysTableCreatedTimeIdxColumns  = 3 // mo_columns (table in mo_tables)
	sysTableCreatedTimeIdxIndexTbl = 4 // __mo_index_unique_mo_tables_logical_id (index table)
	sysTableCreatedTimeSliceLen    = 5
)

func initSysTable(
	ctx context.Context,
	service string,
	mp *mpool.MPool,
	packerPool *fileservice.Pool[*types.Packer],
	sysTablesCreatedTime []*vector.Vector, // slice to store created_time vectors
	part *logtailreplay.Partition,
	cc *cache.CatalogCache,
	tid uint64,
	initCatalog bool) error {

	m := mp

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
		if sysTablesCreatedTime != nil {
			sysTablesCreatedTime[sysTableCreatedTimeIdxCatalog] = bat.Vecs[catalog.MO_DATABASE_CREATED_TIME_IDX]
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
		if sysTablesCreatedTime != nil {
			sysTablesCreatedTime[sysTableCreatedTimeIdxDatabase] = bat.Vecs[catalog.MO_TABLES_CREATED_TIME_IDX]
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
		if sysTablesCreatedTime != nil {
			sysTablesCreatedTime[sysTableCreatedTimeIdxTables] = bat.Vecs[catalog.MO_TABLES_CREATED_TIME_IDX]
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
		if sysTablesCreatedTime != nil {
			sysTablesCreatedTime[sysTableCreatedTimeIdxColumns] = bat.Vecs[catalog.MO_TABLES_CREATED_TIME_IDX]
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
		// Create index table metadata entry (insert into mo_tables)
		indexTbl := catalog.Table{
			AccountId:    0,
			UserId:       0,
			RoleId:       0,
			DatabaseId:   catalog.MO_CATALOG_ID,
			DatabaseName: catalog.MO_CATALOG,
			TableId:      catalog.MO_TABLES_LOGICAL_ID_INDEX_ID,
			TableName:    catalog.MO_TABLES_LOGICAL_ID_INDEX_TABLE_NAME,
			Constraint:   catalog.GetDefines(service).MoTablesLogicalIdIndexConstraint,
			Kind:         catalog.SystemOrdinaryRel,
		}
		indexBat, err := catalog.GenCreateTableTuple(indexTbl, m, packer)
		if err != nil {
			return err
		}
		if sysTablesCreatedTime != nil {
			sysTablesCreatedTime[sysTableCreatedTimeIdxIndexTbl] = indexBat.Vecs[catalog.MO_TABLES_CREATED_TIME_IDX]
		}
		indexIbat, err := fillRandomRowidAndZeroTs(indexBat, m)
		if err != nil {
			indexBat.Clean(m)
			return err
		}
		state.HandleRowsInsert(ctx, indexIbat, catalog.MO_TABLES_CPKEY_IDX, packer, mp)
		if initCatalog {
			cc.InsertTable(indexBat)
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

		// put mo_tables_logical_id_index into mo_columns
		cols, err = catalog.GenColumnsFromDefs(
			0,
			catalog.MO_TABLES_LOGICAL_ID_INDEX_TABLE_NAME,
			catalog.MO_CATALOG,
			catalog.MO_TABLES_LOGICAL_ID_INDEX_ID,
			catalog.MO_CATALOG_ID,
			catalog.GetDefines(service).MoTablesLogicalIdIndexTableDefs)
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
			logtailreplay.NewPartition(e.service, nil, 0, 1, 1, nil)
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_ID}] =
			logtailreplay.NewPartition(e.service, nil, 0, 1, 2, nil)
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}] =
			logtailreplay.NewPartition(e.service, nil, 0, 1, 3, nil)
		// Add partition for index table
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_TABLES_LOGICAL_ID_INDEX_ID}] =
			logtailreplay.NewPartition(e.service, nil, 0, 1, 4, nil)
	}

	// Initialize the sysTablesCreatedTime slice
	e.sysTablesCreatedTime = make([]*vector.Vector, sysTableCreatedTimeSliceLen)

	err := initSysTable(
		ctx,
		e.service,
		e.mp,
		e.packerPool,
		e.sysTablesCreatedTime,
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
		e.sysTablesCreatedTime,
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
		e.sysTablesCreatedTime,
		e.partitions[[2]uint64{catalog.MO_CATALOG_ID, catalog.MO_COLUMNS_ID}],
		newcache,
		catalog.MO_COLUMNS_ID,
		true)
	if err != nil {
		return err
	}

	e.catalog.Store(newcache)
	return nil
}

func (e *Engine) GetLatestCatalogCache() *cache.CatalogCache {
	return e.catalog.Load()
}

func GetSnapshotReadFnWithHandler(
	handleFn func(
		ctx context.Context,
		meta txn.TxnMeta,
		req *cmd_util.SnapshotReadReq,
		resp *cmd_util.SnapshotReadResp,
	) (func(), error),
) func(ctx context.Context, tbl *txnTable, snapshot *types.TS) (resp any, err error) {
	return func(ctx context.Context, tbl *txnTable, snapshot *types.TS) (resp any, err error) {
		req := &cmd_util.SnapshotReadReq{}
		payload := func(tnShardID uint64, parameter string, proc *process.Process) ([]byte, error) {
			ts := snapshot.ToTimestamp()
			req.Snapshot = &ts

			return req.MarshalBinary()
		}
		tableProc := tbl.proc.Load()
		proc := process.NewTopProcess(ctx, tableProc.GetMPool(),
			tableProc.Base.TxnClient, tableProc.GetTxnOperator(),
			tableProc.Base.FileService, tableProc.Base.LockService,
			tableProc.Base.QueryClient, tableProc.Base.Hakeeper,
			tableProc.Base.UdfService, tableProc.Base.Aicm,
			tableProc.Base.TaskService,
		)
		resp = &cmd_util.SnapshotReadResp{}
		payload(0, "", proc)
		handleFn(ctx, txn.TxnMeta{}, req, resp.(*cmd_util.SnapshotReadResp))
		return

	}
}

var RequestSnapshotRead = func(ctx context.Context, tbl *txnTable, snapshot *types.TS) (resp any, err error) {
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
		tableProc.Base.TaskService,
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

// getOrCreateSnapPartBy is used to get or create a snapshot partition state by the given timestamp.
func (e *Engine) getOrCreateSnapPartBy(
	ctx context.Context,
	tbl *txnTable,
	ts types.TS) (*logtailreplay.PartitionState, error) {
	response, err := RequestSnapshotRead(ctx, tbl, &ts)
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
		if len(checkpointEntries) < 1 {
			return false
		}

		// The end time of the penultimate checkpoint must not be less than the ts of the snapshot,
		// because the data of the snapshot may exist in the wal and be collected to the next checkpoint
		end := checkpointEntries[len(checkpointEntries)-1].GetEnd()
		return !end.LT(&ts)
	}

	if !ckpsCanServe() {
		return nil, moerr.NewInternalErrorNoCtxf(
			"No available checkpoints for snapshot read at:%s, table:%s, tid:%v, txn:%s",
			ts.ToString(),
			tbl.tableName,
			tbl.tableId,
			tbl.db.op.Txn().DebugString())
	}

	// Try to find existing snapshot that can serve this timestamp
	if ps := e.snapshotMgr.Find(tbl.db.databaseId, tbl.tableId, ts); ps != nil {
		return ps, nil
	}

	//new snapshot partition and apply checkpoints into it.
	snap := logtailreplay.NewPartition(
		e.service, e.GetLatestCatalogCache(),
		uint64(tbl.accountId), tbl.db.databaseId, tbl.tableId,
		e.getPrefetchOnSubscribed(),
	)
	if tbl.tableId == catalog.MO_TABLES_ID ||
		tbl.tableId == catalog.MO_DATABASE_ID ||
		tbl.tableId == catalog.MO_COLUMNS_ID {
		err := initSysTable(
			ctx,
			e.service,
			e.mp,
			e.packerPool,
			nil, // sysTablesCreatedTime not needed for snapshot
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
		err := logtail.ConsumeCheckpointEntries(
			ctx,
			e.service,
			locations,
			tbl.tableId,
			tbl.tableName,
			tbl.db.databaseId,
			tbl.db.databaseName,
			state.HandleObjectEntry,
			e.mp,
			e.fs)
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logutil.Error("Snapshot consumeSnapCkps failed", zap.Error(err))
		return nil, err
	}
	if !snap.Snapshot().CanServe(ts) {
		return nil, moerr.NewInternalErrorNoCtxf(
			"snapshot partition cannot serve ts after consuming checkpoints, ts:%s, table:%s",
			ts.ToString(), tbl.tableName)
	}

	// Add the new snapshot with LRU eviction
	ps := e.snapshotMgr.Add(
		tbl.db.databaseId,
		tbl.tableId,
		snap,
		tbl.tableName,
		ts,
	)

	// Log total snapshots
	metrics := e.snapshotMgr.GetMetrics()
	logutil.Info(
		"Snapshot-Added",
		zap.Int64("total-snaps-global", metrics.TotalSnapshots.Load()),
	)

	// Trigger GC check if needed
	e.snapshotMgr.MaybeStartGC()

	return ps, nil
}

func (e *Engine) GetOrCreateLatestPart(
	ctx context.Context,
	accId uint64,
	databaseId,
	tableId uint64,
) *logtailreplay.Partition {

	e.Lock()
	defer e.Unlock()
	partition, ok := e.partitions[[2]uint64{databaseId, tableId}]
	if !ok { // create a new table
		partition = logtailreplay.NewPartition(
			e.service, e.GetLatestCatalogCache(),
			accId, databaseId, tableId,
			e.getPrefetchOnSubscribed(),
		)
		e.partitions[[2]uint64{databaseId, tableId}] = partition
	}
	return partition
}

func (e *Engine) LazyLoadLatestCkp(
	ctx context.Context,
	accId uint64,
	tableID uint64,
	tableName string,
	dbID uint64,
	dbName string) (*logtailreplay.Partition, error) {

	part := e.GetOrCreateLatestPart(ctx, accId, dbID, tableID)

	if err := part.ConsumeCheckpoints(
		ctx,
		func(checkpoint string, state *logtailreplay.PartitionState) error {
			err := logtail.ConsumeCheckpointEntries(
				ctx,
				e.service,
				checkpoint,
				tableID,
				tableName,
				dbID,
				dbName,
				state.HandleObjectEntry,
				e.mp,
				e.fs)
			if err != nil {
				return err
			}
			return nil
		},
	); err != nil {
		return nil, err
	}

	return part, nil
}
