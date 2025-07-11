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

package idxcdc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/task"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	"github.com/tidwall/btree"
)

const (
	MOAsyncIndexLogTableName = "mo_async_index_log"
)

type relationFactory func(context.Context) (engine.Relation, client.TxnOperator, error)

type replayFn func(
	ctx context.Context, tableID uint64, accountID uint32, indexID int32,
) (watermark types.TS, errorCode int, errorMsg string, err error)

type deleteFn func(ctx context.Context, tableID uint64, accountID uint32, indexID int32) error

type TxnFactory func() (client.TxnOperator, error)

type CDCTaskExecutor2 struct {
	tables  *btree.BTreeG[*TableInfo_2]
	tableMu sync.RWMutex
	// getInsertWatermarkFn func(
	// 	ctx context.Context,
	// 	tableID uint64,
	// 	accountID int32,
	// 	indexID int32,
	// ) error
	// getFlushWatermarkFn func(
	// 	ctx context.Context,
	// 	tableID uint64,
	// 	watermark types.TS,
	// 	accountID int32,
	// 	indexID int32,
	// 	errorCode int,
	// 	info string,
	// 	errorMsg string,
	// ) error
	// replayFn replayFn
	// deleteFn deleteFn
	packer *types.Packer
	mp     *mpool.MPool
	// spec     *task.CreateCdcDetails

	// watermarkUpdater   WatermarkUpdater
	// logger             *zap.Logger //todo: replace logutil.Infof
	// sqlExecutorFactory func() ie.InternalExecutor
	// attachToTask       func(context.Context, uint64, taskservice.ActiveRoutine) error
	cnUUID string
	// ts                 taskservice.TaskService
	// fs                 fileservice.FileService
	txnFactory func() (client.TxnOperator, error)
	// sinkerFactory func(dbName, tableName string, tableDef []engine.TableDef) (Sinker, error)
	txnEngine            engine.Engine
	asyncIndexLogTableID uint64

	rpcHandleFn func(
		ctx context.Context,
		meta txn.TxnMeta,
		req *cmd_util.GetChangedTableListReq,
		resp *cmd_util.GetChangedTableListResp,
	) (func(), error) // for test

	ctx    context.Context
	cancel context.CancelFunc

	worker Worker
	wg     sync.WaitGroup
}

func GetTxnFactory(
	ctx context.Context,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
) func() (client.TxnOperator, error) {
	return func() (client.TxnOperator, error) {
		return GetTxnOp(ctx, cnEngine, cnTxnClient, "debug cdc")
	}
}

func AsyncIndexCdcTaskExecutorFactory(
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	cdUUID string,
	mp *mpool.MPool,
) func(ctx context.Context, task task.Task) (err error) {
	return func(ctx context.Context, task task.Task) (err error) {
		exec, err := NewCDCTaskExecutor2(ctx, txnEngine, cnTxnClient, cdUUID,nil, mp)
		if err != nil {
			return err
		}
		exec.Start()
		return nil
	}
}

func NewCDCTaskExecutor2(
	ctx context.Context,
	// accountID uint64,
	// spec *task.CreateCdcDetails,
	// sqlExecutorFactory func() ie.InternalExecutor,
	// sinkerFactory func(dbName, tableName string, tableDef []engine.TableDef) (Sinker, error),
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	cdUUID string,
	// rpcHandleFn func(
	// 	ctx context.Context,
	// 	meta txn.TxnMeta,
	// 	req *cmd_util.GetChangedTableListReq,
	// 	resp *cmd_util.GetChangedTableListResp,
	// ) (func(), error),
	// getInsertWatermarkFn func(
	// 	ctx context.Context,
	// 	tableID uint64,
	// 	accountID int32,
	// 	indexID int32,
	// ) error,
	// getFlushWatermarkFn func(
	// 	ctx context.Context,
	// 	tableID uint64,
	// 	watermark types.TS,
	// 	accountID int32,
	// 	indexID int32,
	// 	errorCode int,
	// 	info string,
	// 	errorMsg string,
	// ) error,
	// replayFn replayFn,
	// deleteFn deleteFn,
	txnFactory func() (client.TxnOperator, error),
	mp *mpool.MPool,
) (*CDCTaskExecutor2, error) {
	ctx, cancel := context.WithCancel(ctx)
	worker := NewWorker()
	// ctx = context.WithValue(ctx, defines.TenantIDKey{}, uint32(accountID))
	//TODO: subscribe mo_catalog.mo_async_index_log
	exec := &CDCTaskExecutor2{
		ctx:    ctx,
		cancel: cancel,
		packer: types.NewPacker(),
		tables: btree.NewBTreeGOptions(tableInfoLess, btree.Options{NoLocks: true}),
		// spec:                 spec,
		// sqlExecutorFactory:   sqlExecutorFactory,
		cnUUID:     cdUUID,
		txnFactory: txnFactory,
		// sinkerFactory:        sinkerFactory,
		txnEngine: txnEngine,
		worker:    worker,
		wg:        sync.WaitGroup{},
		// rpcHandleFn:          rpcHandleFn,
		// watermarkUpdater:     newWatermarkUpdater(), //TODO
		tableMu: sync.RWMutex{},
		// getInsertWatermarkFn: getInsertWatermarkFn,
		// getFlushWatermarkFn:  getFlushWatermarkFn,
		// replayFn:             replayFn,
		// deleteFn:             deleteFn,
		mp: mp,
	}
	err := exec.setAsyncIndexLogTableID(ctx)
	if err != nil {
		return nil, err
	}
	err = exec.subscribeMOAsyncIndexLog(ctx)
	if err != nil {
		return nil, err
	}
	logtailreplay.RegisterRowsInsertHook(exec.onAsyncIndexLogInsert)
	return exec, nil
}
func (exec *CDCTaskExecutor2) setAsyncIndexLogTableID(ctx context.Context) (err error) {
	tenantId, err := defines.GetAccountId(ctx)
	txn, err := exec.txnFactory()
	if err != nil {
		return err
	}
	defer txn.Commit(ctx)

	tableIDSql := cdc.CDCSQLBuilder.GetTableIDSQL(
		tenantId,
		catalog.MO_CATALOG,
		MOAsyncIndexLogTableName,
	)
	result, err := ExecWithResult(ctx, tableIDSql, exec.cnUUID, txn)
	if err != nil {
		return err
	}
	defer result.Close()
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		if rows != 1 {
			panic(fmt.Sprintf("invalid rows %d", rows))
		}
		for i := 0; i < rows; i++ {
			exec.asyncIndexLogTableID = vector.MustFixedColWithTypeCheck[uint64](cols[0])[i]
		}
		return true
	})
	return nil
}
func (exec *CDCTaskExecutor2) subscribeMOAsyncIndexLog(ctx context.Context) (err error) {
	sql := cdc.CDCSQLBuilder.AsyncIndexLogSelectSQL()
	txn, err := exec.txnFactory()
	if err != nil {
		return err
	}
	defer txn.Commit(ctx)
	result, err := ExecWithResult(ctx, sql, exec.cnUUID, txn)
	if err != nil {
		return err
	}
	defer result.Close()
	return nil
}

type RpcHandleFn func(
	ctx context.Context,
	meta txn.TxnMeta,
	req *cmd_util.GetChangedTableListReq,
	resp *cmd_util.GetChangedTableListResp,
) (func(), error)

func (exec *CDCTaskExecutor2) SetRpcHandleFn(fn RpcHandleFn) {
	exec.rpcHandleFn = fn
}

// scan candidates
func (exec *CDCTaskExecutor2) getAllTables() []*TableInfo_2 {
	exec.tableMu.RLock()
	defer exec.tableMu.RUnlock()
	items := exec.tables.Items()
	return items
}

// get watermark, register new table, delete
func (exec *CDCTaskExecutor2) getTable(tableID uint64) (*TableInfo_2, bool) {
	exec.tableMu.RLock()
	defer exec.tableMu.RUnlock()
	return exec.tables.Get(&TableInfo_2{tableID: tableID})
}

func (exec *CDCTaskExecutor2) setTable(table *TableInfo_2) {
	exec.tableMu.Lock()
	defer exec.tableMu.Unlock()
	exec.tables.Set(table)
}
func (exec *CDCTaskExecutor2) deleteTableEntry(table *TableInfo_2) {
	exec.tableMu.Lock()
	defer exec.tableMu.Unlock()
	exec.tables.Delete(table)
}

func (exec *CDCTaskExecutor2) Resume() error {
	// restart
	return nil
}
func (exec *CDCTaskExecutor2) Pause() error {
	// stop
	return nil
}
func (exec *CDCTaskExecutor2) Cancel() error {
	// stop
	return nil
}
func (exec *CDCTaskExecutor2) Restart() error {
	// stop
	// restart
	return nil
}
func (exec *CDCTaskExecutor2) Start() {
	exec.replay(exec.ctx)
	exec.wg.Add(1)
	go exec.run()
}

func (exec *CDCTaskExecutor2) Stop() {
	exec.worker.Stop()
	exec.cancel()
	exec.wg.Wait()
}

func (exec *CDCTaskExecutor2) run() {
	defer exec.wg.Done()
	syncTaskTrigger := time.NewTicker(time.Millisecond * 100) // todo
	flushWatermarkTrigger := time.NewTicker(time.Hour)
	gcTrigger := time.NewTicker(time.Hour)
	for {
		select {
		case <-exec.ctx.Done():
			return
		case <-syncTaskTrigger.C:
			candidateTables := exec.getCandidateTables()
			tables, fromTSs, toTS, err := exec.getDirtyTables(exec.ctx, candidateTables, exec.cnUUID, exec.txnEngine)
			if err != nil {
				// logutil.Errorf("cdc task %s get dirty tables failed, err: %v", exec.spec.TaskName, err)
				logutil.Errorf("cdc task get dirty tables failed, err: %v", err)
				continue
			}
			for i, table := range candidateTables {
				_, ok := tables[table.tableID]
				if ok {
					iteration := table.GetSyncTask(exec.ctx, toTS)
					exec.worker.Submit(iteration)
				} else {
					from := types.TimestampToTS(fromTSs[i])
					table.UpdateWatermark(from, toTS)
				}
			}
		case <-flushWatermarkTrigger.C:
			err := exec.FlushWatermarkForAllTables()
			if err != nil {
				logutil.Errorf("flush watermark for all tables failed, err: %v", err)
			}
		case <-gcTrigger.C:
			err := exec.GC(time.Hour) // todo
			if err != nil {
				logutil.Errorf("gc failed, err: %v", err)
			}
		}
	}
}

// For UT
func (exec *CDCTaskExecutor2) GetWatermark(srcTableID uint64, indexName string) (watermark types.TS, ok bool) {
	table, ok := exec.getTable(srcTableID)
	if !ok {
		return
	}
	watermark, ok = table.GetWatermark(indexName)
	return
}

func (exec *CDCTaskExecutor2) onAsyncIndexLogInsert(ctx context.Context, input *api.Batch, tableID uint64) {
	if tableID != exec.asyncIndexLogTableID {
		return
	}
	// first 2 columns are rowID and commitTS
	accountIDVector, err := vector.ProtoVectorToVector(input.Vecs[2])
	if err != nil {
		panic(err)
	}
	accountIDs := vector.MustFixedColWithTypeCheck[uint32](accountIDVector)
	tableIDVector, err := vector.ProtoVectorToVector(input.Vecs[3])
	if err != nil {
		panic(err)
	}
	tableIDs := vector.MustFixedColWithTypeCheck[uint64](tableIDVector)
	indexNameVector, err := vector.ProtoVectorToVector(input.Vecs[4])
	if err != nil {
		panic(err)
	}
	watermarkVector, err := vector.ProtoVectorToVector(input.Vecs[5])
	if err != nil {
		panic(err)
	}
	errorCodeVector, err := vector.ProtoVectorToVector(input.Vecs[6])
	if err != nil {
		panic(err)
	}
	errorCodes := vector.MustFixedColWithTypeCheck[int32](errorCodeVector)
	consumerInfoVector, err := vector.ProtoVectorToVector(input.Vecs[10])
	if err != nil {
		panic(err)
	}
	dropAtVector, err := vector.ProtoVectorToVector(input.Vecs[9])
	if err != nil {
		panic(err)
	}
	for i, tid := range tableIDs {
		watermarkStr := watermarkVector.GetStringAt(i)
		watermark := types.StringToTS(watermarkStr)
		if watermark.IsEmpty() && errorCodes[i] == 0 && dropAtVector.IsNull(uint64(i)) {
			consumerInfoStr := consumerInfoVector.GetStringAt(i)
			go exec.addIndex(ctx, accountIDs[i], tid, watermarkStr, int(errorCodes[i]), consumerInfoStr)
		}
		if !dropAtVector.IsNull(uint64(i)) {
			indexName := indexNameVector.GetStringAt(i)
			go exec.deleteIndex(ctx, accountIDs[i], tid, indexName)
		}
	}

}

func (exec *CDCTaskExecutor2) replay(ctx context.Context) {
	sql := cdc.CDCSQLBuilder.AsyncIndexLogSelectSQL()
	txn, err := exec.txnFactory()
	if err != nil {
		return
	}
	defer txn.Commit(ctx)
	result, err := ExecWithResult(ctx, sql, exec.cnUUID, txn)
	if err != nil {
		return
	}
	defer result.Close()
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		accountIDs := vector.MustFixedColNoTypeCheck[uint32](cols[0])
		tableIDs := vector.MustFixedColNoTypeCheck[uint64](cols[1])
		watermarkVector := cols[3]
		errorCodes := vector.MustFixedColNoTypeCheck[int32](cols[4])
		consumerInfoVector := cols[8]
		dropAtVector := cols[7]
		for i := 0; i < rows; i++ {
			if !dropAtVector.IsNull(uint64(i)) {
				continue
			}
			watermarkStr := watermarkVector.GetStringAt(i)
			consumerInfoStr := consumerInfoVector.GetStringAt(i)
			exec.addIndex(
				ctx,
				accountIDs[i],
				tableIDs[i],
				watermarkStr,
				int(errorCodes[i]),
				consumerInfoStr,
			)
		}
		return true
	})
}

func (exec *CDCTaskExecutor2) addIndex(
	ctx context.Context,
	accountID uint32,
	tableID uint64,
	watermarkStr string,
	errorCode int,
	consumerInfoStr string,
) (err error) {
	consumerInfo := &ConsumerInfo{}
	err = json.Unmarshal([]byte(consumerInfoStr), consumerInfo)
	if err != nil {
		return
	}
	rel, err := exec.getTableByID(ctx, tableID)
	if err != nil {
		return
	}
	watermark := types.StringToTS(watermarkStr)
	tableDef := rel.GetTableDef(ctx)
	var table *TableInfo_2
	table, ok := exec.getTable(tableDef.TblId)
	if !ok {
		table = NewTableInfo_2(
			exec,
			accountID,
			tableDef.DbId,
			tableDef.TblId,
			tableDef.DbName,
			tableDef.Name,
			tableDef,
		)
		exec.setTable(table)
	}
	if errorCode != 0 {
		panic("logic error") // TODO: convert error
	}
	_, err = table.AddSinker(consumerInfo, watermark, nil)
	return
}

func (exec *CDCTaskExecutor2) deleteIndex(
	ctx context.Context,
	accountID uint32,
	tableID uint64,
	indexName string,
) (err error) {
	table, ok := exec.getTable(tableID)
	if !ok {
		return moerr.NewInternalError(ctx, "table not found")
	}
	empty, err := table.DeleteSinker(ctx, indexName)
	if err != nil {
		return
	}
	if empty {
		exec.deleteTableEntry(table)
	}
	return
}

func (exec *CDCTaskExecutor2) getRelation(
	ctx context.Context,
	txnOp client.TxnOperator,
	dbName, tableName string,
) (table engine.Relation, err error) {
	var db engine.Database
	if db, err = exec.txnEngine.Database(ctx, dbName, txnOp); err != nil {
		return
	}

	if table, err = db.Relation(ctx, tableName, nil); err != nil {
		return
	}
	return
}
func (exec *CDCTaskExecutor2) getTableByID(ctx context.Context, tableID uint64) (table engine.Relation, err error) {
	txn, err := exec.txnFactory()
	if err != nil {
		return
	}
	dbName, tableName, err := exec.txnEngine.GetNameById(ctx, txn, tableID)
	if err != nil {
		return
	}
	return exec.getRelation(ctx, txn, dbName, tableName)
}
func (exec *CDCTaskExecutor2) getCandidateTables() []*TableInfo_2 {
	ret := make([]*TableInfo_2, 0)
	items := exec.getAllTables()
	for _, t := range items {
		if !t.IsInitedAndFinished() {
			continue
		}
		ret = append(ret, t)
	}
	return ret
}
func (exec *CDCTaskExecutor2) getDirtyTables(
	ctx context.Context,
	candidateTables []*TableInfo_2,
	service string,
	eng engine.Engine,
) (tables map[uint64]struct{}, fromTS []timestamp.Timestamp, toTS types.TS, err error) {

	accs := make([]uint64, 0, len(candidateTables))
	dbs := make([]uint64, 0, len(candidateTables))
	tbls := make([]uint64, 0, len(candidateTables))
	fromTS = make([]timestamp.Timestamp, 0, len(candidateTables))
	for _, t := range candidateTables {
		accs = append(accs, uint64(t.accountID))
		dbs = append(dbs, t.dbID)
		tbls = append(tbls, t.tableID)
		fromTS = append(fromTS, t.GetMinWaterMark().ToTimestamp())
	}
	// tmpTS := types.TimestampToTS(exec.txnEngine.LatestLogtailAppliedTime())
	tables = make(map[uint64]struct{})
	err = disttae.GetChangedTableList(
		ctx,
		service,
		eng,
		accs,
		dbs,
		tbls,
		fromTS,
		&toTS,
		cmd_util.CheckChanged,
		func(
			accountID int64,
			databaseID int64,
			tableID int64,
			tableName string,
			dbName string,
			relKind string,
			pkSequence int,
			snapshot types.TS,
		) {
			tables[uint64(tableID)] = struct{}{}
		},
		exec.rpcHandleFn,
	)
	return
}

func (exec *CDCTaskExecutor2) FlushWatermarkForAllTables() error {
	tables := exec.getAllTables()
	if len(tables) == 0 {
		return nil
	}
	deleteSqlWriter := &bytes.Buffer{}
	insertSqlWriter := &bytes.Buffer{}
	deleteSqlWriter.WriteString("DELETE FROM mo_catalog.mo_async_index_log WHERE")
	insertSqlWriter.WriteString("INSERT INTO mo_catalog.mo_async_index_log " +
		"(account_id,table_id,index_name,last_sync_txn_ts,err_code,error_msg,info,consumer_config,drop_at) VALUES")
	for i, table := range tables {
		err := table.fillInAsyncIndexLogDeleteSQL(i == 0, deleteSqlWriter)
		if err != nil {
			return err
		}
		err = table.fillInAsyncIndexLogInsertSQL(i == 0, insertSqlWriter)
		if err != nil {
			return err
		}
	}
	deleteSql := deleteSqlWriter.String()
	insertSql := insertSqlWriter.String()
	txn, err := exec.txnFactory()
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			err2 := txn.Rollback(exec.ctx)
			if err2 != nil {
				logutil.Errorf("flush watermark for all tables rollback failed, err: %v", err2)
			}
		} else {
			err = txn.Commit(exec.ctx)
		}
	}()
	if _, err = ExecWithResult(exec.ctx, deleteSql, exec.cnUUID, txn); err != nil {
		return err
	}
	if _, err = ExecWithResult(exec.ctx, insertSql, exec.cnUUID, txn); err != nil {
		return err
	}
	return nil
}

func (exec *CDCTaskExecutor2) GC(cleanupThreshold time.Duration) (err error) {
	txn, err := exec.txnFactory()
	if err != nil {
		return err
	}
	defer txn.Commit(exec.ctx)
	asyncIndexLogGCSql := cdc.CDCSQLBuilder.AsyncIndexLogGCSQL(time.Now().Add(-cleanupThreshold))
	if _, err = ExecWithResult(exec.ctx, asyncIndexLogGCSql, exec.cnUUID, txn); err != nil {
		return err
	}
	asyncIndexIterationsGCSql := cdc.CDCSQLBuilder.AsyncIndexIterationsGCSQL(time.Now().Add(-cleanupThreshold))
	if _, err = ExecWithResult(exec.ctx, asyncIndexIterationsGCSql, exec.cnUUID, txn); err != nil {
		return err
	}
	return err
}

func (exec *CDCTaskExecutor2) String() string {
	tables := exec.getAllTables()
	str := "CDC Task\n"
	for _, t := range tables {
		str += t.String()
	}
	return str
}
