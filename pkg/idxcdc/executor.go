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

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/taskservice"

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

const (
	DefaultGCInterval             = time.Hour
	DefaultGCTTL                  = time.Hour
	DefaultSyncTaskInterval       = time.Millisecond * 100
	DefaultFlushWatermarkInterval = time.Hour
)

type CDCExecutorOption struct {
	GCInterval             time.Duration
	GCTTL                  time.Duration
	SyncTaskInterval       time.Duration
	FlushWatermarkInterval time.Duration
}

type TxnFactory func() (client.TxnOperator, error)

type CDCTaskExecutor struct {
	tables               *btree.BTreeG[*TableInfo_2]
	tableMu              sync.RWMutex
	packer               *types.Packer
	mp                   *mpool.MPool
	cnUUID               string
	txnFactory           func() (client.TxnOperator, error)
	txnEngine            engine.Engine
	asyncIndexLogTableID uint64

	rpcHandleFn func(
		ctx context.Context,
		meta txn.TxnMeta,
		req *cmd_util.GetChangedTableListReq,
		resp *cmd_util.GetChangedTableListResp,
	) (func(), error) // for test
	option *CDCExecutorOption

	ctx    context.Context
	cancel context.CancelFunc

	worker Worker
	wg     sync.WaitGroup

	running   bool
	runningMu sync.Mutex
}

func GetTxnFactory(
	ctx context.Context,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
) func() (client.TxnOperator, error) {
	return func() (client.TxnOperator, error) {
		return GetTxnOp(ctx, cnEngine, cnTxnClient, "default async index cdc executor")
	}
}

func AsyncIndexCdcTaskExecutorFactory(
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	attachToTask func(context.Context, uint64, taskservice.ActiveRoutine) error,
	cdUUID string,
	mp *mpool.MPool,
) func(ctx context.Context, task task.Task) (err error) {
	return func(ctx context.Context, task task.Task) (err error) {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
		exec, err := NewCDCTaskExecutor(
			ctx,
			txnEngine,
			cnTxnClient,
			cdUUID,
			GetTxnFactory(ctx, txnEngine, cnTxnClient),
			nil,
			mp,
		)
		if err != nil {
			return err
		}
		attachToTask(ctx, task.GetID(), exec)

		exec.runningMu.Lock()
		defer exec.runningMu.Unlock()
		if exec.running {
			return nil
		}
		exec.initStateLocked()
		exec.run()
		return nil
	}
}

func NewCDCTaskExecutor(
	ctx context.Context,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	cdUUID string,
	txnFactory func() (client.TxnOperator, error),
	option *CDCExecutorOption,
	mp *mpool.MPool,
) (exec *CDCTaskExecutor, err error) {
	defer func() {
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"Async-Index-CDC-Task Executor init",
			zap.Any("gcInterval", option.GCInterval),
			zap.Any("gcttl", option.GCTTL),
			zap.Any("syncTaskInterval", option.SyncTaskInterval),
			zap.Any("flushWatermarkInterval", option.FlushWatermarkInterval),
			zap.Error(err),
		)
	}()
	if option == nil {
		option = &CDCExecutorOption{
			GCInterval:             DefaultGCInterval,
			GCTTL:                  DefaultGCTTL,
			SyncTaskInterval:       DefaultSyncTaskInterval,
			FlushWatermarkInterval: DefaultFlushWatermarkInterval,
		}
	}
	if txnFactory == nil {
		txnFactory = GetTxnFactory(ctx, txnEngine, cnTxnClient)
	}
	exec = &CDCTaskExecutor{
		ctx:        ctx,
		packer:     types.NewPacker(),
		tables:     btree.NewBTreeGOptions(tableInfoLess, btree.Options{NoLocks: true}),
		cnUUID:     cdUUID,
		txnFactory: txnFactory,
		txnEngine:  txnEngine,
		wg:         sync.WaitGroup{},
		tableMu:    sync.RWMutex{},
		option:     option,
		mp:         mp,
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	err = exec.setAsyncIndexLogTableID(ctx)
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

func (exec *CDCTaskExecutor) setAsyncIndexLogTableID(ctx context.Context) (err error) {
	tenantId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
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
func (exec *CDCTaskExecutor) subscribeMOAsyncIndexLog(ctx context.Context) (err error) {
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

func (exec *CDCTaskExecutor) SetRpcHandleFn(fn RpcHandleFn) {
	exec.rpcHandleFn = fn
}

// scan candidates
func (exec *CDCTaskExecutor) getAllTables() []*TableInfo_2 {
	exec.tableMu.RLock()
	defer exec.tableMu.RUnlock()
	items := exec.tables.Items()
	return items
}

// get watermark, register new table, delete
func (exec *CDCTaskExecutor) getTable(tableID uint64) (*TableInfo_2, bool) {
	exec.tableMu.RLock()
	defer exec.tableMu.RUnlock()
	return exec.tables.Get(&TableInfo_2{tableID: tableID})
}

func (exec *CDCTaskExecutor) setTable(table *TableInfo_2) {
	exec.tableMu.Lock()
	defer exec.tableMu.Unlock()
	exec.tables.Set(table)
}
func (exec *CDCTaskExecutor) deleteTableEntry(table *TableInfo_2) {
	exec.tableMu.Lock()
	defer exec.tableMu.Unlock()
	exec.tables.Delete(table)
}

func (exec *CDCTaskExecutor) Resume() error {
	exec.Start()
	return nil
}
func (exec *CDCTaskExecutor) Pause() error {
	exec.Stop()
	return nil
}
func (exec *CDCTaskExecutor) Cancel() error {
	exec.Stop()
	return nil
}
func (exec *CDCTaskExecutor) Restart() error {
	exec.Stop()
	exec.Start()
	return nil
}
func (exec *CDCTaskExecutor) Start() {
	exec.runningMu.Lock()
	defer exec.runningMu.Unlock()
	if exec.running {
		return
	}
	exec.initStateLocked()
	go exec.run()
}

func (exec *CDCTaskExecutor) initStateLocked() {
	exec.running = true
	logutil.Info(
		"Async-Index-CDC-Task Start",
	)
	ctx, cancel := context.WithCancel(context.Background())
	worker := NewWorker()
	exec.worker = worker
	exec.ctx = ctx
	exec.cancel = cancel
	exec.replay(exec.ctx)
	exec.wg.Add(1)

}

func (exec *CDCTaskExecutor) Stop() {
	exec.runningMu.Lock()
	defer exec.runningMu.Unlock()
	if !exec.running {
		return
	}
	exec.running = false
	logutil.Info(
		"Async-Index-CDC-Task Stop",
	)
	exec.worker.Stop()
	exec.cancel()
	exec.wg.Wait()
	exec.ctx, exec.cancel = nil, nil
	exec.worker = nil
}

func (exec *CDCTaskExecutor) run() {
	logutil.Info(
		"Async-Index-CDC-Task Run",
	)
	defer exec.wg.Done()
	syncTaskTrigger := time.NewTicker(exec.option.SyncTaskInterval)
	flushWatermarkTrigger := time.NewTicker(exec.option.FlushWatermarkInterval)
	gcTrigger := time.NewTicker(exec.option.GCTTL)
	for {
		select {
		case <-exec.ctx.Done():
			return
		case <-syncTaskTrigger.C:
			candidateTables := exec.getCandidateTables()
			tables, fromTSs, toTS, err := exec.getDirtyTables(exec.ctx, candidateTables, exec.cnUUID, exec.txnEngine)
			if msg, injected := objectio.CDCExecutorInjected(); injected && msg == "getDirtyTables" {
				err = moerr.NewInternalErrorNoCtx(msg)
			}
			if err != nil {
				logutil.Error(
					"Async-Index-CDC-Task get dirty tables failed",
					zap.Error(err),
				)
				continue
			}
			for i, table := range candidateTables {
				_, ok := tables[table.tableID]
				if ok {
					iteration := table.GetSyncTask(exec.ctx, toTS)
					if iteration == nil {
						continue
					}
					exec.worker.Submit(iteration)
				} else {
					from := types.TimestampToTS(fromTSs[i])
					table.UpdateWatermark(from, toTS)
				}
			}
		case <-flushWatermarkTrigger.C:
			err := exec.FlushWatermarkForAllTables()
			if err != nil {
				logutil.Error(
					"Async-Index-CDC-Task flush watermark failed",
					zap.Error(err),
				)
			}
		case <-gcTrigger.C:
			err := exec.GC(time.Hour) // todo
			if err != nil {
				logutil.Error(
					"Async-Index-CDC-Task gc failed",
					zap.Error(err),
				)
			}
		}
	}
}

// For UT
func (exec *CDCTaskExecutor) GetWatermark(srcTableID uint64, indexName string) (watermark types.TS, ok bool) {
	table, ok := exec.getTable(srcTableID)
	if !ok {
		return
	}
	watermark, ok = table.GetWatermark(indexName)
	return
}

func (exec *CDCTaskExecutor) onAsyncIndexLogInsert(ctx context.Context, input *api.Batch, tableID uint64) {
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
	watermarkVector, err := vector.ProtoVectorToVector(input.Vecs[6])
	if err != nil {
		panic(err)
	}
	errorCodeVector, err := vector.ProtoVectorToVector(input.Vecs[7])
	if err != nil {
		panic(err)
	}
	errorCodes := vector.MustFixedColWithTypeCheck[int32](errorCodeVector)
	consumerInfoVector, err := vector.ProtoVectorToVector(input.Vecs[11])
	if err != nil {
		panic(err)
	}
	dropAtVector, err := vector.ProtoVectorToVector(input.Vecs[10])
	if err != nil {
		panic(err)
	}
	for i, tid := range tableIDs {
		watermarkStr := watermarkVector.GetStringAt(i)
		watermark := types.StringToTS(watermarkStr)
		if watermark.IsEmpty() && errorCodes[i] == 0 && dropAtVector.IsNull(uint64(i)) {
			consumerInfoStr := consumerInfoVector.GetStringAt(i)
			go exec.addIndex(accountIDs[i], tid, watermarkStr, int(errorCodes[i]), consumerInfoStr)
		}
		if !dropAtVector.IsNull(uint64(i)) {
			indexName := indexNameVector.GetStringAt(i)
			go exec.deleteIndex(accountIDs[i], tid, indexName)
		}
	}

}

func (exec *CDCTaskExecutor) replay(ctx context.Context) {
	var err error
	indexCount := 0
	defer func() {
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"Async-Index-CDC-Task replay",
			zap.Int("indexCount", indexCount),
			zap.Error(err),
		)
	}()
	sql := cdc.CDCSQLBuilder.AsyncIndexLogSelectSQL()
	txn, err := exec.txnFactory()
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
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
			indexCount++
			watermarkStr := watermarkVector.GetStringAt(i)
			consumerInfoStr := consumerInfoVector.GetStringAt(i)
			exec.addIndex(
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

func (exec *CDCTaskExecutor) addIndex(
	accountID uint32,
	tableID uint64,
	watermarkStr string,
	errorCode int,
	consumerInfoStr string,
) (err error) {
	var indexName string
	defer func() {
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"Async-Index-CDC-Task add index",
			zap.Uint32("accountID", accountID),
			zap.Uint64("tableID", tableID),
			zap.String("indexName", indexName),
			zap.String("watermark", watermarkStr),
			zap.Int("errorCode", errorCode),
			zap.Error(err),
		)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountID)
	consumerInfo := &ConsumerInfo{}
	err = json.Unmarshal([]byte(consumerInfoStr), consumerInfo)
	if msg, injected := objectio.CDCExecutorInjected(); injected && msg == "addIndex" {
		err = moerr.NewInternalErrorNoCtx(msg)
	}
	if err != nil {
		return
	}
	indexName = consumerInfo.IndexName
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
	ok, err = table.AddSinker(consumerInfo, watermark, nil)
	if !ok {
		return moerr.NewInternalErrorNoCtx("sinker already exists")
	}
	return
}

func (exec *CDCTaskExecutor) deleteIndex(
	accountID uint32,
	tableID uint64,
	indexName string,
) (err error) {
	defer func() {
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"Async-Index-CDC-Task delete index",
			zap.Uint32("accountID", accountID),
			zap.Uint64("tableID", tableID),
			zap.String("indexName", indexName),
			zap.Error(err),
		)
	}()
	table, ok := exec.getTable(tableID)
	if !ok {
		return moerr.NewInternalErrorNoCtx("table not found")
	}
	empty, err := table.DeleteSinker(indexName)
	if msg, injected := objectio.CDCExecutorInjected(); injected && msg == "deleteIndex" {
		err = moerr.NewInternalErrorNoCtx(msg)
	}
	if err != nil {
		return
	}
	if empty {
		exec.deleteTableEntry(table)
	}
	return
}

func (exec *CDCTaskExecutor) getRelation(
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
func (exec *CDCTaskExecutor) getTableByID(ctx context.Context, tableID uint64) (table engine.Relation, err error) {
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
func (exec *CDCTaskExecutor) getCandidateTables() []*TableInfo_2 {
	ret := make([]*TableInfo_2, 0)
	items := exec.getAllTables()
	for _, t := range items {
		if t.IsEmpty() {
			continue
		}
		if !t.IsInitedAndFinished() {
			continue
		}
		ret = append(ret, t)
	}
	return ret
}
func (exec *CDCTaskExecutor) getDirtyTables(
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

func (exec *CDCTaskExecutor) FlushWatermarkForAllTables() error {
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
		err := table.fillInAsyncIndexLogUpdateSQL(i == 0, insertSqlWriter, deleteSqlWriter)
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
	ctx, cancel := context.WithTimeout(exec.ctx, time.Minute*5)
	defer cancel()
	defer func() {
		if err != nil {
			err2 := txn.Rollback(ctx)
			if err2 != nil {
				logutil.Errorf("flush watermark for all tables rollback failed, err: %v", err2)
			}
		} else {
			err = txn.Commit(ctx)
		}
	}()
	if _, err = ExecWithResult(ctx, deleteSql, exec.cnUUID, txn); err != nil {
		return err
	}
	if _, err = ExecWithResult(ctx, insertSql, exec.cnUUID, txn); err != nil {
		return err
	}
	logutil.Info(
		"Async-Index-CDC-Task flush watermark",
		zap.Any("table count", len(tables)),
	)
	return nil
}

func (exec *CDCTaskExecutor) GC(cleanupThreshold time.Duration) (err error) {
	txn, err := exec.txnFactory()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(exec.ctx, time.Minute*5)
	defer cancel()
	defer txn.Commit(ctx)
	gcTime := time.Now().Add(-cleanupThreshold)
	asyncIndexLogGCSql := cdc.CDCSQLBuilder.AsyncIndexLogGCSQL(gcTime)
	if _, err = ExecWithResult(ctx, asyncIndexLogGCSql, exec.cnUUID, txn); err != nil {
		return err
	}
	asyncIndexIterationsGCSql := cdc.CDCSQLBuilder.AsyncIndexIterationsGCSQL(time.Now().Add(-cleanupThreshold))
	if _, err = ExecWithResult(ctx, asyncIndexIterationsGCSql, exec.cnUUID, txn); err != nil {
		return err
	}
	logutil.Info(
		"Async-Index-CDC-Task GC",
		zap.Any("gcTime", gcTime),
	)
	return err
}

func (exec *CDCTaskExecutor) String() string {
	tables := exec.getAllTables()
	str := "CDC Task\n"
	for _, t := range tables {
		str += t.String()
	}
	return str
}
