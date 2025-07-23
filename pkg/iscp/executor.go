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

package iscp

import (
	"bytes"
	"context"

	"go.uber.org/zap"
	"sync"
	"time"

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
	MOIntraSystemChangePropagationLogTableName = catalog.MO_INTRA_SYSTEM_CHANGE_PROPAGATION_LOG
)

const (
	DefaultGCInterval             = time.Hour
	DefaultGCTTL                  = time.Hour
	DefaultSyncTaskInterval       = time.Millisecond * 100
	DefaultFlushWatermarkInterval = time.Hour

	DefaultRetryTimes    = 5
	DefaultRetryDuration = time.Second
)

type ISCPExecutorOption struct {
	GCInterval             time.Duration
	GCTTL                  time.Duration
	SyncTaskInterval       time.Duration
	FlushWatermarkInterval time.Duration
	RetryTimes             int
}

type TxnFactory func() (client.TxnOperator, error)

type ISCPTaskExecutor struct {
	tables               *btree.BTreeG[*TableEntry]
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
	option *ISCPExecutorOption

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
		return GetTxnOp(ctx, cnEngine, cnTxnClient, "default async index iscp executor")
	}
}

func AsyncIndexISCPTaskExecutorFactory(
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	attachToTask func(context.Context, uint64, taskservice.ActiveRoutine) error,
	cdUUID string,
	mp *mpool.MPool,
) func(ctx context.Context, task task.Task) (err error) {
	return func(ctx context.Context, task task.Task) (err error) {
		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
		exec, err := NewISCPTaskExecutor(
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
		exec.run(ctx)
		return nil
	}
}

func NewISCPTaskExecutor(
	ctx context.Context,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	cdUUID string,
	txnFactory func() (client.TxnOperator, error),
	option *ISCPExecutorOption,
	mp *mpool.MPool,
) (exec *ISCPTaskExecutor, err error) {
	defer func() {
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"Async-Index-ISCP-Task Executor init",
			zap.Any("gcInterval", option.GCInterval),
			zap.Any("gcttl", option.GCTTL),
			zap.Any("syncTaskInterval", option.SyncTaskInterval),
			zap.Any("flushWatermarkInterval", option.FlushWatermarkInterval),
			zap.Any("retryTimes", option.RetryTimes),
			zap.Error(err),
		)
	}()
	if option == nil {
		option = &ISCPExecutorOption{
			GCInterval:             DefaultGCInterval,
			GCTTL:                  DefaultGCTTL,
			SyncTaskInterval:       DefaultSyncTaskInterval,
			FlushWatermarkInterval: DefaultFlushWatermarkInterval,
			RetryTimes:             DefaultRetryTimes,
		}
	}
	if txnFactory == nil {
		txnFactory = GetTxnFactory(ctx, txnEngine, cnTxnClient)
	}
	exec = &ISCPTaskExecutor{
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
	logtailreplay.RegisterRowsInsertHook(exec.onISCPLogInsert)
	return exec, nil
}

func (exec *ISCPTaskExecutor) setAsyncIndexLogTableID(ctx context.Context) (err error) {
	tenantId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	txn, err := exec.txnFactory()
	if err != nil {
		return err
	}
	defer txn.Commit(ctx)

	tableID, err := getTableID(ctx, exec.cnUUID, txn, tenantId, catalog.MO_CATALOG, MOIntraSystemChangePropagationLogTableName)
	if err != nil {
		return err
	}
	exec.asyncIndexLogTableID = tableID
	return nil
}
func (exec *ISCPTaskExecutor) subscribeMOAsyncIndexLog(ctx context.Context) (err error) {
	sql := cdc.CDCSQLBuilder.IntraSystemChangePropagationLogSelectSQL()
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

func (exec *ISCPTaskExecutor) SetRpcHandleFn(fn RpcHandleFn) {
	exec.rpcHandleFn = fn
}

// scan candidates
func (exec *ISCPTaskExecutor) getAllTables() []*TableEntry {
	exec.tableMu.RLock()
	defer exec.tableMu.RUnlock()
	items := exec.tables.Items()
	return items
}

// get watermark, register new table, delete
func (exec *ISCPTaskExecutor) getTable(accountID uint32, tableID uint64) (*TableEntry, bool) {
	exec.tableMu.RLock()
	defer exec.tableMu.RUnlock()
	return exec.tables.Get(&TableEntry{accountID: accountID, tableID: tableID})
}

func (exec *ISCPTaskExecutor) setTable(table *TableEntry) {
	exec.tableMu.Lock()
	defer exec.tableMu.Unlock()
	exec.tables.Set(table)
}
func (exec *ISCPTaskExecutor) deleteTableEntry(table *TableEntry) {
	exec.tableMu.Lock()
	defer exec.tableMu.Unlock()
	exec.tables.Delete(table)
}

func (exec *ISCPTaskExecutor) Resume() error {
	exec.Start()
	return nil
}
func (exec *ISCPTaskExecutor) Pause() error {
	exec.Stop()
	return nil
}
func (exec *ISCPTaskExecutor) Cancel() error {
	exec.Stop()
	return nil
}
func (exec *ISCPTaskExecutor) Restart() error {
	exec.Stop()
	exec.Start()
	return nil
}
func (exec *ISCPTaskExecutor) Start() {
	exec.runningMu.Lock()
	defer exec.runningMu.Unlock()
	if exec.running {
		return
	}
	exec.initStateLocked()
	go exec.run(context.Background())
}

func (exec *ISCPTaskExecutor) initStateLocked() {
	exec.running = true
	logutil.Info(
		"Async-Index-ISCP-Task Start",
	)
	ctx, cancel := context.WithCancel(context.Background())
	worker := NewWorker()
	exec.worker = worker
	exec.ctx = ctx
	exec.cancel = cancel
	exec.replay(exec.ctx)
	exec.wg.Add(1)

}

func (exec *ISCPTaskExecutor) Stop() {
	exec.runningMu.Lock()
	defer exec.runningMu.Unlock()
	if !exec.running {
		return
	}
	exec.running = false
	logutil.Info(
		"Async-Index-ISCP-Task Stop",
	)
	exec.worker.Stop()
	exec.cancel()
	exec.wg.Wait()
	exec.ctx, exec.cancel = nil, nil
	exec.worker = nil
}

func (exec *ISCPTaskExecutor) run(ctx context.Context) {
	logutil.Info(
		"Async-Index-ISCP-Task Run",
	)
	defer func() {
		logutil.Info(
			"Async-Index-ISCP-Task Run Done",
		)
	}()
	defer exec.wg.Done()
	syncTaskTrigger := time.NewTicker(exec.option.SyncTaskInterval)
	flushWatermarkTrigger := time.NewTicker(exec.option.FlushWatermarkInterval)
	gcTrigger := time.NewTicker(exec.option.GCTTL)
	for {
		select {
		case <-ctx.Done():
			return
		case <-exec.ctx.Done():
			return
		case <-syncTaskTrigger.C:
			// get candidate iterations and tables
			iterations, candidateTables, fromTSs := exec.getCandidateTables()
			// check if there are any dirty tables
			tables, toTS, err := exec.getDirtyTables(exec.ctx, candidateTables, fromTSs, exec.cnUUID, exec.txnEngine)
			if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "getDirtyTables" {
				err = moerr.NewInternalErrorNoCtx(msg)
			}
			if err != nil {
				logutil.Error(
					"Async-Index-ISCP-Task get dirty tables failed",
					zap.Error(err),
				)
				continue
			}
			// run iterations with dirty table
			// update watermark for clean tables
			for _, iter := range iterations {
				maxTS := types.MaxTs()
				if iter.to.EQ(&maxTS) {
					iter.to = toTS
				}
				_, ok := tables[iter.table.tableID]
				if ok {
					iter.Init()
					exec.worker.Submit(iter)
				} else {
					iter.table.UpdateWatermark(iter)
				}
			}
		case <-flushWatermarkTrigger.C:
			err := exec.FlushWatermarkForAllTables()
			if err != nil {
				logutil.Error(
					"Async-Index-ISCP-Task flush watermark failed",
					zap.Error(err),
				)
			}
		case <-gcTrigger.C:
			err := exec.GC(time.Hour) // todo
			if err != nil {
				logutil.Error(
					"Async-Index-ISCP-Task gc failed",
					zap.Error(err),
				)
			}
		}
	}
}

// For UT
func (exec *ISCPTaskExecutor) GetWatermark(accountID uint32, srcTableID uint64, indexName string) (watermark types.TS, ok bool) {
	table, ok := exec.getTable(accountID, srcTableID)
	if !ok {
		return
	}
	watermark, ok = table.GetWatermark(indexName)
	return
}

func (exec *ISCPTaskExecutor) onISCPLogInsert(ctx context.Context, input *api.Batch, tableID uint64) {
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
	watermarkVector, err := vector.ProtoVectorToVector(input.Vecs[7])
	if err != nil {
		panic(err)
	}
	errorCodeVector, err := vector.ProtoVectorToVector(input.Vecs[8])
	if err != nil {
		panic(err)
	}
	errorCodes := vector.MustFixedColWithTypeCheck[int32](errorCodeVector)
	consumerInfoVector, err := vector.ProtoVectorToVector(input.Vecs[12])
	if err != nil {
		panic(err)
	}
	dropAtVector, err := vector.ProtoVectorToVector(input.Vecs[11])
	if err != nil {
		panic(err)
	}
	jobConfigVector, err := vector.ProtoVectorToVector(input.Vecs[5])
	if err != nil {
		panic(err)
	}
	for i, tid := range tableIDs {
		watermarkStr := watermarkVector.GetStringAt(i)
		watermark := types.StringToTS(watermarkStr)
		if watermark.IsEmpty() && errorCodes[i] == 0 && dropAtVector.IsNull(uint64(i)) {
			consumerInfoStr := consumerInfoVector.GetStringAt(i)
			jobConfigStr := jobConfigVector.GetStringAt(i)
			go retry(
				func() error {
					return exec.addIndex(accountIDs[i], tid, watermarkStr, int(errorCodes[i]), consumerInfoStr, jobConfigStr)
				},
				exec.option.RetryTimes,
			)
		}
		if !dropAtVector.IsNull(uint64(i)) {
			indexName := indexNameVector.GetStringAt(i)
			go retry(
				func() error {
					return exec.deleteIndex(accountIDs[i], tid, indexName)
				},
				exec.option.RetryTimes,
			)
		}
	}

}

func (exec *ISCPTaskExecutor) replay(ctx context.Context) {
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
			"Async-Index-ISCP-Task replay",
			zap.Int("indexCount", indexCount),
			zap.Error(err),
		)
	}()
	sql := cdc.CDCSQLBuilder.IntraSystemChangePropagationLogSelectSQL()
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
		watermarkVector := cols[5]
		errorCodes := vector.MustFixedColNoTypeCheck[int32](cols[6])
		consumerInfoVector := cols[10]
		dropAtVector := cols[9]
		jobConfigVector := cols[3]
		for i := 0; i < rows; i++ {
			if !dropAtVector.IsNull(uint64(i)) {
				continue
			}
			indexCount++
			watermarkStr := watermarkVector.GetStringAt(i)
			consumerInfoStr := consumerInfoVector.GetStringAt(i)
			jobConfigStr := jobConfigVector.GetStringAt(i)
			retry(
				func() error {
					return exec.addIndex(
						accountIDs[i],
						tableIDs[i],
						watermarkStr,
						int(errorCodes[i]),
						consumerInfoStr,
						jobConfigStr,
					)
				},
				exec.option.RetryTimes,
			)
		}
		return true
	})
}

func (exec *ISCPTaskExecutor) addIndex(
	accountID uint32,
	tableID uint64,
	watermarkStr string,
	errorCode int,
	consumerInfoStr string,
	jobConfigStr string,
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
			"Async-Index-ISCP-Task add index",
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
	consumerInfo, err := UnmarshalConsumerConfig([]byte(consumerInfoStr))
	if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "addIndex" {
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
	jobConfig, err := UnmarshalJobConfig([]byte(jobConfigStr))
	if err != nil {
		return
	}
	watermark := types.StringToTS(watermarkStr)
	tableDef := rel.GetTableDef(ctx)
	var table *TableEntry
	table, ok := exec.getTable(accountID, tableDef.TblId)
	if !ok {
		table = NewTableEntry(
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
	ok, err = table.AddSinker(consumerInfo, jobConfig, watermark, nil)
	if !ok {
		return moerr.NewInternalErrorNoCtx("sinker already exists")
	}
	return
}

func (exec *ISCPTaskExecutor) deleteIndex(
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
			"Async-Index-ISCP-Task delete index",
			zap.Uint32("accountID", accountID),
			zap.Uint64("tableID", tableID),
			zap.String("indexName", indexName),
			zap.Error(err),
		)
	}()
	table, ok := exec.getTable(accountID, tableID)
	if !ok {
		return moerr.NewInternalErrorNoCtx("table not found")
	}
	empty, err := table.DeleteSinker(indexName)
	if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "deleteIndex" {
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

func (exec *ISCPTaskExecutor) getRelation(
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
func (exec *ISCPTaskExecutor) getTableByID(ctx context.Context, tableID uint64) (table engine.Relation, err error) {
	txn, err := exec.txnFactory()
	if err != nil {
		return
	}
	_, _, table, err = cdc.GetRelationById(ctx, exec.txnEngine, txn, tableID)
	if err != nil {
		return
	}
	return
}
func (exec *ISCPTaskExecutor) getCandidateTables() ([]*Iteration, []*TableEntry, []types.TS) {
	tables := make([]*TableEntry, 0)
	fromTSs := make([]types.TS, 0)
	iterations := make([]*Iteration, 0)
	items := exec.getAllTables()
	for _, t := range items {
		if t.IsEmpty() {
			continue
		}
		iters, fromTS := t.getCandidate()
		if len(iters) > 0 {
			iterations = append(iterations, iters...)
			tables = append(tables, t)
			fromTSs = append(fromTSs, fromTS)
		}
	}
	return iterations, tables, fromTSs
}
func (exec *ISCPTaskExecutor) getDirtyTables(
	ctx context.Context,
	candidateTables []*TableEntry,
	fromTSs []types.TS,
	service string,
	eng engine.Engine,
) (tables map[uint64]struct{}, toTS types.TS, err error) {

	accs := make([]uint64, 0, len(candidateTables))
	dbs := make([]uint64, 0, len(candidateTables))
	tbls := make([]uint64, 0, len(candidateTables))
	fromTimestamps := make([]timestamp.Timestamp, 0, len(candidateTables))
	for i, t := range candidateTables {
		accs = append(accs, uint64(t.accountID))
		dbs = append(dbs, t.dbID)
		tbls = append(tbls, t.tableID)
		fromTimestamps = append(fromTimestamps, fromTSs[i].ToTimestamp())
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
		fromTimestamps,
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

func (exec *ISCPTaskExecutor) FlushWatermarkForAllTables() error {
	tables := exec.getAllTables()
	if len(tables) == 0 {
		return nil
	}
	deleteSqlWriter := &bytes.Buffer{}
	insertSqlWriter := &bytes.Buffer{}
	deleteSqlWriter.WriteString("DELETE FROM mo_catalog.mo_intra_system_change_propagation_log WHERE")
	insertSqlWriter.WriteString("INSERT INTO mo_catalog.mo_intra_system_change_propagation_log " +
		"(account_id,table_id,column_names,job_name,job_config,last_sync_txn_ts,err_code,error_msg,info,consumer_config,drop_at) VALUES")
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
		"Async-Index-ISCP-Task flush watermark",
		zap.Any("table count", len(tables)),
	)
	return nil
}

func (exec *ISCPTaskExecutor) GC(cleanupThreshold time.Duration) (err error) {
	txn, err := exec.txnFactory()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(exec.ctx, time.Minute*5)
	defer cancel()
	defer txn.Commit(ctx)
	gcTime := time.Now().Add(-cleanupThreshold)
	asyncIndexLogGCSql := cdc.CDCSQLBuilder.IntraSystemChangePropagationLogGCSQL(gcTime)
	if _, err = ExecWithResult(ctx, asyncIndexLogGCSql, exec.cnUUID, txn); err != nil {
		return err
	}
	logutil.Info(
		"Async-Index-ISCP-Task GC",
		zap.Any("gcTime", gcTime),
	)
	return err
}

func (exec *ISCPTaskExecutor) String() string {
	tables := exec.getAllTables()
	str := "ISCP Task\n"
	for _, t := range tables {
		str += t.String()
	}
	return str
}

func retry(fn func() error, retryTimes int) (err error) {
	for i := 0; i < retryTimes; i++ {
		err = fn()
		if err == nil {
			return
		}
		time.Sleep(DefaultRetryDuration)
	}
	logutil.Errorf("Async-Index-ISCP-Task retry failed, err: %v", err)
	return
}
