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
	"context"

	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/taskservice"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"

	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"

	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/tidwall/btree"
)

const (
	MOISCPLogTableName = catalog.MO_ISCP_LOG
)

const (
	DefaultGCInterval             = time.Hour
	DefaultGCTTL                  = time.Hour
	DefaultSyncTaskInterval       = time.Second * 10
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

func GetTxnFactory(
	ctx context.Context,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
) func() (client.TxnOperator, error) {
	return func() (client.TxnOperator, error) {
		return GetTxnOp(ctx, cnEngine, cnTxnClient, "default iscp executor")
	}
}

func ISCPTaskExecutorFactory(
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
			"ISCP-Task Executor init",
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
		ctx:         ctx,
		packer:      types.NewPacker(),
		tables:      btree.NewBTreeGOptions(tableInfoLess, btree.Options{NoLocks: true}),
		cnUUID:      cdUUID,
		txnFactory:  txnFactory,
		txnEngine:   txnEngine,
		cnTxnClient: cnTxnClient,
		wg:          sync.WaitGroup{},
		tableMu:     sync.RWMutex{},
		option:      option,
		mp:          mp,
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	err = exec.setISCPLogTableID(ctx)
	if err != nil {
		return nil, err
	}
	return exec, nil
}

func (exec *ISCPTaskExecutor) setISCPLogTableID(ctx context.Context) (err error) {
	tenantId, err := defines.GetAccountId(ctx)
	if err != nil {
		return err
	}
	txn, err := exec.txnFactory()
	if err != nil {
		return err
	}
	defer txn.Commit(ctx)

	tableID, err := getTableID(ctx, exec.cnUUID, txn, tenantId, catalog.MO_CATALOG, MOISCPLogTableName)
	if err != nil {
		return err
	}
	exec.iscpLogTableID = tableID
	return nil
}
func (exec *ISCPTaskExecutor) subscribeMOISCPLog(ctx context.Context) (err error) {
	sql := cdc.CDCSQLBuilder.ISCPLogSelectSQL()
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
		"ISCP-Task Start",
	)
	ctx, cancel := context.WithCancel(context.Background())
	worker := NewWorker(exec.cnUUID, exec.txnEngine, exec.cnTxnClient, exec.mp)
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
		"ISCP-Task Stop",
	)
	exec.worker.Stop()
	exec.cancel()
	exec.wg.Wait()
	exec.ctx, exec.cancel = nil, nil
	exec.worker = nil
}

func (exec *ISCPTaskExecutor) run(ctx context.Context) {
	logutil.Info(
		"ISCP-Task Run",
	)
	defer func() {
		logutil.Info(
			"ISCP-Task Run Done",
		)
	}()
	defer exec.wg.Done()
	syncTaskTrigger := time.NewTicker(exec.option.SyncTaskInterval)
	gcTrigger := time.NewTicker(exec.option.GCInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-exec.ctx.Done():
			return
		case <-syncTaskTrigger.C:
			// apply iscp log
			from := exec.iscpLogWm
			to := types.TimestampToTS(exec.txnEngine.LatestLogtailAppliedTime())
			err := exec.applyISCPLog(exec.ctx, from, to)
			if err != nil {
				logutil.Error(
					"ISCP-Task apply iscp log failed",
					zap.String("from", from.ToString()),
					zap.String("to", to.ToString()),
					zap.Error(err),
				)
				continue
			}
			exec.iscpLogWm = to
			// get candidate iterations and tables
			iterations, candidateTables, fromTSs := exec.getCandidateTables()
			if len(iterations) == 0 {
				continue
			}
			// check if there are any dirty tables
			tables, toTS, err := exec.getDirtyTables(exec.ctx, candidateTables, fromTSs, exec.cnUUID, exec.txnEngine)
			if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "getDirtyTables" {
				err = moerr.NewInternalErrorNoCtx(msg)
			}
			if err != nil {
				logutil.Error(
					"ISCP-Task get dirty tables failed",
					zap.Error(err),
				)
				continue
			}
			// run iterations with dirty table
			// update watermark for clean tables
			for _, iter := range iterations {
				maxTS := types.MaxTs()
				if iter.toTS.EQ(&maxTS) {
					iter.toTS = toTS
				}
				_, ok := tables[iter.tableID]
				if ok {
					exec.worker.Submit(iter)
				} else {
					table, ok := exec.getTable(iter.accountID, iter.tableID)
					if !ok {
						logutil.Error(
							"ISCP-Task get table failed",
							zap.Uint32("accountID", iter.accountID),
							zap.Uint64("tableID", iter.tableID),
						)
						continue
					}
					table.UpdateWatermark(iter)
				}
			}
		case <-gcTrigger.C:
			err := exec.GC(exec.option.GCTTL)
			if err != nil {
				logutil.Error(
					"ISCP-Task gc failed",
					zap.Error(err),
				)
			}
		}
	}
}

// For UT
func (exec *ISCPTaskExecutor) GetWatermark(accountID uint32, srcTableID uint64, jobName string) (watermark types.TS, ok bool) {
	table, ok := exec.getTable(accountID, srcTableID)
	if !ok {
		return
	}
	watermark, ok = table.GetWatermark(jobName)
	return
}

func (exec *ISCPTaskExecutor) applyISCPLog(ctx context.Context, from, to types.TS) (err error) {
	rel, txn, err := exec.getTableByID(ctx, exec.iscpLogTableID)
	if err != nil {
		return
	}
	defer txn.Commit(ctx)
	changes, err := CollectChanges(ctx, rel, from, to, exec.mp)
	if err != nil {
		return
	}
	defer changes.Close()

	for {
		var insertData, deleteData *batch.Batch
		insertData, deleteData, _, err = changes.Next(ctx, exec.mp)
		if insertData == nil && deleteData == nil {
			break
		}
		accountIDVector := insertData.Vecs[0]
		accountIDs := vector.MustFixedColWithTypeCheck[uint32](accountIDVector)
		tableIDVector := insertData.Vecs[1]
		tableIDs := vector.MustFixedColWithTypeCheck[uint64](tableIDVector)
		jobNameVector := insertData.Vecs[2]
		jobSpecVector := insertData.Vecs[3]
		jobStateVector := insertData.Vecs[4]
		states := vector.MustFixedColWithTypeCheck[int8](jobStateVector)
		watermarkVector := insertData.Vecs[5]
		dropAtVector := insertData.Vecs[8]
		for i := 0; i < insertData.RowCount(); i++ {

			if dropAtVector.IsNull(uint64(i)) {
				retry(
					func() error {
						return exec.addOrUpdateJob(
							accountIDs[i],
							tableIDs[i],
							jobNameVector.GetStringAt(i),
							states[i],
							watermarkVector.GetStringAt(i),
							jobSpecVector.GetStringAt(i),
						)
					},
					exec.option.RetryTimes,
				)
			} else {
				retry(
					func() error {
						return exec.deleteJob(accountIDs[i], tableIDs[i], jobNameVector.GetStringAt(i))
					},
					exec.option.RetryTimes,
				)
			}
		}
	}

	return
}

func (exec *ISCPTaskExecutor) replay(ctx context.Context) {
	var err error
	jobCount := 0
	defer func() {
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"ISCP-Task replay",
			zap.Int("jobCount", jobCount),
			zap.Error(err),
		)
	}()
	sql := cdc.CDCSQLBuilder.ISCPLogSelectSQL()
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
		accountIDVector := cols[0]
		accountIDs := vector.MustFixedColWithTypeCheck[uint32](accountIDVector)
		tableIDVector := cols[1]
		tableIDs := vector.MustFixedColWithTypeCheck[uint64](tableIDVector)
		jobNameVector := cols[2]
		jobSpecVector := cols[3]
		jobStateVector := cols[4]
		states := vector.MustFixedColWithTypeCheck[int8](jobStateVector)
		watermarkVector := cols[5]
		dropAtVector := cols[8]
		for i := 0; i < rows; i++ {
			if !dropAtVector.IsNull(uint64(i)) {
				continue
			}
			jobCount++
			retry(
				func() error {
					return exec.addOrUpdateJob(
						accountIDs[i],
						tableIDs[i],
						jobNameVector.GetStringAt(i),
						states[i],
						watermarkVector.GetStringAt(i),
						jobSpecVector.GetStringAt(i),
					)
				},
				exec.option.RetryTimes,
			)
		}
		return true
	})
	exec.iscpLogWm = types.TimestampToTS(exec.txnEngine.LatestLogtailAppliedTime())
}

func (exec *ISCPTaskExecutor) addOrUpdateJob(
	accountID uint32,
	tableID uint64,
	jobName string,
	state int8,
	watermarkStr string,
	jobSpecStr string,
) (err error) {
	var newCreate bool
	defer func() {
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"ISCP-Task add job",
			zap.Uint32("accountID", accountID),
			zap.Uint64("tableID", tableID),
			zap.String("jobName", jobName),
			zap.String("watermark", watermarkStr),
			zap.Bool("newcreate", newCreate),
			zap.Error(err),
		)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountID)
	jobSpec, err := UnmarshalJobSpec(jobSpecStr)
	if err != nil {
		return
	}
	watermark := types.StringToTS(watermarkStr)
	var table *TableEntry
	table, ok := exec.getTable(accountID, tableID)
	if !ok {
		var rel engine.Relation
		var txn client.TxnOperator
		rel, txn, err = exec.getTableByID(ctx, tableID)
		if err != nil {
			return
		}
		defer txn.Commit(ctx)
		tableDef := rel.GetTableDef(ctx)
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
	newCreate, err = table.AddOrUpdateSinker(jobName, jobSpec, watermark, state)
	return
}

func (exec *ISCPTaskExecutor) deleteJob(
	accountID uint32,
	tableID uint64,
	jobName string,
) (err error) {
	defer func() {
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"ISCP-Task delete job",
			zap.Uint32("accountID", accountID),
			zap.Uint64("tableID", tableID),
			zap.String("jobName", jobName),
			zap.Error(err),
		)
	}()
	table, ok := exec.getTable(accountID, tableID)
	if !ok {
		return moerr.NewInternalErrorNoCtx("table not found")
	}
	empty, err := table.DeleteSinker(jobName)
	if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "deleteJob" {
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
func (exec *ISCPTaskExecutor) getTableByID(ctx context.Context, tableID uint64) (table engine.Relation, txn client.TxnOperator, err error) {
	txn, err = exec.txnFactory()
	if err != nil {
		return
	}
	_, _, table, err = cdc.GetRelationById(ctx, exec.txnEngine, txn, tableID)
	if err != nil {
		return
	}
	return
}
func (exec *ISCPTaskExecutor) getCandidateTables() ([]*IterationContext, []*TableEntry, []types.TS) {
	tables := make([]*TableEntry, 0)
	fromTSs := make([]types.TS, 0)
	iterations := make([]*IterationContext, 0)
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

func (exec *ISCPTaskExecutor) GC(cleanupThreshold time.Duration) (err error) {
	txn, err := exec.txnFactory()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(exec.ctx, time.Minute*5)
	defer cancel()
	defer txn.Commit(ctx)
	gcTime := time.Now().Add(-cleanupThreshold)
	iscpLogGCSql := cdc.CDCSQLBuilder.ISCPLogGCSQL(gcTime)
	if _, err = ExecWithResult(ctx, iscpLogGCSql, exec.cnUUID, txn); err != nil {
		return err
	}
	logutil.Info(
		"ISCP-Task GC",
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
	logutil.Errorf("ISCP-Task retry failed, err: %v", err)
	return
}
