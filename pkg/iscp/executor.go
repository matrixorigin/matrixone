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
	DefaultFlushWatermarkTTL      = time.Hour

	DefaultRetryTimes    = 5
	DefaultRetryDuration = time.Second
)

type ISCPExecutorOption struct {
	GCInterval             time.Duration
	GCTTL                  time.Duration
	SyncTaskInterval       time.Duration
	FlushWatermarkInterval time.Duration
	FlushWatermarkTTL      time.Duration
	RetryTimes             int
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

func fillDefaultOption(option *ISCPExecutorOption) *ISCPExecutorOption {
	if option == nil {
		option = &ISCPExecutorOption{}
	}
	if option.GCInterval == 0 {
		option.GCInterval = DefaultGCInterval
	}
	if option.GCTTL == 0 {
		option.GCTTL = DefaultGCTTL
	}
	if option.SyncTaskInterval == 0 {
		option.SyncTaskInterval = DefaultSyncTaskInterval
	}
	if option.FlushWatermarkInterval == 0 {
		option.FlushWatermarkInterval = DefaultFlushWatermarkInterval
	}
	if option.FlushWatermarkTTL == 0 {
		option.FlushWatermarkTTL = DefaultFlushWatermarkTTL
	}
	if option.RetryTimes == 0 {
		option.RetryTimes = DefaultRetryTimes
	}
	return option
}

func NewISCPTaskExecutor(
	ctx context.Context,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	cdUUID string,
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
	option = fillDefaultOption(option)
	exec = &ISCPTaskExecutor{
		ctx:         ctx,
		packer:      types.NewPacker(),
		tables:      btree.NewBTreeGOptions(tableInfoLess, btree.Options{NoLocks: true}),
		cnUUID:      cdUUID,
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
	txn, err := getTxn(exec.ctx, exec.txnEngine, exec.cnTxnClient, "setISCPLogTableID")
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
	err := retry(
		func() error {
			return exec.replay(exec.ctx)
		},
		exec.option.RetryTimes,
	)
	if err != nil {
		panic(err)
	}
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
	flushWatermarkTrigger := time.NewTicker(exec.option.FlushWatermarkInterval)
	gcTrigger := time.NewTicker(exec.option.GCInterval)
	for {
		select {
		case <-ctx.Done():
			return
		case <-exec.ctx.Done():
			return
		case <-syncTaskTrigger.C:
			// apply iscp log
			from := exec.iscpLogWm.Next()
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
			tables, toTS, minTS, err := exec.getDirtyTables(exec.ctx, candidateTables, fromTSs, exec.cnUUID, exec.txnEngine)
			if msg, injected := objectio.ISCPExecutorInjected(); injected && msg == "getDirtyTables" {
				err = moerr.NewInternalErrorNoCtx(msg)
			}
			var getDirtyTablesFailed bool
			if err != nil {
				logutil.Error(
					"ISCP-Task get dirty tables failed",
					zap.Error(err),
				)
				getDirtyTablesFailed = true
			}
			// run iterations with dirty table
			// update watermark for clean tables
			for _, iter := range iterations {
				maxTS := types.MaxTs()
				if iter.toTS.EQ(&maxTS) {
					iter.toTS = toTS
				}
				// For initialized iterctx (fromTS is empty), do not check whether the table has changed
				var ok bool
				if iter.fromTS.IsEmpty() || getDirtyTablesFailed || iter.fromTS.LT(&minTS) {
					ok = true
				} else {
					_, ok = tables[iter.tableID]
				}
				table, ok2 := exec.getTable(iter.accountID, iter.tableID)
				if ok {
					// The update on mo_iscp_log may not be available in the next applyISCPLog,
					// so update the in-memory state directly to prevent repeated triggering of the iteration.
					for i, jobName := range iter.jobNames {
						job := table.jobs[JobKey{
							JobName: jobName,
							JobID:   iter.jobIDs[i],
						}]
						job.state = ISCPJobState_Pending
					}
					err := exec.worker.Submit(iter)
					if err != nil {
						for i, jobName := range iter.jobNames {
							job := table.jobs[JobKey{
								JobName: jobName,
								JobID:   iter.jobIDs[i],
							}]
							job.state = ISCPJobState_Completed
						}
						logutil.Error(
							"ISCP-Task submit iteration failed",
							zap.Error(err),
						)
					}
				} else {
					if !ok2 {
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
		case <-flushWatermarkTrigger.C:
			err := exec.FlushWatermarkForAllTables(exec.option.FlushWatermarkTTL)
			if err != nil {
				logutil.Error(
					"ISCP-Task flush watermark failed",
					zap.Error(err),
				)
			}
		case <-gcTrigger.C:
			err := exec.GC(exec.option.GCTTL)
			if err != nil {
				logutil.Error(
					"ISCP-Task gc failed",
					zap.Error(err),
				)
			}
			exec.GCInMemoryJob(exec.option.GCTTL)
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

// For UT
func (exec *ISCPTaskExecutor) GetJobType(accountID uint32, srcTableID uint64, jobName string) (jobType uint16, ok bool) {
	table, ok := exec.getTable(accountID, srcTableID)
	if !ok {
		return
	}
	table.mu.RLock()
	defer table.mu.RUnlock()
	for _, job := range table.jobs {
		if job.jobName == jobName && job.dropAt == 0 {
			jobType = job.jobSpec.GetType()
			ok = true
			break
		}
	}
	return
}

func (exec *ISCPTaskExecutor) applyISCPLog(ctx context.Context, from, to types.TS) (err error) {
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	rel, txn, err := getRelation(exec.txnEngine, exec.cnTxnClient, catalog.System_Account, exec.iscpLogTableID)
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
		if insertData != nil {
			defer insertData.Clean(exec.mp)
		}
		if deleteData != nil {
			defer deleteData.Clean(exec.mp)
		}
		if insertData == nil {
			continue
		}
		accountIDVector := insertData.Vecs[0]
		accountIDs := vector.MustFixedColWithTypeCheck[uint32](accountIDVector)
		tableIDVector := insertData.Vecs[1]
		tableIDs := vector.MustFixedColWithTypeCheck[uint64](tableIDVector)
		jobNameVector := insertData.Vecs[2]
		jobIDVector := insertData.Vecs[3]
		jobIDs := vector.MustFixedColWithTypeCheck[uint64](jobIDVector)
		jobSpecVector := insertData.Vecs[4]
		jobStateVector := insertData.Vecs[5]
		states := vector.MustFixedColWithTypeCheck[int8](jobStateVector)
		watermarkVector := insertData.Vecs[6]
		dropAtVector := insertData.Vecs[9]
		dropAts := vector.MustFixedColWithTypeCheck[types.Timestamp](dropAtVector)
		commitTSVector := insertData.Vecs[11]
		commitTSs := vector.MustFixedColWithTypeCheck[types.TS](commitTSVector)
		type job struct {
			ts     types.TS
			offset int
		}
		type jobName struct {
			accountID uint32
			tableID   uint64
			jobName   string
			jobID     uint64
		}
		jobMap := make(map[jobName]job)
		for i := 0; i < insertData.RowCount(); i++ {
			jobName := jobName{
				accountID: accountIDs[i],
				tableID:   tableIDs[i],
				jobName:   jobNameVector.GetStringAt(i),
				jobID:     jobIDs[i],
			}
			if job, ok := jobMap[jobName]; ok {
				if job.ts.GT(&commitTSs[i]) {
					continue
				}
			}
			jobMap[jobName] = job{
				ts:     commitTSs[i],
				offset: i,
			}
		}
		for _, job := range jobMap {
			var dropAt types.Timestamp
			if !dropAtVector.IsNull(uint64(job.offset)) {
				dropAt = dropAts[job.offset]
			}
			/*
				todo:
				register job & create table
				unregister job & drop table
				apply add, error not found table
			*/
			retry(
				func() error {
					return exec.addOrUpdateJob(
						accountIDs[job.offset],
						tableIDs[job.offset],
						jobNameVector.GetStringAt(job.offset),
						jobIDs[job.offset],
						states[job.offset],
						watermarkVector.GetStringAt(job.offset),
						jobSpecVector.GetBytesAt(job.offset),
						dropAt,
					)
				},
				exec.option.RetryTimes,
			)
		}
	}

	return
}

func (exec *ISCPTaskExecutor) replay(ctx context.Context) (err error) {
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
	txn, err := getTxn(ctx, exec.txnEngine, exec.cnTxnClient, "iscp replay")
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
		jobIDVector := cols[3]
		jobIDs := vector.MustFixedColWithTypeCheck[uint64](jobIDVector)
		jobSpecVector := cols[4]
		jobStateVector := cols[5]
		states := vector.MustFixedColWithTypeCheck[int8](jobStateVector)
		watermarkVector := cols[6]
		dropAtVector := cols[9]
		dropAts := vector.MustFixedColWithTypeCheck[types.Timestamp](dropAtVector)
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
						jobIDs[i],
						states[i],
						watermarkVector.GetStringAt(i),
						jobSpecVector.GetBytesAt(i),
						dropAts[i],
					)
				},
				exec.option.RetryTimes,
			)
		}
		return true
	})
	exec.iscpLogWm = types.TimestampToTS(txn.SnapshotTS())
	return
}

func (exec *ISCPTaskExecutor) addOrUpdateJob(
	accountID uint32,
	tableID uint64,
	jobName string,
	jobID uint64,
	state int8,
	watermarkStr string,
	jobSpecStr []byte,
	dropAt types.Timestamp,
) (err error) {
	var newCreate bool

	defer func() {
		if !newCreate && err == nil && dropAt == 0 {
			return
		}
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"ISCP-Task add or update job",
			zap.Uint32("accountID", accountID),
			zap.Uint64("tableID", tableID),
			zap.String("jobName", jobName),
			zap.Uint64("jobID", jobID),
			zap.String("watermark", watermarkStr),
			zap.Bool("newcreate", newCreate),
			zap.String("dropAt", dropAt.String()),
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
		if dropAt != 0 {
			return
		}
		var rel engine.Relation
		var txn client.TxnOperator
		rel, txn, err = getRelation(exec.txnEngine, exec.cnTxnClient, accountID, tableID)
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
		)
		exec.setTable(table)
	}
	newCreate, err = table.AddOrUpdateSinker(jobName, jobSpec, jobID, watermark, state, dropAt)
	return
}

func (exec *ISCPTaskExecutor) GCInMemoryJob(threshold time.Duration) {
	tables := exec.getAllTables()
	tablesToDelete := make([]*TableEntry, 0)
	for _, table := range tables {
		isEmpty := table.gcInMemoryJob(threshold)
		if isEmpty {
			tablesToDelete = append(tablesToDelete, table)
		}
	}
	tids := make([]uint64, 0, len(tablesToDelete))
	for _, table := range tablesToDelete {
		exec.deleteTableEntry(table)
		tids = append(tids, table.tableID)
	}
	logutil.Infof("ISCP-Task delete table %v", tids)
}

// getCandidateTables returns all candidate IterationContexts, their corresponding TableEntries, and the minimal fromTS for each table.
// Only IterationContexts with non-empty fromTS are included (i.e., initialized iterations are excluded and do not require table change checks).
//   - iterations: all candidate IterationContexts (including initialized iterations)
//   - tables: the TableEntry for each iteration
//   - fromTSs: the minimal fromTS in each table's iterations (not including initialized iterations)
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
) (tables map[uint64]struct{}, toTS types.TS, minTS types.TS, err error) {

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
		&minTS,
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
func (exec *ISCPTaskExecutor) FlushWatermarkForAllTables(ttl time.Duration) error {
	tables := exec.getAllTables()
	if len(tables) == 0 {
		return nil
	}
	txn, err := getTxn(exec.ctx, exec.txnEngine, exec.cnTxnClient, "flush watermark for all tables")
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
	jobCount := 0
	for _, table := range tables {
		flushCount := table.tryFlushWatermark(ctx, txn, ttl)
		jobCount += flushCount
	}
	logutil.Info(
		"ISCP-Task flush watermark",
		zap.Any("table count", len(tables)),
		zap.Int("jobCount", jobCount),
	)
	return nil
}

func (exec *ISCPTaskExecutor) GC(cleanupThreshold time.Duration) (err error) {
	txn, err := getTxn(exec.ctx, exec.txnEngine, exec.cnTxnClient, "iscp gc")
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
