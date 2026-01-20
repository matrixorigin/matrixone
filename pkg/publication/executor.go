// Copyright 2021 Matrix Origin
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

package publication

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/zap"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/config"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/task"
	"github.com/matrixorigin/matrixone/pkg/taskservice"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/tidwall/btree"
)

const (
	MOCcprLogTableName = catalog.MO_CCPR_LOG
)

var running atomic.Bool

const (
	DefaultGCInterval       = time.Hour
	DefaultGCTTL            = time.Hour * 24 * 7 // 7 days
	DefaultSyncTaskInterval = time.Second * 10
	DefaultRetryTimes       = 5
	DefaultRetryInterval    = time.Second
	DefaultRetryDuration    = time.Minute * 10
	SnapshotThreshold       = time.Hour * 24 // 1 day
)

type PublicationExecutorOption struct {
	GCInterval       time.Duration
	GCTTL            time.Duration
	SyncTaskInterval time.Duration
	RetryTimes       int
}

func PublicationTaskExecutorFactory(
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	attachToTask func(context.Context, uint64, taskservice.ActiveRoutine) error,
	cdUUID string,
	mp *mpool.MPool,
	upstreamSQLHelperFactory UpstreamSQLHelperFactory,
	pu *config.ParameterUnit,
) func(ctx context.Context, task task.Task) (err error) {
	// Set getParameterUnitWrapper to return the ParameterUnit passed from factory
	// Similar to CDC's getGlobalPuWrapper, but using the ParameterUnit from service
	if pu != nil {
		SetGetParameterUnitWrapper(func(cnUUID string) *config.ParameterUnit {
			return pu
		})
	}

	return func(ctx context.Context, task task.Task) (err error) {
		var exec *PublicationTaskExecutor

		if !running.CompareAndSwap(false, true) {
			// already running
			logutil.Error("PublicationTaskExecutor is already running")
			return moerr.NewErrExecutorRunning(ctx, "PublicationTaskExecutor")
		}

		ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
		exec, err = NewPublicationTaskExecutor(
			ctx,
			txnEngine,
			cnTxnClient,
			cdUUID,
			nil,
			mp,
			upstreamSQLHelperFactory,
		)
		if err != nil {
			return
		}
		attachToTask(ctx, task.GetID(), exec)

		exec.runningMu.Lock()
		defer exec.runningMu.Unlock()
		if exec.running {
			return
		}
		err = exec.initStateLocked()
		if err != nil {
			return
		}
		exec.run(ctx)
		<-ctx.Done()
		return
	}
}

func fillDefaultOption(option *PublicationExecutorOption) *PublicationExecutorOption {
	if option == nil {
		option = &PublicationExecutorOption{}
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
	if option.RetryTimes == 0 {
		option.RetryTimes = DefaultRetryTimes
	}
	return option
}

func NewPublicationTaskExecutor(
	ctx context.Context,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	cdUUID string,
	option *PublicationExecutorOption,
	mp *mpool.MPool,
	upstreamSQLHelperFactory UpstreamSQLHelperFactory,
) (exec *PublicationTaskExecutor, err error) {
	defer func() {
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"Publication-Task Executor init",
			zap.Any("gcInterval", option.GCInterval),
			zap.Any("gcttl", option.GCTTL),
			zap.Any("syncTaskInterval", option.SyncTaskInterval),
			zap.Any("retryTimes", option.RetryTimes),
			zap.Error(err),
		)
	}()
	option = fillDefaultOption(option)
	exec = &PublicationTaskExecutor{
		ctx:                      ctx,
		tasks:                    btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true}),
		cnUUID:                   cdUUID,
		txnEngine:                txnEngine,
		cnTxnClient:              cnTxnClient,
		wg:                       sync.WaitGroup{},
		taskMu:                   sync.RWMutex{},
		option:                   option,
		mp:                       mp,
		upstreamSQLHelperFactory: upstreamSQLHelperFactory,
	}
	return exec, nil
}

// TaskEntry represents a task entry in the executor
// Only stores taskid, lsn, state, subscriptionState
type TaskEntry struct {
	taskID            uint64
	lsn               uint64
	state             int8       // iteration_state from mo_ccpr_log
	subscriptionState int8       // subscription state: 0=running, 1=error, 2=pause, 3=dropped
	dropAt            *time.Time // drop timestamp from mo_ccpr_log
}

func taskEntryLess(a, b *TaskEntry) bool {
	return a.taskID < b.taskID
}

// PublicationTaskExecutor manages publication tasks
type PublicationTaskExecutor struct {
	tasks                    *btree.BTreeG[*TaskEntry]
	taskMu                   sync.RWMutex
	mp                       *mpool.MPool
	cnUUID                   string
	txnEngine                engine.Engine
	cnTxnClient              client.TxnClient
	ccprLogWm                types.TS
	upstreamSQLHelperFactory UpstreamSQLHelperFactory

	option *PublicationExecutorOption

	ctx    context.Context
	cancel context.CancelFunc

	worker Worker
	wg     sync.WaitGroup

	running   bool
	runningMu sync.Mutex
}

func (exec *PublicationTaskExecutor) Resume() error {
	err := exec.Start()
	if err != nil {
		return err
	}
	return nil
}

func (exec *PublicationTaskExecutor) Pause() error {
	exec.Stop()
	return nil
}

func (exec *PublicationTaskExecutor) Cancel() error {
	exec.Stop()
	return nil
}

func (exec *PublicationTaskExecutor) Restart() error {
	exec.Stop()
	err := exec.Start()
	if err != nil {
		return err
	}
	return nil
}

func (exec *PublicationTaskExecutor) Start() error {
	exec.runningMu.Lock()
	defer exec.runningMu.Unlock()
	if exec.running {
		return nil
	}
	err := exec.initStateLocked()
	if err != nil {
		return err
	}
	go exec.run(context.Background())
	return nil
}

func (exec *PublicationTaskExecutor) initStateLocked() error {
	exec.running = true
	logutil.Info(
		"Publication-Task Start",
	)
	ctx, cancel := context.WithCancel(context.Background())
	worker := NewWorker(exec.cnUUID, exec.txnEngine, exec.cnTxnClient, exec.mp, exec.upstreamSQLHelperFactory)
	exec.worker = worker
	exec.ctx = ctx
	exec.cancel = cancel
	err := retryPublication(
		ctx,
		func() error {
			return exec.replay(exec.ctx)
		},
		exec.option.RetryTimes,
		DefaultRetryInterval,
		DefaultRetryDuration,
	)
	if err != nil {
		return err
	}
	// Update tasks with state != error and drop_at is empty to complete
	err = exec.updateNonErrorTasksToComplete(exec.ctx)
	if err != nil {
		logutil.Error(
			"Publication-Task update non-error tasks to complete failed",
			zap.Error(err),
		)
		// Don't return error, continue execution
	}
	exec.wg.Add(1)
	return nil
}

func (exec *PublicationTaskExecutor) Stop() {
	exec.runningMu.Lock()
	defer exec.runningMu.Unlock()
	if !exec.running {
		return
	}
	exec.running = false
	logutil.Info(
		"Publication-Task Stop",
	)
	exec.worker.Stop()
	exec.cancel()
	exec.wg.Wait()
	exec.ctx, exec.cancel = nil, nil
	exec.worker = nil
}

func (exec *PublicationTaskExecutor) IsRunning() bool {
	exec.runningMu.Lock()
	defer exec.runningMu.Unlock()
	return exec.running
}

func (exec *PublicationTaskExecutor) run(ctx context.Context) {
	logutil.Info(
		"Publication-Task Run",
	)
	defer func() {
		logutil.Info(
			"Publication-Task Run Done",
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
			// apply mo_ccpr_log
			from := exec.ccprLogWm.Next()
			to := types.TimestampToTS(exec.txnEngine.LatestLogtailAppliedTime())
			err := exec.applyCcprLog(exec.ctx, from, to)
			if err == nil {
				exec.ccprLogWm = to
			}
			if err != nil && moerr.IsMoErrCode(err, moerr.ErrStaleRead) {
				err = exec.replay(exec.ctx)
			}
			if err != nil {
				logutil.Error(
					"Publication-Task apply ccpr log failed",
					zap.String("from", from.ToString()),
					zap.String("to", to.ToString()),
					zap.Error(err),
				)
				continue
			}
			// get candidate tasks and trigger if state is not completed
			candidateTasks := exec.getCandidateTasks()
			if len(candidateTasks) == 0 {
				continue
			}
			// check lease before submitting tasks
			ok, err := CheckLeaseWithRetry(exec.ctx, exec.cnUUID, exec.txnEngine, exec.cnTxnClient)
			if err != nil {
				logutil.Error(
					"Publication-Task check lease failed",
					zap.Error(err),
				)
				continue
			}
			if !ok {
				logutil.Error("Publication-Task lease check failed, stopping executor")
				go exec.Stop()
				break
			}
			for _, task := range candidateTasks {
				// Only trigger tasks that are not completed
				err = exec.worker.Submit(task.taskID, task.lsn, task.state)
				if err != nil {
					logutil.Error(
						"Publication-Task submit task failed",
						zap.Uint64("taskID", task.taskID),
						zap.Uint64("lsn", task.lsn),
						zap.Int8("state", task.state),
						zap.Error(err),
					)
					continue
				}
			}
		case <-gcTrigger.C:
			err := GC(exec.ctx, exec.txnEngine, exec.cnTxnClient, exec.cnUUID, exec.upstreamSQLHelperFactory, exec.option.GCTTL)
			if err != nil {
				logutil.Error(
					"Publication-Task gc failed",
					zap.Error(err),
				)
			}
			exec.GCInMemoryTask(exec.option.GCTTL)
		}
	}
}

func (exec *PublicationTaskExecutor) getTask(taskID uint64) (*TaskEntry, bool) {
	exec.taskMu.RLock()
	defer exec.taskMu.RUnlock()
	return exec.tasks.Get(&TaskEntry{taskID: taskID})
}

func (exec *PublicationTaskExecutor) setTask(task *TaskEntry) {
	exec.taskMu.Lock()
	defer exec.taskMu.Unlock()
	exec.tasks.Set(task)
}

func (exec *PublicationTaskExecutor) deleteTaskEntry(task *TaskEntry) {
	exec.taskMu.Lock()
	defer exec.taskMu.Unlock()
	exec.tasks.Delete(task)
}

func (exec *PublicationTaskExecutor) getAllTasks() []*TaskEntry {
	exec.taskMu.RLock()
	defer exec.taskMu.RUnlock()
	items := exec.tasks.Items()
	return items
}

func (exec *PublicationTaskExecutor) getCandidateTasks() []*TaskEntry {
	allTasks := exec.getAllTasks()
	candidates := make([]*TaskEntry, 0)
	for _, task := range allTasks {
		// Only include tasks that subscription state is running and iteration state is completed
		if task.subscriptionState == SubscriptionStateRunning && task.state == IterationStateCompleted {
			candidates = append(candidates, task)
		}
	}
	return candidates
}

func (exec *PublicationTaskExecutor) applyCcprLog(ctx context.Context, from, to types.TS) (err error) {
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	nowTs := exec.txnEngine.LatestLogtailAppliedTime()
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		"publication apply ccpr log",
		0)
	txnOp, err := exec.cnTxnClient.New(ctx, nowTs, createByOpt)
	if txnOp != nil {
		defer txnOp.Commit(ctx)
	}
	if err != nil {
		return
	}
	err = exec.txnEngine.New(ctx, txnOp)
	if err != nil {
		return
	}
	db, err := exec.txnEngine.Database(ctx, catalog.MO_CATALOG, txnOp)
	if err != nil {
		return
	}
	rel, err := db.Relation(ctx, MOCcprLogTableName, nil)
	if err != nil {
		return
	}
	return exec.applyCcprLogWithRel(ctx, rel, from, to)
}

func (exec *PublicationTaskExecutor) applyCcprLogWithRel(ctx context.Context, rel engine.Relation, from, to types.TS) (err error) {
	changes, err := CollectChanges(ctx, rel, from, to, exec.mp)
	if err != nil {
		return
	}
	defer changes.Close()

	for {
		var insertData, deleteData *batch.Batch
		insertData, deleteData, _, err = changes.Next(ctx, exec.mp)
		if err != nil {
			return
		}
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
		// Parse mo_ccpr_log columns:
		// task_id, subscription_name, sync_level, account_id, db_name, table_name,
		// upstream_conn, sync_config, state, iteration_state, iteration_lsn, context,
		// cn_uuid, error_message, created_at, drop_at
		taskIDVector := insertData.Vecs[0]
		taskIDs := vector.MustFixedColWithTypeCheck[uint32](taskIDVector)
		subscriptionStateVector := insertData.Vecs[8]
		subscriptionStates := vector.MustFixedColWithTypeCheck[int8](subscriptionStateVector)
		iterationStateVector := insertData.Vecs[9]
		states := vector.MustFixedColWithTypeCheck[int8](iterationStateVector)
		iterationLSNVector := insertData.Vecs[10]
		lsns := vector.MustFixedColWithTypeCheck[int64](iterationLSNVector)
		// drop_at is at index 15
		dropAtVector := insertData.Vecs[15]
		// commit_ts is typically the last column (after all data columns)
		// The number of columns in mo_ccpr_log is 16 (0-15), so commit_ts should be at index 16
		var commitTSs []types.TS
		if len(insertData.Vecs) > 16 {
			commitTSVector := insertData.Vecs[16]
			commitTSs = vector.MustFixedColWithTypeCheck[types.TS](commitTSVector)
		} else {
			// If commit_ts is not available, use empty TS
			commitTSs = make([]types.TS, insertData.RowCount())
		}

		type taskInfo struct {
			ts     types.TS
			offset int
		}
		type taskKey struct {
			taskID uint64
		}
		taskMap := make(map[taskKey]taskInfo)
		for i := 0; i < insertData.RowCount(); i++ {
			var commitTS types.TS
			if len(commitTSs) > 0 {
				if len(commitTSs) == 1 {
					commitTS = commitTSs[0]
				} else {
					commitTS = commitTSs[i]
				}
			}
			key := taskKey{
				taskID: uint64(taskIDs[i]),
			}
			if task, ok := taskMap[key]; ok {
				if task.ts.GT(&commitTS) {
					continue
				}
			}
			taskMap[key] = taskInfo{
				ts:     commitTS,
				offset: i,
			}
		}
		for _, task := range taskMap {
			subscriptionState := subscriptionStates[task.offset]
			var dropAt *time.Time
			// Check if drop_at is set (indicating dropped), update subscriptionState
			if !dropAtVector.IsNull(uint64(task.offset)) {
				subscriptionState = SubscriptionStateDropped
				// Parse timestamp from vector - drop_at is TIMESTAMP type
				ts := vector.GetFixedAtWithTypeCheck[types.Timestamp](dropAtVector, task.offset)
				// Convert Timestamp to time.Time
				// Timestamp is stored as microseconds since year 1, not Unix epoch
				// Use ToDatetime then ConvertToGoTime to properly convert
				dt := ts.ToDatetime(time.UTC)
				t := dt.ConvertToGoTime(time.UTC)
				dropAt = &t
			}
			exec.addOrUpdateTask(
				uint64(taskIDs[task.offset]),
				uint64(lsns[task.offset]),
				states[task.offset],
				subscriptionState,
				dropAt,
			)
		}
	}

	return
}

func (exec *PublicationTaskExecutor) replay(ctx context.Context) (err error) {
	defer func() {
		var logger func(msg string, fields ...zap.Field)
		if err != nil {
			logger = logutil.Error
		} else {
			logger = logutil.Info
		}
		logger(
			"Publication-Task replay",
			zap.Error(err),
		)
	}()
	sql := `SELECT task_id, iteration_state, iteration_lsn, state, drop_at FROM mo_catalog.mo_ccpr_log`
	txn, err := getTxn(ctx, exec.txnEngine, exec.cnTxnClient, "publication replay")
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
		taskIDVector := cols[0]
		taskIDs := vector.MustFixedColWithTypeCheck[uint32](taskIDVector)
		iterationStateVector := cols[1]
		states := vector.MustFixedColWithTypeCheck[int8](iterationStateVector)
		iterationLSNVector := cols[2]
		lsns := vector.MustFixedColWithTypeCheck[int64](iterationLSNVector)
		subscriptionStateVector := cols[3]
		subscriptionStates := vector.MustFixedColWithTypeCheck[int8](subscriptionStateVector)
		dropAtVector := cols[4]
		for i := 0; i < rows; i++ {
			subscriptionState := subscriptionStates[i]
			var dropAt *time.Time
			// Check if drop_at is set (indicating dropped), update subscriptionState
			if !dropAtVector.IsNull(uint64(i)) {
				subscriptionState = SubscriptionStateDropped
				// Parse timestamp from vector - drop_at is TIMESTAMP type
				ts := vector.GetFixedAtWithTypeCheck[types.Timestamp](dropAtVector, i)
				// Convert Timestamp to time.Time
				// Timestamp is stored as microseconds since year 1, not Unix epoch
				// Use ToDatetime then ConvertToGoTime to properly convert
				dt := ts.ToDatetime(time.UTC)
				t := dt.ConvertToGoTime(time.UTC)
				dropAt = &t
			}
			err = exec.addOrUpdateTask(
				uint64(taskIDs[i]),
				uint64(lsns[i]),
				states[i],
				subscriptionState,
				dropAt,
			)
			if err != nil {
				return false
			}
		}
		return true
	})
	exec.ccprLogWm = types.TimestampToTS(txn.SnapshotTS())
	return
}

func (exec *PublicationTaskExecutor) addOrUpdateTask(
	taskID uint64,
	lsn uint64,
	state int8,
	subscriptionState int8,
	dropAt *time.Time,
) error {
	task, ok := exec.getTask(taskID)
	if !ok {
		logutil.Infof("Publication-Task add task %v", taskID)
		task = &TaskEntry{
			taskID:            taskID,
			lsn:               lsn,
			state:             state,
			subscriptionState: subscriptionState,
			dropAt:            dropAt,
		}
		exec.setTask(task)
		return nil
	}
	logutil.Infof("Publication-Task update task %v-%d-%d-%d", taskID, lsn, state, subscriptionState)
	// Update existing task
	task.lsn = lsn
	task.state = state
	task.subscriptionState = subscriptionState
	task.dropAt = dropAt
	exec.setTask(task)
	return nil
}

func (exec *PublicationTaskExecutor) updateNonErrorTasksToComplete(ctx context.Context) error {
	// Update tasks in database using SQL
	// Update all rows where iteration_state != error and drop_at is NULL
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	txn, err := getTxn(ctx, exec.txnEngine, exec.cnTxnClient, "publication update non-error tasks")
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	defer txn.Commit(ctx)

	// Use SQL to update all rows where state != error and drop_at IS NULL
	updateSQL := fmt.Sprintf(
		`UPDATE mo_catalog.mo_ccpr_log `+
			`SET iteration_state = %d `+
			`WHERE iteration_state != %d AND drop_at IS NULL`,
		IterationStateCompleted,
		IterationStateError,
	)

	result, err := ExecWithResult(ctx, updateSQL, exec.cnUUID, txn)
	if err != nil {
		return err
	}
	defer result.Close()

	logutil.Info("Publication-Task updated non-error tasks with empty drop_at to complete")
	return nil
}

func (exec *PublicationTaskExecutor) GCInMemoryTask(threshold time.Duration) {
	tasks := exec.getAllTasks()
	tasksToDelete := make([]*TaskEntry, 0)
	now := time.Now()
	gcTime := now.Add(-threshold)
	for _, task := range tasks {
		// Delete tasks that are dropped and dropAt is older than threshold
		if task.subscriptionState == SubscriptionStateDropped && task.dropAt != nil {
			if task.dropAt.Before(gcTime) {
				tasksToDelete = append(tasksToDelete, task)
			}
		}
	}
	taskIDs := make([]uint64, 0, len(tasksToDelete))
	for _, task := range tasksToDelete {
		exec.deleteTaskEntry(task)
		taskIDs = append(taskIDs, task.taskID)
	}
	if len(taskIDs) > 0 {
		logutil.Infof("Publication-Task delete tasks %v", taskIDs)
	}
}

func GC(
	ctx context.Context,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	cnUUID string,
	upstreamSQLHelperFactory UpstreamSQLHelperFactory,
	cleanupThreshold time.Duration,
) (err error) {
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// GC tasks with drop_at set and older than threshold
	gcTime := time.Now().Add(-cleanupThreshold)
	snapshotThresholdTime := time.Now().Add(-SnapshotThreshold)

	logutil.Info(
		"Publication-Task GC",
		zap.Any("gcTime", gcTime),
		zap.Any("snapshotThresholdTime", snapshotThresholdTime),
	)

	// Read all mo_ccpr_log records
	txn, err := getTxn(ctx, txnEngine, cnTxnClient, "publication gc read")
	if err != nil {
		logutil.Error("Publication-Task GC failed to create txn for reading", zap.Error(err))
		return err
	}
	defer txn.Commit(ctx)

	sql := `SELECT task_id, state, iteration_state, iteration_lsn, upstream_conn, drop_at FROM mo_catalog.mo_ccpr_log`
	result, err := ExecWithResult(ctx, sql, cnUUID, txn)
	if err != nil {
		logutil.Error("Publication-Task GC failed to query mo_ccpr_log", zap.Error(err))
		return err
	}
	defer result.Close()

	var records []ccprLogRecord
	result.ReadRows(func(rows int, cols []*vector.Vector) bool {
		taskIDVector := cols[0]
		taskIDs := vector.MustFixedColWithTypeCheck[uint32](taskIDVector)
		stateVector := cols[1]
		states := vector.MustFixedColWithTypeCheck[int8](stateVector)
		iterationStateVector := cols[2]
		iterationStates := vector.MustFixedColWithTypeCheck[int8](iterationStateVector)
		iterationLSNVector := cols[3]
		lsns := vector.MustFixedColWithTypeCheck[int64](iterationLSNVector)
		upstreamConnVector := cols[4]
		dropAtVector := cols[5]

		for i := 0; i < rows; i++ {
			var upstreamConn string
			if !upstreamConnVector.IsNull(uint64(i)) {
				upstreamConn = upstreamConnVector.GetStringAt(i)
			}

			var dropAt *time.Time
			if !dropAtVector.IsNull(uint64(i)) {
				// Parse timestamp from vector - drop_at is TIMESTAMP type
				ts := vector.GetFixedAtWithTypeCheck[types.Timestamp](dropAtVector, i)
				// Convert Timestamp to time.Time
				// Timestamp is stored as microseconds since year 1, not Unix epoch
				// Use ToDatetime then ConvertToGoTime to properly convert
				dt := ts.ToDatetime(time.UTC)
				t := dt.ConvertToGoTime(time.UTC)
				dropAt = &t
			}

			records = append(records, ccprLogRecord{
				taskID:         uint64(taskIDs[i]),
				state:          states[i],
				iterationState: iterationStates[i],
				iterationLSN:   uint64(lsns[i]),
				upstreamConn:   upstreamConn,
				dropAt:         dropAt,
			})
		}
		return true
	})

	// Process each record
	for _, record := range records {
		gcRecord(ctx, txnEngine, cnTxnClient, cnUUID, upstreamSQLHelperFactory, record, gcTime, snapshotThresholdTime)
	}

	return nil
}

type ccprLogRecord struct {
	taskID         uint64
	state          int8
	iterationState int8
	iterationLSN   uint64
	upstreamConn   string
	dropAt         *time.Time
}

func gcRecord(
	ctx context.Context,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	cnUUID string,
	upstreamSQLHelperFactory UpstreamSQLHelperFactory,
	record ccprLogRecord,
	gcTime time.Time,
	snapshotThresholdTime time.Time,
) {
	// Create upstream executor for this record
	upstreamExecutor, err := createUpstreamExecutorForGC(ctx, cnUUID, cnTxnClient, txnEngine, upstreamSQLHelperFactory, record.upstreamConn)
	if err != nil {
		logutil.Error("Publication-Task GC failed to create upstream executor",
			zap.Uint64("taskID", record.taskID),
			zap.Error(err),
		)
		return
	}
	defer upstreamExecutor.Close()

	// Query all snapshots for this task
	snapshots, err := queryTaskSnapshots(ctx, upstreamExecutor, record.taskID)
	if err != nil {
		logutil.Error("Publication-Task GC failed to query snapshots",
			zap.Uint64("taskID", record.taskID),
			zap.Error(err),
		)
		return
	}

	// Determine which snapshots to delete
	snapshotsToDelete := determineSnapshotsToDelete(
		record,
		snapshots,
		gcTime,
		snapshotThresholdTime,
	)

	// Delete snapshots (each in a separate transaction)
	for _, snapshotName := range snapshotsToDelete {
		deleteSnapshotInSeparateTxn(ctx, upstreamExecutor, snapshotName, record.taskID)
	}

	// For dropped records, check if we should delete the record itself
	if record.dropAt != nil && record.state == SubscriptionStateDropped {
		// Check if all snapshots are deleted
		remainingSnapshots, err := queryTaskSnapshots(ctx, upstreamExecutor, record.taskID)
		if err != nil {
			logutil.Error("Publication-Task GC failed to query remaining snapshots",
				zap.Uint64("taskID", record.taskID),
				zap.Error(err),
			)
			return
		}

		if len(remainingSnapshots) == 0 && record.dropAt.Before(gcTime) {
			deleteCcprLogRecordInSeparateTxn(ctx, txnEngine, cnTxnClient, cnUUID, record.taskID)
		}
	}
}

func createUpstreamExecutorForGC(
	ctx context.Context,
	cnUUID string,
	cnTxnClient client.TxnClient,
	txnEngine engine.Engine,
	upstreamSQLHelperFactory UpstreamSQLHelperFactory,
	upstreamConn string,
) (SQLExecutor, error) {
	if upstreamConn == "" {
		return nil, moerr.NewInternalError(ctx, "upstream_conn is empty")
	}

	// Check if it's internal_sql_executor
	if strings.HasPrefix(upstreamConn, InternalSQLExecutorType) {
		parts := strings.Split(upstreamConn, ":")
		upstreamAccountID := catalog.System_Account
		if len(parts) == 2 {
			var accountID uint32
			_, err := fmt.Sscanf(parts[1], "%d", &accountID)
			if err == nil {
				upstreamAccountID = accountID
			}
		}

		upstreamExecutor, err := NewInternalSQLExecutor(
			cnUUID,
			cnTxnClient,
			txnEngine,
			upstreamAccountID,
			NewUpstreamConnectionClassifier(),
		)
		if err != nil {
			return nil, err
		}

		// Set upstream SQL helper if factory is available
		if upstreamSQLHelperFactory != nil {
			helper := upstreamSQLHelperFactory(
				nil,
				txnEngine,
				upstreamAccountID,
				upstreamExecutor.GetInternalExec(),
				upstreamExecutor.GetTxnClient(),
			)
			upstreamExecutor.SetUpstreamSQLHelper(helper)
		}

		return upstreamExecutor, nil
	}

	// Parse external connection string
	connConfig, err := ParseUpstreamConnWithDecrypt(ctx, upstreamConn, nil, cnUUID)
	if err != nil {
		return nil, err
	}

	upstreamExecutor, err := NewUpstreamExecutor(
		connConfig.Account,
		connConfig.User,
		connConfig.Password,
		connConfig.Host,
		connConfig.Port,
		-1, // retryTimes: -1 for infinite retry
		0,  // retryDuration: 0 for no limit
		connConfig.Timeout,
		NewUpstreamConnectionClassifier(),
	)
	if err != nil {
		return nil, err
	}

	return upstreamExecutor, nil
}

type snapshotInfo struct {
	name string
	lsn  uint64
	ts   time.Time
}

func queryTaskSnapshots(
	ctx context.Context,
	upstreamExecutor SQLExecutor,
	taskID uint64,
) ([]snapshotInfo, error) {
	// Query snapshots with pattern ccpr_<taskID>_*
	snapshotPattern := fmt.Sprintf("ccpr_%d_%%", taskID)
	sql := fmt.Sprintf(`SELECT sname, ts FROM mo_catalog.mo_snapshots WHERE sname LIKE '%s' ORDER BY sname`, snapshotPattern)

	result, err := upstreamExecutor.ExecSQL(ctx, nil, sql, false, false)
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var snapshots []snapshotInfo
	for result.Next() {
		var snapshotName string
		var tsValue int64
		if err := result.Scan(&snapshotName, &tsValue); err != nil {
			return nil, err
		}

		// Parse LSN from snapshot name: ccpr_<taskID>_<lsn>
		var parsedTaskID, lsn uint64
		_, err := fmt.Sscanf(snapshotName, "ccpr_%d_%d", &parsedTaskID, &lsn)
		if err != nil {
			logutil.Warn("Publication-Task GC failed to parse snapshot name",
				zap.String("snapshotName", snapshotName),
				zap.Error(err),
			)
			continue
		}

		// Convert ts (nanoseconds) to time.Time
		ts := time.Unix(0, tsValue).UTC()
		snapshots = append(snapshots, snapshotInfo{
			name: snapshotName,
			lsn:  lsn,
			ts:   ts,
		})
	}

	return snapshots, nil
}

func determineSnapshotsToDelete(
	record ccprLogRecord,
	snapshots []snapshotInfo,
	gcTime time.Time,
	snapshotThresholdTime time.Time,
) []string {
	var toDelete []string

	if record.dropAt != nil && record.state == SubscriptionStateDropped {
		// For dropped: delete all snapshots
		for _, snap := range snapshots {
			toDelete = append(toDelete, snap.name)
		}
		return toDelete
	}

	// For running, error, pause: delete snapshots with lsn < current_lsn - 1
	currentLSN := record.iterationLSN
	for _, snap := range snapshots {
		if snap.lsn < currentLSN-1 {
			toDelete = append(toDelete, snap.name)
		}
	}

	// For error and pause: also delete snapshots older than snapshot_threshold
	if record.state == SubscriptionStateError || record.state == SubscriptionStatePause {
		for _, snap := range snapshots {
			if snap.ts.Before(snapshotThresholdTime) {
				// Check if not already in toDelete
				alreadyInList := false
				for _, name := range toDelete {
					if name == snap.name {
						alreadyInList = true
						break
					}
				}
				if !alreadyInList {
					toDelete = append(toDelete, snap.name)
				}
			}
		}
	}

	return toDelete
}

func deleteSnapshotInSeparateTxn(
	ctx context.Context,
	upstreamExecutor SQLExecutor,
	snapshotName string,
	taskID uint64,
) {
	// Each SQL operation in a separate transaction, no retry
	// If error occurs, just log and continue
	dropSQL := PublicationSQLBuilder.DropSnapshotIfExistsSQL(snapshotName)
	result, err := upstreamExecutor.ExecSQL(ctx, nil, dropSQL, false, false)
	if err != nil {
		logutil.Error("Publication-Task GC failed to delete snapshot",
			zap.Uint64("taskID", taskID),
			zap.String("snapshotName", snapshotName),
			zap.Error(err),
		)
		return
	}
	defer result.Close()
	logutil.Info("Publication-Task GC deleted snapshot",
		zap.Uint64("taskID", taskID),
		zap.String("snapshotName", snapshotName),
	)
}

func deleteCcprLogRecordInSeparateTxn(
	ctx context.Context,
	txnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	cnUUID string,
	taskID uint64,
) {
	// Each SQL operation in a separate transaction, no retry
	// If error occurs, just log and continue
	txn, err := getTxn(ctx, txnEngine, cnTxnClient, "publication gc delete record")
	if err != nil {
		logutil.Error("Publication-Task GC failed to create txn for deleting record",
			zap.Uint64("taskID", taskID),
			zap.Error(err),
		)
		return
	}
	defer txn.Commit(ctx)

	deleteSQL := fmt.Sprintf(`DELETE FROM mo_catalog.mo_ccpr_log WHERE task_id = %d`, taskID)
	result, err := ExecWithResult(ctx, deleteSQL, cnUUID, txn)
	if err != nil {
		logutil.Error("Publication-Task GC failed to delete mo_ccpr_log record",
			zap.Uint64("taskID", taskID),
			zap.Error(err),
		)
		return
	}
	defer result.Close()
	logutil.Info("Publication-Task GC deleted mo_ccpr_log record",
		zap.Uint64("taskID", taskID),
	)
}

func retryPublication(
	ctx context.Context,
	fn func() error,
	retryTimes int,
	firstInterval time.Duration,
	totalDuration time.Duration,
) (err error) {
	interval := firstInterval
	startTime := time.Now()
	for i := 0; i < retryTimes; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if time.Since(startTime) > totalDuration {
			break
		}
		err = fn()
		if err == nil {
			return
		}
		time.Sleep(interval)
		interval *= 2
	}
	logutil.Errorf("Publication-Task retry failed, err: %v", err)
	return
}

// Helper functions that need to be implemented or imported
var CollectChanges = func(ctx context.Context, rel engine.Relation, fromTs, toTs types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
	return rel.CollectChanges(ctx, fromTs, toTs, false, mp)
}

var ExecWithResult = func(
	ctx context.Context,
	sql string,
	cnUUID string,
	txn client.TxnOperator,
) (executor.Result, error) {
	// This should be implemented similar to iscp's ExecWithResult
	// Import executor package and use it
	v, ok := moruntime.ServiceRuntime(cnUUID).GetGlobalVariables(moruntime.InternalSQLExecutor)
	if !ok {
		panic("missing internal sql executor")
	}

	exec := v.(executor.SQLExecutor)
	opts := executor.Options{}.
		WithDisableIncrStatement().
		WithTxn(txn)

	return exec.Exec(ctx, sql, opts)
}

var getTxn = func(
	ctx context.Context,
	cnEngine engine.Engine,
	cnTxnClient client.TxnClient,
	info string,
) (client.TxnOperator, error) {
	nowTs := cnEngine.LatestLogtailAppliedTime()
	createByOpt := client.WithTxnCreateBy(
		0,
		"",
		info,
		0)
	op, err := cnTxnClient.New(ctx, nowTs, createByOpt)
	if err != nil {
		return nil, err
	}
	err = cnEngine.New(ctx, op)
	if err != nil {
		return nil, err
	}
	return op, nil
}
