// Copyright 2024 Matrix Origin
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
	"errors"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/btree"
)

// ==== executor.go: Restart error path ====

func TestPublicationTaskExecutor_Restart_InitError(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	assert.Panics(t, func() { exec.Restart() })
}

func TestPublicationTaskExecutor_Resume_InitError(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	assert.Panics(t, func() { exec.Resume() })
}

func TestPublicationTaskExecutor_GetTask_Missing(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.tasks = btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true})
	_, found := exec.GetTask("nonexistent")
	assert.False(t, found)
}

func TestPublicationTaskExecutor_GetAllTasks_EmptyTree(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.tasks = btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true})
	assert.Empty(t, exec.getAllTasks())
}

func TestPublicationTaskExecutor_SetDeleteTask(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.tasks = btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true})
	task := TaskEntry{TaskID: "t1"}
	exec.setTask(task)
	got, found := exec.getTask("t1")
	assert.True(t, found)
	assert.Equal(t, "t1", got.TaskID)
	exec.deleteTaskEntry(task)
	_, found = exec.getTask("t1")
	assert.False(t, found)
}

func TestPublicationTaskExecutor_GetCandidateTasks_Mixed(t *testing.T) {
	exec := &PublicationTaskExecutor{}
	exec.tasks = btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true})
	exec.setTask(TaskEntry{TaskID: "t1", SubscriptionState: SubscriptionStateRunning, State: IterationStateCompleted})
	exec.setTask(TaskEntry{TaskID: "t2", SubscriptionState: SubscriptionStatePause, State: IterationStateCompleted})
	exec.setTask(TaskEntry{TaskID: "t3", SubscriptionState: SubscriptionStateRunning, State: IterationStateCompleted})
	exec.setTask(TaskEntry{TaskID: "t4", SubscriptionState: SubscriptionStateRunning, State: IterationStateRunning})
	candidates := exec.getCandidateTasks()
	assert.Equal(t, 2, len(candidates))
}

// ==== iteration.go: IterationContext ====

func TestIterationContext_StringOutput(t *testing.T) {
	ctx := &IterationContext{TaskID: "task123"}
	s := ctx.String()
	assert.Contains(t, s, "task123")
}

func TestIterationContext_Close_NilExecutors(t *testing.T) {
	ctx := &IterationContext{}
	assert.NoError(t, ctx.Close(false))
}

func TestIterationContext_Close_WithUpstreamExecutor(t *testing.T) {
	mock := &mockSQLExecutor{}
	ctx := &IterationContext{UpstreamExecutor: mock}
	assert.NoError(t, ctx.Close(false))
}

func TestIsStale_EmptyMetadata(t *testing.T) {
	assert.False(t, isStale(&ErrorMetadata{}))
}

func TestIsStale_StaleTrue(t *testing.T) {
	assert.True(t, isStale(&ErrorMetadata{Message: "stale read detected"}))
}

// ==== ddl.go: FillDDLOperation nil/empty ====

func TestFillDDLOperation_NilEngine(t *testing.T) {
	err := FillDDLOperation(context.Background(), nil, nil, nil, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "engine is nil")
}

func TestFillDDLOperation_NilDDLMap(t *testing.T) {
	err := FillDDLOperation(context.Background(), nil, nil, nil, nil, nil)
	assert.Error(t, err)
}

func TestCompareTableDefs_EmptySQL(t *testing.T) {
	_, _, err := compareTableDefsAndGenerateAlterStatements(context.Background(), "db1", "t1", "", "")
	assert.Error(t, err)
}

func TestCompareTableDefs_SameTable(t *testing.T) {
	sql := "CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))"
	stmts, _, err := compareTableDefsAndGenerateAlterStatements(context.Background(), "db1", "t1", sql, sql)
	require.NoError(t, err)
	assert.Empty(t, stmts)
}

func TestCompareTableDefs_AddColumn(t *testing.T) {
	_, _, err := compareTableDefsAndGenerateAlterStatements(
		context.Background(), "db1", "t1",
		"CREATE TABLE t1 (id INT PRIMARY KEY)",
		"CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
	)
	// Column add cannot be done inplace
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "inplace")
}

func TestCompareTableDefs_DropColumn(t *testing.T) {
	_, _, err := compareTableDefsAndGenerateAlterStatements(
		context.Background(), "db1", "t1",
		"CREATE TABLE t1 (id INT PRIMARY KEY, name VARCHAR(100))",
		"CREATE TABLE t1 (id INT PRIMARY KEY)",
	)
	// Column drop cannot be done inplace
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "inplace")
}

// ==== filter_object_batch.go ====

func TestFilterBatchBySnapshotTS_NilBatchCov(t *testing.T) {
	_, err := filterBatchBySnapshotTS(context.Background(), nil, types.TS{}, nil)
	assert.NoError(t, err)
}

func TestCreateObjectFromBatch_NilBatchCov(t *testing.T) {
	var stats objectio.ObjectStats
	result, _, err := createObjectFromBatch(context.Background(), nil, &stats, types.TS{}, false, nil, nil, 0, false)
	assert.NoError(t, err)
	assert.True(t, result.IsZero())
}

// ==== filter_object_submit.go ====

func TestSubmitObjectsAsInsert_Empty(t *testing.T) {
	assert.NoError(t, submitObjectsAsInsert(context.Background(), "t1", nil, nil, nil, nil, nil))
}

func TestSubmitObjectsAsInsert_NilEngine(t *testing.T) {
	stats := []*ObjectWithTableInfo{{}}
	err := submitObjectsAsInsert(context.Background(), "t1", nil, nil, stats, nil, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "engine is nil")
}

func TestSubmitObjectsAsDelete_Empty(t *testing.T) {
	assert.NoError(t, submitObjectsAsDelete(context.Background(), "t1", nil, nil, nil, nil))
}

func TestSubmitObjectsAsDelete_NilEngine(t *testing.T) {
	stats := []*ObjectWithTableInfo{{}}
	err := submitObjectsAsDelete(context.Background(), "t1", nil, nil, stats, nil)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "engine is nil")
}

// ==== filter_object_job.go ====

func TestFilterObjectJob_InvalidStats(t *testing.T) {
	job := NewFilterObjectJob(
		context.Background(),
		nil, types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, nil,
	)
	job.Execute()
	result := job.WaitDone().(*FilterObjectJobResult)
	assert.Error(t, result.Err)
}

func TestFilterObjectJob_ValidStatsEmptyContent(t *testing.T) {
	statsBytes := make([]byte, objectio.ObjectStatsLen)
	job := NewFilterObjectJob(
		context.Background(),
		statsBytes, types.TS{}, nil, false, nil, nil, nil, nil, "", "", nil, nil, nil, nil,
	)
	job.Execute()
	result := job.WaitDone().(*FilterObjectJobResult)
	assert.Error(t, result.Err)
}

// ==== sql_executor.go: InternalSQLExecutor ====

func TestInternalSQLExecutor_ConnectNoop(t *testing.T) {
	e := &InternalSQLExecutor{}
	assert.NoError(t, e.Connect())
}

func TestInternalSQLExecutor_CloseNilTxn(t *testing.T) {
	e := &InternalSQLExecutor{}
	assert.NoError(t, e.Close())
}

func TestInternalSQLExecutor_EndTxnNilTxn(t *testing.T) {
	e := &InternalSQLExecutor{}
	assert.NoError(t, e.EndTxn(context.Background(), true))
	assert.NoError(t, e.EndTxn(context.Background(), false))
}

func TestInternalSQLExecutor_Setters(t *testing.T) {
	e := &InternalSQLExecutor{}
	e.SetUpstreamSQLHelper(nil)
	e.SetUTHelper(nil)
	assert.Nil(t, e.GetInternalExec())
	assert.Nil(t, e.GetTxnClient())
}

func TestDefaultSQLExecutorRetryOpt(t *testing.T) {
	opt := DefaultSQLExecutorRetryOption()
	assert.NotNil(t, opt)
	assert.Equal(t, 10, opt.MaxRetries)
	assert.Equal(t, time.Second, opt.RetryInterval)
}

// ==== sql_executor.go: UpstreamExecutor ====

func TestUpstreamExecutor_EndTxnNilTxCov(t *testing.T) {
	e := &UpstreamExecutor{}
	assert.NoError(t, e.EndTxn(context.Background(), true))
}

func TestUpstreamExecutor_ExecSQLTxnUnsupported(t *testing.T) {
	e := &UpstreamExecutor{}
	_, _, err := e.ExecSQL(context.Background(), nil, InvalidAccountID, "SELECT 1", true, false, 0)
	assert.Error(t, err)
}

func TestUpstreamExecutor_CloseNilConnCov(t *testing.T) {
	e := &UpstreamExecutor{}
	assert.NoError(t, e.Close())
}

func TestUpstreamExecutor_InitRetryPolicyCov(t *testing.T) {
	e := &UpstreamExecutor{retryTimes: 3, retryDuration: time.Minute}
	e.initRetryPolicy(nil)
	assert.NotNil(t, e.retryPolicy)
}

func TestUpstreamExecutor_InitRetryPolicyNegative(t *testing.T) {
	e := &UpstreamExecutor{retryTimes: -1, retryDuration: time.Minute}
	e.initRetryPolicy(nil)
	assert.NotNil(t, e.retryPolicy)
}

// ==== sql_executor.go: Result ====

func TestResult_AllNil(t *testing.T) {
	r := &Result{}
	assert.NoError(t, r.Close())
	assert.False(t, r.Next())
	assert.NoError(t, r.Err())
	var s string
	assert.Error(t, r.Scan(&s))
}

func TestResult_InternalResult_EmptyBatches(t *testing.T) {
	mp, _ := mpool.NewMPool("test", 0, mpool.NoFixed)
	ir := &InternalResult{executorResult: buildResult(mp)}
	r := &Result{internalResult: ir}
	assert.False(t, r.Next())
	assert.NoError(t, r.Err())
}

// ==== memory_controller.go ====

func TestInitGlobalMemoryControllerCoverage(t *testing.T) {
	mp, _ := mpool.NewMPool("test_global_cov", 0, mpool.NoFixed)
	ctrl := InitGlobalMemoryController(mp, 1024*1024)
	assert.NotNil(t, ctrl)
}

// ==== ObjectWithTableInfo ====

func TestObjectWithTableInfo_FieldAccess(t *testing.T) {
	obj := &ObjectWithTableInfo{
		DBName: "db1", TableName: "t1", IsTombstone: true, Delete: false,
	}
	assert.Equal(t, "db1", obj.DBName)
	assert.True(t, obj.IsTombstone)
	assert.False(t, obj.Delete)
}

// ==== error_handle.go: DefaultClassifier ====

func TestDefaultClassifier_NilErr(t *testing.T) {
	c := DefaultClassifier{}
	assert.False(t, c.IsRetryable(nil))
}

func TestDefaultClassifier_GenericError(t *testing.T) {
	c := DefaultClassifier{}
	assert.False(t, c.IsRetryable(errors.New("random")))
}

// ==== config.go ====

func TestGetSyncProtectionTTLDurationCoverage(t *testing.T) {
	d := GetSyncProtectionTTLDuration()
	assert.True(t, d > 0)
}

// ==== BTree ====

func TestBTree_SetGetDelete(t *testing.T) {
	bt := btree.NewBTreeGOptions(taskEntryLess, btree.Options{NoLocks: true})
	assert.Equal(t, 0, bt.Len())
	bt.Set(TaskEntry{TaskID: "a"})
	bt.Set(TaskEntry{TaskID: "b"})
	assert.Equal(t, 2, bt.Len())
	entry, found := bt.Get(TaskEntry{TaskID: "a"})
	assert.True(t, found)
	assert.Equal(t, "a", entry.TaskID)
	bt.Delete(TaskEntry{TaskID: "a"})
	assert.Equal(t, 1, bt.Len())
}

// ==== getDatabaseDiff coverage ====

func TestGetDatabaseDiff_NilIterationCtx(t *testing.T) {
	_, _, err := getDatabaseDiff(context.Background(), nil, nil)
	assert.Error(t, err)
}

func TestGetDatabaseDiff_NilUpstreamExecutor(t *testing.T) {
	_, _, err := getDatabaseDiff(context.Background(), &IterationContext{}, nil)
	assert.Error(t, err)
}

func TestGetDatabaseDiff_NilEngine(t *testing.T) {
	mock := &mockSQLExecutor{}
	_, _, err := getDatabaseDiff(context.Background(), &IterationContext{
		UpstreamExecutor: mock,
	}, nil)
	assert.Error(t, err)
}

func TestGetDatabaseDiff_NilLocalTxn(t *testing.T) {
	mock := &mockSQLExecutor{}
	_, _, err := getDatabaseDiff(context.Background(), &IterationContext{
		UpstreamExecutor: mock,
		LocalTxn:         nil,
	}, nil)
	assert.Error(t, err)
}

// ==== ProcessDDLChanges nil checks ====

func TestProcessDDLChanges_NilIterationCtx(t *testing.T) {
	err := ProcessDDLChanges(context.Background(), nil, nil)
	assert.Error(t, err)
}

func TestProcessDDLChanges_NilLocalExecutor(t *testing.T) {
	err := ProcessDDLChanges(context.Background(), nil, &IterationContext{})
	assert.Error(t, err)
}

func TestProcessDDLChanges_NilLocalTxn(t *testing.T) {
	mock := &mockSQLExecutor{}
	err := ProcessDDLChanges(context.Background(), nil, &IterationContext{
		LocalExecutor: mock,
	})
	assert.Error(t, err)
}

// ==== RequestUpstreamSnapshot nil checks ====

func TestRequestUpstreamSnapshot_NilCtx(t *testing.T) {
	err := RequestUpstreamSnapshot(context.Background(), nil)
	assert.Error(t, err)
}

func TestRequestUpstreamSnapshot_NilUpstreamExecutor(t *testing.T) {
	err := RequestUpstreamSnapshot(context.Background(), &IterationContext{})
	assert.Error(t, err)
}

func TestRequestUpstreamSnapshot_EmptySubscriptionAccountName(t *testing.T) {
	mock := &mockSQLExecutor{}
	err := RequestUpstreamSnapshot(context.Background(), &IterationContext{
		UpstreamExecutor: mock,
	})
	assert.Error(t, err)
}

func TestRequestUpstreamSnapshot_EmptySubscriptionName(t *testing.T) {
	mock := &mockSQLExecutor{}
	err := RequestUpstreamSnapshot(context.Background(), &IterationContext{
		UpstreamExecutor:        mock,
		SubscriptionAccountName: "acc1",
	})
	assert.Error(t, err)
}

// ==== WaitForSnapshotFlushed nil checks ====

func TestWaitForSnapshotFlushed_NilCtx(t *testing.T) {
	err := WaitForSnapshotFlushed(context.Background(), nil, 0, 0)
	assert.Error(t, err)
}

func TestWaitForSnapshotFlushed_NilUpstreamExecutor(t *testing.T) {
	err := WaitForSnapshotFlushed(context.Background(), &IterationContext{}, 0, 0)
	assert.Error(t, err)
}

func TestWaitForSnapshotFlushed_EmptySnapshotName(t *testing.T) {
	mock := &mockSQLExecutor{}
	err := WaitForSnapshotFlushed(context.Background(), &IterationContext{
		UpstreamExecutor: mock,
	}, 0, 0)
	assert.Error(t, err)
}

func TestWaitForSnapshotFlushed_DefaultIntervals(t *testing.T) {
	mock := &mockSQLExecutor{
		execSQLFunc: func(ctx context.Context, ar *ActiveRoutine, accountID uint32, query string, useTxn bool, needRetry bool, timeout time.Duration) (*Result, context.CancelFunc, error) {
			return nil, nil, errors.New("not flushed")
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	err := WaitForSnapshotFlushed(ctx, &IterationContext{
		UpstreamExecutor:    mock,
		CurrentSnapshotName: "snap1",
	}, -1, -1)
	assert.Error(t, err)
}

// ==== GetObjectListFromSnapshotDiff nil checks ====

func TestGetObjectListFromSnapshotDiff_NilCtx(t *testing.T) {
	_, _, err := GetObjectListFromSnapshotDiff(context.Background(), nil)
	assert.Error(t, err)
}

func TestGetObjectListFromSnapshotDiff_NilUpstreamExecutor(t *testing.T) {
	_, _, err := GetObjectListFromSnapshotDiff(context.Background(), &IterationContext{})
	assert.Error(t, err)
}

// ==== CleanPrevData nil checks ====

func TestCleanPrevData_NilEngine(t *testing.T) {
	mock := &mockSQLExecutor{}
	err := CleanPrevData(context.Background(), nil, mock, nil, "task1", 0)
	assert.Error(t, err)
}

func TestCleanPrevData_NilExecutor(t *testing.T) {
	err := CleanPrevData(context.Background(), nil, nil, nil, "task1", 0)
	assert.Error(t, err)
}

func TestCleanPrevData_NilTxn(t *testing.T) {
	mock := &mockSQLExecutor{}
	err := CleanPrevData(context.Background(), nil, mock, nil, "task1", 0)
	assert.Error(t, err)
}
