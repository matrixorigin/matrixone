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

package cdc

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type wmMockSQLExecutor struct {
	mp                        map[string]string
	insertRe                  *regexp.Regexp
	updateRe                  *regexp.Regexp
	selectRe                  *regexp.Regexp
	insertOnDuplicateUpdateRe *regexp.Regexp
}

type retryableMockExecutor struct {
	mu            sync.Mutex
	failRemaining int
	failOnCall    int
	execCalls     int
	queryCalls    int
	lastSQL       string
	sqls          []string
	onExec        func()
}

type delayedWatermarkBatchExecutor struct {
	mu             sync.Mutex
	queryStarted   chan struct{}
	releaseQuery   chan struct{}
	queryStartOnce sync.Once
	rowExists      bool
	errMsgWrites   int
	deleteCalls    int
}

type blockingErrorWatermarkExecutor struct {
	writeStarted chan struct{}
	releaseWrite chan struct{}
	startOnce    sync.Once
}

type watermarkProgressExecutor struct {
	watermark  string
	generation string
	queryErr   error
	lastQuery  string
}

type watermarkReadExecutor struct {
	watermark string
}

func (m *watermarkReadExecutor) Exec(context.Context, string, ie.SessionOverrideOptions) error {
	return nil
}

func (m *watermarkReadExecutor) Query(
	_ context.Context,
	_ string,
	_ ie.SessionOverrideOptions,
) ie.InternalExecResult {
	return &InternalExecResultForTest{resultSet: &MysqlResultSetForTest{Data: [][]interface{}{
		{"7", "task", "db", "tbl", m.watermark},
	}}}
}

func (m *watermarkReadExecutor) ApplySessionOverride(ie.SessionOverrideOptions) {}

func (m *watermarkProgressExecutor) Exec(context.Context, string, ie.SessionOverrideOptions) error {
	return nil
}

func (m *watermarkProgressExecutor) Query(
	_ context.Context,
	sql string,
	_ ie.SessionOverrideOptions,
) ie.InternalExecResult {
	m.lastQuery = sql
	if m.queryErr != nil {
		return &InternalExecResultForTest{err: m.queryErr}
	}
	data := make([][]interface{}, 0, 1)
	if m.watermark != "" {
		data = append(data, []interface{}{m.watermark, m.generation})
	}
	return &InternalExecResultForTest{
		resultSet: &MysqlResultSetForTest{Data: data},
	}
}

func (m *watermarkProgressExecutor) ApplySessionOverride(ie.SessionOverrideOptions) {}

type failAddWatermarkExecutor struct {
	insertErr error
}

func (m *retryableMockExecutor) Exec(_ context.Context, sql string, _ ie.SessionOverrideOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.execCalls++
	m.lastSQL = sql
	m.sqls = append(m.sqls, sql)
	if m.failOnCall == m.execCalls {
		return moerr.NewInternalErrorNoCtx("mock exec failure")
	}
	if m.onExec != nil {
		m.onExec()
	}
	if m.failRemaining > 0 {
		m.failRemaining--
		return moerr.NewInternalErrorNoCtx("mock exec failure")
	}
	return nil
}

func (m *retryableMockExecutor) Query(_ context.Context, _ string, _ ie.SessionOverrideOptions) ie.InternalExecResult {
	m.mu.Lock()
	m.queryCalls++
	m.mu.Unlock()
	return &InternalExecResultForTest{}
}

func (m *retryableMockExecutor) ApplySessionOverride(_ ie.SessionOverrideOptions) {}

func (m *delayedWatermarkBatchExecutor) Exec(
	_ context.Context,
	sql string,
	_ ie.SessionOverrideOptions,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case strings.HasPrefix(sql, "DELETE FROM `mo_catalog`.`mo_cdc_watermark`"):
		m.rowExists = false
		m.deleteCalls++
	case strings.Contains(sql, "ON DUPLICATE KEY UPDATE err_msg"):
		m.rowExists = true
		m.errMsgWrites++
	}
	return nil
}

func (m *delayedWatermarkBatchExecutor) Query(
	_ context.Context,
	_ string,
	_ ie.SessionOverrideOptions,
) ie.InternalExecResult {
	m.queryStartOnce.Do(func() { close(m.queryStarted) })
	<-m.releaseQuery
	return &InternalExecResultForTest{
		resultSet: &MysqlResultSetForTest{Data: [][]interface{}{}},
	}
}

func (m *delayedWatermarkBatchExecutor) ApplySessionOverride(_ ie.SessionOverrideOptions) {}

func (m *blockingErrorWatermarkExecutor) Exec(
	_ context.Context,
	sql string,
	_ ie.SessionOverrideOptions,
) error {
	if strings.Contains(sql, "ON DUPLICATE KEY UPDATE err_msg") {
		m.startOnce.Do(func() { close(m.writeStarted) })
		<-m.releaseWrite
	}
	return nil
}

func (m *blockingErrorWatermarkExecutor) Query(
	_ context.Context,
	_ string,
	_ ie.SessionOverrideOptions,
) ie.InternalExecResult {
	return &InternalExecResultForTest{}
}

func (m *blockingErrorWatermarkExecutor) ApplySessionOverride(_ ie.SessionOverrideOptions) {}

func (m *failAddWatermarkExecutor) Exec(_ context.Context, sql string, _ ie.SessionOverrideOptions) error {
	if strings.HasPrefix(sql, "INSERT INTO `mo_catalog`.`mo_cdc_watermark`") {
		return m.insertErr
	}
	return nil
}

func (m *failAddWatermarkExecutor) Query(_ context.Context, sql string, _ ie.SessionOverrideOptions) ie.InternalExecResult {
	if strings.HasPrefix(sql, "SELECT") {
		return &InternalExecResultForTest{
			resultSet: &MysqlResultSetForTest{
				Data: [][]interface{}{},
			},
		}
	}
	return &InternalExecResultForTest{
		resultSet: &MysqlResultSetForTest{
			Data: [][]interface{}{},
		},
	}
}

func (m *failAddWatermarkExecutor) ApplySessionOverride(_ ie.SessionOverrideOptions) {}

func newWmMockSQLExecutor() *wmMockSQLExecutor {
	return &wmMockSQLExecutor{
		mp: make(map[string]string),
		// matches[1] = db_name, matches[2] = table_name, matches[3] = watermark
		insertRe:                  regexp.MustCompile(`^INSERT .* VALUES \(.*\, .*\, \'(.*)\'\, \'(.*)\'\, \'(.*)\'\, \'\'\)$`),
		updateRe:                  regexp.MustCompile(`^UPDATE .* SET watermark\=\'(.*)\' WHERE .* AND db_name \= '(.*)' AND table_name \= '(.*)'$`),
		selectRe:                  regexp.MustCompile(`^SELECT .* AND db_name \= '(.*)' AND table_name \= '(.*)'$`),
		insertOnDuplicateUpdateRe: regexp.MustCompile(`^INSERT .* VALUES \(.*\, .*\, \'(.*)\'\, \'(.*)\'\, \'(.*)\'\, \'\'\) ON DUPLICATE KEY UPDATE watermark \= VALUES\(watermark\)$`),
	}
}

func (m *wmMockSQLExecutor) Exec(_ context.Context, sql string, _ ie.SessionOverrideOptions) error {
	if strings.HasPrefix(sql, "INSERT") {
		matches := m.insertRe.FindStringSubmatch(sql)
		m.mp[GenDbTblKey(matches[1], matches[2])] = matches[3]
	} else if strings.HasPrefix(sql, "UPDATE `mo_catalog`.`mo_cdc_watermark` SET err_msg") {
		// do nothing
	} else if strings.HasPrefix(sql, "UPDATE") {
		matches := m.updateRe.FindStringSubmatch(sql)
		m.mp[GenDbTblKey(matches[2], matches[3])] = matches[1]
	} else if strings.HasPrefix(sql, "DELETE") {
		if strings.Contains(sql, "table_id") {
			delete(m.mp, "db1.t1")
		} else {
			m.mp = make(map[string]string)
		}
	}
	return nil
}

func (m *wmMockSQLExecutor) Query(ctx context.Context, sql string, pts ie.SessionOverrideOptions) ie.InternalExecResult {
	if strings.HasPrefix(sql, "SELECT") {
		matches := m.selectRe.FindStringSubmatch(sql)
		return &InternalExecResultForTest{
			affectedRows: 1,
			resultSet: &MysqlResultSetForTest{
				Columns:    nil,
				Name2Index: nil,
				Data: [][]interface{}{
					{m.mp[GenDbTblKey(matches[1], matches[2])]},
				},
			},
			err: nil,
		}
	}
	return nil
}

func (m *wmMockSQLExecutor) ApplySessionOverride(opts ie.SessionOverrideOptions) {}

func TestAuditAddWatermarkFailureIsReturnedAndNotCached(t *testing.T) {
	insertErr := moerr.NewInternalErrorNoCtx("injected watermark insert failure")
	updater := NewCDCWatermarkUpdater("add-failure", &failAddWatermarkExecutor{
		insertErr: insertErr,
	})
	key := WatermarkKey{
		AccountId: 1,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}
	watermark := types.BuildTS(10, 1)
	job := NewGetOrAddCommittedWMJob(context.Background(), &key, &watermark)

	updater.onJobs(job)

	result := job.GetResult()
	require.ErrorIs(t, result.Err, insertErr)

	updater.RLock()
	_, ok := updater.cacheCommitted[key]
	updater.RUnlock()
	require.False(t, ok)
}

func TestWatermarkUpdater_CommitRetrySuccess(t *testing.T) {
	exec := &retryableMockExecutor{failRemaining: 1}
	updater := NewCDCWatermarkUpdater("retry-success", exec)

	ctx := context.Background()
	key := WatermarkKey{
		AccountId: 1,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}
	ts := types.BuildTS(10, 1)

	err := updater.UpdateWatermarkOnly(ctx, &key, &ts)
	require.NoError(t, err)

	job := NewCommittingWMJob(ctx)
	updater.committingBuffer = append(updater.committingBuffer, job)

	errMsg, err := updater.execBatchUpdateWM()
	require.Error(t, err)
	require.Contains(t, errMsg, "commit watermark batch")
	require.Contains(t, updater.cacheUncommitted, key)
	require.Equal(t, uint32(1), updater.commitFailureCount[key])
	_, opened := updater.commitCircuitOpen[key]
	require.False(t, opened)

	exec.failRemaining = 0
	job = NewCommittingWMJob(ctx)
	updater.committingBuffer = append(updater.committingBuffer, job)

	errMsg, err = updater.execBatchUpdateWM()
	require.NoError(t, err)
	require.Equal(t, "", errMsg)

	_, exists := updater.cacheUncommitted[key]
	require.False(t, exists)
	committed, ok := updater.cacheCommitted[key]
	require.True(t, ok)
	require.Equal(t, ts, committed)
	_, ok = updater.commitFailureCount[key]
	require.False(t, ok)
	_, ok = updater.commitCircuitOpen[key]
	require.False(t, ok)
}

func TestWatermarkUpdater_BoundedBatchPartialFailureRetriesWithoutRegression(t *testing.T) {
	exec := &retryableMockExecutor{failOnCall: 2}
	updater := NewCDCWatermarkUpdater(t.Name(), exec)
	keys := make([]WatermarkKey, watermarkWriteMaxRows+1)
	updater.Lock()
	for i := range keys {
		keys[i] = WatermarkKey{
			AccountId: 1,
			TaskId:    "task",
			DBName:    "db",
			TableName: fmt.Sprintf("table-%03d", i),
		}
		updater.cacheUncommitted[keys[i]] = types.BuildTS(int64(i+1), 1)
	}
	updater.Unlock()
	updater.committingBuffer = append(updater.committingBuffer, NewCommittingWMJob(context.Background()))

	errMsg, err := updater.execBatchUpdateWM()
	require.Error(t, err)
	require.Contains(t, errMsg, "batch 2/2")
	require.Equal(t, 2, exec.execCalls)
	require.Len(t, updater.cacheUncommitted, len(keys))
	require.Empty(t, updater.cacheCommitting)

	newest := types.BuildTS(10_000, 1)
	updater.Lock()
	updater.cacheUncommitted[keys[0]] = newest
	updater.Unlock()
	updater.committingBuffer = append(updater.committingBuffer, NewCommittingWMJob(context.Background()))
	errMsg, err = updater.execBatchUpdateWM()
	require.NoError(t, err)
	require.Empty(t, errMsg)
	require.Equal(t, newest, updater.cacheCommitted[keys[0]])
	require.Empty(t, updater.cacheUncommitted)
}

func TestWatermarkUpdater_DeleteTaskWatermarksDrainsAndFencesCache(t *testing.T) {
	exec := &retryableMockExecutor{}
	updater := NewCDCWatermarkUpdater("delete-task", exec)
	updater.Start()
	defer updater.Stop()

	ctx := context.Background()
	taskKey := WatermarkKey{
		AccountId: 1,
		TaskId:    "dropped-task",
		DBName:    "db",
		TableName: "dropped-table",
	}
	otherKey := WatermarkKey{
		AccountId: 1,
		TaskId:    "running-task",
		DBName:    "db",
		TableName: "running-table",
	}
	taskTS := types.BuildTS(10, 1)
	otherTS := types.BuildTS(20, 1)
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, &taskKey, &taskTS))
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, &otherKey, &otherTS))
	require.NoError(t, updater.ForceFlush(ctx))

	taskFence := NewOwnerFenceForGeneration(time.UnixMicro(10), func(context.Context) error { return nil })
	otherFence := NewOwnerFenceForGeneration(time.UnixMicro(20), func(context.Context) error { return nil })
	circuitGaugeOwned := false
	t.Cleanup(func() {
		if circuitGaugeOwned {
			v2.CdcWatermarkCircuitOpenGauge.Dec()
		}
	})
	claimOnlyKey := WatermarkKey{
		AccountId: 1,
		TaskId:    taskKey.TaskId,
		DBName:    "db",
		TableName: "claim-only-table",
	}
	generationOnlyKey := WatermarkKey{
		AccountId: 1,
		TaskId:    taskKey.TaskId,
		DBName:    "db",
		TableName: "generation-only-table",
	}
	updater.Lock()
	updater.cacheUncommittedGeneration[taskKey] = 10
	updater.cacheCommittingGeneration[taskKey] = 10
	updater.cacheCommittedGeneration[taskKey] = 10
	updater.cacheUncommittedFence[taskKey] = taskFence
	updater.cacheCommittingFence[taskKey] = taskFence
	updater.activeWatermarkFence[taskKey] = taskFence
	updater.errorMetadataCache[taskKey] = &ErrorMetadata{Message: "terminal cleanup"}
	updater.commitFailureCount[taskKey] = watermarkCommitMaxRetries
	updater.commitCircuitOpen[taskKey] = time.Now()
	v2.CdcWatermarkCircuitOpenGauge.Inc()
	circuitGaugeOwned = true
	updater.activeWatermarkFence[claimOnlyKey] = taskFence
	updater.cacheCommittedGeneration[generationOnlyKey] = 10
	updater.cacheCommittedGeneration[otherKey] = 20
	updater.activeWatermarkFence[otherKey] = otherFence
	updater.Unlock()

	deleteErr := updater.DeleteTaskWatermarks(ctx, taskKey.AccountId, taskKey.TaskId)

	updater.RLock()
	_, taskUncommitted := updater.cacheUncommitted[taskKey]
	_, taskCommitting := updater.cacheCommitting[taskKey]
	_, taskCommitted := updater.cacheCommitted[taskKey]
	_, taskUncommittedGeneration := updater.cacheUncommittedGeneration[taskKey]
	_, taskCommittingGeneration := updater.cacheCommittingGeneration[taskKey]
	_, taskGeneration := updater.cacheCommittedGeneration[taskKey]
	_, taskUncommittedFence := updater.cacheUncommittedFence[taskKey]
	_, taskCommittingFence := updater.cacheCommittingFence[taskKey]
	_, taskFenceExists := updater.activeWatermarkFence[taskKey]
	_, taskErrorMetadata := updater.errorMetadataCache[taskKey]
	_, taskFailureCount := updater.commitFailureCount[taskKey]
	_, taskCircuit := updater.commitCircuitOpen[taskKey]
	_, claimOnlyFenceExists := updater.activeWatermarkFence[claimOnlyKey]
	_, generationOnlyExists := updater.cacheCommittedGeneration[generationOnlyKey]
	otherCommitted, otherExists := updater.cacheCommitted[otherKey]
	otherGeneration := updater.cacheCommittedGeneration[otherKey]
	retainedOtherFence := updater.activeWatermarkFence[otherKey]
	updater.RUnlock()
	if !taskCircuit {
		circuitGaugeOwned = false
	}
	require.NoError(t, deleteErr)
	require.False(t, taskUncommitted)
	require.False(t, taskCommitting)
	require.False(t, taskCommitted)
	require.False(t, taskUncommittedGeneration)
	require.False(t, taskCommittingGeneration)
	require.False(t, taskGeneration)
	require.False(t, taskUncommittedFence)
	require.False(t, taskCommittingFence)
	require.False(t, taskFenceExists)
	require.False(t, taskErrorMetadata)
	require.False(t, taskFailureCount)
	require.False(t, taskCircuit)
	require.False(t, claimOnlyFenceExists)
	require.False(t, generationOnlyExists)
	require.True(t, otherExists)
	require.Equal(t, otherTS, otherCommitted)
	require.Equal(t, uint64(20), otherGeneration)
	require.Same(t, otherFence, retainedOtherFence)

	exec.mu.Lock()
	require.Equal(t, CDCSQLBuilder.DeleteWatermarkSQL(taskKey.AccountId, taskKey.TaskId), exec.lastSQL)
	require.GreaterOrEqual(t, exec.execCalls, 2)
	exec.mu.Unlock()
}

func TestWatermarkUpdater_DeleteTaskWatermarksRetriesAfterFlushFailure(t *testing.T) {
	exec := &retryableMockExecutor{failRemaining: 1}
	updater := NewCDCWatermarkUpdater("delete-task-flush-failure", exec)
	updater.Start()
	defer updater.Stop()

	ctx := context.Background()
	key := WatermarkKey{
		AccountId: 2,
		TaskId:    "dropped-task",
		DBName:    "db",
		TableName: "table",
	}
	ts := types.BuildTS(30, 1)
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, &key, &ts))

	require.Error(t, updater.DeleteTaskWatermarks(ctx, key.AccountId, key.TaskId))

	updater.RLock()
	_, uncommittedAfterFailure := updater.cacheUncommitted[key]
	updater.RUnlock()
	require.True(t, uncommittedAfterFailure)

	exec.mu.Lock()
	require.NotEqual(t, CDCSQLBuilder.DeleteWatermarkSQL(key.AccountId, key.TaskId), exec.lastSQL)
	require.Equal(t, 1, exec.execCalls)
	exec.mu.Unlock()

	require.NoError(t, updater.DeleteTaskWatermarks(ctx, key.AccountId, key.TaskId))

	updater.RLock()
	_, uncommitted := updater.cacheUncommitted[key]
	_, committing := updater.cacheCommitting[key]
	_, committed := updater.cacheCommitted[key]
	updater.RUnlock()
	require.False(t, uncommitted)
	require.False(t, committing)
	require.False(t, committed)

	exec.mu.Lock()
	require.Equal(t, CDCSQLBuilder.DeleteWatermarkSQL(key.AccountId, key.TaskId), exec.lastSQL)
	require.Equal(t, 3, exec.execCalls)
	exec.mu.Unlock()
}

func TestWatermarkUpdater_DeleteTaskWatermarksFlushTimeoutKeepsDeleteAuthoritative(t *testing.T) {
	exec := &delayedWatermarkBatchExecutor{
		queryStarted: make(chan struct{}),
		releaseQuery: make(chan struct{}),
	}
	// Model a barrier admitted behind the blocked onJobs batch. It cannot
	// complete before the caller's cleanup deadline.
	updater := NewCDCWatermarkUpdater(
		"delete-task-delayed-batch",
		exec,
		WithCustomizedScheduleJob(func(_ *UpdaterJob) error { return nil }),
	)
	key := WatermarkKey{
		AccountId: 4,
		TaskId:    "dropped-task",
		DBName:    "db",
		TableName: "table",
	}
	watermark := types.BuildTS(50, 1)
	readJob := NewGetOrAddCommittedWMJob(context.Background(), &key, &watermark)
	errMsgJob := NewUpdateWMErrMsgJob(context.Background(), &key, "delayed error")
	batchDone := make(chan struct{})
	go func() {
		updater.onJobs(readJob, errMsgJob)
		close(batchDone)
	}()

	select {
	case <-exec.queryStarted:
	case <-time.After(time.Second):
		require.FailNow(t, "watermark read batch did not block")
	}

	cleanupCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	err := updater.DeleteTaskWatermarks(cleanupCtx, key.AccountId, key.TaskId)
	cancel()
	require.ErrorIs(t, err, context.DeadlineExceeded)

	exec.mu.Lock()
	require.Equal(t, 0, exec.deleteCalls)
	require.False(t, exec.rowExists)
	exec.mu.Unlock()

	// The already-admitted err_msg writer is allowed to finish while the
	// tombstone rejects new work. Since no DELETE was reported successful yet,
	// the lifecycle owner can retry and make deletion terminal afterward.
	close(exec.releaseQuery)
	select {
	case <-batchDone:
	case <-time.After(time.Second):
		require.FailNow(t, "delayed watermark batch did not finish")
	}
	exec.mu.Lock()
	require.Equal(t, 1, exec.errMsgWrites)
	require.True(t, exec.rowExists)
	exec.mu.Unlock()

	updater.customized.scheduleJob = func(job *UpdaterJob) error {
		updater.onJobs(job)
		return nil
	}
	require.NoError(t, updater.DeleteTaskWatermarks(context.Background(), key.AccountId, key.TaskId))

	exec.mu.Lock()
	require.Equal(t, 1, exec.deleteCalls)
	require.False(t, exec.rowExists)
	exec.mu.Unlock()
}

func TestWatermarkUpdater_DeleteTaskWatermarksReturnsDeleteFailureAndKeepsTombstone(t *testing.T) {
	// The flush is the first persistence call; fail the following terminal
	// DELETE specifically so this test remains distinct from flush failures.
	exec := &retryableMockExecutor{failOnCall: 2}
	updater := NewCDCWatermarkUpdater("delete-task-retry", exec, WithCronJobInterval(time.Hour))
	updater.Start()
	defer updater.Stop()

	ctx := context.Background()
	key := WatermarkKey{AccountId: 3, TaskId: "dropped-task", DBName: "db", TableName: "table"}
	ts := types.BuildTS(40, 1)
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, &key, &ts))
	require.Error(t, updater.DeleteTaskWatermarks(ctx, key.AccountId, key.TaskId))

	// The tombstone must reject a late producer even when the lifecycle owner
	// receives the terminal DELETE failure and is responsible for retrying.
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, &key, &ts))
	updater.RLock()
	_, cached := updater.cacheUncommitted[key]
	updater.RUnlock()
	require.False(t, cached)

	exec.mu.Lock()
	require.Equal(t, 2, exec.execCalls)
	require.Equal(t, CDCSQLBuilder.DeleteWatermarkSQL(key.AccountId, key.TaskId), exec.lastSQL)
	exec.mu.Unlock()
}

func TestWatermarkUpdater_GetOrAddCommittedRejectsDeletedTask(t *testing.T) {
	exec := &retryableMockExecutor{}
	updater := NewCDCWatermarkUpdater("get-or-add-deleted", exec)
	updater.Start()
	defer updater.Stop()
	key := &WatermarkKey{AccountId: 4, TaskId: "deleted-task", DBName: "db", TableName: "table"}
	updater.MarkTaskDeleted(key.TaskId)
	ts := types.BuildTS(50, 1)
	ret, err := updater.GetOrAddCommitted(context.Background(), key, &ts)
	require.NoError(t, err)
	require.True(t, ret.IsEmpty())
	exec.mu.Lock()
	require.Empty(t, exec.lastSQL)
	exec.mu.Unlock()
}

func TestWatermarkUpdater_ForceFlushHonorsContext(t *testing.T) {
	updater := NewCDCWatermarkUpdater("force-flush-context", nil)
	updater.customized.scheduleJob = func(*UpdaterJob) error { return nil }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan error, 1)
	go func() { done <- updater.ForceFlush(ctx) }()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("ForceFlush did not honor canceled context")
	}
}

func TestWatermarkUpdater_ForceFlushWaitsForSameBatchErrorWrite(t *testing.T) {
	exec := &blockingErrorWatermarkExecutor{
		writeStarted: make(chan struct{}),
		releaseWrite: make(chan struct{}),
	}
	updater := NewCDCWatermarkUpdater(t.Name(), exec)
	key := WatermarkKey{
		AccountId: 1,
		TaskId:    "task",
		DBName:    "db",
		TableName: "table",
	}
	updater.cacheCommitted[key] = types.BuildTS(1, 1)
	errorJob := NewUpdateWMErrMsgJob(context.Background(), &key, "old error")
	barrierJob := NewCommittingWMJob(context.Background())
	batchDone := make(chan struct{})
	writeReleased := false
	defer func() {
		if !writeReleased {
			close(exec.releaseWrite)
		}
	}()
	go func() {
		updater.onJobs(errorJob, barrierJob)
		close(batchDone)
	}()

	select {
	case <-exec.writeStarted:
	case <-time.After(time.Second):
		t.Fatal("same-batch error watermark write did not start")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	result := barrierJob.WaitDoneContext(waitCtx)
	cancel()
	require.ErrorIs(t, result.Err, context.DeadlineExceeded,
		"the deletion barrier must not complete before a same-batch writer")

	close(exec.releaseWrite)
	writeReleased = true
	select {
	case <-batchDone:
	case <-time.After(time.Second):
		t.Fatal("watermark batch did not finish after releasing the writer")
	}
	require.NoError(t, barrierJob.GetResult().Err)
	require.NoError(t, errorJob.GetResult().Err)
}

func TestWatermarkUpdater_CommitCircuitBreaker(t *testing.T) {
	exec := &retryableMockExecutor{failRemaining: watermarkCommitMaxRetries}
	updater := NewCDCWatermarkUpdater("circuit-breaker", exec)

	ctx := context.Background()
	key := WatermarkKey{
		AccountId: 2,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}
	ts := types.BuildTS(20, 1)
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, &key, &ts))

	for i := 0; i < watermarkCommitMaxRetries; i++ {
		job := NewCommittingWMJob(ctx)
		updater.committingBuffer = append(updater.committingBuffer, job)
		_, err := updater.execBatchUpdateWM()
		require.Error(t, err)
	}

	require.Contains(t, updater.cacheUncommitted, key)
	require.Equal(t, uint32(watermarkCommitMaxRetries), updater.commitFailureCount[key])
	openedAt, opened := updater.commitCircuitOpen[key]
	require.True(t, opened)
	require.True(t, time.Since(openedAt) < watermarkCircuitBreakPeriod)
	require.True(t, updater.IsCircuitBreakerOpen(&key))

	prevCalls := exec.execCalls
	job := NewCommittingWMJob(ctx)
	updater.committingBuffer = append(updater.committingBuffer, job)
	_, err := updater.execBatchUpdateWM()
	require.Error(t, err)
	require.Contains(t, err.Error(), "circuit breaker")
	require.Equal(t, prevCalls, exec.execCalls, "circuit breaker should skip Exec")
	require.Contains(t, updater.cacheUncommitted, key)

	updater.commitCircuitOpen[key] = time.Now().Add(-watermarkCircuitBreakPeriod - time.Millisecond)
	job = NewCommittingWMJob(ctx)
	updater.committingBuffer = append(updater.committingBuffer, job)
	_, err = updater.execBatchUpdateWM()
	require.NoError(t, err)

	_, exists := updater.cacheUncommitted[key]
	require.False(t, exists)
	committed, ok := updater.cacheCommitted[key]
	require.True(t, ok)
	require.Equal(t, ts, committed)
	_, ok = updater.commitCircuitOpen[key]
	require.False(t, ok)
	_, ok = updater.commitFailureCount[key]
	require.False(t, ok)
	require.False(t, updater.IsCircuitBreakerOpen(&key))
}

func TestWatermarkUpdater_ForceFlushRetryIntegration(t *testing.T) {
	exec := &retryableMockExecutor{failRemaining: 1}
	var syncOption UpdateOption = func(u *CDCWatermarkUpdater) {
		u.customized.scheduleJob = func(job *UpdaterJob) error {
			u.onJobs(job)
			return nil
		}
		u.customized.cronJob = func(ctx context.Context) {}
	}
	updater := NewCDCWatermarkUpdater("integration-retry", exec, syncOption)

	ctx := context.Background()
	key := WatermarkKey{
		AccountId: 3,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}
	ts := types.BuildTS(30, 1)
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, &key, &ts))

	err := updater.ForceFlush(ctx)
	require.Error(t, err)
	require.Equal(t, uint32(1), updater.commitFailureCount[key])
	require.Contains(t, updater.cacheUncommitted, key)

	wm, err := updater.GetFromCache(ctx, &key)
	require.NoError(t, err)
	require.Equal(t, ts, wm)

	exec.failRemaining = 0
	err = updater.ForceFlush(ctx)
	require.NoError(t, err)

	wm, err = updater.GetFromCache(ctx, &key)
	require.NoError(t, err)
	require.Equal(t, ts, wm)
	_, inUncommitted := updater.cacheUncommitted[key]
	require.False(t, inUncommitted)
	_, exists := updater.commitFailureCount[key]
	require.False(t, exists)
	_, opened := updater.commitCircuitOpen[key]
	require.False(t, opened)
}

func TestWatermarkUpdater_ForceFlushCircuitBreakerIntegration(t *testing.T) {
	exec := &retryableMockExecutor{failRemaining: watermarkCommitMaxRetries}
	var syncOption UpdateOption = func(u *CDCWatermarkUpdater) {
		u.customized.scheduleJob = func(job *UpdaterJob) error {
			u.onJobs(job)
			return nil
		}
		u.customized.cronJob = func(ctx context.Context) {}
	}
	updater := NewCDCWatermarkUpdater("integration-circuit", exec, syncOption)

	ctx := context.Background()
	key := WatermarkKey{
		AccountId: 4,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}
	ts := types.BuildTS(40, 1)
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, &key, &ts))

	for i := 0; i < watermarkCommitMaxRetries; i++ {
		err := updater.ForceFlush(ctx)
		require.Error(t, err)
	}

	require.Equal(t, uint32(watermarkCommitMaxRetries), updater.commitFailureCount[key])
	require.Contains(t, updater.cacheUncommitted, key)
	openedAt, opened := updater.commitCircuitOpen[key]
	require.True(t, opened)
	require.True(t, time.Since(openedAt) < watermarkCircuitBreakPeriod)
	prevCalls := exec.execCalls

	err := updater.ForceFlush(ctx)
	require.Error(t, err)
	require.Contains(t, err.Error(), "circuit breaker")
	require.Equal(t, prevCalls, exec.execCalls)
	require.Contains(t, updater.cacheUncommitted, key)

	exec.failRemaining = 0
	updater.commitCircuitOpen[key] = time.Now().Add(-watermarkCircuitBreakPeriod - time.Millisecond)

	err = updater.ForceFlush(ctx)
	require.NoError(t, err)

	wm, err := updater.GetFromCache(ctx, &key)
	require.NoError(t, err)
	require.Equal(t, ts, wm)
	_, inUncommitted := updater.cacheUncommitted[key]
	require.False(t, inUncommitted)
	_, opened = updater.commitCircuitOpen[key]
	require.False(t, opened)
	_, exists := updater.commitFailureCount[key]
	require.False(t, exists)
	require.Greater(t, exec.execCalls, prevCalls)
}

func TestWatermarkUpdater_RemoveCachedWM_Idempotent(t *testing.T) {
	exec := &retryableMockExecutor{}
	var syncOption UpdateOption = func(u *CDCWatermarkUpdater) {
		u.customized.scheduleJob = func(job *UpdaterJob) error {
			u.onJobs(job)
			return nil
		}
		u.customized.cronJob = func(ctx context.Context) {}
	}
	updater := NewCDCWatermarkUpdater("remove-idempotent", exec, syncOption)

	ctx := context.Background()
	key := WatermarkKey{
		AccountId: 5,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}
	ts := types.BuildTS(50, 1)

	require.NoError(t, updater.UpdateWatermarkOnly(ctx, &key, &ts))
	require.NoError(t, updater.ForceFlush(ctx))
	require.NoError(t, updater.RemoveCachedWM(ctx, &key, WatermarkCleanupAll))

	updater.RLock()
	_, committedExists := updater.cacheCommitted[key]
	_, uncommittedExists := updater.cacheUncommitted[key]
	_, committingExists := updater.cacheCommitting[key]
	_, errMetaExists := updater.errorMetadataCache[key]
	updater.RUnlock()

	require.False(t, committedExists)
	require.False(t, uncommittedExists)
	require.False(t, committingExists)
	require.False(t, errMetaExists)

	require.NoError(t, updater.RemoveCachedWM(ctx, &key, WatermarkCleanupAll))
}

func TestWatermarkUpdater_RemoveCachedWM_NoExisting(t *testing.T) {
	exec := &retryableMockExecutor{}
	var syncOption UpdateOption = func(u *CDCWatermarkUpdater) {
		u.customized.scheduleJob = func(job *UpdaterJob) error {
			u.onJobs(job)
			return nil
		}
		u.customized.cronJob = func(ctx context.Context) {}
	}
	updater := NewCDCWatermarkUpdater("remove-noexisting", exec, syncOption)

	ctx := context.Background()
	key := WatermarkKey{
		AccountId: 6,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}

	require.NoError(t, updater.RemoveCachedWM(ctx, &key, WatermarkCleanupAll))
}

func TestWatermarkUpdater_RemoveCachedWM_AfterStopUsesFallback(t *testing.T) {
	exec := &retryableMockExecutor{}
	updater := NewCDCWatermarkUpdater("remove-after-stop", exec)
	updater.Start()
	updater.Stop()

	ctx := context.Background()
	key := WatermarkKey{
		AccountId: 7,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}
	ts := types.BuildTS(70, 2)
	now := time.Now()

	updater.Lock()
	updater.cacheCommitted[key] = ts
	updater.cacheUncommitted[key] = ts
	updater.cacheCommitting[key] = ts
	updater.errorMetadataCache[key] = &ErrorMetadata{
		IsRetryable: true,
		RetryCount:  3,
		FirstSeen:   now,
		LastSeen:    now,
		Message:     "fallback cleanup",
	}
	updater.commitCircuitOpen[key] = now
	updater.commitFailureCount[key] = 2
	updater.Unlock()

	require.NoError(t, updater.RemoveCachedWM(ctx, &key, WatermarkCleanupAll))

	updater.RLock()
	_, inCommitted := updater.cacheCommitted[key]
	_, inUncommitted := updater.cacheUncommitted[key]
	_, inCommitting := updater.cacheCommitting[key]
	_, hasErrMeta := updater.errorMetadataCache[key]
	_, circuitOpen := updater.commitCircuitOpen[key]
	_, failureCount := updater.commitFailureCount[key]
	updater.RUnlock()

	require.False(t, inCommitted)
	require.False(t, inUncommitted)
	require.False(t, inCommitting)
	require.False(t, hasErrMeta)
	require.False(t, circuitOpen)
	require.False(t, failureCount)

	_, logExists := updater.fallbackLog.Load(key.String())
	require.True(t, logExists)
}

func TestWatermarkUpdater_RemoveCachedWM_AfterStopRetainsOwnedDiagnostic(t *testing.T) {
	updater := NewCDCWatermarkUpdater(t.Name(), &retryableMockExecutor{})
	updater.Start()
	updater.Stop()

	key := WatermarkKey{
		AccountId: 7, TaskId: t.Name(), DBName: "db", TableName: "tbl",
	}
	t.Cleanup(func() { updater.removeWatermarkMetrics(key) })
	ts := types.BuildTS(70, 2)
	fence := NewOwnerFenceForGeneration(
		time.UnixMicro(123), func(context.Context) error { return nil })
	metadata := &ErrorMetadata{
		IsRetryable: false,
		RetryCount:  MaxRetryCount + 1,
		Message:     "max retry exceeded",
	}

	updater.Lock()
	updater.cacheCommitted[key] = ts
	updater.activeWatermarkFence[key] = fence
	updater.errorMetadataCache[key] = metadata
	v2.CdcTableNonRetryableErrorGauge.WithLabelValues(
		key.String(), "max_retry_exceeded").Set(1)
	updater.Unlock()

	require.NoError(t, updater.RemoveCachedWM(
		context.Background(), &key, WatermarkCleanupKeepDiagnostic))

	updater.RLock()
	_, hasProgress := updater.cacheCommitted[key]
	retainedFence := updater.activeWatermarkFence[key]
	retainedMetadata := updater.errorMetadataCache[key]
	updater.RUnlock()
	require.False(t, hasProgress)
	require.Same(t, fence, retainedFence)
	require.Same(t, metadata, retainedMetadata)
	metric := &dto.Metric{}
	require.NoError(t, v2.CdcTableNonRetryableErrorGauge.WithLabelValues(
		key.String(), "max_retry_exceeded").Write(metric))
	require.Equal(t, float64(1), metric.GetGauge().GetValue())
}

func TestWatermarkUpdater_UpdateErrMsg_AfterStopUsesFallback(t *testing.T) {
	exec := &retryableMockExecutor{}
	updater := NewCDCWatermarkUpdater("update-errmsg-after-stop", exec)
	updater.Start()
	updater.Stop()

	ctx := context.Background()
	key := WatermarkKey{
		AccountId: 9,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}

	require.NoError(t, updater.UpdateWatermarkErrMsg(ctx, &key, "retryable error:temporary", nil))

	updater.RLock()
	meta, exists := updater.errorMetadataCache[key]
	updater.RUnlock()
	require.True(t, exists)
	require.True(t, meta.IsRetryable)
	require.Equal(t, 1, meta.RetryCount)

	require.NoError(t, updater.UpdateWatermarkErrMsg(ctx, &key, "", nil))

	updater.RLock()
	_, exists = updater.errorMetadataCache[key]
	updater.RUnlock()
	require.False(t, exists)
}

func TestWatermarkUpdater_GetOrAddCommitted_AfterStopUsesFallback(t *testing.T) {
	exec := &retryableMockExecutor{}
	updater := NewCDCWatermarkUpdater("get-or-add-after-stop", exec)
	updater.Start()
	updater.Stop()

	ctx := context.Background()
	key := WatermarkKey{
		AccountId: 10,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}
	ts := types.BuildTS(80, 3)
	updater.cacheCommittedGeneration[key] = 99

	ret, err := updater.GetOrAddCommitted(ctx, &key, &ts)
	require.NoError(t, err)
	require.True(t, ret.Equal(&ts))

	updater.RLock()
	committed, ok := updater.cacheCommitted[key]
	_, hasGeneration := updater.cacheCommittedGeneration[key]
	updater.RUnlock()
	require.True(t, ok)
	require.True(t, committed.Equal(&ts))
	require.False(t, hasGeneration,
		"legacy fallback must not retain an unrelated stable generation")
}

func TestWatermarkUpdater_CircuitBreakerHelpers(t *testing.T) {
	exec := &retryableMockExecutor{failRemaining: watermarkCommitMaxRetries}
	var syncOption UpdateOption = func(u *CDCWatermarkUpdater) {
		u.customized.scheduleJob = func(job *UpdaterJob) error {
			u.onJobs(job)
			return nil
		}
		u.customized.cronJob = func(ctx context.Context) {}
	}
	updater := NewCDCWatermarkUpdater("helper-circuit", exec, syncOption)
	updater.Start()
	defer updater.Stop()

	ctx := context.Background()
	key := WatermarkKey{
		AccountId: 8,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}
	ts := types.BuildTS(90, 1)
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, &key, &ts))

	for i := 0; i < watermarkCommitMaxRetries; i++ {
		err := updater.ForceFlush(ctx)
		require.Error(t, err)
	}

	require.True(t, updater.IsCircuitBreakerOpen(&key))
	require.Equal(t, uint32(watermarkCommitMaxRetries), updater.GetCommitFailureCount(&key))

	updater.Lock()
	updater.commitCircuitOpen[key] = time.Now().Add(-watermarkCircuitBreakPeriod * 2)
	updater.Unlock()

	require.False(t, updater.IsCircuitBreakerOpen(&key))
	require.Equal(t, uint32(watermarkCommitMaxRetries), updater.GetCommitFailureCount(&key))

	exec.failRemaining = 0
	require.NoError(t, updater.RemoveCachedWM(ctx, &key, WatermarkCleanupAll))
	require.False(t, updater.IsCircuitBreakerOpen(&key))
	require.Equal(t, uint32(0), updater.GetCommitFailureCount(&key))
}

func TestWatermarkUpdater_NoWatermarkRegressionOnRetry(t *testing.T) {
	exec := &retryableMockExecutor{failRemaining: 1}
	var syncOption UpdateOption = func(u *CDCWatermarkUpdater) {
		u.customized.scheduleJob = func(job *UpdaterJob) error {
			u.onJobs(job)
			return nil
		}
		u.customized.cronJob = func(ctx context.Context) {}
	}
	updater := NewCDCWatermarkUpdater("no-regress", exec, syncOption)

	ctx := context.Background()
	key := WatermarkKey{
		AccountId: 7,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}
	ts1 := types.BuildTS(70, 1)
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, &key, &ts1))

	err := updater.ForceFlush(ctx)
	require.Error(t, err)

	ts2 := types.BuildTS(80, 1)
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, &key, &ts2))

	exec.failRemaining = 0
	err = updater.ForceFlush(ctx)
	require.NoError(t, err)

	wm, err := updater.GetFromCache(ctx, &key)
	require.NoError(t, err)
	require.Equal(t, ts2, wm)
	committed, ok := updater.cacheCommitted[key]
	require.True(t, ok)
	require.Equal(t, ts2, committed)
}

func TestWatermarkUpdater_MockSQLExecutor(t *testing.T) {
	executor := NewMockSQLExecutor()
	err := executor.CreateTable("db1", "t1", []string{"a", "b", "c"}, []string{"a", "b"})
	assert.NoError(t, err)
	err = executor.CreateTable("db1", "t1", []string{"a", "b", "c"}, []string{"a", "b"})
	assert.Error(t, err)
	err = executor.Insert("db1", "t1", []string{"a", "b", "c"}, [][]string{{"1", "2", "3"}, {"4", "5", "6"}}, false)
	assert.NoError(t, err)
	err = executor.Insert("db1", "t1", []string{"a", "b", "c"}, [][]string{{"1", "2", "3"}, {"4", "5", "6"}}, false)
	t.Logf("err: %v", err)
	assert.Error(t, err)
	_, err = executor.GetTableDataByPK("db1", "t2", []string{"1", "2"})
	assert.Error(t, err)
	rows, err := executor.GetTableDataByPK("db1", "t1", []string{"1", "2"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "3"}, rows)
	_, err = executor.GetTableDataByPK("db1", "t1", []string{"1", "2", "3", "4"})
	assert.Error(t, err)

	err = executor.Insert("db1", "t1", []string{"a", "b", "c"}, [][]string{{"1", "2", "33"}, {"4", "5", "66"}}, true)
	assert.NoError(t, err)
	rows, err = executor.GetTableDataByPK("db1", "t1", []string{"1", "2"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"1", "2", "33"}, rows)
	rows, err = executor.GetTableDataByPK("db1", "t1", []string{"4", "5"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"4", "5", "66"}, rows)

	err = executor.Delete("db1", "t1", []string{"1", "3"})
	assert.NoError(t, err)
	err = executor.Delete("db1", "t2", []string{"1", "2"})
	assert.Error(t, err)
	err = executor.Delete("db1", "t1", []string{"1", "2"})
	assert.NoError(t, err)
	rows, err = executor.GetTableDataByPK("db1", "t1", []string{"1", "2"})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(rows))

	err = executor.Delete("db1", "t1", []string{"4", "5"})
	assert.NoError(t, err)
	rows, err = executor.GetTableDataByPK("db1", "t1", []string{"4", "5"})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(rows))

	assert.Equal(t, 0, len(executor.tables[GenDbTblKey("db1", "t1")]))
	assert.Equal(t, 0, len(executor.pkIndexMap[GenDbTblKey("db1", "t1")]))

	err = executor.CreateTable(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{"account_id", "task_id", "db_name", "table_name", "watermark", "err_msg"},
		[]string{"account_id", "task_id", "db_name", "table_name"},
	)
	assert.NoError(t, err)
	u := NewCDCWatermarkUpdater("test", nil)
	jobs := make([]*UpdaterJob, 0, 2)
	jobs = append(jobs, &UpdaterJob{
		Key: &WatermarkKey{
			AccountId: 1,
			TaskId:    "test",
			DBName:    "db1",
			TableName: "t1",
		},
		Watermark: types.BuildTS(1, 1),
	})
	jobs = append(jobs, &UpdaterJob{
		Key: &WatermarkKey{
			AccountId: 2,
			TaskId:    "test",
			DBName:    "db1",
			TableName: "t2",
		},
		Watermark: types.BuildTS(2, 1),
	})
	insertSqls := u.constructAddWMSQLs(jobs)
	require.Len(t, insertSqls, 1)
	insertSql := insertSqls[0]
	t.Logf("insertSql: %s", insertSql)

	err = executor.Exec(context.Background(), insertSql, ie.SessionOverrideOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 2, executor.RowCount("mo_catalog", "mo_cdc_watermark"))
	keys := make(map[WatermarkKey]WatermarkResult)
	keys[*jobs[0].Key] = WatermarkResult{}
	keys[*jobs[1].Key] = WatermarkResult{}
	selectSql := u.constructReadWMSQL(keys)
	t.Logf("selectSql: %s", selectSql)
	tuples := executor.Query(context.Background(), selectSql, ie.SessionOverrideOptions{})
	assert.NoError(t, tuples.Error())
	assert.Equal(t, uint64(2), tuples.RowCount())
	row0, err := tuples.Row(context.Background(), 0)
	assert.NoError(t, err)
	row1, err := tuples.Row(context.Background(), 1)
	assert.NoError(t, err)
	// row0 and row1 disorder
	if row0[0] == "1" {
		assert.Equal(t, []any{"1", "test", "db1", "t1", "1-1"}, row0)
		assert.Equal(t, []any{"2", "test", "db1", "t2", "2-1"}, row1)
		accountId, err := tuples.GetUint64(context.Background(), 0, 0)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), accountId)
		accountId, err = tuples.GetUint64(context.Background(), 1, 0)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), accountId)
		taskId, err := tuples.GetString(context.Background(), 0, 1)
		assert.NoError(t, err)
		assert.Equal(t, "test", taskId)
		taskId, err = tuples.GetString(context.Background(), 1, 1)
		assert.NoError(t, err)
		assert.Equal(t, "test", taskId)
	} else {
		assert.Equal(t, []any{"2", "test", "db1", "t2", "2-1"}, row0)
		assert.Equal(t, []any{"1", "test", "db1", "t1", "1-1"}, row1)
		accountId, err := tuples.GetUint64(context.Background(), 0, 0)
		assert.NoError(t, err)
		assert.Equal(t, uint64(2), accountId)
		accountId, err = tuples.GetUint64(context.Background(), 1, 0)
		assert.NoError(t, err)
		assert.Equal(t, uint64(1), accountId)
		taskId, err := tuples.GetString(context.Background(), 0, 1)
		assert.NoError(t, err)
		assert.Equal(t, "test", taskId)
		taskId, err = tuples.GetString(context.Background(), 1, 1)
		assert.NoError(t, err)
		assert.Equal(t, "test", taskId)
	}

	for i, job := range jobs {
		job.Watermark = types.BuildTS(int64(i+10), 1)
	}

	keys2 := make(map[WatermarkKey]types.TS)
	keys2[*jobs[0].Key] = jobs[0].Watermark
	keys2[*jobs[1].Key] = jobs[1].Watermark

	insertUpdateSqls := u.constructBatchUpdateWMSQLs(keys2)
	require.Len(t, insertUpdateSqls, 1)
	insertUpdateSql := insertUpdateSqls[0]
	t.Logf("insertUpdateSql: %s", insertUpdateSql)
	err = executor.Exec(context.Background(), insertUpdateSql, ie.SessionOverrideOptions{})
	assert.NoError(t, err)
	assert.Equal(t, 2, executor.RowCount("mo_catalog", "mo_cdc_watermark"))

	tuples = executor.Query(context.Background(), selectSql, ie.SessionOverrideOptions{})
	assert.NoError(t, tuples.Error())
	assert.Equal(t, uint64(2), tuples.RowCount())
	row0, err = tuples.Row(context.Background(), 0)
	assert.NoError(t, err)
	row1, err = tuples.Row(context.Background(), 1)
	assert.NoError(t, err)
	if row0[0] == "1" {
		assert.Equal(t, []any{"1", "test", "db1", "t1", "10-1"}, row0)
		assert.Equal(t, []any{"2", "test", "db1", "t2", "11-1"}, row1)
	} else {
		assert.Equal(t, []any{"2", "test", "db1", "t2", "11-1"}, row0)
		assert.Equal(t, []any{"1", "test", "db1", "t1", "10-1"}, row1)
	}

}

// Scenario:
// 1. create a CDCWatermarkUpdater with user-defined cron job
// 2. wait for the cron job to execute 3 times
// 3. check the execution times: should be >= 3
// 4. stop the CDCWatermarkUpdater
// 5. get the execution times
// 5. wait for 5ms
// 6. check the execution times: should be the same as the previous value
// 7. start the CDCWatermarkUpdater
func TestCDCWatermarkUpdater_Basic1(t *testing.T) {
	ie := newWmMockSQLExecutor()
	var cronJobExecNum atomic.Int32
	var wg1 sync.WaitGroup
	wg1.Add(1)
	cronJob := func(ctx context.Context) {
		now := cronJobExecNum.Add(1)
		t.Logf("cronJobExecNum: %d", now)
		if now == 3 {
			wg1.Done()
		}
	}

	u := NewCDCWatermarkUpdater(
		"test",
		ie,
		WithCronJobInterval(time.Millisecond),
		WithCustomizedCronJob(cronJob),
		WithExportStatsInterval(time.Millisecond*5),
	)
	u.Start()
	wg1.Wait()
	assert.GreaterOrEqual(t, cronJobExecNum.Load(), int32(3))
	u.Stop()
	prevNum := cronJobExecNum.Load()
	time.Sleep(time.Millisecond * 5)
	assert.Equal(t, prevNum, cronJobExecNum.Load())
}

func TestCDCWatermarkUpdater_cronRun(t *testing.T) {
	ie := newWmMockSQLExecutor()

	executeError := moerr.NewInternalErrorNoCtx(fmt.Sprintf("%s-execute-error", t.Name()))
	scheduleErr := moerr.NewInternalErrorNoCtx(fmt.Sprintf("%s-schedule-error", t.Name()))

	var passTimes atomic.Uint64
	passScheduler := func(job *UpdaterJob) (err error) {
		job.DoneWithResult(nil)
		passTimes.Add(1)
		return
	}
	var executeErrTimes atomic.Uint64
	executeErrScheduler := func(job *UpdaterJob) (err error) {
		job.DoneWithErr(executeError)
		executeErrTimes.Add(1)
		return
	}
	var scheduleErrTimes atomic.Uint64
	scheduleErrScheduler := func(job *UpdaterJob) (err error) {
		job.DoneWithErr(scheduleErr)
		scheduleErrTimes.Add(1)
		err = scheduleErr
		return
	}
	_ = executeErrScheduler
	_ = scheduleErrScheduler

	implScheduler := passScheduler

	scheduleJob := func(job *UpdaterJob) (err error) {
		return implScheduler(job)
	}
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
		WithCronJobInterval(time.Millisecond),
		WithCronJobErrorSupressTimes(1),
		WithCustomizedScheduleJob(scheduleJob),
	)
	u.Start()
	defer u.Stop()

	// check u.cacheUncommitted is empty logic
	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func() {
		for {
			if u.stats.skipTimes.Load() > 0 {
				wg1.Done()
				break
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg1.Wait()

	ctx := context.Background()

	// add 1 uncommitted watermark and check the execution logic
	err := u.UpdateWatermarkOnly(ctx, new(WatermarkKey), new(types.TS))
	assert.NoError(t, err)

	// wait uncommitted watermark to be commtting
	wg1.Add(2)
	go func() {
		for {
			u.RLock()
			l1 := len(u.cacheCommitting)
			l2 := len(u.cacheUncommitted)
			u.RUnlock()
			if l1 == 1 && l2 == 0 {
				wg1.Done()
				break
			}
			time.Sleep(time.Millisecond)
		}
		for {
			if passTimes.Load() > 0 {
				wg1.Done()
				break
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg1.Wait()
	assert.Equal(t, uint64(1), passTimes.Load())

	// clear cacheCommitting manually
	u.Lock()
	u.cacheCommitting = make(map[WatermarkKey]types.TS)
	u.Unlock()

	implScheduler = executeErrScheduler
	err = u.UpdateWatermarkOnly(ctx, new(WatermarkKey), new(types.TS))
	assert.NoError(t, err)

	wg1.Add(2)
	go func() {
		for {
			if executeErrTimes.Load() > 0 {
				wg1.Done()
				break
			}
			time.Sleep(time.Millisecond)
		}
		for {
			if u.stats.errorTimes.Load() > 0 {
				wg1.Done()
				break
			}
			time.Sleep(time.Millisecond)
		}
	}()
	wg1.Wait()
	assert.Equal(t, uint64(1), executeErrTimes.Load())
	assert.Equal(t, uint64(1), u.stats.errorTimes.Load())
}

func TestCDCWatermarkUpdater_GetFromCache(t *testing.T) {
	ctx := context.Background()
	ie := newWmMockSQLExecutor()
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
	)
	key1 := new(WatermarkKey)
	key1.AccountId = 1
	wm1 := types.BuildTS(1, 1)
	wm2 := types.BuildTS(2, 1)
	err := u.UpdateWatermarkOnly(ctx, key1, &wm1)
	assert.NoError(t, err)

	key2 := new(WatermarkKey)
	key2.AccountId = 2

	// 1. only cacheUncommitted
	_, err = u.GetFromCache(ctx, key2)
	assert.ErrorIs(t, err, ErrNoWatermarkFound)

	rWM, err := u.GetFromCache(ctx, key1)
	assert.NoError(t, err)
	assert.True(t, wm1.EQ(&rWM))

	// 2. only cacheCommitting
	u.cacheUncommitted = make(map[WatermarkKey]types.TS)
	u.cacheCommitting = make(map[WatermarkKey]types.TS)
	u.cacheCommitting[*key1] = wm1
	rWM, err = u.GetFromCache(ctx, key1)
	assert.NoError(t, err)
	assert.True(t, wm1.EQ(&rWM))
	_, err = u.GetFromCache(ctx, key2)
	assert.ErrorIs(t, err, ErrNoWatermarkFound)

	// 3. only cacheCommitted
	u.cacheUncommitted = make(map[WatermarkKey]types.TS)
	u.cacheCommitting = make(map[WatermarkKey]types.TS)
	u.cacheCommitted = make(map[WatermarkKey]types.TS)
	u.cacheCommitted[*key1] = wm1
	rWM, err = u.GetFromCache(ctx, key1)
	assert.NoError(t, err)
	assert.True(t, wm1.EQ(&rWM))
	_, err = u.GetFromCache(ctx, key2)
	assert.ErrorIs(t, err, ErrNoWatermarkFound)

	// 4. cacheUncommitted and cacheCommitting same key with different watermark
	u.cacheUncommitted = make(map[WatermarkKey]types.TS)
	u.cacheCommitting = make(map[WatermarkKey]types.TS)
	u.cacheCommitting[*key1] = wm1
	u.cacheUncommitted[*key1] = wm2
	rWM, err = u.GetFromCache(ctx, key1)
	assert.NoError(t, err)
	assert.Truef(t, wm2.EQ(&rWM), "wm2: %s, rWM: %s", wm2.ToString(), rWM.ToString())
}

// test constructReadWMSQL
func TestCDCWatermarkUpdater_constructReadWMSQL(t *testing.T) {
	ie := newWmMockSQLExecutor()
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
	)
	keys := make(map[WatermarkKey]WatermarkResult)
	key1 := new(WatermarkKey)
	key1.AccountId = 1
	key1.TaskId = "test"
	key1.DBName = "db1"
	key1.TableName = "t1"
	key2 := new(WatermarkKey)
	key2.AccountId = 2
	key2.TaskId = "test"
	key2.DBName = "db2"
	key2.TableName = "t2"
	ts1 := types.BuildTS(1, 1)
	ts2 := types.BuildTS(2, 1)
	keys[*key1] = WatermarkResult{
		Watermark: ts1,
		Ok:        true,
	}
	keys[*key2] = WatermarkResult{
		Watermark: ts2,
		Ok:        true,
	}
	expectedSql1 := "SELECT account_id, task_id, db_name, table_name, watermark FROM " +
		"`mo_catalog`.`mo_cdc_watermark` WHERE " +
		"(account_id = 1 AND task_id = 'test' AND db_name = 'db1' AND table_name = 't1') OR " +
		"(account_id = 2 AND task_id = 'test' AND db_name = 'db2' AND table_name = 't2')"
	expectedSql2 := "SELECT account_id, task_id, db_name, table_name, watermark FROM " +
		"`mo_catalog`.`mo_cdc_watermark` WHERE " +
		"(account_id = 2 AND task_id = 'test' AND db_name = 'db2' AND table_name = 't2') OR " +
		"(account_id = 1 AND task_id = 'test' AND db_name = 'db1' AND table_name = 't1')"
	realSql := u.constructReadWMSQL(keys)
	assert.True(t, expectedSql1 == realSql || expectedSql2 == realSql)
}

func TestCDCWatermarkUpdater_constructAddWMSQL(t *testing.T) {
	ie := newWmMockSQLExecutor()
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
	)
	keys := make([]*UpdaterJob, 0, 1)
	key1 := new(WatermarkKey)
	key1.AccountId = 1
	key1.TaskId = "test"
	key1.DBName = "db1"
	key1.TableName = "t1"
	ts1 := types.BuildTS(1, 1)
	keys = append(keys, &UpdaterJob{
		Key:       key1,
		Watermark: ts1,
	})
	key2 := new(WatermarkKey)
	key2.AccountId = 2
	key2.TaskId = "test"
	key2.DBName = "db2"
	key2.TableName = "t2"
	ts2 := types.BuildTS(2, 1)
	keys = append(keys, &UpdaterJob{
		Key:       key2,
		Watermark: ts2,
	})
	key3 := new(WatermarkKey)
	key3.AccountId = 3
	key3.TaskId = "test"
	key3.DBName = "db3"
	key3.TableName = "t3"
	ts3 := types.BuildTS(3, 1)
	keys = append(keys, &UpdaterJob{
		Key:       key3,
		Watermark: ts3,
	})
	sqls := u.constructAddWMSQLs(keys)
	require.Len(t, sqls, 1)
	realSql := sqls[0]
	assert.Contains(t, realSql, "INNER JOIN (SELECT account_id, task_id FROM `mo_catalog`.`mo_cdc_task`")
	assert.Contains(t, realSql, "FOR UPDATE)")
	assert.Contains(t, realSql, "SELECT 1 AS account_id, 'test' AS task_id, 'db1' AS db_name")
	assert.Contains(t, realSql, "SELECT 2, 'test', 'db2', 't2', '2-1', ''")
	assert.Contains(t, realSql, "SELECT 3, 'test', 'db3', 't3', '3-1', ''")
}

func TestCDCWatermarkUpdater_constructBatchUpdateWMSQL(t *testing.T) {
	ie := newWmMockSQLExecutor()
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
	)
	keys := make(map[WatermarkKey]types.TS)
	key1 := new(WatermarkKey)
	key1.AccountId = 1
	key1.TaskId = "test"
	key1.DBName = "db1"
	key1.TableName = "t1"
	ts1 := types.BuildTS(1, 1)
	keys[*key1] = ts1
	key2 := new(WatermarkKey)
	key2.AccountId = 2
	key2.TaskId = "test"
	key2.DBName = "db2"
	key2.TableName = "t2"
	ts2 := types.BuildTS(2, 1)
	keys[*key2] = ts2
	key3 := new(WatermarkKey)
	key3.AccountId = 3
	key3.TaskId = "test"
	key3.DBName = "db3"
	key3.TableName = "t3"
	ts3 := types.BuildTS(3, 1)
	keys[*key3] = ts3
	sqls := u.constructBatchUpdateWMSQLs(keys)
	require.Len(t, sqls, 1)
	realSql := sqls[0]
	assert.Contains(t, realSql, "INNER JOIN (SELECT account_id, task_id FROM `mo_catalog`.`mo_cdc_task`")
	assert.Contains(t, realSql, "FOR UPDATE)")
	assert.Contains(t, realSql, "ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)")
	assert.Contains(t, realSql, "SELECT 1 AS account_id")
	assert.Contains(t, realSql, "SELECT 2, 'test', 'db2', 't2', '2-1'")
	assert.Contains(t, realSql, "SELECT 3, 'test', 'db3', 't3', '3-1'")
}

func TestCDCWatermarkUpdaterPartitionsStableMonotonicWatermarks(t *testing.T) {
	exec := &retryableMockExecutor{}
	updater := NewCDCWatermarkUpdater(t.Name(), exec)
	legacyKey := &WatermarkKey{AccountId: 1, TaskId: "legacy", DBName: "db", TableName: "t1"}
	stableKey := &WatermarkKey{AccountId: 1, TaskId: "stable", DBName: "db", TableName: "t2"}
	watermark := types.BuildTS(100, 2)
	require.NoError(t, updater.UpdateWatermarkOnly(context.Background(), legacyKey, &watermark))
	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(context.Background(), NewOwnerFenceForGeneration(
			time.UnixMicro(123), func(context.Context) error { return nil }), 22),
		stableKey,
		&watermark,
	))

	updater.committingBuffer = append(updater.committingBuffer, NewCommittingWMJob(context.Background()))
	_, err := updater.execBatchUpdateWM()
	require.NoError(t, err)
	require.Equal(t, 2, exec.execCalls)
	require.Len(t, exec.sqls, 2)
	if strings.HasPrefix(exec.sqls[0], "UPDATE `mo_catalog`.`mo_cdc_watermark` AS w") {
		exec.sqls[0], exec.sqls[1] = exec.sqls[1], exec.sqls[0]
	}
	require.Contains(t, exec.sqls[0], "INSERT INTO")
	require.Contains(t, exec.sqls[0], "'legacy'")
	require.True(t, strings.HasPrefix(exec.sqls[1], "UPDATE `mo_catalog`.`mo_cdc_watermark` AS w"))
	require.NotContains(t, exec.sqls[1], "INSERT INTO")
	require.NotContains(t, exec.sqls[1], "ON DUPLICATE KEY")
	require.Contains(t, exec.sqls[1], "SUBSTRING_INDEX")
	require.Contains(t, exec.sqls[1], "source_table_id")
	require.Contains(t, exec.sqls[1], "22 AS source_table_id")
	require.Contains(t, exec.sqls[1], "123 AS owner_generation")
	require.NotContains(t, exec.sqls[1], "mo_cdc_snapshot")
	require.Contains(t, exec.sqls[1], "v.owner_generation = w.owner_generation")
	stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, exec.sqls[1], 1)
	require.NoError(t, err)
	stmt.Free()
	require.Contains(t, exec.sqls[1], "'stable'")
}

func TestCDCWatermarkUpdaterIsolatesSQLFailureByProtocolBatch(t *testing.T) {
	exec := &retryableMockExecutor{failRemaining: 1}
	updater := NewCDCWatermarkUpdater(t.Name(), exec)
	legacyKey := &WatermarkKey{AccountId: 1, TaskId: "legacy", DBName: "db", TableName: "t1"}
	stableKey := &WatermarkKey{AccountId: 1, TaskId: "stable", DBName: "db", TableName: "t2"}
	watermark := types.BuildTS(100, 2)
	require.NoError(t, updater.UpdateWatermarkOnly(context.Background(), legacyKey, &watermark))
	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(
			context.Background(), NewOwnerFenceForGeneration(time.UnixMicro(1), func(context.Context) error { return nil }), 22),
		stableKey,
		&watermark,
	))

	updater.committingBuffer = append(updater.committingBuffer, NewCommittingWMJob(context.Background()))
	_, err := updater.execBatchUpdateWM()
	require.Error(t, err)
	require.Equal(t, 2, exec.execCalls)
	require.Equal(t, watermark, updater.cacheUncommitted[*legacyKey])
	_, legacyCommitted := updater.cacheCommitted[*legacyKey]
	require.False(t, legacyCommitted)
	require.Equal(t, watermark, updater.cacheCommitted[*stableKey])
	require.Equal(t, uint64(22), updater.cacheCommittedGeneration[*stableKey])
	_, stableRetried := updater.cacheUncommitted[*stableKey]
	require.False(t, stableRetried)
}

func TestCDCWatermarkUpdaterLoadsProgressAsOneDurableTuple(t *testing.T) {
	exec := &watermarkProgressExecutor{watermark: "123-4", generation: "19"}
	updater := NewCDCWatermarkUpdater(t.Name(), exec)
	key := &WatermarkKey{AccountId: 7, TaskId: "t'ask", DBName: "d'b", TableName: "t'bl"}

	watermark, generation, err := updater.GetWatermarkProgress(context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, types.BuildTS(123, 4), watermark)
	require.Equal(t, uint64(19), generation)
	require.Contains(t, exec.lastQuery, "SELECT watermark, source_table_id")
	require.Contains(t, exec.lastQuery, "task_id = 't''ask'")
	require.Contains(t, exec.lastQuery, "db_name = 'd''b'")
	require.Contains(t, exec.lastQuery, "table_name = 't''bl'")

	cachedWatermark, cachedGeneration, err := updater.GetFromCacheWithGeneration(
		context.Background(), key)
	require.NoError(t, err)
	require.Equal(t, watermark, cachedWatermark)
	require.Equal(t, generation, cachedGeneration)
}

func TestCDCWatermarkUpdaterLegacyReadMaintainsProgressTupleInvariant(t *testing.T) {
	key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: "tbl"}
	candidate := types.BuildTS(1, 0)

	t.Run("malformed catalog watermark returns error without panic", func(t *testing.T) {
		updater := NewCDCWatermarkUpdater(t.Name(), &watermarkReadExecutor{watermark: "corrupt"})
		job := NewGetOrAddCommittedWMJob(context.Background(), key, &candidate)
		updater.onJobs(job)
		require.ErrorContains(t, job.GetResult().Err, "invalid CDC watermark")
		require.Empty(t, updater.cacheCommitted)
	})

	t.Run("legacy projection clears stale generation sidecar", func(t *testing.T) {
		updater := NewCDCWatermarkUpdater(t.Name(), &watermarkReadExecutor{watermark: "123-4"})
		updater.cacheCommittedGeneration[*key] = 99
		job := NewGetOrAddCommittedWMJob(context.Background(), key, &candidate)
		updater.onJobs(job)
		require.NoError(t, job.GetResult().Err)
		require.Equal(t, types.BuildTS(123, 4), updater.cacheCommitted[*key])
		_, hasGeneration := updater.cacheCommittedGeneration[*key]
		require.False(t, hasGeneration)
	})
}

func TestCDCWatermarkUpdaterRejectsInvalidDurableProgress(t *testing.T) {
	key := &WatermarkKey{AccountId: 7, TaskId: "task", DBName: "db", TableName: "tbl"}

	t.Run("missing row is retryable", func(t *testing.T) {
		updater := NewCDCWatermarkUpdater(t.Name(), &watermarkProgressExecutor{})
		_, _, err := updater.GetWatermarkProgress(context.Background(), key)
		require.True(t, IsRetryableSnapshotEpochError(err))
	})

	t.Run("backend error is retryable", func(t *testing.T) {
		backendErr := errors.New("catalog unavailable")
		updater := NewCDCWatermarkUpdater(t.Name(), &watermarkProgressExecutor{queryErr: backendErr})
		_, _, err := updater.GetWatermarkProgress(context.Background(), key)
		require.ErrorIs(t, err, backendErr)
		require.True(t, IsRetryableSnapshotEpochError(err))
	})

	t.Run("caller cancellation remains control flow", func(t *testing.T) {
		updater := NewCDCWatermarkUpdater(t.Name(), &watermarkProgressExecutor{
			queryErr: context.Canceled,
		})
		_, _, err := updater.GetWatermarkProgress(context.Background(), key)
		require.ErrorIs(t, err, context.Canceled)
		require.False(t, IsRetryableSnapshotEpochError(err))
	})

	for _, invalid := range []string{"bad", "1-bad", "1-2-3", "-1-0"} {
		t.Run("malformed "+invalid, func(t *testing.T) {
			updater := NewCDCWatermarkUpdater(t.Name(), &watermarkProgressExecutor{
				watermark:  invalid,
				generation: "19",
			})
			_, _, err := updater.GetWatermarkProgress(context.Background(), key)
			require.Error(t, err)
			require.False(t, IsRetryableSnapshotEpochError(err))
		})
	}
}

func TestCDCWatermarkUpdaterOrdersProgressByGenerationBeforeTimestamp(t *testing.T) {
	updater := NewCDCWatermarkUpdater(t.Name(), &retryableMockExecutor{})
	key := &WatermarkKey{AccountId: 1, TaskId: "task", DBName: "db", TableName: "tbl"}
	oldHighWatermark := types.BuildTS(1000, 0)
	newLowWatermark := types.BuildTS(100, 0)
	oldFence := NewOwnerFenceForGeneration(time.Unix(100, 0), func(context.Context) error { return nil })
	newFence := NewOwnerFenceForGeneration(time.Unix(200, 0), func(context.Context) error { return nil })

	updater.cacheCommitted[*key] = oldHighWatermark
	updater.cacheCommittedGeneration[*key] = 11
	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(context.Background(), newFence, 12), key, &newLowWatermark))
	require.Equal(t, newLowWatermark, updater.cacheUncommitted[*key])
	require.Equal(t, uint64(12), updater.cacheUncommittedGeneration[*key])

	// A delayed old owner cannot win by presenting a numerically larger
	// timestamp from the retired source relation.
	staleWatermark := types.BuildTS(2000, 0)
	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(context.Background(), oldFence, 11), key, &staleWatermark))
	require.Equal(t, newLowWatermark, updater.cacheUncommitted[*key])
	require.Equal(t, uint64(12), updater.cacheUncommittedGeneration[*key])
}

func TestCDCWatermarkUpdaterRejectsRetiredSameProcessOwnerAfterCommit(t *testing.T) {
	updater := NewCDCWatermarkUpdater(t.Name(), &retryableMockExecutor{})
	key := &WatermarkKey{AccountId: 1, TaskId: "task", DBName: "db", TableName: "tbl"}
	oldFence := NewOwnerFenceForGeneration(time.Unix(100, 0), func(context.Context) error { return nil })
	newFence := NewOwnerFenceForGeneration(time.Unix(200, 0), func(context.Context) error { return nil })

	updater.Lock()
	require.True(t, updater.activateWatermarkFenceLocked(*key, oldFence))
	require.True(t, updater.activateWatermarkFenceLocked(*key, newFence))
	updater.Unlock()

	// The old target commit may finish after same-CN Resume/Restart published
	// the new fence. It must not recreate local progress that takeover cleared.
	stale := types.BuildTS(200, 0)
	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(context.Background(), oldFence, 12), key, &stale))
	require.NotContains(t, updater.cacheUncommitted, *key)

	fresh := types.BuildTS(100, 0)
	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(context.Background(), newFence, 12), key, &fresh))
	require.Equal(t, fresh, updater.cacheUncommitted[*key])
	require.Same(t, newFence, updater.cacheUncommittedFence[*key])
}

func TestCDCWatermarkUpdaterRejectsOwnerFenceWithoutSourceGeneration(t *testing.T) {
	updater := NewCDCWatermarkUpdater(t.Name(), &retryableMockExecutor{})
	key := &WatermarkKey{AccountId: 1, TaskId: "task", DBName: "db", TableName: "tbl"}
	watermark := types.BuildTS(100, 0)
	fence := NewOwnerFenceForGeneration(time.UnixMicro(1), func(context.Context) error { return nil })

	err := updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(context.Background(), fence, 0), key, &watermark)
	require.ErrorContains(t, err, "source table generation")
	require.Empty(t, updater.cacheUncommitted)

	err = updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(context.Background(), NewOwnerFence(func(context.Context) error { return nil }), 12),
		key, &watermark)
	require.ErrorContains(t, err, "durable owner generation")
	require.Empty(t, updater.cacheUncommitted)
}

func TestCDCWatermarkUpdaterRetriesTransientOwnerCheck(t *testing.T) {
	exec := &retryableMockExecutor{}
	updater := NewCDCWatermarkUpdater(t.Name(), exec)
	key := &WatermarkKey{AccountId: 1, TaskId: "task", DBName: "db", TableName: "tbl"}
	watermark := types.BuildTS(100, 0)
	backendErr := errors.New("task storage unavailable")
	fence := NewOwnerFenceForGeneration(time.UnixMicro(1), func(context.Context) error { return backendErr })
	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(context.Background(), fence, 12), key, &watermark))

	updater.committingBuffer = append(updater.committingBuffer, NewCommittingWMJob(context.Background()))
	errMsg, err := updater.execBatchUpdateWM()
	require.ErrorIs(t, err, backendErr)
	require.Contains(t, errMsg, backendErr.Error())
	require.NotContains(t, errMsg, "commit sql")
	require.Zero(t, exec.execCalls, "unverified owner must not publish watermark SQL")
	require.Equal(t, watermark, updater.cacheUncommitted[*key])
	require.Equal(t, uint64(12), updater.cacheUncommittedGeneration[*key])
	require.Same(t, fence, updater.cacheUncommittedFence[*key])
}

func TestCDCWatermarkUpdaterIsolatesTransientFenceFailurePerKey(t *testing.T) {
	exec := &retryableMockExecutor{}
	updater := NewCDCWatermarkUpdater(t.Name(), exec)
	goodKey := &WatermarkKey{AccountId: 1, TaskId: "good", DBName: "db", TableName: "tbl"}
	retryKey := &WatermarkKey{AccountId: 1, TaskId: "retry", DBName: "db", TableName: "tbl"}
	watermark := types.BuildTS(100, 0)
	backendErr := errors.New("task storage unavailable")
	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(
			context.Background(), NewOwnerFenceForGeneration(time.UnixMicro(1), func(context.Context) error { return nil }), 12),
		goodKey,
		&watermark,
	))
	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(
			context.Background(), NewOwnerFenceForGeneration(time.UnixMicro(1), func(context.Context) error { return backendErr }), 12),
		retryKey,
		&watermark,
	))

	updater.committingBuffer = append(updater.committingBuffer, NewCommittingWMJob(context.Background()))
	_, err := updater.execBatchUpdateWM()
	require.ErrorIs(t, err, backendErr)
	require.Equal(t, 1, exec.execCalls, "verified keys should still make progress")
	require.Equal(t, watermark, updater.cacheCommitted[*goodKey])
	require.Equal(t, uint64(12), updater.cacheCommittedGeneration[*goodKey])
	_, goodRetried := updater.cacheUncommitted[*goodKey]
	require.False(t, goodRetried)
	_, goodFailed := updater.commitFailureCount[*goodKey]
	require.False(t, goodFailed, "an unrelated fence outage must not trip this key's circuit")
	require.Equal(t, watermark, updater.cacheUncommitted[*retryKey])
	require.Equal(t, uint32(1), updater.commitFailureCount[*retryKey])
}

func TestCDCWatermarkUpdaterFailedWriteKeepsNewOwnerForEqualProgress(t *testing.T) {
	exec := &retryableMockExecutor{failRemaining: 1}
	updater := NewCDCWatermarkUpdater(t.Name(), exec)
	key := &WatermarkKey{AccountId: 1, TaskId: "task", DBName: "db", TableName: "tbl"}
	watermark := types.BuildTS(100, 0)
	oldFence := NewOwnerFenceForGeneration(time.Unix(100, 0), func(context.Context) error { return nil })
	newFence := NewOwnerFenceForGeneration(time.Unix(200, 0), func(context.Context) error { return nil })
	require.NoError(t, updater.UpdateWatermarkOnly(
		WithWatermarkOwnerFence(context.Background(), oldFence, 12), key, &watermark))

	// Simulate the replacement owner publishing the same idempotent progress
	// while the old owner's SQL is in flight and then fails.
	exec.onExec = func() {
		require.NoError(t, updater.UpdateWatermarkOnly(
			WithWatermarkOwnerFence(context.Background(), newFence, 12), key, &watermark))
		exec.onExec = nil
	}
	updater.committingBuffer = append(updater.committingBuffer, NewCommittingWMJob(context.Background()))
	_, err := updater.execBatchUpdateWM()
	require.Error(t, err)
	require.Equal(t, watermark, updater.cacheUncommitted[*key])
	require.Equal(t, uint64(12), updater.cacheUncommittedGeneration[*key])
	require.Same(t, newFence, updater.cacheUncommittedFence[*key])
}

func TestCDCWatermarkUpdater_constructBatchUpdateWMErrMsgSQL(t *testing.T) {
	ie := newWmMockSQLExecutor()
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
	)
	jobs := make([]*UpdaterJob, 0, 1)
	key1 := new(WatermarkKey)
	key1.AccountId = 1
	key1.TaskId = "test"
	key1.DBName = "db1"
	key1.TableName = "t1"
	ts1 := types.BuildTS(1, 1)
	jobs = append(jobs, &UpdaterJob{
		Key:       key1,
		Watermark: ts1,
		ErrMsg:    "err1",
	})
	key2 := new(WatermarkKey)
	key2.AccountId = 2
	key2.TaskId = "test"
	key2.DBName = "db2"
	key2.TableName = "t2"
	ts2 := types.BuildTS(2, 1)
	jobs = append(jobs, &UpdaterJob{
		Key:       key2,
		Watermark: ts2,
		ErrMsg:    "",
	})
	sqls := u.constructBatchUpdateWMErrMsgSQLs(jobs)
	require.Len(t, sqls, 1)
	realSql := sqls[0]
	assert.Contains(t, realSql, "INNER JOIN (SELECT account_id, task_id FROM `mo_catalog`.`mo_cdc_task`")
	assert.Contains(t, realSql, "FOR UPDATE)")
	assert.Contains(t, realSql, "ON DUPLICATE KEY UPDATE err_msg = VALUES(err_msg)")
	assert.Contains(t, realSql, "SELECT 1 AS account_id")
	assert.Contains(t, realSql, "SELECT 2, 'test', 'db2', 't2', ''")
}

func TestCDCWatermarkUpdaterConstructOwnedErrorUpdateSQL(t *testing.T) {
	u := NewCDCWatermarkUpdater(t.Name(), newWmMockSQLExecutor())
	key := &WatermarkKey{AccountId: 7, TaskId: "stable", DBName: "db", TableName: "tbl"}
	fence := NewOwnerFenceForGeneration(
		time.UnixMicro(123), func(context.Context) error { return nil })
	job := NewUpdateWMErrMsgJob(
		WithWatermarkOwnerFence(context.Background(), fence, 11), key, "failed")

	sqls := u.constructBatchUpdateWMErrMsgSQLs([]*UpdaterJob{job})
	require.Len(t, sqls, 1)
	require.True(t, strings.HasPrefix(sqls[0], "UPDATE `mo_catalog`.`mo_cdc_watermark` AS w"))
	require.Contains(t, sqls[0], "w.owner_generation = v.owner_generation")
	require.Contains(t, sqls[0], "123 AS owner_generation")
	require.Contains(t, sqls[0], "SET w.err_msg = v.err_msg")
	require.NotContains(t, sqls[0], "INSERT INTO")
	require.NotContains(t, sqls[0], "ON DUPLICATE KEY")
}

func TestCDCWatermarkUpdaterRejectsOwnerErrorWithoutDurableGeneration(t *testing.T) {
	u := NewCDCWatermarkUpdater(t.Name(), newWmMockSQLExecutor())
	key := &WatermarkKey{AccountId: 7, TaskId: "stable", DBName: "db", TableName: "tbl"}
	fence := NewOwnerFence(func(context.Context) error { return nil })

	err := u.UpdateWatermarkErrMsg(
		WithWatermarkOwnerFence(context.Background(), fence, 11),
		key,
		"failed",
		&ErrorContext{IsRetryable: true},
	)
	require.ErrorContains(t, err, "durable owner generation")
}

func TestCDCWatermarkUpdaterOwnedErrorDoesNotReadMissingProgress(t *testing.T) {
	exec := &retryableMockExecutor{}
	u := NewCDCWatermarkUpdater(t.Name(), exec, WithCronJobInterval(time.Hour))
	u.Start()
	defer u.Stop()
	key := &WatermarkKey{AccountId: 7, TaskId: "stable", DBName: "db", TableName: "tbl"}
	fence := NewOwnerFenceForGeneration(
		time.UnixMicro(123), func(context.Context) error { return nil })
	u.activeWatermarkFence[*key] = fence

	require.NoError(t, u.UpdateWatermarkErrMsg(
		WithWatermarkOwnerFence(context.Background(), fence, 11),
		key,
		"failed",
		&ErrorContext{IsRetryable: true},
	))
	exec.mu.Lock()
	defer exec.mu.Unlock()
	require.Equal(t, 0, exec.queryCalls)
	require.Equal(t, 1, exec.execCalls)
	require.NotContains(t, exec.lastSQL, "INSERT INTO")
}

func TestCDCWatermarkUpdaterRetiredOwnerCannotMutateLocalErrorState(t *testing.T) {
	u := NewCDCWatermarkUpdater(t.Name(), newWmMockSQLExecutor())
	key := &WatermarkKey{AccountId: 7, TaskId: "stable", DBName: "db", TableName: "tbl"}
	oldFence := NewOwnerFenceForGeneration(
		time.UnixMicro(123), func(context.Context) error { return nil })
	newFence := NewOwnerFenceForGeneration(
		time.UnixMicro(124), func(context.Context) error { return nil })
	existing := &ErrorMetadata{Message: "new owner error", RetryCount: 1, IsRetryable: true}
	u.activeWatermarkFence[*key] = newFence
	u.errorMetadataCache[*key] = existing
	oldCtx := WithWatermarkOwnerFence(context.Background(), oldFence, 11)

	require.NoError(t, u.UpdateWatermarkErrMsg(
		oldCtx, key, "old owner error", &ErrorContext{IsRetryable: true}))
	require.Same(t, existing, u.errorMetadataCache[*key])
	require.NoError(t, u.UpdateWatermarkErrMsg(oldCtx, key, "", nil))
	require.Same(t, existing, u.errorMetadataCache[*key])
}

func TestCDCWatermarkUpdaterLostClaimCannotMutateErrorBeforeReplacementClaim(t *testing.T) {
	u := NewCDCWatermarkUpdater(t.Name(), newWmMockSQLExecutor())
	key := &WatermarkKey{AccountId: 7, TaskId: "stable", DBName: "db", TableName: "tbl"}
	lostFence := NewOwnerFenceForGeneration(time.UnixMicro(123), func(ctx context.Context) error {
		return moerr.NewInvalidTask(ctx, "old-cn", 1)
	})
	existing := &ErrorMetadata{Message: "current diagnostic", RetryCount: 1, IsRetryable: true}
	u.activeWatermarkFence[*key] = lostFence
	u.errorMetadataCache[*key] = existing
	lostCtx := WithWatermarkOwnerFence(context.Background(), lostFence, 11)

	require.NoError(t, u.UpdateWatermarkErrMsg(
		lostCtx, key, "ordinary source error", &ErrorContext{IsRetryable: true}))
	require.Same(t, existing, u.errorMetadataCache[*key])
	require.NoError(t, u.UpdateWatermarkErrMsg(lostCtx, key, "", nil))
	require.Same(t, existing, u.errorMetadataCache[*key])
}

func TestCDCWatermarkUpdaterDiagnosticOwnerCheckBackendFailureIsRetryable(t *testing.T) {
	u := NewCDCWatermarkUpdater(t.Name(), newWmMockSQLExecutor())
	key := &WatermarkKey{AccountId: 7, TaskId: "stable", DBName: "db", TableName: "tbl"}
	backendErr := moerr.NewInternalErrorNoCtx("taskservice unavailable")
	fence := NewOwnerFenceForGeneration(time.UnixMicro(123), func(context.Context) error {
		return backendErr
	})
	err := u.UpdateWatermarkErrMsg(
		WithWatermarkOwnerFence(context.Background(), fence, 11),
		key,
		"ordinary source error",
		&ErrorContext{IsRetryable: true},
	)
	require.Error(t, err)
	require.True(t, IsRetryableOwnerFenceError(err))
	require.NotContains(t, u.errorMetadataCache, *key)

	timeoutFence := NewOwnerFenceForGeneration(time.UnixMicro(124), func(context.Context) error {
		return context.DeadlineExceeded
	})
	err = u.UpdateWatermarkErrMsg(
		WithWatermarkOwnerFence(context.Background(), timeoutFence, 11),
		key,
		"ordinary source error",
		&ErrorContext{IsRetryable: true},
	)
	require.Error(t, err)
	require.True(t, IsRetryableOwnerFenceError(err))
}

func TestCDCWatermarkUpdaterCanceledOwnerCheckDoesNotPublishDiagnostic(t *testing.T) {
	u := NewCDCWatermarkUpdater(t.Name(), newWmMockSQLExecutor())
	key := &WatermarkKey{AccountId: 7, TaskId: "stable", DBName: "db", TableName: "tbl"}
	fence := NewOwnerFenceForGeneration(time.UnixMicro(123), func(ctx context.Context) error {
		return ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.NoError(t, u.UpdateWatermarkErrMsg(
		WithWatermarkOwnerFence(ctx, fence, 11),
		key,
		"shutdown error",
		&ErrorContext{IsRetryable: true},
	))
	require.NotContains(t, u.errorMetadataCache, *key)
}

func TestCDCWatermarkUpdaterReplacementDropsPreviousErrorRetryState(t *testing.T) {
	u := NewCDCWatermarkUpdater(t.Name(), newWmMockSQLExecutor())
	key := WatermarkKey{AccountId: 7, TaskId: t.Name(), DBName: "db", TableName: "tbl"}
	t.Cleanup(func() { u.removeWatermarkErrorMetrics(key) })
	oldFence := NewOwnerFenceForGeneration(
		time.UnixMicro(123), func(context.Context) error { return nil })
	newFence := NewOwnerFenceForGeneration(
		time.UnixMicro(124), func(context.Context) error { return nil })
	u.activeWatermarkFence[key] = oldFence
	u.errorMetadataCache[key] = &ErrorMetadata{RetryCount: MaxRetryCount}
	v2.CdcTableNonRetryableErrorGauge.WithLabelValues(
		key.String(), "max_retry_exceeded").Set(1)

	require.True(t, u.activateWatermarkFenceLocked(key, newFence))
	require.NotContains(t, u.errorMetadataCache, key)
	metric := &dto.Metric{}
	require.NoError(t, v2.CdcTableNonRetryableErrorGauge.WithLabelValues(
		key.String(), "max_retry_exceeded").Write(metric))
	require.Zero(t, metric.GetGauge().GetValue())
}

func TestCDCWatermarkUpdaterFailedStreamRetirementPreservesRetryState(t *testing.T) {
	u := NewCDCWatermarkUpdater(
		t.Name(), &retryableMockExecutor{}, WithCronJobInterval(time.Hour))
	u.Start()
	t.Cleanup(u.Stop)

	key := &WatermarkKey{
		AccountId: 7, TaskId: t.Name(), DBName: "db", TableName: "tbl",
	}
	t.Cleanup(func() {
		require.NoError(t, u.RemoveCachedWM(
			context.Background(), key, WatermarkCleanupAll))
	})
	fence := NewOwnerFenceForGeneration(
		time.UnixMicro(123), func(context.Context) error { return nil })
	ctx := WithWatermarkOwnerFence(context.Background(), fence, 11)

	for want := 1; want <= MaxRetryCount+1; want++ {
		u.Lock()
		require.True(t, u.activateWatermarkFenceLocked(*key, fence))
		u.Unlock()

		require.NoError(t, u.UpdateWatermarkErrMsg(
			ctx, key, "persistent failure", &ErrorContext{IsRetryable: true}))

		u.RLock()
		metadata := u.errorMetadataCache[*key]
		u.RUnlock()
		require.NotNil(t, metadata)
		require.Equal(t, want, metadata.RetryCount)
		require.Equal(t, want <= MaxRetryCount, metadata.IsRetryable)

		require.NoError(t, u.RemoveCachedWM(
			context.Background(), key, WatermarkCleanupKeepDiagnostic))
		u.RLock()
		retained := u.errorMetadataCache[*key]
		active := u.activeWatermarkFence[*key]
		u.RUnlock()
		require.NotNil(t, retained)
		require.Equal(t, want, retained.RetryCount)
		require.Same(t, fence, active)
	}

	metric := &dto.Metric{}
	require.NoError(t, v2.CdcTableNonRetryableErrorGauge.WithLabelValues(
		key.String(), "max_retry_exceeded").Write(metric))
	require.Equal(t, float64(1), metric.GetGauge().GetValue())

	require.NoError(t, u.RemoveCachedWM(
		context.Background(), key, WatermarkCleanupAll))
	u.RLock()
	_, retained := u.errorMetadataCache[*key]
	u.RUnlock()
	require.False(t, retained)
	metric.Reset()
	require.NoError(t, v2.CdcTableNonRetryableErrorGauge.WithLabelValues(
		key.String(), "max_retry_exceeded").Write(metric))
	require.Zero(t, metric.GetGauge().GetValue())
}

func TestCDCWatermarkUpdaterErrorMetricClassificationIsExclusive(t *testing.T) {
	u := NewCDCWatermarkUpdater(t.Name(), &retryableMockExecutor{}, WithCronJobInterval(time.Hour))
	u.Start()
	defer u.Stop()
	key := &WatermarkKey{AccountId: 7, TaskId: t.Name(), DBName: "db", TableName: "tbl"}
	fence := NewOwnerFenceForGeneration(
		time.UnixMicro(123), func(context.Context) error { return nil })
	u.activeWatermarkFence[*key] = fence
	ctx := WithWatermarkOwnerFence(context.Background(), fence, 11)
	readGauge := func(errorType string) float64 {
		metric := &dto.Metric{}
		require.NoError(t, v2.CdcTableNonRetryableErrorGauge.WithLabelValues(
			key.String(), errorType).Write(metric))
		return metric.GetGauge().GetValue()
	}

	require.NoError(t, u.UpdateWatermarkErrMsg(
		ctx, key, "connection failed", &ErrorContext{}))
	require.Equal(t, float64(1), readGauge("network"))

	require.NoError(t, u.UpdateWatermarkErrMsg(
		ctx, key, "commit failed", &ErrorContext{}))
	require.Zero(t, readGauge("network"))
	require.Equal(t, float64(1), readGauge("commit"))
}

func TestCDCWatermarkUpdater_GuardedWatermarkSQLIsBoundedAndDeterministic(t *testing.T) {
	u := NewCDCWatermarkUpdater(t.Name(), newWmMockSQLExecutor())
	keys := make(map[WatermarkKey]types.TS, 1001)
	for i := 1000; i >= 0; i-- {
		keys[WatermarkKey{
			AccountId: 7,
			TaskId:    "same-task",
			DBName:    fmt.Sprintf("db-%04d", i%17),
			TableName: fmt.Sprintf("table-%04d", i),
		}] = types.BuildTS(int64(i+1), 1)
	}

	sqls := u.constructBatchUpdateWMSQLs(keys)
	require.Len(t, sqls, 6)
	for _, sql := range sqls {
		rows := strings.Count(sql, " UNION ALL ") + 1
		require.LessOrEqual(t, rows, watermarkWriteMaxRows)
		require.LessOrEqual(t, len(sql), watermarkWriteMaxSQLBytes)
		require.Equal(t, 1, strings.Count(sql,
			"(account_id = 7 AND task_id = 'same-task')"))
		require.Contains(t, sql, "FOR UPDATE")
	}

	// Map iteration order must not make SQL text or lock acquisition order vary.
	require.Equal(t, sqls, u.constructBatchUpdateWMSQLs(keys))
}

func TestCDCWatermarkUpdater_GuardedWatermarkSQLSplitsOnBytes(t *testing.T) {
	rows := make([]guardedWatermarkRow, 0, 4)
	for i := 0; i < 4; i++ {
		rows = append(rows, guardedWatermarkRow{
			accountID: 9,
			taskID:    "large-error-task",
			dbName:    "db",
			tableName: fmt.Sprintf("table-%d", i),
			value:     strings.Repeat("x", watermarkWriteMaxSQLBytes/3),
		})
	}

	sqls := buildGuardedWatermarkSQLBatches(
		rows,
		watermarkErrorRowSQL,
		CDCSQLBuilder.GuardedWatermarkErrorUpdateSQL,
	)
	require.Len(t, sqls, 2)
	for _, sql := range sqls {
		require.LessOrEqual(t, len(sql), watermarkWriteMaxSQLBytes)
		require.Equal(t, 1, strings.Count(sql,
			"(account_id = 9 AND task_id = 'large-error-task')"))
	}
}

func TestCDCWatermarkUpdater_ErrorMessageRespectsCatalogBound(t *testing.T) {
	executor := &retryableMockExecutor{}
	u := NewCDCWatermarkUpdater(t.Name(), executor, WithCronJobInterval(time.Hour))
	u.Start()
	defer u.Stop()
	key := &WatermarkKey{AccountId: 1, TaskId: "task", DBName: "db", TableName: "table"}
	u.cacheCommitted[*key] = types.BuildTS(1, 1)
	longMessage := strings.Repeat("界", CDCWatermarkErrMsgMaxLen+100)
	for range MaxRetryCount + 1 {
		require.NoError(t, u.UpdateWatermarkErrMsg(
			context.Background(),
			key,
			longMessage,
			&ErrorContext{IsRetryable: true},
		))
	}
	flushCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	require.NoError(t, u.ForceFlush(flushCtx))
	cancel()

	executor.mu.Lock()
	persistedSQL := executor.lastSQL
	executor.mu.Unlock()
	parsed, err := ParseInsert(persistedSQL)
	require.NoError(t, err)
	require.Len(t, parsed.rows, 1)
	require.Len(t, parsed.rows[0], 5)
	persisted := parsed.rows[0][4]
	require.True(t, utf8.ValidString(persisted))
	require.LessOrEqual(t, utf8.RuneCountInString(persisted), CDCWatermarkErrMsgMaxLen)
	require.False(t, u.errorMetadataCache[*key].IsRetryable)
	require.Contains(t, u.errorMetadataCache[*key].Message, "max retry exceeded")
	require.LessOrEqual(t, utf8.RuneCountInString(u.errorMetadataCache[*key].Message), CDCWatermarkErrMsgMaxLen)
}

func BenchmarkCDCWatermarkUpdaterConstructGuardedBatch(b *testing.B) {
	for _, count := range []int{1000, 5000} {
		b.Run(fmt.Sprintf("tables-%d", count), func(b *testing.B) {
			u := NewCDCWatermarkUpdater(b.Name(), newWmMockSQLExecutor())
			keys := make(map[WatermarkKey]types.TS, count)
			for i := 0; i < count; i++ {
				keys[WatermarkKey{
					AccountId: 1,
					TaskId:    "benchmark-task",
					DBName:    "benchmark-db",
					TableName: fmt.Sprintf("table-%05d", i),
				}] = types.BuildTS(int64(i+1), 1)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = u.constructBatchUpdateWMSQLs(keys)
			}
			b.StopTimer()
			sqls := u.constructBatchUpdateWMSQLs(keys)
			totalBytes := 0
			maxBytes := 0
			for _, sql := range sqls {
				totalBytes += len(sql)
				maxBytes = max(maxBytes, len(sql))
			}
			b.ReportMetric(float64(len(sqls)), "batches")
			b.ReportMetric(float64(maxBytes), "max-SQL-B")
			b.ReportMetric(float64(totalBytes), "total-SQL-B")
		})
	}
}

func TestCDCWatermarkUpdater_execBatchUpdateWMErrMsg(t *testing.T) {
	ie := NewMockSQLExecutor()
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
	)
	jobs := make([]*UpdaterJob, 0, 2)
	key1 := new(WatermarkKey)
	key1.AccountId = 1
	key1.TaskId = "test"
	key1.DBName = "db1"
	key1.TableName = "t1"

	jobs = append(jobs, NewUpdateWMErrMsgJob(
		context.Background(),
		key1,
		"err1",
	))

	key2 := new(WatermarkKey)
	key2.AccountId = 2
	key2.TaskId = "test"
	key2.DBName = "db2"
	key2.TableName = "t2"
	jobs = append(jobs, NewUpdateWMErrMsgJob(
		context.Background(),
		key2,
		"err2",
	))

	err := ie.CreateTable(
		`mo_catalog`,
		`mo_cdc_watermark`,
		[]string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
		[]string{"account_id", "task_id", "db_name", "table_name"},
	)
	assert.NoError(t, err)

	err = ie.Insert(
		`mo_catalog`,
		`mo_cdc_watermark`,
		[]string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
		[][]string{{"1", "test", "db1", "t1", ""}, {"2", "test", "db2", "t2", ""}},
		false,
	)
	assert.NoError(t, err)

	u.committingErrMsgBuffer = jobs
	errMsg, err := u.execBatchUpdateWMErrMsg()
	assert.NoError(t, err)
	assert.Equal(t, "", errMsg)

	rowCount := ie.RowCount(`mo_catalog`, `mo_cdc_watermark`)
	assert.Equal(t, 2, rowCount)

	tuple, err := ie.GetTableDataByPK(
		`mo_catalog`,
		`mo_cdc_watermark`,
		[]string{"1", "test", "db1", "t1"},
	)
	assert.NoError(t, err)
	assert.Equal(t, []string{"1", "test", "db1", "t1", "err1"}, tuple)

	tuple, err = ie.GetTableDataByPK(
		`mo_catalog`,
		`mo_cdc_watermark`,
		[]string{"2", "test", "db2", "t2"},
	)
	assert.NoError(t, err)
	assert.Equal(t, []string{"2", "test", "db2", "t2", "err2"}, tuple)
}

func TestCDCWatermarkUpdater_ParseInsert(t *testing.T) {
	expectedSql := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(1, 'test', 'db1', 't1', '1-1')," +
		"(2, 'test', 'db2', 't2', '2-1')," +
		"(3, 'test', 'db3', 't3', '3-1')"
	result, err := ParseInsert(expectedSql)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.kind)
	assert.Equal(t, "mo_catalog", result.dbName)
	assert.Equal(t, "mo_cdc_watermark", result.tableName)
	assert.Equal(t, []string{"account_id", "task_id", "db_name", "table_name", "watermark"}, result.projectionColumns)
	assert.Equal(t, [][]string{{"1", "test", "db1", "t1", "1-1"}, {"2", "test", "db2", "t2", "2-1"}, {"3", "test", "db3", "t3", "3-1"}}, result.rows)

	expectedSql = "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark, err_msg) VALUES " +
		"(1, 'test', 'db1', 't1', '1-1', '')," +
		"(2, 'test', 'db2', 't2', '2-1', 'err1')," +
		"(3, 'test', 'db3', 't3', '3-1', '') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark,err_msg)"
	result, err = ParseInsertOnDuplicateUpdate(expectedSql)
	assert.NoError(t, err)
	assert.Equal(t, 4, result.kind)
	assert.Equal(t, "mo_catalog", result.dbName)
	assert.Equal(t, "mo_cdc_watermark", result.tableName)
	assert.Equal(t, []string{"account_id", "task_id", "db_name", "table_name", "watermark", "err_msg"}, result.projectionColumns)
	assert.Equal(t, [][]string{{"1", "test", "db1", "t1", "1-1", ""}, {"2", "test", "db2", "t2", "2-1", "err1"}, {"3", "test", "db3", "t3", "3-1", ""}}, result.rows)
	assert.Equal(t, []string{"watermark", "err_msg"}, result.updateColumns)

	expectedSql = "INSERT INTO `mo_catalog`.`mo_cdc_watermark` VALUES (1, 'test', 'db1', 't1', '1-1', ''),(1, 'test', 'db1', 't2', '2-1', '')"
	result, err = ParseInsert(expectedSql)
	assert.NoError(t, err)
	assert.Equal(t, 0, result.kind)
	assert.Equal(t, "mo_catalog", result.dbName)
	assert.Equal(t, "mo_cdc_watermark", result.tableName)
	assert.Equal(t, []string{}, result.projectionColumns)
}

func TestCDCWatermarkUpdater_ParseUpdate(t *testing.T) {
	expectedSql := "UPDATE `db1`.`t1` SET col3 = '1-1' WHERE col1 = 1"
	result, err := ParseUpdate(expectedSql)
	assert.NoError(t, err)
	assert.Equal(t, 1, result.kind)
	assert.Equal(t, "db1", result.dbName)
	assert.Equal(t, "t1", result.tableName)
	assert.Equal(t, []string{"col3"}, result.updateColumns)
	assert.Equal(t, [][]string{{"1"}}, result.pkFilters)

	expectedSql = "UPDATE `db1`.`t1` SET col3 = '1-1', col4 = 4, col5 = '5-1' WHERE col1 = 1 AND col2 = 'test'"
	result, err = ParseUpdate(expectedSql)
	assert.NoError(t, err)
	assert.Equal(t, 1, result.kind)
	assert.Equal(t, "db1", result.dbName)
	assert.Equal(t, "t1", result.tableName)
	assert.Equal(t, []string{"col3", "col4", "col5"}, result.updateColumns)
	assert.Equal(t, [][]string{{"1", "test"}}, result.pkFilters)
}

func TestCDCWatermarkUpdater_ParseSelectByPKs(t *testing.T) {
	expectedSql := "SELECT col1, col2, col3 FROM `db1`.`t1` WHERE (col1 = 1 AND col2 = 'test') OR (col1 = 2 AND col2 = 'test2')"
	result, err := ParseSelectByPKs(expectedSql)
	assert.NoError(t, err)
	assert.Equal(t, 3, result.kind)
	assert.Equal(t, "db1", result.dbName)
	assert.Equal(t, "t1", result.tableName)
	assert.Equal(t, []string{"col1", "col2", "col3"}, result.projectionColumns)
	assert.Equal(t, [][]string{{"1", "test"}, {"2", "test2"}}, result.pkFilters)
}

func TestCDCWatermarkUpdater_CDCWatermarkUpdaterRun(t *testing.T) {
	ie := NewMockSQLExecutor()
	err := ie.CreateTable(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{"account_id", "task_id", "db_name", "table_name", "watermark", "err_msg"},
		[]string{"account_id", "task_id", "db_name", "table_name"},
	)
	assert.NoError(t, err)
	u := NewCDCWatermarkUpdater(
		t.Name(),
		ie,
		WithCronJobInterval(time.Millisecond*1),
	)
	u.Start()
	defer u.Stop()

	ctx := context.Background()

	ts := types.BuildTS(1, 1)
	key := &WatermarkKey{
		AccountId: 1,
		TaskId:    "task1",
		DBName:    "db1",
		TableName: "t1",
	}
	ret, err := u.GetOrAddCommitted(
		ctx,
		key,
		&ts,
	)
	assert.NoError(t, err)
	assert.Equal(t, ts, ret)

	ret, err = u.GetOrAddCommitted(
		ctx,
		key,
		&ts,
	)
	assert.NoError(t, err)
	assert.Equal(t, ts, ret)

	var smallTs types.TS
	ret, err = u.GetOrAddCommitted(
		ctx,
		key,
		&smallTs,
	)
	assert.NoError(t, err)
	assert.Equal(t, ts, ret)

	ret, err = u.GetFromCache(
		ctx,
		key,
	)
	assert.NoError(t, err)
	assert.Equal(t, ts, ret)

	assert.Equal(t, 1, ie.RowCount("mo_catalog", "mo_cdc_watermark"))

	for i := 0; i < 5; i++ {
		nts := types.BuildTS(int64(i+1), 1)
		err = u.UpdateWatermarkOnly(
			ctx,
			key,
			&nts,
		)
		assert.NoError(t, err)
		assert.NoError(t, err)
		ret, err = u.GetFromCache(
			ctx,
			key,
		)
		assert.NoError(t, err)
		assert.Equal(t, nts, ret)
		time.Sleep(time.Millisecond * 1)
	}
	testutils.WaitExpect(
		5000,
		func() bool {
			tuple, err := ie.GetTableDataByPK(
				"mo_catalog",
				"mo_cdc_watermark",
				[]string{"1", "task1", "db1", "t1"},
			)
			t.Logf("tuple: %v", tuple)
			return err == nil && tuple[4] == "5-1"
		},
	)
	assert.Equal(t, 1, ie.RowCount("mo_catalog", "mo_cdc_watermark"))

	var tasksWg sync.WaitGroup

	runTaskFunc := func(
		wg *sync.WaitGroup,
		key *WatermarkKey,
		physicalStart int64,
	) {
		defer wg.Done()
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(4)))

		logic := uint32(0)
		candidateTS := types.BuildTS(physicalStart, logic)
		logic++
		persistedTS, err := u.GetOrAddCommitted(
			ctx,
			key,
			&candidateTS,
		)
		assert.NoError(t, err)
		assert.True(t, candidateTS.LE(&persistedTS))

		for i := 0; i < 20; i++ {
			ts := types.BuildTS(physicalStart, logic)
			logic++
			err = u.UpdateWatermarkOnly(
				ctx,
				key,
				&ts,
			)
			assert.NoError(t, err)
			cacheTS, err := u.GetFromCache(
				ctx,
				key,
			)
			assert.NoError(t, err)
			assert.True(t, ts.EQ(&cacheTS))
			time.Sleep(time.Microsecond * time.Duration(rand.Intn(1000)))
		}
	}

	tasksWg.Add(5)
	keys := make([]*WatermarkKey, 0, 5)
	for i := 0; i < 5; i++ {
		key := &WatermarkKey{
			AccountId: 1,
			TaskId:    fmt.Sprintf("task%d", i+10),
			DBName:    "db1",
			TableName: "t1",
		}
		keys = append(keys, key)
		go runTaskFunc(&tasksWg, key, int64(i+100000))
	}

	tasksWg.Wait()
	assert.Equal(t, 6, ie.RowCount("mo_catalog", "mo_cdc_watermark"))
	for _, key := range keys {
		testutils.WaitExpect(
			5000,
			func() bool {
				tuple, err := ie.GetTableDataByPK(
					"mo_catalog",
					"mo_cdc_watermark",
					[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
				)
				t.Logf("tuple: %v", tuple)
				if err != nil {
					return false
				}
				ts := types.StringToTS(tuple[4])
				return ts.Logical() >= 20
			},
		)
	}
}

func TestCDCWatermarkUpdater_UpdateWatermarkErrMsg(t *testing.T) {
	u, ie := InitCDCWatermarkUpdaterForTest(t)
	u.Start()
	defer u.Stop()

	key := &WatermarkKey{
		AccountId: 1,
		TaskId:    "task1",
		DBName:    "db1",
		TableName: "t1",
	}

	err := u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"err1",
		nil, // Legacy format
	)
	assert.NoError(t, err)

	ts := types.BuildTS(1, 1)
	ret, err := u.GetOrAddCommitted(
		context.Background(),
		key,
		&ts,
	)
	assert.NoError(t, err)
	assert.Equal(t, ts, ret)

	err = u.UpdateWatermarkErrMsg(
		context.Background(),
		key,
		"err1",
		nil, // Legacy format
	)
	assert.NoError(t, err)

	tuple, err := ie.GetTableDataByPK(
		"mo_catalog",
		"mo_cdc_watermark",
		[]string{fmt.Sprintf("%d", key.AccountId), key.TaskId, key.DBName, key.TableName},
	)
	assert.NoError(t, err)
	// Table now has 6 columns: account_id, task_id, db_name, table_name, watermark, err_msg
	assert.Len(t, tuple, 6)
	assert.Equal(t, fmt.Sprintf("%d", key.AccountId), tuple[0])
	assert.Equal(t, key.TaskId, tuple[1])
	assert.Equal(t, key.DBName, tuple[2])
	assert.Equal(t, key.TableName, tuple[3])
	// tuple[4] is watermark
	// tuple[5] is err_msg

	// Error message is now formatted as "N:timestamp:message" (non-retryable)
	// Note: In "N:timestamp:message" format, retry count is always 0 (not tracked for non-retryable)
	formattedErrMsg := tuple[5]
	metadata := ParseErrorMetadata(formattedErrMsg)
	require.NotNil(t, metadata)
	assert.False(t, metadata.IsRetryable)
	assert.Equal(t, 0, metadata.RetryCount) // Non-retryable format doesn't track retry count
	assert.Contains(t, metadata.Message, "err1")
}

func TestCDCWatermarkUpdater_RemoveThenUpdateErrMsg(t *testing.T) {
	ctx := context.Background()
	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	key := &WatermarkKey{
		AccountId: 1,
		TaskId:    "task",
		DBName:    "db",
		TableName: "tbl",
	}
	wm := types.BuildTS(100, 0)

	require.NoError(t, updater.UpdateWatermarkOnly(ctx, key, &wm))
	require.NoError(t, updater.ForceFlush(ctx))
	require.NoError(t, updater.RemoveCachedWM(ctx, key, WatermarkCleanupAll))

	// UpdateWatermarkErrMsg is expected to succeed even after RemoveCachedWM; current implementation returns ErrNoWatermarkFound.
	err := updater.UpdateWatermarkErrMsg(ctx, key, "boom", nil)
	require.NoError(t, err)
}

// TestCDCWatermarkUpdater_MarkUnmarkTaskPaused tests pause state management
func TestCDCWatermarkUpdater_MarkUnmarkTaskPaused(t *testing.T) {
	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	taskId := "test-task-pause"

	_, paused := updater.pausedTasks.Load(taskId)
	require.False(t, paused)

	updater.MarkTaskPaused(taskId)
	pauseTime, paused := updater.pausedTasks.Load(taskId)
	require.True(t, paused)
	require.NotZero(t, pauseTime)

	updater.UnmarkTaskPaused(taskId)
	_, paused = updater.pausedTasks.Load(taskId)
	require.False(t, paused)
}

func TestCDCWatermarkUpdater_DeletedTaskDominatesConcurrentPause(t *testing.T) {
	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	taskID := "test-task-deleted-before-pause"

	updater.MarkTaskDeleted(taskID)
	updater.MarkTaskPaused(taskID)

	_, deleted := updater.deletedTasks.Load(taskID)
	require.True(t, deleted)
	_, paused := updater.pausedTasks.Load(taskID)
	require.False(t, paused)
}

// TestCDCWatermarkUpdater_PauseBlocksWatermarkUpdate tests that paused tasks block watermark updates
func TestCDCWatermarkUpdater_PauseBlocksWatermarkUpdate(t *testing.T) {
	ctx := context.Background()
	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	taskId := "test-task-block"
	key := &WatermarkKey{
		AccountId: 1,
		TaskId:    taskId,
		DBName:    "test_db",
		TableName: "test_table",
	}

	wm1 := types.BuildTS(1000, 1)
	err := updater.UpdateWatermarkOnly(ctx, key, &wm1)
	require.NoError(t, err)

	err = updater.ForceFlush(ctx)
	require.NoError(t, err)

	cachedWm, err := updater.GetFromCache(ctx, key)
	require.NoError(t, err)
	require.Equal(t, wm1, cachedWm)

	updater.MarkTaskPaused(taskId)

	wm2 := types.BuildTS(2000, 1)
	err = updater.UpdateWatermarkOnly(ctx, key, &wm2)
	require.NoError(t, err)

	err = updater.ForceFlush(ctx)
	require.NoError(t, err)

	cachedWm, err = updater.GetFromCache(ctx, key)
	require.NoError(t, err)
	require.Equal(t, wm1, cachedWm)

	updater.UnmarkTaskPaused(taskId)

	wm3 := types.BuildTS(3000, 1)
	err = updater.UpdateWatermarkOnly(ctx, key, &wm3)
	require.NoError(t, err)

	err = updater.ForceFlush(ctx)
	require.NoError(t, err)

	cachedWm, err = updater.GetFromCache(ctx, key)
	require.NoError(t, err)
	require.Equal(t, wm3, cachedWm)
}

// TestCDCWatermarkUpdater_MultipleTasksPauseIndependently tests that pausing one task doesn't affect others
func TestCDCWatermarkUpdater_MultipleTasksPauseIndependently(t *testing.T) {
	ctx := context.Background()
	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	task1 := "task-1"
	task2 := "task-2"

	key1 := &WatermarkKey{
		AccountId: 1,
		TaskId:    task1,
		DBName:    "db1",
		TableName: "table1",
	}

	key2 := &WatermarkKey{
		AccountId: 1,
		TaskId:    task2,
		DBName:    "db2",
		TableName: "table2",
	}

	wm1_v1 := types.BuildTS(1000, 1)
	wm2_v1 := types.BuildTS(2000, 1)

	require.NoError(t, updater.UpdateWatermarkOnly(ctx, key1, &wm1_v1))
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, key2, &wm2_v1))
	require.NoError(t, updater.ForceFlush(ctx))

	updater.MarkTaskPaused(task1)

	wm1_v2 := types.BuildTS(1100, 1)
	wm2_v2 := types.BuildTS(2100, 1)

	require.NoError(t, updater.UpdateWatermarkOnly(ctx, key1, &wm1_v2))
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, key2, &wm2_v2))
	require.NoError(t, updater.ForceFlush(ctx))

	cached1, err := updater.GetFromCache(ctx, key1)
	require.NoError(t, err)
	require.Equal(t, wm1_v1, cached1)

	cached2, err := updater.GetFromCache(ctx, key2)
	require.NoError(t, err)
	require.Equal(t, wm2_v2, cached2)
}

// TestCDCWatermarkUpdater_PauseRestartCycle tests multiple pause/restart cycles
func TestCDCWatermarkUpdater_PauseRestartCycle(t *testing.T) {
	ctx := context.Background()
	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	taskId := "lifecycle-task"
	key := &WatermarkKey{
		AccountId: 1,
		TaskId:    taskId,
		DBName:    "db",
		TableName: "tbl",
	}

	for cycle := 0; cycle < 3; cycle++ {
		wmRunning := types.BuildTS(int64(1000*(cycle+1)), 1)
		require.NoError(t, updater.UpdateWatermarkOnly(ctx, key, &wmRunning))
		require.NoError(t, updater.ForceFlush(ctx))

		cached, err := updater.GetFromCache(ctx, key)
		require.NoError(t, err)
		require.Equal(t, wmRunning, cached)

		updater.MarkTaskPaused(taskId)

		wmPaused := types.BuildTS(int64(1000*(cycle+1)+500), 1)
		require.NoError(t, updater.UpdateWatermarkOnly(ctx, key, &wmPaused))
		require.NoError(t, updater.ForceFlush(ctx))

		cached, err = updater.GetFromCache(ctx, key)
		require.NoError(t, err)
		require.Equal(t, wmRunning, cached)

		updater.UnmarkTaskPaused(taskId)

		wmRestart := types.BuildTS(int64(1000*(cycle+2)), 1)
		require.NoError(t, updater.UpdateWatermarkOnly(ctx, key, &wmRestart))
		require.NoError(t, updater.ForceFlush(ctx))

		cached, err = updater.GetFromCache(ctx, key)
		require.NoError(t, err)
		require.Equal(t, wmRestart, cached)
	}
}

// mockExecutorForScanErrors is a mock executor specifically for testing scanAndUpdateNonRetryableErrorMetrics
type mockExecutorForScanErrors struct {
	queryFunc func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult
}

func (m *mockExecutorForScanErrors) Exec(ctx context.Context, sql string, opts ie.SessionOverrideOptions) error {
	return nil
}

func (m *mockExecutorForScanErrors) Query(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, sql, opts)
	}
	return &InternalExecResultForTest{
		affectedRows: 0,
		resultSet: &MysqlResultSetForTest{
			Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
			Data:    [][]interface{}{},
		},
		err: nil,
	}
}

func (m *mockExecutorForScanErrors) ApplySessionOverride(opts ie.SessionOverrideOptions) {}

// TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_QueryFailed tests query failure scenario
func TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_QueryFailed(t *testing.T) {
	mockExec := &mockExecutorForScanErrors{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			return &InternalExecResultForTest{
				err: moerr.NewInternalErrorNoCtx("query failed"),
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-query-failed", mockExec)
	validWatermarks := make(map[string]bool)
	validWatermarks["1.task1.db1.t1"] = true

	// Should return early without error
	updater.scanAndUpdateNonRetryableErrorMetrics(context.Background(), validWatermarks, false)
}

// TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_EmptyResult tests empty result set
func TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_EmptyResult(t *testing.T) {
	mockExec := &mockExecutorForScanErrors{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			return &InternalExecResultForTest{
				affectedRows: 0,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data:    [][]interface{}{},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-empty-result", mockExec)
	validWatermarks := make(map[string]bool)
	updater.previousErrorLabels = map[string]bool{
		"1.task1.db1.t1": true,
	}

	// Should clean up previous labels
	updater.scanAndUpdateNonRetryableErrorMetrics(context.Background(), validWatermarks, false)
	require.Empty(t, updater.previousErrorLabels)
}

// TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_ParseAccountIdFailed tests account_id parsing failure
func TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_ParseAccountIdFailed(t *testing.T) {
	mockExec := &mockExecutorForScanErrors{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			return &InternalExecResultForTest{
				affectedRows: 1,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data: [][]interface{}{
						{"invalid", "task1", "db1", "t1", "N:1234567890:table not found"},
					},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-parse-failed", mockExec)
	validWatermarks := make(map[string]bool)
	validWatermarks["1.task1.db1.t1"] = true

	// Should skip the row and continue
	updater.scanAndUpdateNonRetryableErrorMetrics(context.Background(), validWatermarks, false)
}

// TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_EmptyErrMsg tests empty error message
func TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_EmptyErrMsg(t *testing.T) {
	mockExec := &mockExecutorForScanErrors{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			return &InternalExecResultForTest{
				affectedRows: 1,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data: [][]interface{}{
						{"1", "task1", "db1", "t1", ""},
					},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-empty-errmsg", mockExec)
	validWatermarks := make(map[string]bool)
	validWatermarks["1.task1.db1.t1"] = true

	// Should skip rows with empty error message
	updater.scanAndUpdateNonRetryableErrorMetrics(context.Background(), validWatermarks, false)
}

// TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_OrphanTable tests orphan table filtering
func TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_OrphanTable(t *testing.T) {
	mockExec := &mockExecutorForScanErrors{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			return &InternalExecResultForTest{
				affectedRows: 1,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data: [][]interface{}{
						{"1", "task1", "db1", "t1", "N:1234567890:table not found"},
					},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-orphan", mockExec)
	validWatermarks := make(map[string]bool)
	// Table not in validWatermarks, should be skipped

	// Should skip orphan tables
	updater.scanAndUpdateNonRetryableErrorMetrics(context.Background(), validWatermarks, false)
}

// TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_LegacyFormat tests legacy error format (treated as non-retryable)
func TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_LegacyFormat(t *testing.T) {
	tableLabel := "1.task1.db1.t1"
	mockExec := &mockExecutorForScanErrors{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			return &InternalExecResultForTest{
				affectedRows: 1,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data: [][]interface{}{
						{"1", "task1", "db1", "t1", "legacy error message"}, // Legacy format - treated as non-retryable
					},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-legacy-format", mockExec)
	validWatermarks := make(map[string]bool)
	validWatermarks[tableLabel] = true

	// Legacy format errors are treated as non-retryable and should be tracked
	updater.scanAndUpdateNonRetryableErrorMetrics(context.Background(), validWatermarks, false)
	require.Contains(t, updater.previousErrorLabels, tableLabel)
}

// TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_RetryableError tests retryable error (should not set metric)
func TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_RetryableError(t *testing.T) {
	now := time.Now()
	mockExec := &mockExecutorForScanErrors{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			return &InternalExecResultForTest{
				affectedRows: 1,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data: [][]interface{}{
						{"1", "task1", "db1", "t1", fmt.Sprintf("R:1:%d:%d:connection timeout", now.Unix(), now.Unix())},
					},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-retryable", mockExec)
	validWatermarks := make(map[string]bool)
	validWatermarks["1.task1.db1.t1"] = true

	// Retryable errors should not set metrics
	updater.scanAndUpdateNonRetryableErrorMetrics(context.Background(), validWatermarks, false)
	require.Empty(t, updater.previousErrorLabels)
}

// TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_NonRetryableError tests non-retryable error (should set metric)
func TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_NonRetryableError(t *testing.T) {
	now := time.Now()
	tableLabel := "1.task1.db1.t1"
	mockExec := &mockExecutorForScanErrors{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			return &InternalExecResultForTest{
				affectedRows: 1,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data: [][]interface{}{
						{"1", "task1", "db1", "t1", fmt.Sprintf("N:%d:table not found", now.Unix())},
					},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-non-retryable", mockExec)
	validWatermarks := make(map[string]bool)
	validWatermarks[tableLabel] = true

	// Non-retryable errors should set metrics
	updater.scanAndUpdateNonRetryableErrorMetrics(context.Background(), validWatermarks, false)
	require.Contains(t, updater.previousErrorLabels, tableLabel)
}

// TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_ErrorTypes tests different error types
func TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_ErrorTypes(t *testing.T) {
	now := time.Now()
	testCases := []struct {
		name      string
		errMsg    string
		errorType string
	}{
		{"network", fmt.Sprintf("N:%d:connection timeout", now.Unix()), "network"},
		{"commit", fmt.Sprintf("N:%d:commit failed", now.Unix()), "commit"},
		{"table_relation", fmt.Sprintf("N:%d:table not found", now.Unix()), "table_relation"},
		{"sinker", fmt.Sprintf("N:%d:sinker error", now.Unix()), "sinker"},
		{"max_retry_exceeded", fmt.Sprintf("N:%d:max retry exceeded (5): connection failed", now.Unix()), "max_retry_exceeded"},
		{"unknown", fmt.Sprintf("N:%d:unknown error", now.Unix()), "unknown"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tableLabel := fmt.Sprintf("1.task1.db1.t1-%s", tc.name)
			mockExec := &mockExecutorForScanErrors{
				queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
					return &InternalExecResultForTest{
						affectedRows: 1,
						resultSet: &MysqlResultSetForTest{
							Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
							Data: [][]interface{}{
								{"1", "task1", "db1", fmt.Sprintf("t1-%s", tc.name), tc.errMsg},
							},
						},
						err: nil,
					}
				},
			}
			updater := NewCDCWatermarkUpdater(fmt.Sprintf("test-error-type-%s", tc.name), mockExec)
			validWatermarks := make(map[string]bool)
			validWatermarks[tableLabel] = true

			updater.scanAndUpdateNonRetryableErrorMetrics(context.Background(), validWatermarks, false)
			require.Contains(t, updater.previousErrorLabels, tableLabel)
		})
	}
}

// TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_DiffCleanup tests diff-based cleanup
func TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_DiffCleanup(t *testing.T) {
	now := time.Now()
	tableLabel1 := "1.task1.db1.t1"
	tableLabel2 := "1.task2.db2.t2"
	tableLabel3 := "1.task3.db3.t3"

	mockExec := &mockExecutorForScanErrors{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			return &InternalExecResultForTest{
				affectedRows: 2,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data: [][]interface{}{
						{"1", "task1", "db1", "t1", fmt.Sprintf("N:%d:table not found", now.Unix())},
						{"1", "task2", "db2", "t2", fmt.Sprintf("N:%d:connection failed", now.Unix())},
					},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-diff-cleanup", mockExec)
	validWatermarks := make(map[string]bool)
	validWatermarks[tableLabel1] = true
	validWatermarks[tableLabel2] = true
	validWatermarks[tableLabel3] = true

	// Set previous labels (tableLabel3 will be cleaned up)
	updater.previousErrorLabels = map[string]bool{
		tableLabel1: true,
		tableLabel2: true,
		tableLabel3: true,
	}

	updater.scanAndUpdateNonRetryableErrorMetrics(context.Background(), validWatermarks, false)

	// tableLabel1 and tableLabel2 should remain, tableLabel3 should be removed
	require.Contains(t, updater.previousErrorLabels, tableLabel1)
	require.Contains(t, updater.previousErrorLabels, tableLabel2)
	require.NotContains(t, updater.previousErrorLabels, tableLabel3)
}

// TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_QueryFailedSkipsCleanup tests that queryFailed=true skips cleanup
func TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_QueryFailedSkipsCleanup(t *testing.T) {
	now := time.Now()
	tableLabel := "1.task1.db1.t1"

	mockExec := &mockExecutorForScanErrors{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			return &InternalExecResultForTest{
				affectedRows: 1,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data: [][]interface{}{
						{"1", "task1", "db1", "t1", fmt.Sprintf("N:%d:table not found", now.Unix())},
					},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-query-failed-skip", mockExec)
	validWatermarks := make(map[string]bool)
	validWatermarks[tableLabel] = true

	// Set previous labels
	updater.previousErrorLabels = map[string]bool{
		tableLabel: true,
	}

	// With queryFailed=true, cleanup should be skipped
	updater.scanAndUpdateNonRetryableErrorMetrics(context.Background(), validWatermarks, true)

	// previousErrorLabels should remain unchanged
	require.Contains(t, updater.previousErrorLabels, tableLabel)
}

// TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_MultipleTables tests multiple tables scenario
func TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_MultipleTables(t *testing.T) {
	now := time.Now()
	mockExec := &mockExecutorForScanErrors{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			return &InternalExecResultForTest{
				affectedRows: 3,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data: [][]interface{}{
						{"1", "task1", "db1", "t1", fmt.Sprintf("N:%d:table not found", now.Unix())},
						{"1", "task2", "db2", "t2", fmt.Sprintf("N:%d:connection timeout", now.Unix())},
						{"1", "task3", "db3", "t3", fmt.Sprintf("R:1:%d:%d:retryable error", now.Unix(), now.Unix())},
					},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-multiple-tables", mockExec)
	validWatermarks := make(map[string]bool)
	validWatermarks["1.task1.db1.t1"] = true
	validWatermarks["1.task2.db2.t2"] = true
	validWatermarks["1.task3.db3.t3"] = true

	updater.scanAndUpdateNonRetryableErrorMetrics(context.Background(), validWatermarks, false)

	// Only non-retryable errors should be in previousErrorLabels
	require.Contains(t, updater.previousErrorLabels, "1.task1.db1.t1")
	require.Contains(t, updater.previousErrorLabels, "1.task2.db2.t2")
	require.NotContains(t, updater.previousErrorLabels, "1.task3.db3.t3")
}

// TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_MixedScenarios tests mixed scenarios
func TestCDCWatermarkUpdater_scanAndUpdateNonRetryableErrorMetrics_MixedScenarios(t *testing.T) {
	now := time.Now()
	mockExec := &mockExecutorForScanErrors{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			return &InternalExecResultForTest{
				affectedRows: 4,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data: [][]interface{}{
						{"1", "task1", "db1", "t1", ""},                                                         // Empty error - should skip
						{"1", "task2", "db2", "t2", fmt.Sprintf("N:%d:table not found", now.Unix())},            // Non-retryable
						{"1", "task3", "db3", "t3", fmt.Sprintf("R:1:%d:%d:retryable", now.Unix(), now.Unix())}, // Retryable
						{"1", "task5", "db5", "t5", fmt.Sprintf("N:%d:commit failed", now.Unix())},              // Non-retryable
					},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-mixed", mockExec)
	validWatermarks := make(map[string]bool)
	validWatermarks["1.task1.db1.t1"] = true
	validWatermarks["1.task2.db2.t2"] = true
	validWatermarks["1.task3.db3.t3"] = true
	validWatermarks["1.task5.db5.t5"] = true

	updater.scanAndUpdateNonRetryableErrorMetrics(context.Background(), validWatermarks, false)

	// Only valid non-retryable errors should be tracked
	require.Contains(t, updater.previousErrorLabels, "1.task2.db2.t2")
	require.Contains(t, updater.previousErrorLabels, "1.task5.db5.t5")
	require.NotContains(t, updater.previousErrorLabels, "1.task1.db1.t1")
	require.NotContains(t, updater.previousErrorLabels, "1.task3.db3.t3")
}

// mockExecutorForWrapCronJob is a mock executor specifically for testing wrapCronJob
type mockExecutorForWrapCronJob struct {
	queryFunc func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult
}

func (m *mockExecutorForWrapCronJob) Exec(ctx context.Context, sql string, opts ie.SessionOverrideOptions) error {
	return nil
}

func (m *mockExecutorForWrapCronJob) Query(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, sql, opts)
	}
	return &InternalExecResultForTest{
		affectedRows: 0,
		resultSet: &MysqlResultSetForTest{
			Columns: []string{"account_id", "task_id", "db_name", "table_name"},
			Data:    [][]interface{}{},
		},
		err: nil,
	}
}

func (m *mockExecutorForWrapCronJob) ApplySessionOverride(opts ie.SessionOverrideOptions) {}

// TestCDCWatermarkUpdater_wrapCronJob_NotExportTime tests that stats are not exported when interval not reached
func TestCDCWatermarkUpdater_wrapCronJob_NotExportTime(t *testing.T) {
	mockExec := &mockExecutorForWrapCronJob{}
	updater := NewCDCWatermarkUpdater("test-not-export-time", mockExec, WithExportStatsInterval(time.Hour))
	updater.stats.lastExportTime = time.Now() // Just set, so interval not reached

	jobExecuted := false
	job := func(ctx context.Context) {
		jobExecuted = true
	}

	wrappedJob := updater.wrapCronJob(job)
	wrappedJob(context.Background())

	require.True(t, jobExecuted)
	require.Equal(t, uint64(1), updater.stats.runTimes.Load())
	// Stats should not be exported (lastExportTime should not be updated)
	require.True(t, time.Since(updater.stats.lastExportTime) < time.Hour)
}

// TestCDCWatermarkUpdater_wrapCronJob_ExportTimeReached tests that stats are exported when interval reached
func TestCDCWatermarkUpdater_wrapCronJob_ExportTimeReached(t *testing.T) {
	mockExec := &mockExecutorForWrapCronJob{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			// Mock the JOIN query for valid watermarks
			if strings.Contains(sql, "INNER JOIN") {
				return &InternalExecResultForTest{
					affectedRows: 1,
					resultSet: &MysqlResultSetForTest{
						Columns: []string{"account_id", "task_id", "db_name", "table_name"},
						Data: [][]interface{}{
							{"1", "task1", "db1", "t1"},
						},
					},
					err: nil,
				}
			}
			// Mock the error scan query
			return &InternalExecResultForTest{
				affectedRows: 0,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data:    [][]interface{}{},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-export-time", mockExec, WithExportStatsInterval(time.Millisecond))
	updater.stats.lastExportTime = time.Now().Add(-time.Hour) // Set to past, so interval reached

	key := WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "t1"}
	watermark := types.BuildTS(1000, 1)
	updater.Lock()
	updater.cacheCommitted[key] = watermark
	updater.Unlock()

	jobExecuted := false
	job := func(ctx context.Context) {
		jobExecuted = true
	}

	wrappedJob := updater.wrapCronJob(job)
	time.Sleep(time.Millisecond * 10) // Ensure interval passed
	wrappedJob(context.Background())

	require.True(t, jobExecuted)
	require.Equal(t, uint64(1), updater.stats.runTimes.Load())
	// Stats should be exported (lastExportTime should be updated)
	require.True(t, time.Since(updater.stats.lastExportTime) < time.Second)
}

// TestCDCWatermarkUpdater_wrapCronJob_QueryValidWatermarksFailed tests query failure scenario
func TestCDCWatermarkUpdater_wrapCronJob_QueryValidWatermarksFailed(t *testing.T) {
	mockExec := &mockExecutorForWrapCronJob{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			if strings.Contains(sql, "INNER JOIN") {
				return &InternalExecResultForTest{
					err: moerr.NewInternalErrorNoCtx("query failed"),
				}
			}
			// Error scan query also fails
			return &InternalExecResultForTest{
				err: moerr.NewInternalErrorNoCtx("query failed"),
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-query-failed", mockExec, WithExportStatsInterval(time.Millisecond))
	updater.stats.lastExportTime = time.Now().Add(-time.Hour)

	key := WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "t1"}
	watermark := types.BuildTS(1000, 1)
	updater.Lock()
	updater.cacheCommitted[key] = watermark
	updater.Unlock()

	jobExecuted := false
	job := func(ctx context.Context) {
		jobExecuted = true
	}

	wrappedJob := updater.wrapCronJob(job)
	time.Sleep(time.Millisecond * 10)
	wrappedJob(context.Background())

	require.True(t, jobExecuted)
	// Key should not be removed because queryFailed = true
	updater.RLock()
	_, exists := updater.cacheCommitted[key]
	updater.RUnlock()
	require.True(t, exists)
}

// TestCDCWatermarkUpdater_wrapCronJob_OrphanKeysCleanup tests orphan key cleanup
func TestCDCWatermarkUpdater_wrapCronJob_OrphanKeysCleanup(t *testing.T) {
	mockExec := &mockExecutorForWrapCronJob{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			if strings.Contains(sql, "INNER JOIN") {
				// Only task1 is valid, task2 is orphan
				return &InternalExecResultForTest{
					affectedRows: 1,
					resultSet: &MysqlResultSetForTest{
						Columns: []string{"account_id", "task_id", "db_name", "table_name"},
						Data: [][]interface{}{
							{"1", "task1", "db1", "t1"},
						},
					},
					err: nil,
				}
			}
			// Error scan query
			return &InternalExecResultForTest{
				affectedRows: 0,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data:    [][]interface{}{},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-orphan-cleanup", mockExec, WithExportStatsInterval(time.Millisecond))
	updater.stats.lastExportTime = time.Now().Add(-time.Hour)

	key1 := WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "t1"}
	key2 := WatermarkKey{AccountId: 1, TaskId: "task2", DBName: "db2", TableName: "t2"}
	key3 := WatermarkKey{AccountId: 1, TaskId: "task3", DBName: "db3", TableName: "t3"}
	watermark1 := types.BuildTS(1000, 1)
	watermark2 := types.BuildTS(2000, 1)
	activeFence := NewOwnerFenceForGeneration(time.UnixMicro(2), func(context.Context) error { return nil })
	bufferedFence := NewOwnerFenceForGeneration(time.UnixMicro(1), func(context.Context) error { return nil })
	circuitGaugeOwned := false
	t.Cleanup(func() {
		if circuitGaugeOwned {
			v2.CdcWatermarkCircuitOpenGauge.Dec()
		}
	})

	updater.Lock()
	updater.cacheCommitted[key1] = watermark1
	updater.cacheCommitted[key2] = watermark2
	updater.cacheCommitted[key3] = watermark2
	updater.cacheUncommitted[key2] = watermark2
	updater.cacheCommitting[key2] = watermark2
	updater.cacheCommittedGeneration[key2] = 2
	updater.cacheCommittedGeneration[key3] = 2
	updater.cacheUncommittedGeneration[key2] = 1
	updater.cacheCommittingGeneration[key2] = 1
	updater.cacheUncommittedFence[key2] = bufferedFence
	updater.cacheCommittingFence[key2] = bufferedFence
	updater.activeWatermarkFence[key3] = activeFence
	updater.errorMetadataCache[key2] = &ErrorMetadata{Message: "test"}
	updater.commitCircuitOpen[key2] = time.Now()
	updater.commitFailureCount[key2] = 5
	v2.CdcWatermarkCircuitOpenGauge.Inc()
	circuitGaugeOwned = true
	updater.Unlock()

	job := func(ctx context.Context) {}
	wrappedJob := updater.wrapCronJob(job)
	wrappedJob(context.Background())

	// key1 should remain (valid)
	updater.RLock()
	_, exists1 := updater.cacheCommitted[key1]
	_, exists2 := updater.cacheCommitted[key2]
	retainedActiveWatermark, exists3 := updater.cacheCommitted[key3]
	_, existsUncommitted := updater.cacheUncommitted[key2]
	_, existsCommitting := updater.cacheCommitting[key2]
	_, existsCommittedGeneration := updater.cacheCommittedGeneration[key2]
	_, existsUncommittedGeneration := updater.cacheUncommittedGeneration[key2]
	_, existsCommittingGeneration := updater.cacheCommittingGeneration[key2]
	_, existsUncommittedFence := updater.cacheUncommittedFence[key2]
	_, existsCommittingFence := updater.cacheCommittingFence[key2]
	retainedActiveGeneration := updater.cacheCommittedGeneration[key3]
	retainedActiveFence := updater.activeWatermarkFence[key3]
	_, existsErrMeta := updater.errorMetadataCache[key2]
	_, existsCircuit := updater.commitCircuitOpen[key2]
	_, existsFailureCount := updater.commitFailureCount[key2]
	updater.RUnlock()
	if !existsCircuit {
		circuitGaugeOwned = false
	}

	require.True(t, exists1)
	require.False(t, exists2)
	require.True(t, exists3)
	require.Equal(t, watermark2, retainedActiveWatermark)
	require.False(t, existsUncommitted)
	require.False(t, existsCommitting)
	require.False(t, existsCommittedGeneration)
	require.False(t, existsUncommittedGeneration)
	require.False(t, existsCommittingGeneration)
	require.False(t, existsUncommittedFence)
	require.False(t, existsCommittingFence)
	require.Equal(t, uint64(2), retainedActiveGeneration)
	require.Same(t, activeFence, retainedActiveFence)
	require.False(t, existsErrMeta)
	require.False(t, existsCircuit)
	require.False(t, existsFailureCount)
}

// TestCDCWatermarkUpdater_wrapCronJob_EmptyWatermark tests empty watermark is skipped
func TestCDCWatermarkUpdater_wrapCronJob_EmptyWatermark(t *testing.T) {
	mockExec := &mockExecutorForWrapCronJob{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			if strings.Contains(sql, "INNER JOIN") {
				return &InternalExecResultForTest{
					affectedRows: 0,
					resultSet: &MysqlResultSetForTest{
						Columns: []string{"account_id", "task_id", "db_name", "table_name"},
						Data:    [][]interface{}{},
					},
					err: nil,
				}
			}
			return &InternalExecResultForTest{
				affectedRows: 0,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data:    [][]interface{}{},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-empty-watermark", mockExec, WithExportStatsInterval(time.Millisecond))
	updater.stats.lastExportTime = time.Now().Add(-time.Hour)

	key := WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "t1"}
	var emptyWatermark types.TS // Empty watermark
	updater.Lock()
	updater.cacheCommitted[key] = emptyWatermark
	updater.Unlock()

	job := func(ctx context.Context) {}
	wrappedJob := updater.wrapCronJob(job)
	time.Sleep(time.Millisecond * 10)
	wrappedJob(context.Background())

	// Empty watermark should be skipped (not processed for metrics)
	updater.RLock()
	_, exists := updater.cacheCommitted[key]
	updater.RUnlock()
	require.True(t, exists) // Still exists because it's empty and not processed
}

// TestCDCWatermarkUpdater_wrapCronJob_ValidWatermarkMetrics tests valid watermark metrics update
func TestCDCWatermarkUpdater_wrapCronJob_ValidWatermarkMetrics(t *testing.T) {
	mockExec := &mockExecutorForWrapCronJob{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			if strings.Contains(sql, "INNER JOIN") {
				return &InternalExecResultForTest{
					affectedRows: 1,
					resultSet: &MysqlResultSetForTest{
						Columns: []string{"account_id", "task_id", "db_name", "table_name"},
						Data: [][]interface{}{
							{"1", "task1", "db1", "t1"},
						},
					},
					err: nil,
				}
			}
			return &InternalExecResultForTest{
				affectedRows: 0,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data:    [][]interface{}{},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-valid-metrics", mockExec, WithExportStatsInterval(time.Millisecond))
	updater.stats.lastExportTime = time.Now().Add(-time.Hour)

	key := WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "t1"}
	// Use a watermark from the past to test lag calculation
	pastTime := time.Now().Add(-10 * time.Second)
	watermark := types.BuildTS(pastTime.Unix(), 0)
	updater.Lock()
	updater.cacheCommitted[key] = watermark
	updater.Unlock()

	job := func(ctx context.Context) {}
	wrappedJob := updater.wrapCronJob(job)
	time.Sleep(time.Millisecond * 10)
	wrappedJob(context.Background())

	// Valid watermark should remain
	updater.RLock()
	_, exists := updater.cacheCommitted[key]
	updater.RUnlock()
	require.True(t, exists)
}

// TestCDCWatermarkUpdater_wrapCronJob_OrphanDoubleCheck tests TOCTOU race condition handling
func TestCDCWatermarkUpdater_wrapCronJob_OrphanDoubleCheck(t *testing.T) {
	key := WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "t1"}
	orphanWatermark := types.BuildTS(1000, 1)
	replacementWatermark := types.BuildTS(2000, 1)
	replacementFence := NewOwnerFenceForGeneration(time.UnixMicro(2), func(context.Context) error { return nil })
	var updater *CDCWatermarkUpdater
	mockExec := &mockExecutorForWrapCronJob{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			if strings.Contains(sql, "INNER JOIN") {
				// Replace the local generation after the cron job snapshots its
				// candidates but before it removes the catalog-orphaned entry.
				updater.Lock()
				updater.cacheCommitted[key] = replacementWatermark
				updater.cacheCommittedGeneration[key] = 2
				updater.activeWatermarkFence[key] = replacementFence
				updater.Unlock()
				return &InternalExecResultForTest{
					affectedRows: 0,
					resultSet: &MysqlResultSetForTest{
						Columns: []string{"account_id", "task_id", "db_name", "table_name"},
						Data:    [][]interface{}{},
					},
					err: nil,
				}
			}
			return &InternalExecResultForTest{
				affectedRows: 0,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data:    [][]interface{}{},
				},
				err: nil,
			}
		},
	}
	updater = NewCDCWatermarkUpdater("test-double-check", mockExec, WithExportStatsInterval(time.Millisecond))
	updater.stats.lastExportTime = time.Now().Add(-time.Hour)

	updater.Lock()
	updater.cacheCommitted[key] = orphanWatermark
	updater.cacheCommittedGeneration[key] = 1
	updater.Unlock()

	wrappedJob := updater.wrapCronJob(func(context.Context) {})
	wrappedJob(context.Background())

	// The catalog result described the old local generation. It must not remove
	// the replacement that was published while the query was in flight.
	updater.RLock()
	retainedWatermark, exists := updater.cacheCommitted[key]
	retainedGeneration := updater.cacheCommittedGeneration[key]
	retainedFence := updater.activeWatermarkFence[key]
	updater.RUnlock()
	require.True(t, exists)
	require.Equal(t, replacementWatermark, retainedWatermark)
	require.Equal(t, uint64(2), retainedGeneration)
	require.Same(t, replacementFence, retainedFence)
}

// TestCDCWatermarkUpdater_wrapCronJob_MultipleKeysMixed tests multiple keys with mixed valid/orphan
func TestCDCWatermarkUpdater_wrapCronJob_MultipleKeysMixed(t *testing.T) {
	mockExec := &mockExecutorForWrapCronJob{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			if strings.Contains(sql, "INNER JOIN") {
				// task1 and task3 are valid, task2 is orphan
				return &InternalExecResultForTest{
					affectedRows: 2,
					resultSet: &MysqlResultSetForTest{
						Columns: []string{"account_id", "task_id", "db_name", "table_name"},
						Data: [][]interface{}{
							{"1", "task1", "db1", "t1"},
							{"1", "task3", "db3", "t3"},
						},
					},
					err: nil,
				}
			}
			return &InternalExecResultForTest{
				affectedRows: 0,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data:    [][]interface{}{},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-multiple-mixed", mockExec, WithExportStatsInterval(time.Millisecond))
	updater.stats.lastExportTime = time.Now().Add(-time.Hour)

	key1 := WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "t1"}
	key2 := WatermarkKey{AccountId: 1, TaskId: "task2", DBName: "db2", TableName: "t2"}
	key3 := WatermarkKey{AccountId: 1, TaskId: "task3", DBName: "db3", TableName: "t3"}
	watermark := types.BuildTS(1000, 1)

	updater.Lock()
	updater.cacheCommitted[key1] = watermark
	updater.cacheCommitted[key2] = watermark
	updater.cacheCommitted[key3] = watermark
	updater.Unlock()

	job := func(ctx context.Context) {}
	wrappedJob := updater.wrapCronJob(job)
	time.Sleep(time.Millisecond * 10)
	wrappedJob(context.Background())

	// key1 and key3 should remain, key2 should be removed
	updater.RLock()
	_, exists1 := updater.cacheCommitted[key1]
	_, exists2 := updater.cacheCommitted[key2]
	_, exists3 := updater.cacheCommitted[key3]
	updater.RUnlock()

	require.True(t, exists1)
	require.False(t, exists2)
	require.True(t, exists3)
}

// TestCDCWatermarkUpdater_wrapCronJob_StatsExport tests statistics export
func TestCDCWatermarkUpdater_wrapCronJob_StatsExport(t *testing.T) {
	mockExec := &mockExecutorForWrapCronJob{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			if strings.Contains(sql, "INNER JOIN") {
				return &InternalExecResultForTest{
					affectedRows: 0,
					resultSet: &MysqlResultSetForTest{
						Columns: []string{"account_id", "task_id", "db_name", "table_name"},
						Data:    [][]interface{}{},
					},
					err: nil,
				}
			}
			return &InternalExecResultForTest{
				affectedRows: 0,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data:    [][]interface{}{},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-stats-export", mockExec, WithExportStatsInterval(time.Millisecond))
	updater.stats.lastExportTime = time.Now().Add(-time.Hour)

	key1 := WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "t1"}
	key2 := WatermarkKey{AccountId: 1, TaskId: "task2", DBName: "db2", TableName: "t2"}
	watermark := types.BuildTS(1000, 1)

	updater.Lock()
	updater.cacheUncommitted[key1] = watermark
	updater.cacheCommitting[key2] = watermark
	updater.cacheCommitted[key1] = watermark
	updater.stats.skipTimes.Store(5)
	updater.stats.errorTimes.Store(2)
	updater.Unlock()

	job := func(ctx context.Context) {}
	wrappedJob := updater.wrapCronJob(job)
	time.Sleep(time.Millisecond * 10)
	wrappedJob(context.Background())

	// Verify stats are exported
	require.Equal(t, uint64(1), updater.stats.runTimes.Load())
	require.True(t, time.Since(updater.stats.lastExportTime) < time.Second)
}

// TestCDCWatermarkUpdater_wrapCronJob_NoKeysToRemove tests scenario with no orphan keys
func TestCDCWatermarkUpdater_wrapCronJob_NoKeysToRemove(t *testing.T) {
	mockExec := &mockExecutorForWrapCronJob{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			if strings.Contains(sql, "INNER JOIN") {
				return &InternalExecResultForTest{
					affectedRows: 1,
					resultSet: &MysqlResultSetForTest{
						Columns: []string{"account_id", "task_id", "db_name", "table_name"},
						Data: [][]interface{}{
							{"1", "task1", "db1", "t1"},
						},
					},
					err: nil,
				}
			}
			return &InternalExecResultForTest{
				affectedRows: 0,
				resultSet: &MysqlResultSetForTest{
					Columns: []string{"account_id", "task_id", "db_name", "table_name", "err_msg"},
					Data:    [][]interface{}{},
				},
				err: nil,
			}
		},
	}
	updater := NewCDCWatermarkUpdater("test-no-orphans", mockExec, WithExportStatsInterval(time.Millisecond))
	updater.stats.lastExportTime = time.Now().Add(-time.Hour)

	key := WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "t1"}
	watermark := types.BuildTS(1000, 1)

	updater.Lock()
	updater.cacheCommitted[key] = watermark
	updater.Unlock()

	job := func(ctx context.Context) {}
	wrappedJob := updater.wrapCronJob(job)
	time.Sleep(time.Millisecond * 10)
	wrappedJob(context.Background())

	// Key should remain (no orphans to remove)
	updater.RLock()
	_, exists := updater.cacheCommitted[key]
	updater.RUnlock()
	require.True(t, exists)
}
