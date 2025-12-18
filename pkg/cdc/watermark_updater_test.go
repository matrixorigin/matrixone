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
	"fmt"
	"math/rand"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	ie "github.com/matrixorigin/matrixone/pkg/util/internalExecutor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
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
	execCalls     int
	lastSQL       string
}

func (m *retryableMockExecutor) Exec(_ context.Context, sql string, _ ie.SessionOverrideOptions) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.execCalls++
	m.lastSQL = sql
	if m.failRemaining > 0 {
		m.failRemaining--
		return moerr.NewInternalErrorNoCtx("mock exec failure")
	}
	return nil
}

func (m *retryableMockExecutor) Query(_ context.Context, _ string, _ ie.SessionOverrideOptions) ie.InternalExecResult {
	return &InternalExecResultForTest{}
}

func (m *retryableMockExecutor) ApplySessionOverride(_ ie.SessionOverrideOptions) {}

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
	require.Contains(t, errMsg, "commit sql")
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
	require.NoError(t, updater.RemoveCachedWM(ctx, &key))

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

	require.NoError(t, updater.RemoveCachedWM(ctx, &key))
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

	require.NoError(t, updater.RemoveCachedWM(ctx, &key))
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

	require.NoError(t, updater.RemoveCachedWM(ctx, &key))

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

	ret, err := updater.GetOrAddCommitted(ctx, &key, &ts)
	require.NoError(t, err)
	require.True(t, ret.Equal(&ts))

	updater.RLock()
	committed, ok := updater.cacheCommitted[key]
	updater.RUnlock()
	require.True(t, ok)
	require.True(t, committed.Equal(&ts))
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
	require.NoError(t, updater.RemoveCachedWM(ctx, &key))
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
	insertSql := u.constructAddWMSQL(jobs)
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

	insertUpdateSql := u.constructBatchUpdateWMSQL(keys2)
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
	realSql := u.constructAddWMSQL(keys)
	expectedSql := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"VALUES " +
		"(1, 'test', 'db1', 't1', '1-1', '')," +
		"(2, 'test', 'db2', 't2', '2-1', '')," +
		"(3, 'test', 'db3', 't3', '3-1', '')"

	assert.Equal(t, expectedSql, realSql)
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
	expectedSql1 := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(1, 'test', 'db1', 't1', '1-1')," +
		"(2, 'test', 'db2', 't2', '2-1')," +
		"(3, 'test', 'db3', 't3', '3-1') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"
	expectedSql2 := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(3, 'test', 'db3', 't3', '3-1')," +
		"(2, 'test', 'db2', 't2', '2-1')," +
		"(1, 'test', 'db1', 't1', '1-1') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"
	expectedSql3 := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(3, 'test', 'db3', 't3', '3-1')," +
		"(1, 'test', 'db1', 't1', '1-1')," +
		"(2, 'test', 'db2', 't2', '2-1') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"
	expectedSql4 := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(2, 'test', 'db2', 't2', '2-1')," +
		"(3, 'test', 'db3', 't3', '3-1')," +
		"(1, 'test', 'db1', 't1', '1-1') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"
	expectedSql5 := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(2, 'test', 'db2', 't2', '2-1')," +
		"(1, 'test', 'db1', 't1', '1-1')," +
		"(3, 'test', 'db3', 't3', '3-1') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"
	expectedSql6 := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, watermark) VALUES " +
		"(1, 'test', 'db1', 't1', '1-1')," +
		"(2, 'test', 'db2', 't2', '2-1')," +
		"(3, 'test', 'db3', 't3', '3-1') " +
		"ON DUPLICATE KEY UPDATE watermark = VALUES(watermark)"
	realSql := u.constructBatchUpdateWMSQL(keys)
	assert.True(
		t,
		expectedSql1 == realSql ||
			expectedSql2 == realSql ||
			expectedSql3 == realSql ||
			expectedSql4 == realSql ||
			expectedSql5 == realSql ||
			expectedSql6 == realSql,
	)
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
	realSql := u.constructBatchUpdateWMErrMsgSQL(jobs)
	expectedSql := "INSERT INTO `mo_catalog`.`mo_cdc_watermark` " +
		"(account_id, task_id, db_name, table_name, err_msg) VALUES " +
		"(1, 'test', 'db1', 't1', 'err1')," +
		"(2, 'test', 'db2', 't2', '') " +
		"ON DUPLICATE KEY UPDATE err_msg = VALUES(err_msg)"
	assert.Equal(t, expectedSql, realSql)
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
	require.NoError(t, updater.RemoveCachedWM(ctx, key))

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
	watermark1 := types.BuildTS(1000, 1)
	watermark2 := types.BuildTS(2000, 1)

	updater.Lock()
	updater.cacheCommitted[key1] = watermark1
	updater.cacheCommitted[key2] = watermark2
	updater.cacheUncommitted[key2] = watermark2
	updater.cacheCommitting[key2] = watermark2
	updater.errorMetadataCache[key2] = &ErrorMetadata{Message: "test"}
	updater.commitCircuitOpen[key2] = time.Now()
	updater.commitFailureCount[key2] = 5
	updater.Unlock()

	job := func(ctx context.Context) {}
	wrappedJob := updater.wrapCronJob(job)
	time.Sleep(time.Millisecond * 10)
	wrappedJob(context.Background())

	// key1 should remain (valid)
	updater.RLock()
	_, exists1 := updater.cacheCommitted[key1]
	_, exists2 := updater.cacheCommitted[key2]
	_, existsUncommitted := updater.cacheUncommitted[key2]
	_, existsCommitting := updater.cacheCommitting[key2]
	_, existsErrMeta := updater.errorMetadataCache[key2]
	_, existsCircuit := updater.commitCircuitOpen[key2]
	_, existsFailureCount := updater.commitFailureCount[key2]
	updater.RUnlock()

	require.True(t, exists1)
	require.False(t, exists2)
	require.False(t, existsUncommitted)
	require.False(t, existsCommitting)
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
	mockExec := &mockExecutorForWrapCronJob{
		queryFunc: func(ctx context.Context, sql string, opts ie.SessionOverrideOptions) ie.InternalExecResult {
			if strings.Contains(sql, "INNER JOIN") {
				// No valid watermarks
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
	updater := NewCDCWatermarkUpdater("test-double-check", mockExec, WithExportStatsInterval(time.Millisecond))
	updater.stats.lastExportTime = time.Now().Add(-time.Hour)

	key := WatermarkKey{AccountId: 1, TaskId: "task1", DBName: "db1", TableName: "t1"}
	watermark := types.BuildTS(1000, 1)

	updater.Lock()
	updater.cacheCommitted[key] = watermark
	updater.Unlock()

	// Simulate key being removed between collection and cleanup
	job := func(ctx context.Context) {
		// Remove key during job execution (simulating race condition)
		updater.Lock()
		delete(updater.cacheCommitted, key)
		updater.Unlock()
	}

	wrappedJob := updater.wrapCronJob(job)
	time.Sleep(time.Millisecond * 10)
	wrappedJob(context.Background())

	// Key should be removed (double-check should handle it)
	updater.RLock()
	_, exists := updater.cacheCommitted[key]
	updater.RUnlock()
	require.False(t, exists)
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
