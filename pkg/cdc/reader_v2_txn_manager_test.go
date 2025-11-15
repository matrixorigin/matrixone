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
	"sync"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockSinker for testing
type mockSinker struct {
	beginCalled    bool
	commitCalled   bool
	rollbackCalled bool
	dummyCalled    bool
	clearCalled    bool
	err            error
	rollbackErr    error // Error to return when SendRollback is called
}

func (m *mockSinker) Run(ctx context.Context, ar *ActiveRoutine)    {}
func (m *mockSinker) Sink(ctx context.Context, data *DecoderOutput) {}
func (m *mockSinker) Close()                                        {}

func (m *mockSinker) SendBegin() {
	m.beginCalled = true
}

func (m *mockSinker) SendCommit() {
	m.commitCalled = true
}

func (m *mockSinker) SendRollback() {
	m.rollbackCalled = true
	if m.rollbackErr != nil {
		m.err = m.rollbackErr
	}
}

func (m *mockSinker) SendDummy() {
	m.dummyCalled = true
}

func (m *mockSinker) Error() error {
	return m.err
}

func (m *mockSinker) ClearError() {
	m.clearCalled = true
	m.err = nil
}

func (m *mockSinker) Reset() {}

func (m *mockSinker) reset() {
	m.beginCalled = false
	m.commitCalled = false
	m.rollbackCalled = false
	m.dummyCalled = false
	m.clearCalled = false
	m.err = nil
	m.rollbackErr = nil
}

type recordingSinker struct {
	mu          sync.Mutex
	ops         []string
	err         error
	rollbackErr error
	commitErr   error
	beginErr    error
}

func newRecordingSinker() *recordingSinker {
	return &recordingSinker{}
}

func (s *recordingSinker) record(op string) {
	s.mu.Lock()
	s.ops = append(s.ops, op)
	s.mu.Unlock()
}

func (s *recordingSinker) opsSnapshot() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]string, len(s.ops))
	copy(cp, s.ops)
	return cp
}

func (s *recordingSinker) resetOps() {
	s.mu.Lock()
	s.ops = nil
	s.mu.Unlock()
}

func (s *recordingSinker) setError(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
}

func (s *recordingSinker) setCommitError(err error) {
	s.mu.Lock()
	s.commitErr = err
	s.mu.Unlock()
}

func (s *recordingSinker) setRollbackError(err error) {
	s.mu.Lock()
	s.rollbackErr = err
	s.mu.Unlock()
}

func (s *recordingSinker) setBeginError(err error) {
	s.mu.Lock()
	s.beginErr = err
	s.mu.Unlock()
}

func (s *recordingSinker) Run(context.Context, *ActiveRoutine)  {}
func (s *recordingSinker) Sink(context.Context, *DecoderOutput) {}
func (s *recordingSinker) Close()                               {}
func (s *recordingSinker) Reset()                               {}

func (s *recordingSinker) SendBegin() {
	var err error
	s.mu.Lock()
	s.ops = append(s.ops, "begin")
	err = s.beginErr
	s.mu.Unlock()
	if err != nil {
		s.setError(err)
	}
}

func (s *recordingSinker) SendCommit() {
	var err error
	s.mu.Lock()
	s.ops = append(s.ops, "commit")
	err = s.commitErr
	s.mu.Unlock()
	if err != nil {
		s.setError(err)
	}
}

func (s *recordingSinker) SendDummy() {
	s.record("dummy")
}

func (s *recordingSinker) SendRollback() {
	var rollbackErr error
	s.mu.Lock()
	rollbackErr = s.rollbackErr
	s.mu.Unlock()

	s.record("rollback")
	if rollbackErr != nil {
		s.setError(rollbackErr)
	}
}

func (s *recordingSinker) Error() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

func (s *recordingSinker) ClearError() {
	s.record("clear")
	s.setError(nil)
}

// mockWatermarkUpdater for testing
type mockWatermarkUpdater struct {
	watermarks      map[string]types.TS
	updateCalled    bool
	getFromCacheErr error
}

func newMockWatermarkUpdater() *mockWatermarkUpdater {
	return &mockWatermarkUpdater{
		watermarks: make(map[string]types.TS),
	}
}

func (m *mockWatermarkUpdater) RemoveCachedWM(ctx context.Context, key *WatermarkKey) error {
	delete(m.watermarks, m.keyString(key))
	return nil
}

func (m *mockWatermarkUpdater) UpdateWatermarkErrMsg(ctx context.Context, key *WatermarkKey, errMsg string, errorCtx *ErrorContext) error {
	return nil
}

func (m *mockWatermarkUpdater) GetFromCache(ctx context.Context, key *WatermarkKey) (types.TS, error) {
	if m.getFromCacheErr != nil {
		return types.TS{}, m.getFromCacheErr
	}
	ts, ok := m.watermarks[m.keyString(key)]
	if !ok {
		return types.TS{}, moerr.NewInternalError(ctx, "watermark not found")
	}
	return ts, nil
}

func (m *mockWatermarkUpdater) GetOrAddCommitted(ctx context.Context, key *WatermarkKey, watermark *types.TS) (types.TS, error) {
	keyStr := m.keyString(key)
	if ts, ok := m.watermarks[keyStr]; ok {
		return ts, nil
	}
	m.watermarks[keyStr] = *watermark
	return *watermark, nil
}

func (m *mockWatermarkUpdater) UpdateWatermarkOnly(ctx context.Context, key *WatermarkKey, watermark *types.TS) error {
	m.updateCalled = true
	m.watermarks[m.keyString(key)] = *watermark
	return nil
}

func (m *mockWatermarkUpdater) IsCircuitBreakerOpen(key *WatermarkKey) bool {
	return false
}

func (m *mockWatermarkUpdater) GetCommitFailureCount(key *WatermarkKey) uint32 {
	return 0
}

func (m *mockWatermarkUpdater) keyString(key *WatermarkKey) string {
	return key.TaskId + ":" + key.DBName + ":" + key.TableName
}

func TestNewTransactionManager(t *testing.T) {
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()

	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	assert.NotNil(t, tm)
	assert.Equal(t, sinker, tm.sinker)
	assert.Equal(t, updater, tm.watermarkUpdater)
	assert.Equal(t, uint64(1), tm.accountId)
	assert.Equal(t, "task1", tm.taskId)
	assert.Equal(t, "db1", tm.dbName)
	assert.Equal(t, "table1", tm.tableName)
	assert.Nil(t, tm.tracker)
}

func TestTransactionManager_BeginTransaction(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.TS{}
	toTs := (&fromTs).Next()

	err := tm.BeginTransaction(ctx, fromTs, toTs)

	assert.NoError(t, err)
	assert.True(t, sinker.beginCalled)
	assert.NotNil(t, tm.tracker)
	assert.True(t, tm.tracker.hasBegin)
	assert.False(t, tm.tracker.hasCommitted)
	assert.False(t, tm.tracker.hasRolledBack)
}

func TestTransactionManager_BeginTransaction_WithError(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	sinker.err = moerr.NewInternalError(ctx, "test error")
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.TS{}
	toTs := (&fromTs).Next()

	err := tm.BeginTransaction(ctx, fromTs, toTs)

	assert.Error(t, err)
	assert.True(t, sinker.beginCalled)
	// Tracker is created but not marked as begun due to error
	assert.NotNil(t, tm.tracker)
	assert.False(t, tm.tracker.hasBegin)
}

func TestTransactionManager_BeginTransaction_RollsBackUnfinished(t *testing.T) {
	ctx := context.Background()
	sinker := newRecordingSinker()
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	oldFrom := types.TS{}
	oldTo := (&oldFrom).Next()
	pending := NewTransactionTracker(oldFrom, oldTo)
	pending.MarkBegin()
	tm.tracker = pending

	newFrom := oldTo
	newTo := (&newFrom).Next()
	err := tm.BeginTransaction(ctx, newFrom, newTo)
	require.NoError(t, err)

	require.NotEqual(t, pending, tm.tracker)
	require.NotNil(t, tm.tracker)
	assert.Equal(t, newFrom, tm.tracker.GetFromTs())
	assert.Equal(t, newTo, tm.tracker.GetToTs())

	require.Equal(t, []string{"clear", "rollback", "dummy", "begin"}, sinker.opsSnapshot())
}

func TestTransactionManager_CommitTransaction(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.TS{}
	toTs := (&fromTs).Next()

	// Begin first
	err := tm.BeginTransaction(ctx, fromTs, toTs)
	assert.NoError(t, err)

	// Reset sinker state
	sinker.reset()

	// Commit
	err = tm.CommitTransaction(ctx)

	assert.NoError(t, err)
	assert.True(t, sinker.commitCalled)
	assert.True(t, sinker.dummyCalled)
	assert.True(t, updater.updateCalled)
	assert.True(t, tm.tracker.hasCommitted)
	assert.True(t, tm.tracker.IsWatermarkUpdated())
	assert.False(t, tm.tracker.NeedsRollback())

	// Verify watermark was updated
	wm, err := updater.GetFromCache(ctx, tm.watermarkKey)
	assert.NoError(t, err)
	assert.Equal(t, toTs, wm)
}

func TestTransactionManager_CommitTransaction_WithoutBegin(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	// Commit without begin should not error
	err := tm.CommitTransaction(ctx)

	assert.NoError(t, err)
	assert.False(t, sinker.commitCalled)
}

func TestTransactionManager_CommitTransaction_WithError(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.TS{}
	toTs := (&fromTs).Next()

	// Begin first
	err := tm.BeginTransaction(ctx, fromTs, toTs)
	assert.NoError(t, err)

	// Set error for commit
	sinker.err = moerr.NewInternalError(ctx, "commit error")

	// Commit should fail
	err = tm.CommitTransaction(ctx)

	assert.Error(t, err)
	assert.True(t, sinker.commitCalled)
	assert.False(t, tm.tracker.hasCommitted)
	assert.True(t, tm.tracker.NeedsRollback())
}

func TestTransactionManager_EnsureCleanup_AfterCommitFailure(t *testing.T) {
	ctx := context.Background()
	sinker := newRecordingSinker()
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	require.NoError(t, tm.BeginTransaction(ctx, fromTs, toTs))

	commitErr := moerr.NewInternalError(ctx, "commit error")
	sinker.setError(commitErr)

	err := tm.CommitTransaction(ctx)
	require.ErrorIs(t, err, commitErr)
	require.True(t, tm.tracker.NeedsRollback())

	err = tm.EnsureCleanup(ctx)
	require.NoError(t, err)
	require.False(t, tm.tracker.NeedsRollback())

	require.Equal(t, []string{
		"begin",
		"commit",
		"dummy",
		"clear",
		"rollback",
		"dummy",
	}, sinker.opsSnapshot())
	require.NoError(t, sinker.Error())
}

func TestTransactionManager_RollbackTransaction(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.TS{}
	toTs := (&fromTs).Next()

	// Begin first
	err := tm.BeginTransaction(ctx, fromTs, toTs)
	assert.NoError(t, err)

	// Reset sinker state
	sinker.reset()

	// Rollback
	err = tm.RollbackTransaction(ctx)

	assert.NoError(t, err)
	assert.True(t, sinker.clearCalled)
	assert.True(t, sinker.rollbackCalled)
	assert.True(t, sinker.dummyCalled)
	assert.True(t, tm.tracker.hasRolledBack)
	assert.False(t, tm.tracker.NeedsRollback())
}

func TestTransactionManager_RollbackTransaction_WithoutBegin(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	// Rollback without begin should not error
	err := tm.RollbackTransaction(ctx)

	assert.NoError(t, err)
	assert.False(t, sinker.rollbackCalled)
}

func TestTransactionManager_RollbackTransaction_WithError(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.TS{}
	toTs := (&fromTs).Next()

	// Begin first
	err := tm.BeginTransaction(ctx, fromTs, toTs)
	assert.NoError(t, err)

	// Set error to be returned when SendRollback is called
	sinker.rollbackErr = moerr.NewInternalError(ctx, "rollback error")

	// Rollback should return error but still mark as rolled back
	err = tm.RollbackTransaction(ctx)

	assert.Error(t, err)
	assert.True(t, sinker.rollbackCalled)
	assert.True(t, tm.tracker.hasRolledBack)
}

func TestTransactionManager_EnsureCleanup_NoRollbackNeeded(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.TS{}
	toTs := (&fromTs).Next()

	// Begin and commit
	err := tm.BeginTransaction(ctx, fromTs, toTs)
	assert.NoError(t, err)

	err = tm.CommitTransaction(ctx)
	assert.NoError(t, err)

	// Reset sinker state
	sinker.reset()

	// Ensure cleanup should not rollback
	err = tm.EnsureCleanup(ctx)

	assert.NoError(t, err)
	assert.False(t, sinker.rollbackCalled)
}

func TestTransactionManager_EnsureCleanup_RollbackNeeded(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.TS{}
	toTs := (&fromTs).Next()

	// Begin but don't commit
	err := tm.BeginTransaction(ctx, fromTs, toTs)
	assert.NoError(t, err)

	// Reset sinker state
	sinker.reset()

	// Ensure cleanup should rollback
	err = tm.EnsureCleanup(ctx)

	assert.NoError(t, err)
	assert.True(t, sinker.rollbackCalled)
}

func TestTransactionManager_EnsureCleanup_GetFromCacheFails(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()
	updater.getFromCacheErr = moerr.NewInternalError(ctx, "cache error")
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.TS{}
	toTs := (&fromTs).Next()

	// Begin but don't commit
	err := tm.BeginTransaction(ctx, fromTs, toTs)
	assert.NoError(t, err)

	// Reset sinker state
	sinker.reset()

	// Ensure cleanup should still rollback based on tracker state
	err = tm.EnsureCleanup(ctx)

	assert.NoError(t, err)
	assert.True(t, sinker.rollbackCalled)
}

func TestTransactionManager_EnsureCleanup_WatermarkMismatch(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.TS{}
	toTs := (&fromTs).Next()

	// Initialize watermark with fromTs
	updater.UpdateWatermarkOnly(ctx, tm.watermarkKey, &fromTs)

	// Begin transaction
	err := tm.BeginTransaction(ctx, fromTs, toTs)
	assert.NoError(t, err)

	// Manually mark as committed (simulate crash after commit but before watermark update)
	tm.tracker.MarkCommit()

	// But don't update watermark to toTs
	// Reset sinker state
	sinker.reset()

	// Ensure cleanup should detect watermark mismatch and rollback
	err = tm.EnsureCleanup(ctx)

	assert.NoError(t, err)
	assert.True(t, sinker.rollbackCalled)
}

func TestTransactionManager_EnsureCleanup_WatermarkMismatch_Idempotent(t *testing.T) {
	ctx := context.Background()
	sinker := newRecordingSinker()
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.TS{}
	toTs := (&fromTs).Next()
	require.NoError(t, tm.BeginTransaction(ctx, fromTs, toTs))
	require.NoError(t, updater.UpdateWatermarkOnly(ctx, tm.watermarkKey, &fromTs))

	tm.tracker.MarkCommit()

	sinker.resetOps()

	err := tm.EnsureCleanup(ctx)
	require.NoError(t, err)
	require.False(t, tm.tracker.NeedsRollback())
	require.Equal(t, []string{"clear", "rollback", "dummy"}, sinker.opsSnapshot())

	err = tm.EnsureCleanup(ctx)
	require.NoError(t, err)
	require.Equal(t, []string{"clear", "rollback", "dummy"}, sinker.opsSnapshot())
}

func TestTransactionManager_Reset(t *testing.T) {
	ctx := context.Background()
	sinker := &mockSinker{}
	updater := newMockWatermarkUpdater()
	tm := NewTransactionManager(sinker, updater, 1, "task1", "db1", "table1")

	fromTs := types.TS{}
	toTs := (&fromTs).Next()

	// Begin transaction
	err := tm.BeginTransaction(ctx, fromTs, toTs)
	assert.NoError(t, err)
	assert.NotNil(t, tm.tracker)

	// Reset
	tm.Reset()

	assert.Nil(t, tm.tracker)
}
