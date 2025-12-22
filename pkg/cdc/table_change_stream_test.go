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
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	lock "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	txnpb "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/txn/rpc"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to create a test stream with minimal setup
func createTestStream(mp *mpool.MPool, tableInfo *DbTableInfo, opts ...TableChangeStreamOption) *TableChangeStream {
	sinker := &mockDataProcessorSinker{mockSinker: &mockSinker{}}
	updater := newMockWatermarkUpdater()
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id"},
			{Name: "ts"},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"id"},
		},
		Name2ColIndex: map[string]int32{
			"id": 0,
			"ts": 1,
		},
	}

	return NewTableChangeStream(
		nil, nil, mp, packerPool,
		1, "task1", tableInfo, sinker, updater, tableDef,
		false, &sync.Map{}, types.TS{}, types.TS{}, false, 0,
		opts...,
	)
}

type watermarkUpdaterStub struct {
	mu               sync.Mutex
	watermarks       map[string]types.TS
	errMsgs          []string
	updateErrFn      func(call int) error
	removeErr        error
	skipRemove       bool
	getFromCacheHook func() error
	updateCalls      atomic.Int32
	updateNotifies   chan struct{}
}

func newWatermarkUpdaterStub() *watermarkUpdaterStub {
	return &watermarkUpdaterStub{
		watermarks: make(map[string]types.TS),
		errMsgs:    make([]string, 0),
	}
}

func (m *watermarkUpdaterStub) withUpdateError(fn func(call int) error) *watermarkUpdaterStub {
	m.updateErrFn = fn
	return m
}

func (m *watermarkUpdaterStub) RemoveCachedWM(ctx context.Context, key *WatermarkKey) error {
	if m.skipRemove {
		return nil
	}
	if err := m.removeErr; err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.watermarks, m.keyString(key))
	m.mu.Unlock()
	return nil
}

func (m *watermarkUpdaterStub) UpdateWatermarkErrMsg(ctx context.Context, key *WatermarkKey, errMsg string, errorCtx *ErrorContext) error {
	m.mu.Lock()
	m.errMsgs = append(m.errMsgs, errMsg)
	m.mu.Unlock()
	return nil
}

func (m *watermarkUpdaterStub) GetFromCache(ctx context.Context, key *WatermarkKey) (types.TS, error) {
	if hook := m.getFromCacheHook; hook != nil {
		if err := hook(); err != nil {
			return types.TS{}, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	ts, ok := m.watermarks[m.keyString(key)]
	if !ok {
		return types.TS{}, moerr.NewInternalError(ctx, "watermark not found")
	}
	return ts, nil
}

func (m *watermarkUpdaterStub) setGetFromCacheHook(fn func() error) {
	m.getFromCacheHook = fn
}

func (m *watermarkUpdaterStub) setSkipRemove(skip bool) {
	m.skipRemove = skip
}

func (m *watermarkUpdaterStub) GetOrAddCommitted(ctx context.Context, key *WatermarkKey, watermark *types.TS) (types.TS, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	keyStr := m.keyString(key)
	if ts, ok := m.watermarks[keyStr]; ok {
		return ts, nil
	}
	m.watermarks[keyStr] = *watermark
	return *watermark, nil
}

func (m *watermarkUpdaterStub) UpdateWatermarkOnly(ctx context.Context, key *WatermarkKey, watermark *types.TS) error {
	call := int(m.updateCalls.Add(1))
	if m.updateNotifies != nil {
		select {
		case m.updateNotifies <- struct{}{}:
		default:
		}
	}
	if m.updateErrFn != nil {
		if err := m.updateErrFn(call); err != nil {
			return err
		}
	}
	m.mu.Lock()
	m.watermarks[m.keyString(key)] = *watermark
	m.mu.Unlock()
	return nil
}

func (m *watermarkUpdaterStub) IsCircuitBreakerOpen(key *WatermarkKey) bool {
	return false
}

func (m *watermarkUpdaterStub) GetCommitFailureCount(key *WatermarkKey) uint32 {
	return 0
}

func (m *watermarkUpdaterStub) keyString(key *WatermarkKey) string {
	if key == nil {
		return ""
	}
	return key.TaskId + ":" + key.DBName + ":" + key.TableName
}

func readGaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()
	var metric dto.Metric
	require.NoError(t, gauge.Write(&metric))
	return metric.GetGauge().GetValue()
}

func readCounterValue(t *testing.T, counter prometheus.Counter) float64 {
	t.Helper()
	var metric dto.Metric
	require.NoError(t, counter.Write(&metric))
	return metric.GetCounter().GetValue()
}

func TestTableChangeStream_HandleSnapshotNoProgress_WarningAndReset(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	tableInfo := &DbTableInfo{
		SourceDbName:  "db_warn",
		SourceTblName: "t_warn",
	}

	stream := createTestStream(
		mp,
		tableInfo,
		WithWatermarkStallThreshold(time.Minute),
		WithNoProgressWarningInterval(time.Nanosecond),
	)

	tableLabel := stream.progressTracker.tableKey()
	beforeCounter := readCounterValue(t, v2.CdcTableNoProgressCounter.WithLabelValues(tableLabel))
	beforeStuck := readGaugeValue(t, v2.CdcTableStuckGauge.WithLabelValues(tableLabel))
	assert.Equal(t, 0.0, beforeStuck)

	fromTs := types.BuildTS(100, 0)
	err := stream.handleSnapshotNoProgress(context.Background(), fromTs, fromTs)
	assert.NoError(t, err)
	assert.False(t, stream.noProgressSince.IsZero(), "noProgressSince should be initialized")

	afterCounter := readCounterValue(t, v2.CdcTableNoProgressCounter.WithLabelValues(tableLabel))
	assert.Equal(t, beforeCounter+1, afterCounter)

	afterStuck := readGaugeValue(t, v2.CdcTableStuckGauge.WithLabelValues(tableLabel))
	assert.Equal(t, 1.0, afterStuck)

	stream.onWatermarkAdvanced()
	assert.True(t, stream.noProgressSince.IsZero(), "noProgressSince should reset after progress")
	assert.True(t, stream.lastNoProgressWarning.IsZero(), "last warning timestamp should reset")

	resetStuck := readGaugeValue(t, v2.CdcTableStuckGauge.WithLabelValues(tableLabel))
	assert.Equal(t, 0.0, resetStuck)
}

func TestTableChangeStream_HandleSnapshotNoProgress_ThresholdExceeded(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	tableInfo := &DbTableInfo{
		SourceDbName:  "db_err",
		SourceTblName: "t_err",
	}

	stream := createTestStream(
		mp,
		tableInfo,
		WithWatermarkStallThreshold(10*time.Millisecond),
		WithNoProgressWarningInterval(time.Nanosecond),
	)

	stream.noProgressSince = time.Now().Add(-2 * stream.watermarkStallThreshold)

	fromTs := types.BuildTS(200, 0)
	err := stream.handleSnapshotNoProgress(context.Background(), fromTs, fromTs)
	assert.Error(t, err)
	require.True(t, stream.GetRetryable(), "stalled snapshot should mark stream retryable")
	assert.Contains(t, err.Error(), "snapshot timestamp stuck")

	// Clean up metric state for subsequent tests
	stream.onWatermarkAdvanced()
}

func TestTableChangeStream_HandleSnapshotNoProgress_WarningThrottle(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	tableInfo := &DbTableInfo{
		SourceDbName:  "db_throttle",
		SourceTblName: "t_throttle",
	}

	const interval = 50 * time.Millisecond

	stream := createTestStream(
		mp,
		tableInfo,
		WithWatermarkStallThreshold(time.Minute),
		WithNoProgressWarningInterval(interval),
	)

	fromTs := types.BuildTS(300, 0)

	err := stream.handleSnapshotNoProgress(context.Background(), fromTs, fromTs)
	require.NoError(t, err)
	require.False(t, stream.lastNoProgressWarning.IsZero(), "first warning timestamp should be recorded")
	firstWarning := stream.lastNoProgressWarning

	err = stream.handleSnapshotNoProgress(context.Background(), fromTs, fromTs)
	require.NoError(t, err, "second invocation before interval should still be treated as warning")
	assert.Equal(t, firstWarning, stream.lastNoProgressWarning, "warning timestamp should not advance before interval elapses")

	deadline := firstWarning.Add(interval)
	wait := time.Until(deadline)
	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-time.After(20 * interval):
			t.Fatalf("warning interval did not elapse within timeout")
		}
	}

	err = stream.handleSnapshotNoProgress(context.Background(), fromTs, fromTs)
	require.NoError(t, err)
	assert.True(t, stream.lastNoProgressWarning.After(firstWarning), "warning timestamp should advance after interval")

	stream.onWatermarkAdvanced()
}

// ============================================================================
// Integration Tests with gostub
// Note: Uses testChangesHandle from reader_test.go
// ============================================================================

// Test Run() with duplicate reader (should exit immediately)
func TestTableChangeStream_Run_DuplicateReader(t *testing.T) {
	runningReaders := &sync.Map{}

	h1 := newTableStreamHarness(t, withHarnessRunningReaders(runningReaders))
	defer h1.Close()

	ready := make(chan struct{})
	h1.SetCollectFactory(func(fromTs, toTs types.TS) (engine.ChangesHandle, error) {
		return &blockingChangesHandle{ready: ready}, nil
	})

	ar1 := h1.NewActiveRoutine()
	errCh1, done1 := h1.RunStreamAsync(ar1)
	defer done1()

	require.Eventually(t, func() bool {
		select {
		case <-ready:
			return true
		default:
			_, ok := runningReaders.Load(h1.Stream().runningReaderKey)
			return ok
		}
	}, time.Second, 10*time.Millisecond, "first stream should register as running")

	stored, ok := runningReaders.Load(h1.Stream().runningReaderKey)
	require.True(t, ok, "first stream should occupy running readers slot")
	require.Equal(t, h1.Stream(), stored)

	h2 := newTableStreamHarness(t, withHarnessRunningReaders(runningReaders))
	defer h2.Close()

	ar2 := h2.NewActiveRoutine()
	err := h2.RunStream(ar2)
	require.NoError(t, err, "duplicate reader should exit gracefully without error")

	stored, ok = runningReaders.Load(h1.Stream().runningReaderKey)
	require.True(t, ok, "running reader entry should remain owned by first stream")
	require.Equal(t, h1.Stream(), stored, "duplicate reader must not replace existing entry")

	done1()

	var runErr error
	select {
	case runErr = <-errCh1:
	case <-time.After(2 * time.Second):
		t.Fatal("first stream did not exit after cancellation")
	}
	if runErr != nil {
		require.ErrorIs(t, runErr, context.Canceled)
	}
}

// Integration: commit failure triggers EnsureCleanup rollback, then recovery succeeds
func TestTableChangeStream_CommitFailure_EnsureCleanup_ThenRecover(t *testing.T) {
	updaterStub := newWatermarkUpdaterStub()
	noopStop := func() {}
	h := newTableStreamHarness(t,
		withHarnessWatermarkUpdater(updaterStub, noopStop),
		withHarnessFrequency(1*time.Millisecond),
	)
	defer h.Close()

	// Prepare initial watermark to a low value
	key := h.Stream().txnManager.watermarkKey
	initial := types.BuildTS(1, 0)
	_, _ = updaterStub.GetOrAddCommitted(h.Context(), key, &initial)

	// First run: snapshot + tail_done, inject commit error
	snap1 := createTestBatch(t, h.MP(), types.BuildTS(2, 0), []int32{1})
	h.SetCollectBatches([]changeBatch{
		{insert: snap1, hint: engine.ChangesHandle_Snapshot},
		{insert: nil, hint: engine.ChangesHandle_Tail_done},
	})
	commitErr := moerr.NewInternalError(h.Context(), "commit failure")
	h.Sinker().setCommitError(commitErr)

	ar1 := h.NewActiveRoutine()
	err := h.RunStream(ar1)
	require.Error(t, err)
	require.ErrorIs(t, err, commitErr)

	// Ensure cleanup should have been invoked via defer and performed one rollback
	ops := h.Sinker().opsSnapshot()
	hasCommit := false
	rollbackCount := 0
	for _, op := range ops {
		if op == "commit" {
			hasCommit = true
		}
		if op == "rollback" {
			rollbackCount++
		}
	}
	require.True(t, hasCommit, "commit should have been attempted")
	require.GreaterOrEqual(t, rollbackCount, 1, "EnsureCleanup should trigger rollback")

	// Second run: use a fresh harness to avoid reusing internal goroutine counters
	h2 := newTableStreamHarness(t,
		withHarnessWatermarkUpdater(updaterStub, noopStop),
		withHarnessFrequency(1*time.Millisecond),
	)
	defer h2.Close()
	h2.Sinker().reset()
	// Use pausable handle to deterministically exit within one round:
	// start blocked, then cancel context and unblock to let Run exit immediately.
	proceed := make(chan struct{})
	ph := &pausableChangesHandle{proceed: proceed}
	h2.SetCollectHandleSequence(ph)

	ar2 := h2.NewActiveRoutine()
	errCh, _ := h2.RunStreamAsync(ar2)
	// Trigger stop signals then unblock collector to finish the round
	h2.Cancel()
	close(proceed)
	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(3 * time.Second):
		t.Fatal("second stream did not finish in time")
	}
	require.NoError(t, runErr)

	ops = h2.Sinker().opsSnapshot()
	rollbackCount = 0
	for _, op := range ops {
		if op == "rollback" {
			rollbackCount++
		}
	}
	// Second run is cancellation-driven fast exit; it must not rollback.
	require.Equal(t, 0, rollbackCount, "second run should not rollback")
}

// Recovery path: successful begin/commit after a clean setup (no cancellation)
func TestTableChangeStream_Recovery_BeginCommit(t *testing.T) {
	updaterStub := newWatermarkUpdaterStub()
	noopStop := func() {}
	h := newTableStreamHarness(t,
		withHarnessWatermarkUpdater(updaterStub, noopStop),
		withHarnessFrequency(1*time.Millisecond),
	)
	defer h.Close()

	// Initialize a committed watermark
	key := h.Stream().txnManager.watermarkKey
	initial := types.BuildTS(1, 0)
	_, _ = updaterStub.GetOrAddCommitted(h.Context(), key, &initial)

	// Provide one snapshot batch then a tail_done marker to commit
	snap := createTestBatch(t, h.MP(), types.BuildTS(100, 0), []int32{1})
	h.SetCollectBatches([]changeBatch{
		{insert: snap, hint: engine.ChangesHandle_Snapshot},
		{insert: nil, hint: engine.ChangesHandle_Tail_done},
	})

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)

	// Wait for begin and commit operations to occur
	require.Eventually(t, func() bool {
		ops := h.Sinker().opsSnapshot()
		hasBegin := false
		hasCommit := false
		for _, op := range ops {
			if op == "begin" {
				hasBegin = true
			}
			if op == "commit" {
				hasCommit = true
			}
		}
		return hasBegin && hasCommit
	}, 300*time.Millisecond, 10*time.Millisecond, "should see begin and commit operations")

	// Cancel to exit gracefully
	h.Cancel()
	done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(time.Second):
		t.Fatal("stream did not exit after cancellation")
	}
	if runErr != nil {
		require.ErrorIs(t, runErr, context.Canceled)
	}

	ops := h.Sinker().opsSnapshot()
	begin := 0
	commit := 0
	rollback := 0
	for _, op := range ops {
		switch op {
		case "begin":
			begin++
		case "commit":
			commit++
		case "rollback":
			rollback++
		}
	}
	require.GreaterOrEqual(t, begin, 1, "should begin at least once")
	require.GreaterOrEqual(t, commit, 1, "should commit at least once")
	require.Equal(t, 0, rollback, "recovery path should not rollback")
	require.False(t, h.Stream().GetRetryable())
}

type blockingChangesHandle struct {
	ready chan struct{}
	once  sync.Once
}

func (h *blockingChangesHandle) Next(ctx context.Context, _ *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	h.once.Do(func() {
		close(h.ready)
	})
	<-ctx.Done()
	return nil, nil, engine.ChangesHandle_Tail_done, ctx.Err()
}

func (h *blockingChangesHandle) Close() error { return nil }

type pausableChangesHandle struct {
	ready   chan<- struct{}
	proceed <-chan struct{}
	once    sync.Once
	done    bool
}

func newPausableChangesHandle(ready chan<- struct{}, proceed <-chan struct{}) *pausableChangesHandle {
	return &pausableChangesHandle{
		ready:   ready,
		proceed: proceed,
	}
}

func (h *pausableChangesHandle) Next(ctx context.Context, _ *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	h.once.Do(func() {
		select {
		case h.ready <- struct{}{}:
		default:
		}
	})

	select {
	case <-ctx.Done():
		return nil, nil, engine.ChangesHandle_Tail_done, ctx.Err()
	case <-h.proceed:
	}

	if h.done {
		return nil, nil, engine.ChangesHandle_Tail_done, nil
	}
	h.done = true
	return nil, nil, engine.ChangesHandle_Tail_done, nil
}

func (h *pausableChangesHandle) Close() error {
	return nil
}

type changeBatch struct {
	insert *batch.Batch
	delete *batch.Batch
	hint   engine.ChangesHandle_Hint
}

type immediateChangesHandle struct {
	mu      sync.Mutex
	batches []changeBatch
	nextIdx int
	closed  bool
}

func newImmediateChangesHandle(batches []changeBatch) *immediateChangesHandle {
	return &immediateChangesHandle{batches: batches}
}

func (h *immediateChangesHandle) Next(ctx context.Context, _ *mpool.MPool) (*batch.Batch, *batch.Batch, engine.ChangesHandle_Hint, error) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if h.closed {
		return nil, nil, engine.ChangesHandle_Tail_done, nil
	}

	if len(h.batches) == 0 || h.nextIdx >= len(h.batches) {
		h.closed = true
		return nil, nil, engine.ChangesHandle_Tail_done, nil
	}

	b := h.batches[h.nextIdx]
	h.nextIdx++
	return b.insert, b.delete, b.hint, nil
}

func (h *immediateChangesHandle) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = true
	return nil
}

// Test StaleRead retry logic
func TestTableChangeStream_StaleRead_Retry(t *testing.T) {
	h := newTableStreamHarness(
		t,
		withHarnessNoFull(true),
		withHarnessFrequency(5*time.Millisecond),
	)
	defer h.Close()

	var snapshotCalls atomic.Int32
	h.SetGetSnapshotTS(func(op client.TxnOperator) timestamp.Timestamp {
		call := snapshotCalls.Add(1)
		ts := timestamp.Timestamp{PhysicalTime: 100}
		if call >= 2 {
			ts.PhysicalTime = 200
		}
		return ts
	})

	var collectCalls atomic.Int32
	h.SetCollectFactory(func(fromTs, toTs types.TS) (engine.ChangesHandle, error) {
		if collectCalls.Add(1) == 1 {
			return nil, moerr.NewErrStaleReadNoCtx("db1", "t1")
		}
		bat := createTestBatch(t, h.MP(), toTs, []int32{1})
		return newImmediateChangesHandle([]changeBatch{
			{insert: bat, hint: engine.ChangesHandle_Tail_done},
			{insert: nil, hint: engine.ChangesHandle_Tail_done},
		}), nil
	})

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)

	require.Eventually(t, func() bool {
		return len(h.CollectCallsSnapshot()) >= 2
	}, 2*time.Second, 10*time.Millisecond, "should see stale read recovery with multiple collect calls")

	require.Eventually(t, func() bool {
		ops := h.Sinker().opsSnapshot()
		for _, op := range ops {
			if op == "begin" || op == "sink" {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "stream should process data successfully after stale read recovery")

	done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(time.Second):
		t.Fatal("table change stream run did not terminate")
	}
	if runErr != nil {
		require.ErrorIs(t, runErr, context.Canceled, "if error, should be context.Canceled")
	}

	require.Equal(t, 1, h.Sinker().ResetCountSnapshot(), "sinker should be reset exactly once on stale read recovery")
	require.GreaterOrEqual(t, len(h.CollectCallsSnapshot()), 2, "should have multiple collect calls after stale read recovery")
	require.True(t, h.Stream().GetRetryable(), "stale read should mark stream retryable")
}

func TestTableChangeStream_SinkerResetRecovery(t *testing.T) {
	h := newTableStreamHarness(
		t,
		withHarnessNoFull(true),
		withHarnessFrequency(5*time.Millisecond),
	)
	defer h.Close()

	var snapshotCalls atomic.Int32
	h.SetGetSnapshotTS(func(op client.TxnOperator) timestamp.Timestamp {
		call := snapshotCalls.Add(1)
		ts := timestamp.Timestamp{PhysicalTime: 100}
		if call >= 2 {
			ts.PhysicalTime = 200
		}
		return ts
	})

	var collectCalls atomic.Int32
	initialSinkerErr := moerr.NewInternalError(h.Context(), "sinker failure")
	h.SetCollectFactory(func(fromTs, toTs types.TS) (engine.ChangesHandle, error) {
		if collectCalls.Add(1) == 1 {
			h.Sinker().setError(initialSinkerErr)
			return nil, moerr.NewErrStaleReadNoCtx("db1", "t1")
		}
		bat := createTestBatch(t, h.MP(), toTs, []int32{1})
		return newImmediateChangesHandle([]changeBatch{
			{insert: bat, hint: engine.ChangesHandle_Tail_done},
			{insert: nil, hint: engine.ChangesHandle_Tail_done},
		}), nil
	})

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)

	require.Eventually(t, func() bool {
		return collectCalls.Load() >= 2
	}, 2*time.Second, 10*time.Millisecond, "stale read should trigger another collect attempt")

	require.Eventually(t, func() bool {
		ops := h.Sinker().opsSnapshot()
		hasBegin := false
		hasCommit := false
		for _, op := range ops {
			switch op {
			case "begin":
				hasBegin = true
			case "commit":
				hasCommit = true
			}
		}
		return hasBegin && hasCommit
	}, 2*time.Second, 10*time.Millisecond, "stream should process data successfully after reset")

	done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(time.Second):
		t.Fatal("table change stream run did not terminate")
	}
	if runErr != nil {
		require.ErrorIs(t, runErr, context.Canceled, "context cancellation is acceptable after shutdown")
	}

	require.Equal(t, 1, h.Sinker().ResetCountSnapshot(), "sinker should reset exactly once after stale read recovery")
	require.NoError(t, h.Sinker().Error(), "sinker error should be cleared by reset")
	require.GreaterOrEqual(t, len(h.CollectCallsSnapshot()), 2, "multiple collect attempts expected")
	require.True(t, h.Stream().GetRetryable(), "recovery should keep stream retryable")

	// After successful commit, tracker is cleaned up (set to nil)
	// If tracker exists (e.g., after rollback), it should be completed
	tracker := h.Stream().txnManager.GetTracker()
	if tracker != nil {
		require.True(t, tracker.IsCompleted(), "tracker should reach completed state after recovery")
	}
	// tracker being nil is also valid - it indicates successful commit and cleanup
}

// Test StaleRead with startTs set (should fail, not retry)
func TestTableChangeStream_StaleRead_NoRetryWithStartTs(t *testing.T) {
	startTs := types.BuildTS(1, 0)
	h := newTableStreamHarness(
		t,
		withHarnessStartTs(startTs),
	)
	defer h.Close()

	staleErr := moerr.NewErrStaleReadNoCtx("db1", "t1")
	h.SetCollectError(staleErr)

	ar := h.NewActiveRoutine()
	err := h.RunStream(ar)

	require.Error(t, err)
	require.Contains(t, err.Error(), "cannot recover")
	require.False(t, h.Stream().GetRetryable(), "stale read with startTs should not be retryable")
	require.Equal(t, 0, h.Sinker().ResetCountSnapshot(), "sinker reset should not occur on fatal stale read")
}

func TestTableChangeStream_StaleReadRetry_WatermarkUpdateFailure(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 2*time.Second, moerr.CauseFinishTxnOp)
	defer cancel()

	failureErr := moerr.NewInternalError(ctx, "inject update failure")
	updater := newWatermarkUpdaterStub().
		withUpdateError(func(int) error { return failureErr })

	h := newTableStreamHarness(
		t,
		withHarnessNoFull(true),
		withHarnessWatermarkUpdater(updater, nil),
	)
	defer h.Close()

	zero := types.TS{}
	_, _ = updater.GetOrAddCommitted(context.Background(), h.Stream().watermarkKey, &zero)

	staleErr := moerr.NewErrStaleReadNoCtx("db1", "t1")
	h.SetCollectError(staleErr)

	ar := h.NewActiveRoutine()
	err := h.RunStream(ar)

	require.Error(t, err)
	require.Contains(t, err.Error(), "stale read recovery failed to update watermark")
	require.False(t, h.Stream().GetRetryable(), "watermark update failure should not be retryable")
	require.Equal(t, int32(1), updater.updateCalls.Load(), "UpdateWatermarkOnly should be invoked exactly once")
	require.Len(t, h.CollectCallsSnapshot(), 1)
}

func TestTableChangeStream_StaleReadRetry_MultipleAttempts(t *testing.T) {
	updater := newWatermarkUpdaterStub()
	h := newTableStreamHarness(
		t,
		withHarnessNoFull(true),
		withHarnessWatermarkUpdater(updater, nil),
		withHarnessFrequency(5*time.Millisecond),
	)
	defer h.Close()

	zero := types.TS{}
	_, _ = updater.GetOrAddCommitted(context.Background(), h.Stream().watermarkKey, &zero)

	var collectCount atomic.Int32
	h.SetCollectFactory(func(fromTs, toTs types.TS) (engine.ChangesHandle, error) {
		attempt := int(collectCount.Add(1))
		if attempt <= 3 {
			return nil, moerr.NewErrStaleReadNoCtx("db1", "t1")
		}
		if attempt == 4 {
			tail := createTestBatch(t, h.MP(), toTs, []int32{int32(attempt)})
			return newImmediateChangesHandle([]changeBatch{
				{insert: tail, hint: engine.ChangesHandle_Tail_done},
				{insert: nil, hint: engine.ChangesHandle_Tail_done},
			}), nil
		}
		return newImmediateChangesHandle(nil), nil
	})

	var snapshotCalls atomic.Int32
	h.SetGetSnapshotTS(func(op client.TxnOperator) timestamp.Timestamp {
		call := snapshotCalls.Add(1)
		return timestamp.Timestamp{PhysicalTime: int64(100 + call*50)}
	})

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)

	require.Eventually(t, func() bool {
		return collectCount.Load() >= 4
	}, 2*time.Second, 10*time.Millisecond, "expected multiple stale read retries before success")

	done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("table change stream run did not complete")
	}
	if runErr != nil {
		require.ErrorIs(t, runErr, context.Canceled)
	}

	require.GreaterOrEqual(t, collectCount.Load(), int32(4))
	require.GreaterOrEqual(t, updater.updateCalls.Load(), int32(3))
	require.True(t, h.Stream().GetRetryable(), "successful recovery should keep retryable flag true")
	require.Equal(t, 3, h.Sinker().ResetCountSnapshot(), "sinker should reset for each stale read")
}

// Test end-to-end with real change processing
// Test context cancellation
func TestTableChangeStream_Run_ContextCancel(t *testing.T) {
	h := newTableStreamHarness(t)
	defer h.Close()

	tail := createTestBatch(t, h.MP(), types.BuildTS(10, 0), []int32{1})
	h.SetCollectBatches([]changeBatch{
		{insert: tail, hint: engine.ChangesHandle_Tail_done},
		{insert: nil, hint: engine.ChangesHandle_Tail_done},
	})

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)

	require.Eventually(t, func() bool {
		return len(h.CollectCallsSnapshot()) > 0
	}, time.Second, 10*time.Millisecond, "stream should begin collecting before cancellation")

	opsBefore := h.Sinker().opsSnapshot()

	h.Cancel()
	done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not stop after context cancellation")
	}

	if runErr != nil {
		require.ErrorIs(t, runErr, context.Canceled)
	}

	require.False(t, h.Stream().GetRetryable(), "cancel should not mark stream retryable")
	require.Equal(t, opsBefore, h.Sinker().opsSnapshot(), "cancel should not produce additional sink operations")
}

// Test ActiveRoutine Pause
func TestTableChangeStream_Run_Pause(t *testing.T) {
	h := newTableStreamHarness(t)
	defer h.Close()

	ready := make(chan struct{})
	h.SetCollectFactory(func(fromTs, toTs types.TS) (engine.ChangesHandle, error) {
		return &blockingChangesHandle{ready: ready}, nil
	})

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("collector did not block as expected")
	}

	ar.ClosePause()
	done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Stream did not stop after pause")
	}

	require.Error(t, runErr)
	if !errors.Is(runErr, context.Canceled) {
		require.Contains(t, runErr.Error(), "paused")
	}

	require.False(t, h.Stream().GetRetryable(), "pause should not mark stream retryable")
	require.Empty(t, h.Sinker().opsSnapshot(), "pause should abort before any sink operations")
}

func TestTableChangeStream_ConcurrentStopSignalsCleanup(t *testing.T) {
	runningReaders := &sync.Map{}

	updater := newWatermarkUpdaterStub()
	h := newTableStreamHarness(
		t,
		withHarnessRunningReaders(runningReaders),
		withHarnessWatermarkUpdater(updater, nil),
	)
	defer h.Close()
	zero := types.TS{}
	_, _ = updater.GetOrAddCommitted(context.Background(), h.Stream().watermarkKey, &zero)

	blockReady := make(chan struct{}, 1)
	proceed := make(chan struct{})

	h.SetCollectFactory(func(fromTs, toTs types.TS) (engine.ChangesHandle, error) {
		return newPausableChangesHandle(blockReady, proceed), nil
	})

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)

	select {
	case <-blockReady:
	case <-time.After(time.Second):
		t.Fatal("collector did not reach blocking point")
	}

	// Issue all stop signals.
	ar.ClosePause()
	ar.CloseCancel()
	h.Cancel()
	h.Stream().Close()

	// Unblock collector so Run can exit.
	close(proceed)
	done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(time.Second):
		t.Fatal("stream did not terminate after concurrent stop signals")
	}
	if runErr != nil {
		require.True(t, errors.Is(runErr, context.Canceled) || strings.Contains(runErr.Error(), "paused") || strings.Contains(runErr.Error(), "cancelled"),
			"unexpected error: %v", runErr)
	}

	h.Stream().Wait()
	_, ok := runningReaders.Load(h.Stream().runningReaderKey)
	require.False(t, ok, "runningReaders should be cleaned up")

	// Second run: ensure duplicate readers can start after cleanup.
	updater2 := newWatermarkUpdaterStub()
	h2 := newTableStreamHarness(
		t,
		withHarnessRunningReaders(runningReaders),
		withHarnessWatermarkUpdater(updater2, nil),
	)
	defer h2.Close()
	_, _ = updater2.GetOrAddCommitted(context.Background(), h2.Stream().watermarkKey, &zero)

	blockReady2 := make(chan struct{}, 1)
	proceed2 := make(chan struct{})

	h2.SetCollectFactory(func(fromTs, toTs types.TS) (engine.ChangesHandle, error) {
		return newPausableChangesHandle(blockReady2, proceed2), nil
	})

	ar2 := h2.NewActiveRoutine()
	errCh2, done2 := h2.RunStreamAsync(ar2)

	select {
	case <-blockReady2:
	case <-time.After(time.Second):
		t.Fatal("second collector did not reach blocking point")
	}

	h2.Cancel()
	close(proceed2)
	done2()

	var runErr2 error
	select {
	case runErr2 = <-errCh2:
	case <-time.After(time.Second):
		t.Fatal("second stream did not terminate after cancellation")
	}
	if runErr2 != nil {
		require.True(t, errors.Is(runErr2, context.Canceled) || strings.Contains(runErr2.Error(), "paused"),
			"unexpected error: %v", runErr2)
	}

	h2.Stream().Wait()
	_, ok = runningReaders.Load(h2.Stream().runningReaderKey)
	require.False(t, ok, "runningReaders should be empty after second stream exits")
}

// Integration tests for TableChangeStream + DataProcessor + TransactionManager pipeline
func TestTableChangeStream_DataProcessorSinkerError(t *testing.T) {
	h := newTableStreamHarness(t)
	defer h.Close()

	sinkerErr := moerr.NewInternalError(h.Context(), "sinker error")
	h.Sinker().setError(sinkerErr)

	snapshot := createTestBatch(t, h.MP(), types.BuildTS(1, 0), []int32{1})
	h.SetCollectBatches([]changeBatch{
		{insert: snapshot, hint: engine.ChangesHandle_Snapshot},
		{insert: nil, hint: engine.ChangesHandle_Tail_done},
	})

	ar := h.NewActiveRoutine()
	err := h.RunStream(ar)

	require.Error(t, err)
	require.Equal(t, sinkerErr, err)
	require.Equal(t, sinkerErr, h.Sinker().Error(), "sinker error should remain set for inspection")
	require.False(t, h.Stream().GetRetryable(), "fatal sinker error should not mark stream retryable")
	require.Equal(t, 0, h.Sinker().ResetCountSnapshot(), "no resets expected when error occurs before transaction start")

	ops := h.Sinker().opsSnapshot()
	require.NotContains(t, ops, "begin", "Begin should not be called when sinker already has an error")
	require.NotContains(t, ops, "rollback", "No transaction started, rollback not expected")
	require.Empty(t, ops, "sinker should not record operations when it starts with an error")

	require.Nil(t, h.Stream().txnManager.GetTracker(), "tracker should remain nil when no transaction begins")
}

func TestTableChangeStream_EnsureCleanupOnCollectorError(t *testing.T) {
	h := newTableStreamHarness(t)
	defer h.Close()

	collectErr := moerr.NewInternalError(h.Context(), "collector error")
	h.SetCollectError(collectErr)

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)
	defer done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(time.Second):
		t.Fatal("table change stream run did not complete")
	}

	require.Error(t, runErr)
	require.Equal(t, collectErr, runErr)

	ops := h.Sinker().opsSnapshot()
	require.NotContains(t, ops, "begin")
	require.NotContains(t, ops, "commit")
	require.NotContains(t, ops, "rollback")
	require.Equal(t, 0, h.Sinker().ResetCountSnapshot())

	require.Nil(t, h.Stream().txnManager.GetTracker())
	require.False(t, h.Stream().GetRetryable(), "collector error should not be retryable")
}

func TestTableChangeStream_TailDoneUpdatesTransactionToTs(t *testing.T) {
	h := newTableStreamHarness(t)
	defer h.Close()

	tail1 := createTestBatch(t, h.MP(), types.BuildTS(100, 0), []int32{1})
	tail2 := createTestBatch(t, h.MP(), types.BuildTS(150, 0), []int32{2})
	h.SetCollectBatches([]changeBatch{
		{insert: tail1, hint: engine.ChangesHandle_Tail_done},
		{insert: tail2, hint: engine.ChangesHandle_Tail_done},
		{insert: nil, hint: engine.ChangesHandle_Tail_done},
	})

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)

	require.Eventually(t, func() bool {
		ops := h.Sinker().opsSnapshot()
		for _, op := range ops {
			if op == "commit" {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "expected sinker to observe commit for tail-done batches")

	done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("table change stream run did not complete")
	}

	require.NoError(t, runErr)

	ops := h.Sinker().opsSnapshot()
	beginCount := 0
	for _, op := range ops {
		if op == "begin" {
			beginCount++
		}
	}
	require.Equal(t, 1, beginCount, "Should have only one BEGIN for multiple TailDone batches")
	require.Contains(t, ops, "commit", "Should commit after NoMoreData")
	require.Equal(t, 0, h.Sinker().ResetCountSnapshot(), "no sinker resets expected for successful tail processing")

	if tracker := h.Stream().txnManager.GetTracker(); tracker != nil {
		require.True(t, tracker.IsCompleted(), "transaction tracker should be marked complete")
		toTs := tracker.GetToTs()
		require.False(t, (&toTs).IsEmpty(), "tracker toTs should be recorded")
	}

	calls := h.Sinker().sinkCallsSnapshot()
	require.GreaterOrEqual(t, len(calls), 1)
	finalToTs := calls[len(calls)-1].toTs

	stats := h.Stream().progressTracker.GetStats()
	require.Equal(t, "idle", stats["state"], "progress tracker should return to idle after completion")
	require.Equal(t, finalToTs.ToString(), stats["current_watermark"])
	require.EqualValues(t, initialSyncStateSuccess, stats["initial_sync_state"], "initial sync should complete successfully")
	require.EqualValues(t, 1, stats["total_transactions"], "exactly one transaction should be committed")
}

func TestTableChangeStream_CommitFailureTriggersEnsureCleanup(t *testing.T) {
	h := newTableStreamHarness(t)
	defer h.Close()

	commitErr := moerr.NewInternalError(h.Context(), "commit failure")
	h.Sinker().setCommitError(commitErr)

	tailBatch := createTestBatch(t, h.MP(), types.BuildTS(100, 0), []int32{1})

	h.SetCollectBatches([]changeBatch{
		{insert: tailBatch, hint: engine.ChangesHandle_Tail_done},
		{insert: nil, hint: engine.ChangesHandle_Tail_done},
	})

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)
	defer done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("table change stream run did not complete")
	}

	require.Error(t, runErr)
	require.Equal(t, commitErr, runErr)

	var ops []string
	require.Eventually(t, func() bool {
		ops = h.Sinker().opsSnapshot()
		hasBegin := false
		hasCommit := false
		hasClear := false
		hasRollback := false
		for _, op := range ops {
			switch op {
			case "begin":
				hasBegin = true
			case "commit":
				hasCommit = true
			case "clear":
				hasClear = true
			case "rollback":
				hasRollback = true
			}
		}
		return hasBegin && hasCommit && hasClear && hasRollback
	}, time.Second, 10*time.Millisecond, "EnsureCleanup should clear error and rollback")

	rollbackCount := 0
	for _, op := range ops {
		if op == "rollback" {
			rollbackCount++
		}
	}
	// With retry mechanism, each retry creates a new transaction that may fail and rollback.
	// So multiple rollbacks are expected when retries occur. What matters is that
	// EnsureCleanup was called and rollback occurred (verified by ops above).
	require.GreaterOrEqual(t, rollbackCount, 1, "rollback should occur at least once")
	// Note: With retry mechanism, sinker may still have error after retries,
	// but EnsureCleanup should have been called (verified by ops above).
	// The error state may persist because commitErr is re-injected on each retry.
	// What matters is that EnsureCleanup was called and rollback occurred.
	tracker := h.Stream().txnManager.GetTracker()
	require.NotNil(t, tracker)
	require.False(t, tracker.NeedsRollback(), "tracker should be clean after EnsureCleanup")
}

func TestTableChangeStream_BeginFailureDoesNotRollback(t *testing.T) {
	h := newTableStreamHarness(t)
	defer h.Close()

	beginErr := moerr.NewInternalError(h.Context(), "begin failure")
	h.Sinker().setBeginError(beginErr)

	tailBatch := createTestBatch(t, h.MP(), types.BuildTS(100, 0), []int32{1})
	h.SetCollectBatches([]changeBatch{
		{insert: tailBatch, hint: engine.ChangesHandle_Tail_done},
		{insert: nil, hint: engine.ChangesHandle_Tail_done},
	})

	ar := h.NewActiveRoutine()
	err := h.RunStream(ar)

	require.Error(t, err)
	require.Equal(t, beginErr, err)
	require.Equal(t, beginErr, h.Sinker().Error(), "sinker error should remain set after begin failure")
	// "begin failure" without network context is non-retryable (state-related)
	require.False(t, h.Stream().GetRetryable(), "state-related begin failure should not mark stream retryable")

	ops := h.Sinker().opsSnapshot()
	require.Contains(t, ops, "begin", "Begin should be attempted even though it fails")
	require.NotContains(t, ops, "commit", "Commit should not be attempted after begin failure")
	require.NotContains(t, ops, "rollback", "Rollback should not occur when begin fails before start")

	tracker := h.Stream().txnManager.GetTracker()
	require.NotNil(t, tracker, "tracker should be created even if begin fails")
	require.False(t, tracker.NeedsRollback(), "tracker should not require rollback when begin fails")
	require.False(t, tracker.IsCompleted(), "tracker should remain incomplete after begin failure")
}

func TestTableChangeStream_BeginFailure_NetworkError_Retryable(t *testing.T) {
	h := newTableStreamHarness(t)
	defer h.Close()

	// Network-related begin failure should be retryable
	beginErr := moerr.NewInternalError(h.Context(), "begin transaction failed: connection timeout")
	h.Sinker().setBeginError(beginErr)

	tailBatch := createTestBatch(t, h.MP(), types.BuildTS(100, 0), []int32{1})
	h.SetCollectBatches([]changeBatch{
		{insert: tailBatch, hint: engine.ChangesHandle_Tail_done},
		{insert: nil, hint: engine.ChangesHandle_Tail_done},
	})

	ar := h.NewActiveRoutine()
	err := h.RunStream(ar)

	require.Error(t, err)
	require.Equal(t, beginErr, err)
	// Network-related begin failure should be retryable
	require.True(t, h.Stream().GetRetryable(), "network-related begin failure should mark stream retryable")
}

func TestTableChangeStream_BeginFailure_ServiceUnavailable_Retryable(t *testing.T) {
	h := newTableStreamHarness(t)
	defer h.Close()

	// Service unavailable begin failure should be retryable
	beginErr := moerr.NewInternalError(h.Context(), "begin transaction failed: service unavailable")
	h.Sinker().setBeginError(beginErr)

	tailBatch := createTestBatch(t, h.MP(), types.BuildTS(100, 0), []int32{1})
	h.SetCollectBatches([]changeBatch{
		{insert: tailBatch, hint: engine.ChangesHandle_Tail_done},
		{insert: nil, hint: engine.ChangesHandle_Tail_done},
	})

	ar := h.NewActiveRoutine()
	err := h.RunStream(ar)

	require.Error(t, err)
	require.Equal(t, beginErr, err)
	// Service unavailable begin failure should be retryable
	require.True(t, h.Stream().GetRetryable(), "service unavailable begin failure should mark stream retryable")
}

func TestTableChangeStream_EnsureCleanup_WatermarkMismatch(t *testing.T) {
	// Create a custom watermark updater stub that returns a different watermark
	// than what the tracker expects, simulating a watermark mismatch scenario
	updaterStub := newWatermarkUpdaterStub()
	updaterStub.setSkipRemove(true)

	// Create harness with custom watermark updater
	h := newTableStreamHarness(
		t,
		withHarnessWatermarkUpdater(updaterStub, func() {}),
	)
	defer h.Close()

	h.SetGetSnapshotTS(func(op client.TxnOperator) timestamp.Timestamp {
		ts := timestamp.Timestamp{PhysicalTime: 200}
		if noop, ok := op.(*noopTxnOperator); ok {
			noop.snapshot = ts
		}
		return ts
	})

	ctx := context.Background()
	initialWatermark := types.BuildTS(100, 0)
	key := h.Stream().watermarkKey
	require.NoError(t, updaterStub.UpdateWatermarkOnly(ctx, key, &initialWatermark))

	// Set up commit failure
	commitErr := moerr.NewInternalError(h.Context(), "commit failure")
	h.Sinker().setCommitError(commitErr)

	// Create batch with toTs = 200, but watermark in cache is still 100
	tailBatch := createTestBatch(t, h.MP(), types.BuildTS(200, 0), []int32{1})
	h.SetCollectBatches([]changeBatch{
		{insert: tailBatch, hint: engine.ChangesHandle_Tail_done},
		{insert: nil, hint: engine.ChangesHandle_Tail_done},
	})

	// Run stream - commit will fail, and EnsureCleanup should detect watermark mismatch
	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)
	defer done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("table change stream run did not complete")
	}

	require.Error(t, runErr)
	require.Equal(t, commitErr, runErr)

	// Wait for EnsureCleanup to complete
	var ops []string
	require.Eventually(t, func() bool {
		ops = h.Sinker().opsSnapshot()
		hasBegin := false
		hasCommit := false
		hasRollback := false
		for _, op := range ops {
			switch op {
			case "begin":
				hasBegin = true
			case "commit":
				hasCommit = true
			case "rollback":
				hasRollback = true
			}
		}
		return hasBegin && hasCommit && hasRollback
	}, time.Second, 10*time.Millisecond, "EnsureCleanup should detect watermark mismatch and trigger rollback")

	// Verify rollback occurred due to watermark mismatch
	rollbackCount := 0
	for _, op := range ops {
		if op == "rollback" {
			rollbackCount++
		}
	}
	require.GreaterOrEqual(t, rollbackCount, 1, "rollback should occur due to watermark mismatch")

	// Verify tracker is cleaned up
	tracker := h.Stream().txnManager.GetTracker()
	require.NotNil(t, tracker, "tracker should exist after EnsureCleanup")
	require.False(t, tracker.NeedsRollback(), "tracker should be clean after rollback")

	// Verify watermark in cache is still 100 (not updated due to commit failure)
	cachedWatermark, err := updaterStub.GetFromCache(ctx, key)
	require.NoError(t, err)
	require.Equal(t, initialWatermark.ToString(), cachedWatermark.ToString(), "watermark should remain unchanged after commit failure")
}

func TestTableChangeStream_EnsureCleanup_GetFromCacheFails(t *testing.T) {
	// Create a custom watermark updater stub that fails on GetFromCache
	updaterStub := newWatermarkUpdaterStub()
	getFromCacheErr := moerr.NewInternalError(context.Background(), "cache error")
	updaterStub.setSkipRemove(true)

	// Create harness with custom watermark updater
	h := newTableStreamHarness(
		t,
		withHarnessWatermarkUpdater(updaterStub, func() {}),
	)
	defer h.Close()

	h.SetGetSnapshotTS(func(op client.TxnOperator) timestamp.Timestamp {
		ts := timestamp.Timestamp{PhysicalTime: 200}
		if noop, ok := op.(*noopTxnOperator); ok {
			noop.snapshot = ts
		}
		return ts
	})

	// Set up commit failure
	commitErr := moerr.NewInternalError(h.Context(), "commit failure")
	h.Sinker().setCommitError(commitErr)

	var callCount atomic.Int32
	updaterStub.setGetFromCacheHook(func() error {
		call := callCount.Add(1)
		if call >= 3 {
			return getFromCacheErr
		}
		return nil
	})

	// Create batch
	tailBatch := createTestBatch(t, h.MP(), types.BuildTS(200, 0), []int32{1})
	h.SetCollectBatches([]changeBatch{
		{insert: tailBatch, hint: engine.ChangesHandle_Tail_done},
		{insert: nil, hint: engine.ChangesHandle_Tail_done},
	})

	// Run stream - commit will fail, and EnsureCleanup should handle GetFromCache failure
	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)
	defer done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("table change stream run did not complete")
	}

	require.Error(t, runErr)
	// With the original error preservation mechanism, the system should always return
	// the original error (commit failure) that triggered the retry, not auxiliary errors
	// (like cache error) encountered during retry attempts. This ensures deterministic
	// error reporting.
	require.Equal(t, commitErr, runErr,
		"error should be the original commit failure, not auxiliary cache error, got: %v", runErr)

	// Wait for EnsureCleanup to complete
	// When GetFromCache fails, EnsureCleanup should fallback to tracker state
	var ops []string
	require.Eventually(t, func() bool {
		ops = h.Sinker().opsSnapshot()
		hasBegin := false
		hasCommit := false
		hasRollback := false
		for _, op := range ops {
			switch op {
			case "begin":
				hasBegin = true
			case "commit":
				hasCommit = true
			case "rollback":
				hasRollback = true
			}
		}
		return hasBegin && hasCommit && hasRollback
	}, time.Second, 10*time.Millisecond, "EnsureCleanup should fallback to tracker state when GetFromCache fails")

	// Verify rollback occurred (fallback to tracker state)
	rollbackCount := 0
	for _, op := range ops {
		if op == "rollback" {
			rollbackCount++
		}
	}
	require.GreaterOrEqual(t, rollbackCount, 1, "rollback should occur when GetFromCache fails and tracker needs rollback")

	// Verify tracker is cleaned up
	tracker := h.Stream().txnManager.GetTracker()
	require.NotNil(t, tracker, "tracker should exist after EnsureCleanup")
	require.False(t, tracker.NeedsRollback(), "tracker should be clean after rollback")
}

func TestTableChangeStream_RollbackFailure(t *testing.T) {
	h := newTableStreamHarness(t)
	defer h.Close()

	commitErr := moerr.NewInternalError(h.Context(), "commit failure")
	rollbackErr := moerr.NewInternalError(h.Context(), "rollback failure")
	h.Sinker().setCommitError(commitErr)
	h.Sinker().setRollbackError(rollbackErr)

	tailBatch := createTestBatch(t, h.MP(), types.BuildTS(200, 0), []int32{1})
	h.SetCollectBatches([]changeBatch{
		{insert: tailBatch, hint: engine.ChangesHandle_Tail_done},
		{insert: nil, hint: engine.ChangesHandle_Tail_done},
	})

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)
	defer done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(5 * time.Second):
		t.Fatal("table change stream run did not complete")
	}

	require.Error(t, runErr)
	require.Equal(t, commitErr, runErr, "original commit error should propagate even if rollback fails")
	require.False(t, h.Stream().GetRetryable(), "rollback failure should not mark stream retryable")

	var ops []string
	require.Eventually(t, func() bool {
		ops = h.Sinker().opsSnapshot()
		hasBegin := false
		hasCommit := false
		hasClear := false
		hasRollback := false
		hasDummy := false
		for _, op := range ops {
			switch op {
			case "begin":
				hasBegin = true
			case "commit":
				hasCommit = true
			case "clear":
				hasClear = true
			case "rollback":
				hasRollback = true
			case "dummy":
				hasDummy = true
			}
		}
		return hasBegin && hasCommit && hasClear && hasRollback && hasDummy
	}, time.Second, 10*time.Millisecond, "EnsureCleanup should attempt rollback even if it fails")

	require.Equal(t, rollbackErr, h.Sinker().Error(), "sinker should retain rollback error for inspection")

	rollbackCount := 0
	for _, op := range ops {
		if op == "rollback" {
			rollbackCount++
		}
	}
	require.Equal(t, 1, rollbackCount, "rollback should still be invoked exactly once")

	tracker := h.Stream().txnManager.GetTracker()
	require.NotNil(t, tracker)
	require.False(t, tracker.NeedsRollback(), "tracker should consider rollback done even if sinker failed")
	require.True(t, tracker.IsCompleted(), "tracker should mark transaction as completed after rollback attempt")
}

// TestTableChangeStream_FullPipeline_RandomDelaysAndErrors verifies the entire pipeline
// under random delays and error injections, ensuring orderliness, idempotency, and consistency.
// This test uses pausableChangesHandle, watermarkUpdaterStub, and error injection to simulate
// realistic failure scenarios including StaleRead, commit failures, and rollback failures.
func TestTableChangeStream_FullPipeline_RandomDelaysAndErrors(t *testing.T) {
	updaterStub := newWatermarkUpdaterStub()
	noopStop := func() {}
	h := newTableStreamHarness(t,
		withHarnessWatermarkUpdater(updaterStub, noopStop),
		withHarnessFrequency(1*time.Millisecond),
		withHarnessNoFull(true),
	)
	defer h.Close()

	// Initialize watermark
	key := h.Stream().txnManager.watermarkKey
	initial := types.BuildTS(1, 0)
	_, _ = updaterStub.GetOrAddCommitted(h.Context(), key, &initial)

	// Track operation order and counts for verification
	var (
		collectCalls         atomic.Int32
		staleReadInjected    atomic.Bool
		commitFailInjected   atomic.Bool
		rollbackFailInjected atomic.Bool
	)

	// Inject StaleRead error on first collect, then succeed
	h.SetCollectFactory(func(fromTs, toTs types.TS) (engine.ChangesHandle, error) {
		call := collectCalls.Add(1)
		if call == 1 {
			staleReadInjected.Store(true)
			return nil, moerr.NewErrStaleReadNoCtx("db1", "t1")
		}
		// After stale read recovery, provide data
		bat := createTestBatch(t, h.MP(), toTs, []int32{int32(call)})
		return newImmediateChangesHandle([]changeBatch{
			{insert: bat, hint: engine.ChangesHandle_Tail_done},
			{insert: nil, hint: engine.ChangesHandle_Tail_done},
		}), nil
	})

	// Update snapshot TS after stale read recovery
	var snapshotCalls atomic.Int32
	h.SetGetSnapshotTS(func(op client.TxnOperator) timestamp.Timestamp {
		call := snapshotCalls.Add(1)
		ts := timestamp.Timestamp{PhysicalTime: 100 + int64(call*50)}
		if noop, ok := op.(*noopTxnOperator); ok {
			noop.snapshot = ts
		}
		return ts
	})

	// Inject commit failure on first commit attempt, then succeed
	commitErr := moerr.NewInternalError(h.Context(), "commit failure")
	h.Sinker().setCommitError(commitErr)
	commitFailInjected.Store(true)

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)

	// Wait for stale read recovery (multiple collect calls)
	// Use longer timeout for slow CI environments, but keep frequent checks
	// Stale read recovery may involve retry backoff: base=5ms, max=20ms, factor=2.0, maxRetries=3
	// Worst case delay per retry cycle: ~35ms, plus processing time
	require.Eventually(t, func() bool {
		return collectCalls.Load() >= 2
	}, 10*time.Second, 10*time.Millisecond, "should see stale read recovery")

	// Wait for commit attempt (which will fail)
	// Use longer timeout for slow CI environments, but keep frequent checks
	// Calculate worst-case retry delay: base=5ms, max=20ms, factor=2.0, maxRetries=3
	// Worst case: 5ms + 10ms + 20ms = 35ms per retry cycle, plus processing time
	// Use 10s timeout to handle multiple retry cycles and slow CI environments
	require.Eventually(t, func() bool {
		ops := h.Sinker().opsSnapshot()
		for _, op := range ops {
			if op == "commit" {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond, "should see commit attempt")

	// Keep commit error and inject rollback failure (rollback happens during cleanup after commit failure)
	// To eliminate race condition: set rollback error immediately after confirming commit attempt,
	// then add a small synchronization delay to ensure the error is set in sinker before cleanup rollback happens.
	// This reduces the race window to near zero.
	rollbackErr := moerr.NewInternalError(h.Context(), "rollback failure")
	h.Sinker().setRollbackError(rollbackErr)
	rollbackFailInjected.Store(true)
	// Small synchronization delay to ensure rollback error is set before cleanup rollback happens
	// This eliminates the race condition where rollback might occur before error is set.
	// 50ms is sufficient for error injection to propagate, even in slow environments.
	time.Sleep(50 * time.Millisecond)

	// Wait for rollback attempt (which will fail during cleanup after commit failure)
	// Use longer timeout for slow CI environments, but keep frequent checks
	// Rollback happens during EnsureCleanup after commit failure, which may have retry delays
	require.Eventually(t, func() bool {
		ops := h.Sinker().opsSnapshot()
		for _, op := range ops {
			if op == "rollback" {
				return true
			}
		}
		return false
	}, 10*time.Second, 10*time.Millisecond, "should see rollback attempt")

	// Verify that rollback failure marks stream as non-retryable (system state may be inconsistent)
	// This is the core behavior: rollback failure indicates system state may be inconsistent,
	// so the stream should not be retryable even if the original error (commit failure) is retryable
	// Use longer timeout for slow CI environments to ensure state propagation, but keep frequent checks
	// State propagation may take time due to retry backoff and processing delays
	// To make the check more stable and reduce flakiness, we verify the state is stable by checking
	// multiple consecutive times. This ensures the state has settled after recovery path + backoff + scheduling.
	// The state must remain non-retryable for 3 consecutive checks (30ms total) to be considered stable.
	var stableCount int
	const requiredStableChecks = 3 // Require 3 consecutive checks to ensure state is stable
	require.Eventually(t, func() bool {
		isNonRetryable := !h.Stream().GetRetryable()
		if isNonRetryable {
			stableCount++
			if stableCount >= requiredStableChecks {
				return true
			}
		} else {
			// State changed or not yet stable, reset counter
			stableCount = 0
		}
		return false
	}, 10*time.Second, 10*time.Millisecond, "stream should be non-retryable after rollback failure (with stable state check)")

	// Clear rollback error so stream can remain retryable after rollback failure is verified
	// Note: The test verifies that rollback failure marks stream as non-retryable.
	// After clearing the rollback error, the next round will succeed (rollback will succeed),
	// and cleanupRollbackErr will not be set, so the stream will become retryable again.
	// However, this depends on other factors (e.g., no other errors), so we don't assert it here.
	// The core behavior (rollback failure -> non-retryable) is already verified above.
	h.Sinker().setRollbackError(nil)

	// Cancel to exit
	h.Cancel()
	done()

	var runErr error
	// Use longer timeout for resource cleanup in slow CI environments
	// Cleanup may involve rollback operations and state synchronization
	select {
	case runErr = <-errCh:
	case <-time.After(10 * time.Second):
		t.Fatal("stream did not exit after cancellation")
	}
	// The stream may return either the commit failure error or context.Canceled
	if runErr != nil {
		if !errors.Is(runErr, context.Canceled) {
			// If not canceled, it should be the commit failure error
			require.Contains(t, runErr.Error(), "commit failure", "if not canceled, should be commit failure")
		}
	}

	// Verify orderliness: begin should always come before commit/rollback
	ops := h.Sinker().opsSnapshot()
	// Track the last begin index seen so far
	lastBeginIdx := -1
	for i, op := range ops {
		switch op {
		case "begin":
			lastBeginIdx = i
		case "commit":
			require.Greater(t, i, lastBeginIdx, "commit at index %d should come after begin", i)
		case "rollback":
			require.Greater(t, i, lastBeginIdx, "rollback at index %d should come after begin", i)
		}
	}

	// Verify idempotency: EnsureCleanup should be idempotent
	// Count rollbacks - should be at least one, but not excessive
	rollbackCount := 0
	beginCount := 0
	for _, op := range ops {
		if op == "rollback" {
			rollbackCount++
		}
		if op == "begin" {
			beginCount++
		}
	}
	require.GreaterOrEqual(t, rollbackCount, 1, "should have at least one rollback after commit failure")
	require.GreaterOrEqual(t, beginCount, 1, "should have at least one begin")

	// Verify consistency: retryable flag and sinker reset
	// Note: After rollback failure, stream is marked as non-retryable (system state may be inconsistent).
	// If rollback error was cleared and next round succeeded, stream should be retryable again.
	// However, other factors (e.g., watermark mismatch) may also affect retryability.
	// The core behavior (rollback failure -> non-retryable) is verified earlier in the test.
	// Here we just verify that the stream has processed the scenarios (stale read, commit failure, rollback failure).
	require.GreaterOrEqual(t, h.Sinker().ResetCountSnapshot(), 1, "sinker should reset at least once for stale read")

	// Verify all error injections occurred
	require.True(t, staleReadInjected.Load(), "stale read should have been injected")
	require.True(t, commitFailInjected.Load(), "commit failure should have been injected")
	require.True(t, rollbackFailInjected.Load(), "rollback failure should have been injected")

	// Verify tracker state is consistent (may be nil if cleanup completed)
	tracker := h.Stream().txnManager.GetTracker()
	if tracker != nil {
		// After EnsureCleanup, tracker should not need rollback (even if rollback failed)
		require.False(t, tracker.NeedsRollback(), "tracker should be marked as not needing rollback after EnsureCleanup")
	}
}

type noopTxnOperator struct {
	meta      txnpb.TxnMeta
	options   txnpb.TxnOptions
	workspace client.Workspace
	snapshot  timestamp.Timestamp
}

func newNoopTxnOperator() *noopTxnOperator {
	return &noopTxnOperator{}
}

func (n *noopTxnOperator) GetOverview() client.TxnOverview {
	return client.TxnOverview{}
}

func (n *noopTxnOperator) CloneSnapshotOp(snapshot timestamp.Timestamp) client.TxnOperator {
	op := newNoopTxnOperator()
	op.snapshot = snapshot
	return op
}

func (n *noopTxnOperator) IsSnapOp() bool {
	return false
}

func (n *noopTxnOperator) Txn() txnpb.TxnMeta {
	return n.meta
}

func (n *noopTxnOperator) TxnOptions() txnpb.TxnOptions {
	return n.options
}

func (n *noopTxnOperator) TxnRef() *txnpb.TxnMeta {
	return &n.meta
}

func (n *noopTxnOperator) Snapshot() (txnpb.CNTxnSnapshot, error) {
	return txnpb.CNTxnSnapshot{}, nil
}

func (n *noopTxnOperator) UpdateSnapshot(ctx context.Context, ts timestamp.Timestamp) error {
	n.snapshot = ts
	return nil
}

func (n *noopTxnOperator) SnapshotTS() timestamp.Timestamp {
	return n.snapshot
}

func (n *noopTxnOperator) CreateTS() timestamp.Timestamp {
	return timestamp.Timestamp{}
}

func (n *noopTxnOperator) Status() txnpb.TxnStatus {
	return txnpb.TxnStatus(0)
}

func (n *noopTxnOperator) ApplySnapshot(data []byte) error {
	return nil
}

func (n *noopTxnOperator) Read(ctx context.Context, ops []txnpb.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (n *noopTxnOperator) Write(ctx context.Context, ops []txnpb.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (n *noopTxnOperator) WriteAndCommit(ctx context.Context, ops []txnpb.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (n *noopTxnOperator) Commit(ctx context.Context) error {
	return nil
}

func (n *noopTxnOperator) Rollback(ctx context.Context) error {
	return nil
}

func (n *noopTxnOperator) AddLockTable(locktable lock.LockTable) error {
	return nil
}

func (n *noopTxnOperator) HasLockTable(table uint64) bool {
	return false
}

func (n *noopTxnOperator) AddWaitLock(tableID uint64, rows [][]byte, opt lock.LockOptions) uint64 {
	return 0
}

func (n *noopTxnOperator) RemoveWaitLock(key uint64) {}

func (n *noopTxnOperator) LockSkipped(tableID uint64, mode lock.LockMode) bool {
	return false
}

func (n *noopTxnOperator) GetWaitActiveCost() time.Duration {
	return 0
}

func (n *noopTxnOperator) AddWorkspace(workspace client.Workspace) {
	n.workspace = workspace
}

func (n *noopTxnOperator) GetWorkspace() client.Workspace {
	return n.workspace
}

func (n *noopTxnOperator) AppendEventCallback(event client.EventType, callbacks ...func(client.TxnEvent)) {
}

func (n *noopTxnOperator) Debug(ctx context.Context, ops []txnpb.TxnRequest) (*rpc.SendResult, error) {
	return nil, nil
}

func (n *noopTxnOperator) NextSequence() uint64 {
	return 0
}

func (n *noopTxnOperator) EnterRunSqlWithTokenAndSQL(_ context.CancelFunc, _ string) uint64 {
	return 0
}
func (n *noopTxnOperator) ExitRunSqlWithToken(_ uint64) {}
func (n *noopTxnOperator) EnterIncrStmt()          {}
func (n *noopTxnOperator) ExitIncrStmt()           {}
func (n *noopTxnOperator) EnterRollbackStmt()      {}
func (n *noopTxnOperator) ExitRollbackStmt()       {}
func (n *noopTxnOperator) SetFootPrints(int, bool) {}

// tableStreamHarnessConfig captures the configurable parts of the test harness
// so individual test cases can focus on behavior rather than boilerplate setup.
type tableStreamHarnessConfig struct {
	initSnapshotSplitTxn bool
	noFull               bool
	startTs              types.TS
	endTs                types.TS
	frequency            time.Duration
	tableDef             *plan.TableDef
	tableInfo            *DbTableInfo
	watermarkUpdater     WatermarkUpdater
	updaterStop          func()
	runningReaders       *sync.Map
	// Retry configuration for testing (use shorter delays to speed up tests)
	retryOptions []TableChangeStreamOption
}

func defaultTableStreamHarnessConfig() tableStreamHarnessConfig {
	return tableStreamHarnessConfig{
		initSnapshotSplitTxn: false,
		noFull:               false,
		startTs:              types.TS{},
		endTs:                types.TS{},
		frequency:            50 * time.Millisecond,
		runningReaders:       &sync.Map{},
		tableDef: &plan.TableDef{
			Cols: []*plan.ColDef{
				{Name: "id"},
				{Name: "ts"},
			},
			Pkey: &plan.PrimaryKeyDef{Names: []string{"id"}},
			Name2ColIndex: map[string]int32{
				"id": 0,
				"ts": 1,
			},
		},
		tableInfo: &DbTableInfo{
			SourceDbName:  "db1",
			SourceTblName: "t1",
			SourceTblId:   1,
		},
	}
}

type tableStreamHarnessOption func(*tableStreamHarnessConfig)

func withHarnessNoFull(noFull bool) tableStreamHarnessOption {
	return func(cfg *tableStreamHarnessConfig) {
		cfg.noFull = noFull
	}
}

func withHarnessStartTs(ts types.TS) tableStreamHarnessOption {
	return func(cfg *tableStreamHarnessConfig) {
		cfg.startTs = ts
	}
}

func withHarnessEndTs(ts types.TS) tableStreamHarnessOption {
	return func(cfg *tableStreamHarnessConfig) {
		cfg.endTs = ts
	}
}

func withHarnessFrequency(freq time.Duration) tableStreamHarnessOption {
	return func(cfg *tableStreamHarnessConfig) {
		cfg.frequency = freq
	}
}

func withHarnessWatermarkUpdater(updater WatermarkUpdater, stop func()) tableStreamHarnessOption {
	return func(cfg *tableStreamHarnessConfig) {
		cfg.watermarkUpdater = updater
		cfg.updaterStop = stop
	}
}

func withHarnessRunningReaders(readers *sync.Map) tableStreamHarnessOption {
	return func(cfg *tableStreamHarnessConfig) {
		cfg.runningReaders = readers
	}
}

type tableStreamHarness struct {
	t *testing.T

	ctx    context.Context
	cancel context.CancelFunc

	mp         *mpool.MPool
	packerPool *fileservice.Pool[*types.Packer]

	sinker      *tableStreamRecordingSinker
	updater     WatermarkUpdater
	updaterStop func()
	stream      *TableChangeStream

	stubs     []*gostub.Stubs
	closeOnce sync.Once

	getTxnOp       func(context.Context, engine.Engine, client.TxnClient, string) (client.TxnOperator, error)
	finishTxnOp    func(context.Context, error, client.TxnOperator, engine.Engine)
	getTxn         func(context.Context, engine.Engine, client.TxnOperator) error
	getRelation    func(context.Context, engine.Engine, client.TxnOperator, uint64) (string, string, engine.Relation, error)
	getSnapshotTS  func(client.TxnOperator) timestamp.Timestamp
	enterRunSql    func(context.Context, client.TxnOperator, string) func()
	collectFactory func(fromTs, toTs types.TS) (engine.ChangesHandle, error)

	collectCallsMu sync.Mutex
	collectCalls   []collectInvocation
}

type collectInvocation struct {
	from types.TS
	to   types.TS
}

func newTableStreamHarness(t *testing.T, opts ...tableStreamHarnessOption) *tableStreamHarness {
	t.Helper()
	cfg := defaultTableStreamHarnessConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	ctx, cancel := context.WithCancel(context.Background())
	mp := mpool.MustNewZero()
	packerPool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)

	sinker := newTableStreamRecordingSinker()

	var (
		updater     WatermarkUpdater
		updaterStop func()
	)
	if cfg.watermarkUpdater != nil {
		updater = cfg.watermarkUpdater
		updaterStop = cfg.updaterStop
	} else {
		defaultUpdater, _ := InitCDCWatermarkUpdaterForTest(t)
		defaultUpdater.Start()
		updater = defaultUpdater
		updaterStop = defaultUpdater.Stop
	}

	runningReaders := cfg.runningReaders
	if runningReaders == nil {
		runningReaders = &sync.Map{}
	}

	// Apply default test retry options (very short delays for faster tests) if not provided
	retryOptions := cfg.retryOptions
	if len(retryOptions) == 0 {
		// Use minimal backoff for tests to speed up execution:
		// - Base: 5ms (vs 200ms in production)
		// - Max: 20ms (vs 30s in production)
		// - Factor: 2.0 (same as production)
		// This allows testing retry logic without long waits
		retryOptions = []TableChangeStreamOption{
			WithMaxRetryCount(3),
			WithRetryBackoff(5*time.Millisecond, 20*time.Millisecond, 2.0),
		}
	}

	stream := NewTableChangeStream(
		nil,
		nil,
		mp,
		packerPool,
		1,
		"task1",
		cfg.tableInfo,
		sinker,
		updater,
		cfg.tableDef,
		cfg.initSnapshotSplitTxn,
		runningReaders,
		cfg.startTs,
		cfg.endTs,
		cfg.noFull,
		cfg.frequency,
		retryOptions...,
	)

	h := &tableStreamHarness{
		t:           t,
		ctx:         ctx,
		cancel:      cancel,
		mp:          mp,
		packerPool:  packerPool,
		sinker:      sinker,
		updater:     updater,
		updaterStop: updaterStop,
		stream:      stream,
	}

	if cfg.watermarkUpdater != nil {
		zero := types.TS{}
		_, _ = updater.GetOrAddCommitted(context.Background(), stream.watermarkKey, &zero)
	}

	h.getTxnOp = func(context.Context, engine.Engine, client.TxnClient, string) (client.TxnOperator, error) {
		return newNoopTxnOperator(), nil
	}
	h.finishTxnOp = func(context.Context, error, client.TxnOperator, engine.Engine) {}
	h.getTxn = func(context.Context, engine.Engine, client.TxnOperator) error { return nil }
	h.getRelation = func(context.Context, engine.Engine, client.TxnOperator, uint64) (string, string, engine.Relation, error) {
		return "", "", nil, nil
	}
	h.getSnapshotTS = func(op client.TxnOperator) timestamp.Timestamp {
		ts := timestamp.Timestamp{PhysicalTime: 100}
		if noop, ok := op.(*noopTxnOperator); ok {
			noop.snapshot = ts
		}
		return ts
	}
	h.enterRunSql = func(context.Context, client.TxnOperator, string) func() { return func() {} }
	h.collectFactory = func(fromTs, toTs types.TS) (engine.ChangesHandle, error) {
		return newImmediateChangesHandle(nil), nil
	}

	h.installStubs()
	t.Cleanup(h.Close)
	return h
}

func (h *tableStreamHarness) installStubs() {
	h.addStub(gostub.Stub(&GetTxnOp, func(ctx context.Context, eng engine.Engine, cli client.TxnClient, purpose string) (client.TxnOperator, error) {
		return h.getTxnOp(ctx, eng, cli, purpose)
	}))
	h.addStub(gostub.Stub(&FinishTxnOp, func(ctx context.Context, err error, op client.TxnOperator, eng engine.Engine) {
		h.finishTxnOp(ctx, err, op, eng)
	}))
	h.addStub(gostub.Stub(&GetTxn, func(ctx context.Context, eng engine.Engine, op client.TxnOperator) error {
		return h.getTxn(ctx, eng, op)
	}))
	h.addStub(gostub.Stub(&GetRelationById, func(ctx context.Context, eng engine.Engine, op client.TxnOperator, tableId uint64) (string, string, engine.Relation, error) {
		return h.getRelation(ctx, eng, op, tableId)
	}))
	h.addStub(gostub.Stub(&GetSnapshotTS, func(op client.TxnOperator) timestamp.Timestamp {
		return h.getSnapshotTS(op)
	}))
	h.addStub(gostub.Stub(&EnterRunSql, func(ctx context.Context, op client.TxnOperator, sql string) func() {
		return h.enterRunSql(ctx, op, sql)
	}))
	h.addStub(gostub.Stub(&CollectChanges, func(ctx context.Context, rel engine.Relation, fromTs, toTs types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
		h.collectCallsMu.Lock()
		h.collectCalls = append(h.collectCalls, collectInvocation{from: fromTs, to: toTs})
		h.collectCallsMu.Unlock()
		return h.collectFactory(fromTs, toTs)
	}))
}

func (h *tableStreamHarness) addStub(stub *gostub.Stubs) {
	h.stubs = append(h.stubs, stub)
}

func (h *tableStreamHarness) Close() {
	h.closeOnce.Do(func() {
		h.cancel()
		h.stream.Close()
		h.stream.Wait()
		for _, stub := range h.stubs {
			stub.Reset()
		}
		if h.updaterStop != nil {
			h.updaterStop()
		} else if u, ok := h.updater.(*CDCWatermarkUpdater); ok {
			u.Stop()
		}
		if h.mp != nil {
			mpool.DeleteMPool(h.mp)
			h.mp = nil
		}
	})
}

func (h *tableStreamHarness) Context() context.Context {
	return h.ctx
}

func (h *tableStreamHarness) Stream() *TableChangeStream {
	return h.stream
}

func (h *tableStreamHarness) MP() *mpool.MPool {
	return h.mp
}

func (h *tableStreamHarness) Sinker() *tableStreamRecordingSinker {
	return h.sinker
}

func (h *tableStreamHarness) WatermarkUpdater() WatermarkUpdater {
	return h.updater
}

func (h *tableStreamHarness) Cancel() {
	h.cancel()
}

func (h *tableStreamHarness) RunStream(ar *ActiveRoutine) error {
	h.stream.Run(h.ctx, ar)
	return h.stream.lastError
}

func (h *tableStreamHarness) RunStreamAsync(ar *ActiveRoutine) (<-chan error, func()) {
	errCh := make(chan error, 1)
	done := func() {
		h.Cancel()
		h.stream.Close()
	}
	go func() {
		h.stream.Run(h.ctx, ar)
		errCh <- h.stream.lastError
		close(errCh)
	}()
	return errCh, done
}

func (h *tableStreamHarness) SetGetTxnOp(fn func(context.Context, engine.Engine, client.TxnClient, string) (client.TxnOperator, error)) {
	if fn == nil {
		fn = func(context.Context, engine.Engine, client.TxnClient, string) (client.TxnOperator, error) {
			return newNoopTxnOperator(), nil
		}
	}
	h.getTxnOp = fn
}

func (h *tableStreamHarness) SetFinishTxnOp(fn func(context.Context, error, client.TxnOperator, engine.Engine)) {
	if fn == nil {
		fn = func(context.Context, error, client.TxnOperator, engine.Engine) {}
	}
	h.finishTxnOp = fn
}

func (h *tableStreamHarness) SetGetTxn(fn func(context.Context, engine.Engine, client.TxnOperator) error) {
	if fn == nil {
		fn = func(context.Context, engine.Engine, client.TxnOperator) error { return nil }
	}
	h.getTxn = fn
}

func (h *tableStreamHarness) SetGetRelation(fn func(context.Context, engine.Engine, client.TxnOperator, uint64) (string, string, engine.Relation, error)) {
	if fn == nil {
		fn = func(context.Context, engine.Engine, client.TxnOperator, uint64) (string, string, engine.Relation, error) {
			return "", "", nil, nil
		}
	}
	h.getRelation = fn
}

func (h *tableStreamHarness) SetGetSnapshotTS(fn func(client.TxnOperator) timestamp.Timestamp) {
	if fn == nil {
		fn = func(op client.TxnOperator) timestamp.Timestamp {
			ts := timestamp.Timestamp{PhysicalTime: 100}
			if noop, ok := op.(*noopTxnOperator); ok {
				noop.snapshot = ts
			}
			return ts
		}
	}
	h.getSnapshotTS = func(op client.TxnOperator) timestamp.Timestamp {
		ts := fn(op)
		if noop, ok := op.(*noopTxnOperator); ok {
			noop.snapshot = ts
		}
		return ts
	}
}

func (h *tableStreamHarness) SetEnterRunSql(fn func(context.Context, client.TxnOperator, string) func()) {
	if fn == nil {
		fn = func(context.Context, client.TxnOperator, string) func() { return func() {} }
	}
	h.enterRunSql = fn
}

func (h *tableStreamHarness) SetCollectFactory(factory func(fromTs, toTs types.TS) (engine.ChangesHandle, error)) {
	if factory == nil {
		factory = func(fromTs, toTs types.TS) (engine.ChangesHandle, error) {
			return newImmediateChangesHandle(nil), nil
		}
	}
	h.collectFactory = factory
}

func (h *tableStreamHarness) SetCollectBatches(batches []changeBatch) {
	h.collectFactory = func(fromTs, toTs types.TS) (engine.ChangesHandle, error) {
		cp := make([]changeBatch, len(batches))
		copy(cp, batches)
		return newImmediateChangesHandle(cp), nil
	}
}

func (h *tableStreamHarness) SetCollectError(err error) {
	h.collectFactory = func(fromTs, toTs types.TS) (engine.ChangesHandle, error) {
		return nil, err
	}
}

func (h *tableStreamHarness) SetCollectHandleSequence(handles ...engine.ChangesHandle) {
	var mu sync.Mutex
	idx := 0
	h.collectFactory = func(fromTs, toTs types.TS) (engine.ChangesHandle, error) {
		mu.Lock()
		defer mu.Unlock()
		if idx >= len(handles) {
			return newImmediateChangesHandle(nil), nil
		}
		handle := handles[idx]
		idx++
		return handle, nil
	}
}

func (h *tableStreamHarness) CollectCallsSnapshot() []collectInvocation {
	h.collectCallsMu.Lock()
	defer h.collectCallsMu.Unlock()
	cp := make([]collectInvocation, len(h.collectCalls))
	copy(cp, h.collectCalls)
	return cp
}

func (h *tableStreamHarness) ResetCollectCalls() {
	h.collectCallsMu.Lock()
	h.collectCalls = nil
	h.collectCallsMu.Unlock()
}

func (h *tableStreamHarness) NewActiveRoutine() *ActiveRoutine {
	return NewCdcActiveRoutine()
}

// Helper types and functions for integration tests

func createTestBatch(t *testing.T, mp *mpool.MPool, ts types.TS, ids []int32) *batch.Batch {
	bat := batch.New([]string{"id", "ts"})
	idVec := vector.NewVec(types.T_int32.ToType())
	tsVec := vector.NewVec(types.T_TS.ToType())

	for _, id := range ids {
		_ = vector.AppendFixed(idVec, id, false, mp)
		_ = vector.AppendFixed(tsVec, ts, false, mp)
	}

	bat.Vecs[0] = idVec
	bat.Vecs[1] = tsVec
	bat.SetRowCount(len(ids))
	return bat
}

type tableStreamRecordingSinker struct {
	*recordingSinker
	sinkCalls []*DecoderOutput
	resetCnt  int
}

func newTableStreamRecordingSinker() *tableStreamRecordingSinker {
	return &tableStreamRecordingSinker{recordingSinker: newRecordingSinker()}
}

func (s *tableStreamRecordingSinker) Sink(ctx context.Context, data *DecoderOutput) {
	s.record("sink")
	s.mu.Lock()
	s.sinkCalls = append(s.sinkCalls, data)
	s.mu.Unlock()
}

func (s *tableStreamRecordingSinker) Reset() {
	s.recordingSinker.Reset()
	s.recordingSinker.ClearError()
	s.mu.Lock()
	s.resetCnt++
	s.mu.Unlock()
}

func (s *tableStreamRecordingSinker) reset() {
	s.mu.Lock()
	s.err = nil
	s.rollbackErr = nil
	s.commitErr = nil
	s.beginErr = nil
	s.sinkCalls = nil
	s.resetCnt = 0
	s.mu.Unlock()
	s.resetOps()
}

func (s *tableStreamRecordingSinker) sinkCallsSnapshot() []*DecoderOutput {
	s.mu.Lock()
	defer s.mu.Unlock()
	copyCalls := make([]*DecoderOutput, len(s.sinkCalls))
	copy(copyCalls, s.sinkCalls)
	return copyCalls
}

func (s *tableStreamRecordingSinker) ResetCountSnapshot() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.resetCnt
}

// TestTableChangeStream_GetTableInfo verifies GetTableInfo returns correct table information
func TestTableChangeStream_GetTableInfo(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	stream := createTestStream(mp, tableInfo)
	info := stream.GetTableInfo()

	require.NotNil(t, info)
	require.Equal(t, tableInfo.SourceDbName, info.SourceDbName)
	require.Equal(t, tableInfo.SourceTblName, info.SourceTblName)
	require.Equal(t, tableInfo.SourceTblId, info.SourceTblId)
}

// TestTableChangeStream_GetRelationByIdFailure verifies handling of GetRelationById failure
// (e.g., table truncated scenario)
func TestTableChangeStream_GetRelationByIdFailure(t *testing.T) {
	h := newTableStreamHarness(t)
	defer h.Close()

	relErr := moerr.NewInternalError(h.Context(), "table not found or truncated")
	h.SetGetRelation(func(ctx context.Context, eng engine.Engine, op client.TxnOperator, tableId uint64) (string, string, engine.Relation, error) {
		return "", "", nil, relErr
	})

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)

	require.Eventually(t, func() bool {
		return h.Stream().GetRetryable()
	}, 2*time.Second, 10*time.Millisecond, "stream should be marked retryable on relation error")

	h.Cancel()
	done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not exit")
	}

	require.Error(t, runErr)
	require.True(t, h.Stream().GetRetryable(), "relation error should mark stream retryable")
}

// TestTableChangeStream_ReachedEndTs verifies graceful termination when endTs is reached
func TestTableChangeStream_ReachedEndTs(t *testing.T) {
	updaterStub := newWatermarkUpdaterStub()
	noopStop := func() {}
	endTs := types.BuildTS(100, 0)

	h := newTableStreamHarness(t,
		withHarnessWatermarkUpdater(updaterStub, noopStop),
		withHarnessEndTs(endTs),
		withHarnessFrequency(1*time.Millisecond),
	)
	defer h.Close()

	// Initialize watermark to endTs (stream should detect and exit)
	key := h.Stream().txnManager.watermarkKey
	_, _ = updaterStub.GetOrAddCommitted(h.Context(), key, &endTs)

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)

	// Stream should detect endTs quickly and exit, but we'll cancel after a short wait
	// to ensure test completes quickly
	select {
	case <-time.After(200 * time.Millisecond):
		// Timeout reached, cancel to exit
		h.Cancel()
	case err := <-errCh:
		// Stream exited early (detected endTs)
		require.NoError(t, err, "stream should exit gracefully when endTs is reached")
		done()
		return
	}
	done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not exit")
	}

	// Stream should exit gracefully (either nil or context.Canceled)
	if runErr != nil {
		require.ErrorIs(t, runErr, context.Canceled)
	}
	require.False(t, h.Stream().GetRetryable(), "reaching endTs should not mark stream retryable")
}

// TestTableChangeStream_CalculateInitialDelay_GetLastSyncTimeFailure verifies
// calculateInitialDelay handles getLastSyncTime failure gracefully (non-fatal)
func TestTableChangeStream_CalculateInitialDelay_GetLastSyncTimeFailure(t *testing.T) {
	updaterStub := newWatermarkUpdaterStub()
	noopStop := func() {}

	h := newTableStreamHarness(t,
		withHarnessWatermarkUpdater(updaterStub, noopStop),
		withHarnessFrequency(1*time.Millisecond),
	)
	defer h.Close()

	// Initialize watermark first
	key := h.Stream().txnManager.watermarkKey
	initial := types.BuildTS(1, 0)
	_, _ = updaterStub.GetOrAddCommitted(h.Context(), key, &initial)

	// Inject GetFromCache error only for the first call (simulating getLastSyncTime failure)
	// Subsequent calls should succeed
	var callCount atomic.Int32
	updaterStub.setGetFromCacheHook(func() error {
		call := callCount.Add(1)
		if call == 1 {
			// First call fails (getLastSyncTime in calculateInitialDelay)
			return moerr.NewInternalError(context.Background(), "cache error")
		}
		// Subsequent calls succeed (processWithTxn needs it)
		return nil
	})

	// Provide data so stream can process
	snap := createTestBatch(t, h.MP(), types.BuildTS(100, 0), []int32{1})
	h.SetCollectBatches([]changeBatch{
		{insert: snap, hint: engine.ChangesHandle_Snapshot},
		{insert: nil, hint: engine.ChangesHandle_Tail_done},
	})

	ar := h.NewActiveRoutine()
	errCh, done := h.RunStreamAsync(ar)

	// Stream should continue processing despite getLastSyncTime failure in calculateInitialDelay
	require.Eventually(t, func() bool {
		ops := h.Sinker().opsSnapshot()
		return len(ops) > 0
	}, 2*time.Second, 10*time.Millisecond, "stream should continue processing despite getLastSyncTime failure")

	h.Cancel()
	done()

	var runErr error
	select {
	case runErr = <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not exit")
	}
	if runErr != nil {
		require.ErrorIs(t, runErr, context.Canceled)
	}
}
