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
	"sync/atomic"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTableChangeStream(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

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
			{Name: "name"},
			{Name: "ts"},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"id"},
		},
		Name2ColIndex: map[string]int32{
			"id":   0,
			"name": 1,
			"ts":   2,
		},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "table1",
		SourceTblId:   1,
	}

	runningReaders := &sync.Map{}
	startTs := types.TS{}
	endTs := (&startTs).Next()

	stream := NewTableChangeStream(
		nil, // cnTxnClient
		nil, // cnEngine
		mp,
		packerPool,
		1, // accountId
		"task1",
		tableInfo,
		sinker,
		updater,
		tableDef,
		false, // initSnapshotSplitTxn
		runningReaders,
		startTs,
		endTs,
		false, // noFull
		200*time.Millisecond,
	)

	assert.NotNil(t, stream)
	assert.Equal(t, mp, stream.mp)
	assert.Equal(t, sinker, stream.sinker)
	assert.Equal(t, updater, stream.watermarkUpdater)
	assert.Equal(t, uint64(1), stream.accountId)
	assert.Equal(t, "task1", stream.taskId)
	assert.Equal(t, tableInfo, stream.tableInfo)
	assert.Equal(t, tableDef, stream.tableDef)
	assert.NotNil(t, stream.txnManager)
	assert.NotNil(t, stream.dataProcessor)
	assert.Equal(t, 200*time.Millisecond, stream.frequency)
	assert.Equal(t, 2, stream.insTsColIdx)           // len(Cols)-1
	assert.Equal(t, 0, stream.insCompositedPkColIdx) // single PK
	assert.Equal(t, 1, stream.delTsColIdx)
	assert.Equal(t, 0, stream.delCompositedPkColIdx)
}

func TestNewTableChangeStream_CompositePK(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

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
			{Name: "name"},
			{Name: "cpk"}, // Composite PK column
			{Name: "ts"},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"id", "name"}, // Composite PK
		},
		Name2ColIndex: map[string]int32{
			"id":   0,
			"name": 1,
			"cpk":  2,
			"ts":   3,
		},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "table1",
		SourceTblId:   1,
	}

	stream := NewTableChangeStream(
		nil, nil, mp, packerPool,
		1, "task1", tableInfo, sinker, updater, tableDef,
		false, &sync.Map{}, types.TS{}, types.TS{}, false, 0,
	)

	assert.NotNil(t, stream)
	assert.Equal(t, 3, stream.insTsColIdx)           // len(Cols)-1
	assert.Equal(t, 2, stream.insCompositedPkColIdx) // Composite PK col
}

func TestTableChangeStream_ForceNextInterval(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	stream := createTestStream(mp, &DbTableInfo{})

	assert.False(t, stream.force)

	stream.forceNextInterval(100 * time.Millisecond)

	assert.True(t, stream.force)
}

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
	mu             sync.Mutex
	watermarks     map[string]types.TS
	errMsgs        []string
	updateErrFn    func(call int) error
	removeErr      error
	updateCalls    atomic.Int32
	updateNotifies chan struct{}
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

func (m *watermarkUpdaterStub) withNotify(ch chan struct{}) *watermarkUpdaterStub {
	m.updateNotifies = ch
	return m
}

func (m *watermarkUpdaterStub) RemoveCachedWM(ctx context.Context, key *WatermarkKey) error {
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
	m.mu.Lock()
	defer m.mu.Unlock()
	ts, ok := m.watermarks[m.keyString(key)]
	if !ok {
		return types.TS{}, moerr.NewInternalError(ctx, "watermark not found")
	}
	return ts, nil
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

func (m *watermarkUpdaterStub) loadWatermark(key *WatermarkKey) (types.TS, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts, ok := m.watermarks[m.keyString(key)]
	return ts, ok
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
	assert.True(t, stream.retryable, "stalled snapshot should mark stream retryable")
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

func TestTableChangeStream_HandleSnapshotNoProgress_Defaults(t *testing.T) {
	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	tableInfo := &DbTableInfo{
		SourceDbName:  "db_default",
		SourceTblName: "t_default",
	}

	stream := createTestStream(mp, tableInfo)

	assert.Equal(t, defaultWatermarkStallThreshold, stream.watermarkStallThreshold)
	assert.Equal(t, defaultNoProgressWarningInterval, stream.noProgressWarningInterval)

	fromTs := types.BuildTS(400, 0)
	err := stream.handleSnapshotNoProgress(context.Background(), fromTs, fromTs)
	assert.NoError(t, err)

	stream.onWatermarkAdvanced()
}

// ============================================================================
// Integration Tests with gostub
// Note: Uses testChangesHandle from reader_test.go
// ============================================================================

// Test Run() integration with mocked dependencies
func TestTableChangeStream_Run_Integration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mp := mpool.MustNewZero()
	pool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)

	var packer *types.Packer
	put := pool.Get(&packer)
	defer put.Put()

	// Setup stubs
	stub1 := gostub.Stub(&GetTxnOp,
		func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			return nil, nil
		})
	defer stub1.Reset()

	stub2 := gostub.Stub(&FinishTxnOp,
		func(ctx context.Context, inputErr error, txnOp client.TxnOperator, cnEngine engine.Engine) {})
	defer stub2.Reset()

	stub3 := gostub.Stub(&GetTxn,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator) error {
			return nil
		})
	defer stub3.Reset()

	stub4 := gostub.Stub(&GetRelationById,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator, tableId uint64) (dbName string, tblName string, rel engine.Relation, err error) {
			return "", "", nil, nil
		})
	defer stub4.Reset()

	stub5 := gostub.Stub(&GetSnapshotTS,
		func(txnOp client.TxnOperator) timestamp.Timestamp {
			return timestamp.Timestamp{
				PhysicalTime: 100,
				LogicalTime:  0,
			}
		})
	defer stub5.Reset()

	stub6 := gostub.Stub(&CollectChanges,
		func(ctx context.Context, rel engine.Relation, fromTs, toTs types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
			return newTestChangesHandle("test", "t1", 20, 23, types.TS{}, mp, packer), nil
		})
	defer stub6.Reset()

	stub7 := gostub.Stub(&EnterRunSql, func(client.TxnOperator) {})
	defer stub7.Reset()

	stub8 := gostub.Stub(&ExitRunSql, func(client.TxnOperator) {})
	defer stub8.Reset()

	// Create watermark updater
	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	// Create table definition
	// Column order MUST match batch layout: user cols | cpk | commit-ts
	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "a"},   // User column
			{Name: "b"},   // User column
			{Name: "cpk"}, // Composite PK column
			{Name: "ts"},  // Commit timestamp (MUST be last)
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"a", "b"},
		},
		Name2ColIndex: map[string]int32{
			"a":   0,
			"b":   1,
			"cpk": 2,
			"ts":  3,
		},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	sinker := NewConsoleSinker(nil, nil)
	runningReaders := &sync.Map{}

	stream := NewTableChangeStream(
		nil, // cnTxnClient
		nil, // cnEngine
		mp,
		pool,
		1, // accountId
		"task1",
		tableInfo,
		sinker,
		updater,
		tableDef,
		false, // initSnapshotSplitTxn
		runningReaders,
		types.TS{},
		types.TS{},
		false, // noFull
		300*time.Millisecond,
	)

	// Run in background
	go stream.Run(ctx, NewCdcActiveRoutine())

	// Ensure the stream goroutine is running, then cancel immediately
	stream.start.Wait()
	cancel()

	// Wait for completion
	stream.Wait()

	// Verify runningReaders is cleaned up
	count := 0
	runningReaders.Range(func(_, _ interface{}) bool {
		count++
		return true
	})
	assert.Equal(t, 0, count, "runningReaders should be empty after Run exits")
}

// Test Run() with duplicate reader (should exit immediately)
func TestTableChangeStream_Run_DuplicateReader(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
	}

	runningReaders := &sync.Map{}

	stream1 := createTestStream(mp, tableInfo)
	stream1.runningReaders = runningReaders

	stream2 := createTestStream(mp, tableInfo)
	stream2.runningReaders = runningReaders

	// Start first stream
	key := GenDbTblKey(tableInfo.SourceDbName, tableInfo.SourceTblName)
	runningReaders.Store(key, stream1)

	// Second stream should exit immediately
	ar := NewCdcActiveRoutine()
	stream2.Run(ctx, ar)

	// Verify first stream is still in runningReaders
	_, exists := runningReaders.Load(key)
	assert.True(t, exists)
}

// Regression test: Pause must not hang when collector blocks on context cancellation.
func TestTableChangeStream_PauseDrainsBlockedCollector(t *testing.T) {
	ctx := context.Background()
	mp := mpool.MustNewZero()
	pool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "a"},
			{Name: "ts"},
		},
		Pkey: &plan.PrimaryKeyDef{Names: []string{"a"}},
		Name2ColIndex: map[string]int32{
			"a":  0,
			"ts": 1,
		},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	// Prepare stubs for transactional helpers
	stubs := []*gostub.Stubs{
		gostub.Stub(&GetTxnOp, func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			return nil, nil
		}),
		gostub.Stub(&FinishTxnOp, func(context.Context, error, client.TxnOperator, engine.Engine) {}),
		gostub.Stub(&GetTxn, func(context.Context, engine.Engine, client.TxnOperator) error { return nil }),
		gostub.Stub(&GetRelationById, func(context.Context, engine.Engine, client.TxnOperator, uint64) (string, string, engine.Relation, error) {
			return "", "", nil, nil
		}),
		gostub.Stub(&GetSnapshotTS, func(client.TxnOperator) timestamp.Timestamp {
			return timestamp.Timestamp{PhysicalTime: 1}
		}),
		gostub.Stub(&EnterRunSql, func(client.TxnOperator) {}),
		gostub.Stub(&ExitRunSql, func(client.TxnOperator) {}),
	}
	defer func() {
		for _, stub := range stubs {
			stub.Reset()
		}
	}()

	blockingHandleReady := make(chan struct{})
	collectStub := gostub.Stub(&CollectChanges, func(context.Context, engine.Relation, types.TS, types.TS, *mpool.MPool) (engine.ChangesHandle, error) {
		return &blockingChangesHandle{ready: blockingHandleReady}, nil
	})
	defer collectStub.Reset()

	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	stream := NewTableChangeStream(
		nil,
		nil,
		mp,
		pool,
		1,
		"task1",
		tableInfo,
		NewConsoleSinker(nil, nil),
		updater,
		tableDef,
		false,
		&sync.Map{},
		types.TS{},
		types.TS{},
		false,
		200*time.Millisecond,
		func(opts *tableChangeStreamOptions) {
			opts.watermarkStallThreshold = 5 * time.Millisecond
		},
	)

	ar := NewCdcActiveRoutine()
	go stream.Run(ctx, ar)

	select {
	case <-blockingHandleReady:
	case <-time.After(2 * time.Second):
		t.Fatalf("collector did not block as expected")
	}

	done := make(chan struct{})
	go func() {
		stream.Wait()
		close(done)
	}()

	ar.ClosePause()
	stream.Close()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("stream wait did not finish after pause")
	}
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
	ctx, cancel := context.WithTimeoutCause(context.Background(), 2*time.Second, moerr.CauseFinishTxnOp)

	mp := mpool.MustNewZero()
	var wg sync.WaitGroup
	pool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)

	var packer *types.Packer
	put := pool.Get(&packer)

	// Track read attempts
	var readCount atomic.Int32
	readDone := make(chan struct{}, 10)

	// Setup stubs
	stub1 := gostub.Stub(&GetTxnOp,
		func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			return nil, nil
		})

	stub2 := gostub.Stub(&FinishTxnOp,
		func(ctx context.Context, inputErr error, txnOp client.TxnOperator, cnEngine engine.Engine) {})

	stub3 := gostub.Stub(&GetTxn,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator) error {
			return nil
		})

	stub4 := gostub.Stub(&GetRelationById,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator, tableId uint64) (dbName string, tblName string, rel engine.Relation, err error) {
			count := readCount.Load()
			if count == 0 {
				// First attempt: StaleRead error
				readDone <- struct{}{}
				readCount.Add(1)
				return "", "", nil, moerr.NewErrStaleReadNoCtx("", "test stale read")
			}
			// Second attempt: Success
			readDone <- struct{}{}
			readCount.Add(1)
			return "", "", nil, nil
		})

	stub5 := gostub.Stub(&GetSnapshotTS,
		func(txnOp client.TxnOperator) timestamp.Timestamp {
			return timestamp.Timestamp{
				PhysicalTime: 100,
				LogicalTime:  0,
			}
		})

	stub6 := gostub.Stub(&CollectChanges,
		func(ctx context.Context, rel engine.Relation, fromTs, toTs types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
			return newTestChangesHandle("test", "t1", 20, 23, types.TS{}, mp, packer), nil
		})

	stub7 := gostub.Stub(&EnterRunSql, func(client.TxnOperator) {})

	stub8 := gostub.Stub(&ExitRunSql, func(client.TxnOperator) {})
	stubs := []*gostub.Stubs{stub1, stub2, stub3, stub4, stub5, stub6, stub7, stub8}
	defer func() {
		cancel()
		wg.Wait()
		put.Put()
		for _, stub := range stubs {
			stub.Reset()
		}
	}()

	// Create watermark updater
	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "ts"},
			{Name: "a"},
			{Name: "b"},
			{Name: "cpk"},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"a", "b"},
		},
		Name2ColIndex: map[string]int32{
			"ts":  0,
			"a":   1,
			"b":   2,
			"cpk": 3,
		},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	sinker := NewConsoleSinker(nil, nil)

	stream := NewTableChangeStream(
		nil, nil, mp, pool,
		1, "task1", tableInfo,
		sinker, updater, tableDef,
		false, &sync.Map{},
		types.TS{}, types.TS{}, // No time range limit
		true, // noFull = true (allows StaleRead retry)
		50*time.Millisecond,
	)

	// Run in background
	wg.Add(1)
	go func() {
		defer wg.Done()
		stream.Run(ctx, NewCdcActiveRoutine())
	}()

	// Wait for first attempt (StaleRead)
	select {
	case <-readDone:
		t.Log("First read attempt (should get StaleRead)")
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for first read")
	}

	// Wait for retry attempt (should succeed)
	select {
	case <-readDone:
		t.Log("Second read attempt (retry after StaleRead)")
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for retry")
	}

	// Verify retry happened
	assert.GreaterOrEqual(t, readCount.Load(), int32(2), "Should have at least 2 read attempts (initial + retry)")
	cancel()
}

// Test StaleRead with startTs set (should fail, not retry)
func TestTableChangeStream_StaleRead_NoRetryWithStartTs(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 2*time.Second, moerr.CauseFinishTxnOp)
	defer cancel()

	mp := mpool.MustNewZero()
	pool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)

	// Track call count to avoid infinite loop
	var callCount atomic.Int32

	// Setup stubs
	stub1 := gostub.Stub(&GetTxnOp,
		func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			return nil, nil
		})
	defer stub1.Reset()

	stub2 := gostub.Stub(&FinishTxnOp,
		func(ctx context.Context, inputErr error, txnOp client.TxnOperator, cnEngine engine.Engine) {})
	defer stub2.Reset()

	stub3 := gostub.Stub(&GetTxn,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator) error {
			return nil
		})
	defer stub3.Reset()

	stub4 := gostub.Stub(&GetRelationById,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator, tableId uint64) (dbName string, tblName string, rel engine.Relation, err error) {
			// Only return StaleRead on first call, then return a different error to stop the loop
			if callCount.Add(1) == 1 {
				return "", "", nil, moerr.NewErrStaleReadNoCtx("", "test stale read")
			}
			// Return a different error to break the loop
			return "", "", nil, moerr.NewInternalError(ctx, "stopping test")
		})
	defer stub4.Reset()

	stub5 := gostub.Stub(&GetSnapshotTS,
		func(txnOp client.TxnOperator) timestamp.Timestamp {
			return timestamp.Timestamp{PhysicalTime: 100}
		})
	defer stub5.Reset()

	stub6 := gostub.Stub(&EnterRunSql, func(client.TxnOperator) {})
	defer stub6.Reset()

	stub7 := gostub.Stub(&ExitRunSql, func(client.TxnOperator) {})
	defer stub7.Reset()

	// Use simple mock watermark updater (avoid complex mock SQL executor)
	updater := newMockWatermarkUpdater()
	startTs := types.BuildTS(1, 0)

	// Initialize watermark
	wKey := &WatermarkKey{
		AccountId: 1,
		TaskId:    "task1",
		DBName:    "db1",
		TableName: "t1",
	}
	_, _ = updater.GetOrAddCommitted(context.Background(), wKey, &startTs)

	tableDef := &plan.TableDef{
		Cols:          []*plan.ColDef{{Name: "id"}, {Name: "ts"}},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "ts": 1},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	sinker := NewConsoleSinker(nil, nil)

	stream := NewTableChangeStream(
		nil, nil, mp, pool,
		1, "task1", tableInfo,
		sinker, updater, tableDef,
		false, &sync.Map{},
		startTs, types.TS{}, // startTs is set
		false, // noFull = false (StaleRead should NOT retry)
		50*time.Millisecond,
	)

	// Run should exit quickly due to fatal StaleRead error
	stream.Run(ctx, NewCdcActiveRoutine())

	// Verify error was set and NOT retryable
	assert.NotNil(t, stream.lastError, "Should have error")
	assert.False(t, stream.retryable, "Should NOT be retryable")
	assert.Contains(t, stream.lastError.Error(), "cannot recover", "Error should indicate cannot recover")
}

func TestTableChangeStream_StaleReadRetry_WatermarkUpdateFailure(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 2*time.Second, moerr.CauseFinishTxnOp)
	defer cancel()

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	pool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	watermarkNotify := make(chan struct{}, 1)
	failureErr := moerr.NewInternalError(ctx, "inject update failure")
	updater := newWatermarkUpdaterStub().
		withUpdateError(func(call int) error {
			return failureErr
		}).
		withNotify(watermarkNotify)

	var readCount atomic.Int32
	stub1 := gostub.Stub(&GetTxnOp,
		func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			return nil, nil
		})
	stub2 := gostub.Stub(&FinishTxnOp,
		func(ctx context.Context, inputErr error, txnOp client.TxnOperator, cnEngine engine.Engine) {})
	stub3 := gostub.Stub(&GetTxn,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator) error {
			return nil
		})
	stub4 := gostub.Stub(&GetRelationById,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator, tableId uint64) (string, string, engine.Relation, error) {
			if readCount.Add(1) > 1 {
				t.Fatalf("unexpected retry after update failure")
			}
			return "", "", nil, moerr.NewErrStaleReadNoCtx("", "test stale read")
		})
	stub5 := gostub.Stub(&GetSnapshotTS,
		func(txnOp client.TxnOperator) timestamp.Timestamp {
			return timestamp.Timestamp{
				PhysicalTime: 100,
				LogicalTime:  0,
			}
		})
	stub6 := gostub.Stub(&CollectChanges,
		func(ctx context.Context, rel engine.Relation, fromTs, toTs types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
			t.Fatalf("collect changes should not be reached when watermark update fails")
			return nil, nil
		})
	stub7 := gostub.Stub(&EnterRunSql, func(client.TxnOperator) {})
	stub8 := gostub.Stub(&ExitRunSql, func(client.TxnOperator) {})
	stubs := []*gostub.Stubs{stub1, stub2, stub3, stub4, stub5, stub6, stub7, stub8}
	defer func() {
		for _, s := range stubs {
			s.Reset()
		}
	}()

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "ts"},
			{Name: "a"},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"a"},
		},
		Name2ColIndex: map[string]int32{
			"ts": 0,
			"a":  1,
		},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	sinker := NewConsoleSinker(nil, nil)

	stream := NewTableChangeStream(
		nil, nil, mp, pool,
		1, "task1", tableInfo,
		sinker, updater, tableDef,
		false, &sync.Map{},
		types.TS{}, types.TS{},
		true, // allow retry but failure should abort
		50*time.Millisecond,
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		stream.Run(ctx, NewCdcActiveRoutine())
	}()

	select {
	case <-watermarkNotify:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for watermark update attempt")
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for stream to exit after watermark failure")
	}

	require.NotNil(t, stream.lastError)
	require.Contains(t, stream.lastError.Error(), "failed to update watermark")
	require.False(t, stream.retryable, "failure to update watermark should be treated as non-retryable")
	require.Equal(t, int32(1), updater.updateCalls.Load())
	require.Equal(t, int32(1), readCount.Load())

	if _, ok := updater.loadWatermark(stream.watermarkKey); ok {
		t.Fatalf("watermark should not be recorded on failure")
	}
}

func TestTableChangeStream_StaleReadRetry_MultipleAttempts(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 2*time.Second, moerr.CauseFinishTxnOp)
	defer cancel()

	mp := mpool.MustNewZero()
	defer mpool.DeleteMPool(mp)

	pool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) {},
	)

	var packer *types.Packer
	put := pool.Get(&packer)

	readDone := make(chan struct{}, 8)
	updateNotify := make(chan struct{}, 8)

	updater := newWatermarkUpdaterStub().withNotify(updateNotify)

	var readCount atomic.Int32
	stub1 := gostub.Stub(&GetTxnOp,
		func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			return nil, nil
		})
	stub2 := gostub.Stub(&FinishTxnOp,
		func(ctx context.Context, inputErr error, txnOp client.TxnOperator, cnEngine engine.Engine) {})
	stub3 := gostub.Stub(&GetTxn,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator) error {
			return nil
		})
	stub4 := gostub.Stub(&GetRelationById,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator, tableId uint64) (string, string, engine.Relation, error) {
			attempt := int(readCount.Add(1))
			select {
			case readDone <- struct{}{}:
			default:
			}
			if attempt <= 3 {
				return "", "", nil, moerr.NewErrStaleReadNoCtx("", "transient stale read")
			}
			return "", "", nil, nil
		})
	stub5 := gostub.Stub(&GetSnapshotTS,
		func(txnOp client.TxnOperator) timestamp.Timestamp {
			return timestamp.Timestamp{
				PhysicalTime: 200,
				LogicalTime:  0,
			}
		})
	stub6 := gostub.Stub(&CollectChanges,
		func(ctx context.Context, rel engine.Relation, fromTs, toTs types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
			return newTestChangesHandle("test", "t1", 20, 23, types.TS{}, mp, packer), nil
		})
	stub7 := gostub.Stub(&EnterRunSql, func(client.TxnOperator) {})
	stub8 := gostub.Stub(&ExitRunSql, func(client.TxnOperator) {})
	stubs := []*gostub.Stubs{stub1, stub2, stub3, stub4, stub5, stub6, stub7, stub8}
	defer func() {
		put.Put()
		for _, s := range stubs {
			s.Reset()
		}
	}()

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "ts"},
			{Name: "a"},
			{Name: "b"},
			{Name: "cpk"},
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"a", "b"},
		},
		Name2ColIndex: map[string]int32{
			"ts":  0,
			"a":   1,
			"b":   2,
			"cpk": 3,
		},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	sinker := NewConsoleSinker(nil, nil)

	stream := NewTableChangeStream(
		nil, nil, mp, pool,
		1, "task1", tableInfo,
		sinker, updater, tableDef,
		false, &sync.Map{},
		types.TS{}, types.TS{},
		true,
		50*time.Millisecond,
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		stream.Run(ctx, NewCdcActiveRoutine())
	}()

	for i := 0; i < 3; i++ {
		select {
		case <-readDone:
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for stale read attempt %d", i+1)
		}
		select {
		case <-updateNotify:
		case <-time.After(1 * time.Second):
			t.Fatalf("timeout waiting for watermark update %d", i+1)
		}
	}

	select {
	case <-readDone:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for successful read after retries")
	}

	require.Eventually(t, func() bool {
		return stream.lastError == nil
	}, time.Second, 10*time.Millisecond, "expected successful retry to clear lastError")

	stream.cancelRun()

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for stream stop after cancel")
	}

	cancel()

	require.Nil(t, stream.lastError, "successful retry should not leave residual error")
	require.GreaterOrEqual(t, readCount.Load(), int32(4))
	require.GreaterOrEqual(t, updater.updateCalls.Load(), int32(3), "expected watermark updates for each stale read attempt")

	// Watermark updates are issued asynchronously; cleanup clears cached state.
	// Presence of stored watermark is not guaranteed post-cleanup, so we rely on
	// updateCalls/count assertions above for coverage.
}

// Test end-to-end with real change processing
func TestTableChangeStream_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeoutCause(context.Background(), 2*time.Second, moerr.CauseFinishTxnOp)
	defer cancel()

	mp := mpool.MustNewZero()
	pool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)

	var packer *types.Packer
	put := pool.Get(&packer)
	defer put.Put()

	// Setup stubs
	stub1 := gostub.Stub(&GetTxnOp,
		func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			return nil, nil
		})
	defer stub1.Reset()

	stub2 := gostub.Stub(&FinishTxnOp,
		func(ctx context.Context, inputErr error, txnOp client.TxnOperator, cnEngine engine.Engine) {})
	defer stub2.Reset()

	stub3 := gostub.Stub(&GetTxn,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator) error {
			return nil
		})
	defer stub3.Reset()

	stub4 := gostub.Stub(&GetRelationById,
		func(ctx context.Context, cnEngine engine.Engine, txnOp client.TxnOperator, tableId uint64) (dbName string, tblName string, rel engine.Relation, err error) {
			return "", "", nil, nil
		})
	defer stub4.Reset()

	stub5 := gostub.Stub(&GetSnapshotTS,
		func(txnOp client.TxnOperator) timestamp.Timestamp {
			return timestamp.Timestamp{
				PhysicalTime: 100,
				LogicalTime:  0,
			}
		})
	defer stub5.Reset()

	stub6 := gostub.Stub(&CollectChanges,
		func(ctx context.Context, rel engine.Relation, fromTs, toTs types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
			return newTestChangesHandle("test", "t1", 20, 23, types.TS{}, mp, packer), nil
		})
	defer stub6.Reset()

	stub7 := gostub.Stub(&EnterRunSql, func(client.TxnOperator) {})
	defer stub7.Reset()

	stub8 := gostub.Stub(&ExitRunSql, func(client.TxnOperator) {})
	defer stub8.Reset()

	// Create watermark updater
	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	// Create table definition
	// Column order MUST match batch layout: user cols | cpk | commit-ts
	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "a"},   // User column
			{Name: "b"},   // User column
			{Name: "cpk"}, // Composite PK column
			{Name: "ts"},  // Commit timestamp (MUST be last)
		},
		Pkey: &plan.PrimaryKeyDef{
			Names: []string{"a", "b"},
		},
		Name2ColIndex: map[string]int32{
			"a":   0,
			"b":   1,
			"cpk": 2,
			"ts":  3,
		},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	// Use console sinker for simplicity
	sinker := NewConsoleSinker(nil, nil)

	// Initialize watermark
	wKey := &WatermarkKey{
		AccountId: 1,
		TaskId:    "task1",
		DBName:    "db1",
		TableName: "t1",
	}
	initialTs := types.TS{}
	_, err := updater.GetOrAddCommitted(context.Background(), wKey, &initialTs)
	assert.NoError(t, err)

	stream := NewTableChangeStream(
		nil, nil, mp, pool,
		1, "task1", tableInfo,
		sinker, updater, tableDef,
		false, &sync.Map{},
		types.TS{}, types.TS{},
		false,
		300*time.Millisecond,
	)

	// Run and wait
	ar := NewCdcActiveRoutine()
	go stream.Run(ctx, ar)
	stream.Wait()

	// Verify stream completed (no panic, graceful exit)
	// Note: In this test, context timeout causes graceful exit before watermark update
	// The key validation is that the test completes without panic or hang
	t.Log("Stream completed end-to-end test successfully")
}

// Test context cancellation
func TestTableChangeStream_Run_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	mp := mpool.MustNewZero()
	pool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)

	// Setup minimal stubs
	opCalled := make(chan struct{})
	allowReturn := make(chan struct{})
	stubs := []*gostub.Stubs{
		gostub.Stub(&GetTxnOp,
			func(innerCtx context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
				select {
				case <-opCalled:
				default:
					close(opCalled)
				}
				select {
				case <-allowReturn:
					return nil, nil
				case <-innerCtx.Done():
					return nil, innerCtx.Err()
				}
			}),
		gostub.Stub(&FinishTxnOp,
			func(context.Context, error, client.TxnOperator, engine.Engine) {}),
		gostub.Stub(&GetTxn,
			func(context.Context, engine.Engine, client.TxnOperator) error { return nil }),
		gostub.Stub(&GetRelationById,
			func(context.Context, engine.Engine, client.TxnOperator, uint64) (string, string, engine.Relation, error) {
				return "", "", nil, nil
			}),
		gostub.Stub(&GetSnapshotTS,
			func(client.TxnOperator) timestamp.Timestamp {
				return timestamp.Timestamp{PhysicalTime: 100}
			}),
		gostub.Stub(&CollectChanges,
			func(context.Context, engine.Relation, types.TS, types.TS, *mpool.MPool) (engine.ChangesHandle, error) {
				return newImmediateChangesHandle(nil), nil
			}),
		gostub.Stub(&EnterRunSql, func(client.TxnOperator) {}),
		gostub.Stub(&ExitRunSql, func(client.TxnOperator) {}),
	}
	defer func() {
		for _, stub := range stubs {
			stub.Reset()
		}
	}()

	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	tableDef := &plan.TableDef{
		Cols:          []*plan.ColDef{{Name: "id"}, {Name: "ts"}},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "ts": 1},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	sinker := NewConsoleSinker(nil, nil)

	stream := NewTableChangeStream(
		nil, nil, mp, pool,
		1, "task1", tableInfo,
		sinker, updater, tableDef,
		false, &sync.Map{},
		types.TS{}, types.TS{},
		false,
		50*time.Millisecond,
	)

	// Run in background
	started := make(chan struct{})
	go func() {
		close(started)
		stream.Run(ctx, NewCdcActiveRoutine())
	}()

	<-started
	<-opCalled

	// Cancel context
	cancel()
	close(allowReturn)

	// Wait for stream to stop
	done := make(chan struct{})
	go func() {
		stream.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Stream stopped gracefully after context cancellation")
	case <-time.After(2 * time.Second):
		t.Fatal("Stream did not stop after context cancellation")
	}
}

// Test ActiveRoutine Pause
func TestTableChangeStream_Run_Pause(t *testing.T) {
	ctx := context.Background()

	mp := mpool.MustNewZero()
	pool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)

	var wg sync.WaitGroup
	defer wg.Wait()

	opCalled := make(chan struct{})
	release := make(chan struct{})
	runExited := make(chan struct{})
	activeCalls := new(sync.WaitGroup)
	stubs := []*gostub.Stubs{
		gostub.Stub(&GetTxnOp,
			func(innerCtx context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
				activeCalls.Add(1)
				defer activeCalls.Done()

				select {
				case <-opCalled:
				default:
					close(opCalled)
				}

				select {
				case <-release:
					return nil, nil
				case <-innerCtx.Done():
					return nil, innerCtx.Err()
				}
			}),
		gostub.Stub(&FinishTxnOp,
			func(context.Context, error, client.TxnOperator, engine.Engine) {}),
		gostub.Stub(&GetTxn,
			func(context.Context, engine.Engine, client.TxnOperator) error { return nil }),
		gostub.Stub(&GetRelationById,
			func(context.Context, engine.Engine, client.TxnOperator, uint64) (string, string, engine.Relation, error) {
				return "", "", nil, nil
			}),
		gostub.Stub(&GetSnapshotTS,
			func(client.TxnOperator) timestamp.Timestamp {
				return timestamp.Timestamp{PhysicalTime: 100}
			}),
		gostub.Stub(&CollectChanges,
			func(context.Context, engine.Relation, types.TS, types.TS, *mpool.MPool) (engine.ChangesHandle, error) {
				return newImmediateChangesHandle(nil), nil
			}),
		gostub.Stub(&EnterRunSql, func(client.TxnOperator) {}),
		gostub.Stub(&ExitRunSql, func(client.TxnOperator) {}),
	}
	defer func() {
		activeCalls.Wait()
		for _, stub := range stubs {
			stub.Reset()
		}
	}()

	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	tableDef := &plan.TableDef{
		Cols:          []*plan.ColDef{{Name: "id"}, {Name: "ts"}},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "ts": 1},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	sinker := NewConsoleSinker(nil, nil)
	ar := NewCdcActiveRoutine()

	stream := NewTableChangeStream(
		nil, nil, mp, pool,
		1, "task1", tableInfo,
		sinker, updater, tableDef,
		false, &sync.Map{},
		types.TS{}, types.TS{},
		false,
		50*time.Millisecond,
	)

	// Run in background
	started := make(chan struct{})
	runDone := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		close(started)
		stream.Run(ctx, ar)
		close(runExited)
		close(runDone)
	}()

	<-started
	<-opCalled

	// Pause
	ar.ClosePause()
	close(release)
	<-runExited

	// Wait for stream to stop
	select {
	case <-runDone:
		t.Log("Stream stopped gracefully after pause")
	case <-time.After(2 * time.Second):
		t.Fatal("Stream did not stop after pause")
	}

	stream.Wait()
}

func TestTableChangeStream_ConcurrentStopSignalsCleanup(t *testing.T) {
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()

	mp := mpool.MustNewZero()
	pool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)

	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id"},
			{Name: "ts"},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "ts": 1},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	runningReaders := &sync.Map{}
	sinker := &mockSinker{}

	var collectCalls atomic.Int32
	var secondRunStarted atomic.Bool
	var isFirstRun atomic.Bool
	isFirstRun.Store(true)
	var activeStubCalls atomic.Int32
	blockReady := make(chan struct{})
	secondCollectReady := make(chan struct{}, 1)

	trackCall := func() func() {
		activeStubCalls.Add(1)
		return func() { activeStubCalls.Add(-1) }
	}

	stubs := []*gostub.Stubs{
		gostub.Stub(&GetTxnOp, func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			done := trackCall()
			defer done()
			if !isFirstRun.Load() {
				if secondRunStarted.CompareAndSwap(false, true) {
					select {
					case secondCollectReady <- struct{}{}:
					default:
					}
				}
			}
			return nil, nil
		}),
		gostub.Stub(&FinishTxnOp, func(ctx context.Context, _ error, _ client.TxnOperator, _ engine.Engine) {
			done := trackCall()
			defer done()
		}),
		gostub.Stub(&GetTxn, func(ctx context.Context, _ engine.Engine, _ client.TxnOperator) error {
			done := trackCall()
			defer done()
			return nil
		}),
		gostub.Stub(&GetRelationById, func(ctx context.Context, _ engine.Engine, _ client.TxnOperator, _ uint64) (string, string, engine.Relation, error) {
			done := trackCall()
			defer done()
			return "", "", nil, nil
		}),
		gostub.Stub(&GetSnapshotTS, func(_ client.TxnOperator) timestamp.Timestamp {
			done := trackCall()
			defer done()
			return timestamp.Timestamp{PhysicalTime: 1}
		}),
		gostub.Stub(&EnterRunSql, func(client.TxnOperator) {
			done := trackCall()
			defer done()
		}),
		gostub.Stub(&ExitRunSql, func(client.TxnOperator) {
			done := trackCall()
			defer done()
		}),
		gostub.Stub(&CollectChanges, func(ctx context.Context, rel engine.Relation, fromTs, toTs types.TS, mp *mpool.MPool) (engine.ChangesHandle, error) {
			done := trackCall()
			defer done()
			callIndex := int(collectCalls.Add(1))
			if isFirstRun.Load() {
				if callIndex == 1 {
					return &blockingChangesHandle{ready: blockReady}, nil
				}
				if callIndex == 2 {
					// signal once the second invocation is about to return
					go func() {
						secondCollectReady <- struct{}{}
					}()
					return newImmediateChangesHandle(nil), nil
				}
				return newImmediateChangesHandle(nil), nil
			}
			// Subsequent runs should return immediately but still signal readiness on first call.
			if secondRunStarted.CompareAndSwap(false, true) {
				go func() {
					select {
					case secondCollectReady <- struct{}{}:
					default:
					}
				}()
			}
			return newImmediateChangesHandle(nil), nil
		}),
	}

	t.Cleanup(func() {
		require.Eventually(t, func() bool {
			return activeStubCalls.Load() == 0
		}, 2*time.Second, 10*time.Millisecond, "stub calls should complete before reset")
		for _, stub := range stubs {
			stub.Reset()
		}
	})

	stream := NewTableChangeStream(
		nil, nil, mp, pool,
		1, "task1", tableInfo,
		sinker, updater, tableDef,
		false, runningReaders,
		types.TS{}, types.TS{},
		false,
		50*time.Millisecond,
	)

	ar := NewCdcActiveRoutine()

	runDone := make(chan struct{})
	go func() {
		stream.Run(ctx, ar)
		close(runDone)
	}()

	select {
	case <-blockReady:
	case <-time.After(2 * time.Second):
		t.Fatal("first CollectChanges invocation did not block")
	}

	start := make(chan struct{})
	var stopWG sync.WaitGroup
	stopWG.Add(4)
	go func() {
		defer stopWG.Done()
		<-start
		ar.ClosePause()
	}()
	go func() {
		defer stopWG.Done()
		<-start
		ar.CloseCancel()
	}()
	go func() {
		defer stopWG.Done()
		<-start
		cancelCtx()
	}()
	go func() {
		defer stopWG.Done()
		<-start
		stream.Close()
	}()

	close(start)
	stopWG.Wait()

	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("stream did not terminate after concurrent stop signals")
	}

	stream.Wait()

	if _, ok := runningReaders.Load(stream.runningReaderKey); ok {
		t.Fatal("runningReaders still contains entry after cleanup")
	}

	isFirstRun.Store(false)
	secondRunStarted.Store(false)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	ar2 := NewCdcActiveRoutine()

	// Reset counters and channels for the second stream
	collectCalls.Store(0)
	secondCollectReady = make(chan struct{}, 1)

	stream2 := NewTableChangeStream(
		nil, nil, mp, pool,
		1, "task1", tableInfo,
		sinker, updater, tableDef,
		false, runningReaders,
		types.TS{}, types.TS{},
		false,
		50*time.Millisecond,
	)

	secondRun := make(chan struct{})
	go func() {
		stream2.Run(ctx2, ar2)
		close(secondRun)
	}()

	require.Eventually(t, func() bool {
		select {
		case <-secondCollectReady:
			return true
		default:
			return secondRunStarted.Load()
		}
	}, 2*time.Second, 10*time.Millisecond)

	cancel2()

	select {
	case <-secondRun:
	case <-time.After(2 * time.Second):
		t.Fatal("second stream did not terminate after cancellation")
	}

	stream2.Wait()

	if _, ok := runningReaders.Load(stream2.runningReaderKey); ok {
		t.Fatal("runningReaders still contains entry after second cleanup")
	}

}

// Integration tests for TableChangeStream + DataProcessor + TransactionManager pipeline
func TestTableChangeStream_DataProcessorSinkerError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mp := mpool.MustNewZero()
	pool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id"},
			{Name: "ts"},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "ts": 1},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	sinker := newTableStreamRecordingSinker()
	sinker.setError(moerr.NewInternalError(ctx, "sinker error"))

	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	stubs := []*gostub.Stubs{
		gostub.Stub(&GetTxnOp, func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			return nil, nil
		}),
		gostub.Stub(&FinishTxnOp, func(context.Context, error, client.TxnOperator, engine.Engine) {}),
		gostub.Stub(&GetTxn, func(context.Context, engine.Engine, client.TxnOperator) error { return nil }),
		gostub.Stub(&GetRelationById, func(context.Context, engine.Engine, client.TxnOperator, uint64) (string, string, engine.Relation, error) {
			return "", "", nil, nil
		}),
		gostub.Stub(&GetSnapshotTS, func(client.TxnOperator) timestamp.Timestamp {
			return timestamp.Timestamp{PhysicalTime: 100}
		}),
		gostub.Stub(&EnterRunSql, func(client.TxnOperator) {}),
		gostub.Stub(&ExitRunSql, func(client.TxnOperator) {}),
	}
	t.Cleanup(func() {
		for _, stub := range stubs {
			stub.Reset()
		}
	})

	var collectCalls atomic.Int32
	collectStub := gostub.Stub(&CollectChanges, func(_ context.Context, _ engine.Relation, fromTs, toTs types.TS, _ *mpool.MPool) (engine.ChangesHandle, error) {
		callIdx := int(collectCalls.Add(1))
		if callIdx == 1 {
			bat := createTestBatch(t, mp, types.BuildTS(1, 0), []int32{1})
			handle := newImmediateChangesHandle([]changeBatch{
				{insert: bat, hint: engine.ChangesHandle_Snapshot},
				{insert: nil, hint: engine.ChangesHandle_Tail_done},
			})
			return handle, nil
		}
		return newImmediateChangesHandle(nil), nil
	})
	defer collectStub.Reset()

	stream := NewTableChangeStream(
		nil, nil, mp, pool,
		1, "task1", tableInfo,
		sinker, updater, tableDef,
		false, &sync.Map{},
		types.TS{}, types.TS{},
		false,
		50*time.Millisecond,
	)

	ar := NewCdcActiveRoutine()
	stream.Run(ctx, ar)

	require.Error(t, stream.lastError)
	require.Contains(t, stream.lastError.Error(), "sinker error")

	ops := sinker.opsSnapshot()
	require.NotContains(t, ops, "begin", "Begin should not be called when sinker already has an error")
	require.NotContains(t, ops, "rollback", "No transaction started, rollback not expected")
}

func TestTableChangeStream_EnsureCleanupOnCollectorError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mp := mpool.MustNewZero()
	pool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id"},
			{Name: "ts"},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "ts": 1},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	sinker := newRecordingSinker()
	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	stubs := []*gostub.Stubs{
		gostub.Stub(&GetTxnOp, func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			return nil, nil
		}),
		gostub.Stub(&FinishTxnOp, func(context.Context, error, client.TxnOperator, engine.Engine) {}),
		gostub.Stub(&GetTxn, func(context.Context, engine.Engine, client.TxnOperator) error { return nil }),
		gostub.Stub(&GetRelationById, func(context.Context, engine.Engine, client.TxnOperator, uint64) (string, string, engine.Relation, error) {
			return "", "", nil, nil
		}),
		gostub.Stub(&GetSnapshotTS, func(client.TxnOperator) timestamp.Timestamp {
			return timestamp.Timestamp{PhysicalTime: 100}
		}),
		gostub.Stub(&EnterRunSql, func(client.TxnOperator) {}),
		gostub.Stub(&ExitRunSql, func(client.TxnOperator) {}),
	}
	t.Cleanup(func() {
		for _, stub := range stubs {
			stub.Reset()
		}
	})

	collectErr := moerr.NewInternalError(ctx, "collector error")
	collectStub := gostub.Stub(&CollectChanges, func(_ context.Context, _ engine.Relation, _ types.TS, _ types.TS, _ *mpool.MPool) (engine.ChangesHandle, error) {
		return nil, collectErr
	})
	defer collectStub.Reset()

	stream := NewTableChangeStream(
		nil, nil, mp, pool,
		1, "task1", tableInfo,
		sinker, updater, tableDef,
		false, &sync.Map{},
		types.TS{}, types.TS{},
		false,
		50*time.Millisecond,
	)

	ar := NewCdcActiveRoutine()
	stream.Run(ctx, ar)

	require.Error(t, stream.lastError)
	require.Equal(t, collectErr, stream.lastError)

	// EnsureCleanup should not be called if no transaction was started
	ops := sinker.opsSnapshot()
	require.NotContains(t, ops, "rollback", "No transaction begun, so no rollback needed")
}

func TestTableChangeStream_TailDoneUpdatesTransactionToTs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mp := mpool.MustNewZero()
	pool := fileservice.NewPool(
		128,
		func() *types.Packer { return types.NewPacker() },
		func(packer *types.Packer) { packer.Reset() },
		func(packer *types.Packer) { packer.Close() },
	)

	tableDef := &plan.TableDef{
		Cols: []*plan.ColDef{
			{Name: "id"},
			{Name: "ts"},
		},
		Pkey:          &plan.PrimaryKeyDef{Names: []string{"id"}},
		Name2ColIndex: map[string]int32{"id": 0, "ts": 1},
	}

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "t1",
		SourceTblId:   1,
	}

	sinker := newRecordingSinker()
	updater, _ := InitCDCWatermarkUpdaterForTest(t)
	updater.Start()
	defer updater.Stop()

	stubs := []*gostub.Stubs{
		gostub.Stub(&GetTxnOp, func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			return nil, nil
		}),
		gostub.Stub(&FinishTxnOp, func(context.Context, error, client.TxnOperator, engine.Engine) {}),
		gostub.Stub(&GetTxn, func(context.Context, engine.Engine, client.TxnOperator) error { return nil }),
		gostub.Stub(&GetRelationById, func(context.Context, engine.Engine, client.TxnOperator, uint64) (string, string, engine.Relation, error) {
			return "", "", nil, nil
		}),
		gostub.Stub(&GetSnapshotTS, func(client.TxnOperator) timestamp.Timestamp {
			return timestamp.Timestamp{PhysicalTime: 200}
		}),
		gostub.Stub(&EnterRunSql, func(client.TxnOperator) {}),
		gostub.Stub(&ExitRunSql, func(client.TxnOperator) {}),
	}
	t.Cleanup(func() {
		for _, stub := range stubs {
			stub.Reset()
		}
	})

	var collectCalls atomic.Int32
	collectStub := gostub.Stub(&CollectChanges, func(_ context.Context, _ engine.Relation, fromTs, toTs types.TS, _ *mpool.MPool) (engine.ChangesHandle, error) {
		callIdx := int(collectCalls.Add(1))
		if callIdx == 1 {
			bat1 := createTestBatch(t, mp, types.BuildTS(100, 0), []int32{1})
			bat2 := createTestBatch(t, mp, types.BuildTS(150, 0), []int32{2})
			handle := newImmediateChangesHandle([]changeBatch{
				{insert: bat1, hint: engine.ChangesHandle_Tail_done},
				{insert: bat2, hint: engine.ChangesHandle_Tail_done},
				{insert: nil, hint: engine.ChangesHandle_Tail_done}, // NoMoreData
			})
			return handle, nil
		}
		return newImmediateChangesHandle(nil), nil
	})
	defer collectStub.Reset()

	stream := NewTableChangeStream(
		nil, nil, mp, pool,
		1, "task1", tableInfo,
		sinker, updater, tableDef,
		false, &sync.Map{},
		types.TS{}, types.TS{},
		false,
		50*time.Millisecond,
	)

	ar := NewCdcActiveRoutine()
	runDone := make(chan struct{})
	go func() {
		stream.Run(ctx, ar)
		close(runDone)
	}()

	var ops []string
	require.Eventually(t, func() bool {
		ops = sinker.opsSnapshot()
		beginCount := 0
		hasCommit := false
		for _, op := range ops {
			if op == "begin" {
				beginCount++
			}
			if op == "commit" {
				hasCommit = true
			}
		}
		return beginCount == 1 && hasCommit
	}, 2*time.Second, 10*time.Millisecond)

	require.NoError(t, stream.lastError)

	// Verify only one BEGIN was called (transaction persisted across TailDone batches)
	beginCount := 0
	for _, op := range ops {
		if op == "begin" {
			beginCount++
		}
	}
	require.Equal(t, 1, beginCount, "Should have only one BEGIN for multiple TailDone batches")

	// Verify COMMIT was called
	require.Contains(t, ops, "commit", "Should commit after NoMoreData")

	cancel()
	stream.Close()
	select {
	case <-runDone:
	case <-time.After(time.Second):
		t.Fatal("stream did not shut down")
	}
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

func (s *tableStreamRecordingSinker) reset() {
	s.mu.Lock()
	s.err = nil
	s.rollbackErr = nil
	s.commitErr = nil
	s.beginErr = nil
	s.sinkCalls = nil
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
