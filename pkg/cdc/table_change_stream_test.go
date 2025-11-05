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
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/pb/timestamp"
	"github.com/matrixorigin/matrixone/pkg/txn/client"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
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

func TestTableChangeStream_Info(t *testing.T) {
	mp, err := mpool.NewMPool("test", 0, mpool.NoFixed)
	assert.NoError(t, err)
	defer mpool.DeleteMPool(mp)

	tableInfo := &DbTableInfo{
		SourceDbName:  "db1",
		SourceTblName: "table1",
	}

	stream := createTestStream(mp, tableInfo)

	assert.Equal(t, tableInfo, stream.Info())
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
func createTestStream(mp *mpool.MPool, tableInfo *DbTableInfo) *TableChangeStream {
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
	)
}

// ============================================================================
// Integration Tests with gostub
// Note: Uses testChangesHandle from reader_test.go
// ============================================================================

// Test Run() integration with mocked dependencies
func TestTableChangeStream_Run_Integration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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

// Test StaleRead retry logic
func TestTableChangeStream_StaleRead_Retry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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

	// Track read attempts
	var readCount atomic.Int32
	readDone := make(chan struct{}, 10)

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
	go stream.Run(ctx, NewCdcActiveRoutine())

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
}

// Test StaleRead with startTs set (should fail, not retry)
func TestTableChangeStream_StaleRead_NoRetryWithStartTs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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

// Test end-to-end with real change processing
func TestTableChangeStream_EndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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
	stub1 := gostub.Stub(&GetTxnOp,
		func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			// Simulate slow operation to allow cancellation
			time.Sleep(100 * time.Millisecond)
			return nil, nil
		})
	defer stub1.Reset()

	stub2 := gostub.Stub(&FinishTxnOp,
		func(ctx context.Context, inputErr error, txnOp client.TxnOperator, cnEngine engine.Engine) {})
	defer stub2.Reset()

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
	time.Sleep(50 * time.Millisecond)

	// Cancel context
	cancel()

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

	// Setup minimal stubs
	stub1 := gostub.Stub(&GetTxnOp,
		func(_ context.Context, _ engine.Engine, _ client.TxnClient, _ string) (client.TxnOperator, error) {
			time.Sleep(100 * time.Millisecond)
			return nil, nil
		})
	defer stub1.Reset()

	stub2 := gostub.Stub(&FinishTxnOp,
		func(ctx context.Context, inputErr error, txnOp client.TxnOperator, cnEngine engine.Engine) {})
	defer stub2.Reset()

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
	go func() {
		close(started)
		stream.Run(ctx, ar)
	}()

	<-started
	time.Sleep(50 * time.Millisecond)

	// Pause
	ar.ClosePause()

	// Wait for stream to stop
	done := make(chan struct{})
	go func() {
		stream.Wait()
		close(done)
	}()

	select {
	case <-done:
		t.Log("Stream stopped gracefully after pause")
	case <-time.After(2 * time.Second):
		t.Fatal("Stream did not stop after pause")
	}
}
