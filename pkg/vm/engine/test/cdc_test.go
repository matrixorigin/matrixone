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

package test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/cdc"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/util/fault"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
)

func TestCDCRetryFromZero(t *testing.T) {

	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		taskId       = uuid.New().String()
		taskName     = "test_cdc_retry"
		databaseName = "test_db"
		tableName    = "test_table"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	// Create an additional table to serve as sink dest
	sinkDatabaseName := "sink_db"
	sinkTableName := "sink_table"

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)

	require.NoError(t, err)
	// Create test data
	bat := CreateDBAndTableForCDC(t, disttaeEngine, ctxWithTimeout, databaseName, tableName, 10)
	bats := bat.Split(10)
	defer bat.Close()

	// Get table info
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, databaseName, tableName)
	require.NoError(t, err)
	_ = rel.GetTableID(ctxWithTimeout)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	// append 1
	appendFn := func(idx int) {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		require.Nil(t, rel.Append(ctxWithTimeout, bats[idx]))
		require.Nil(t, txn.Commit(ctxWithTimeout))
	}

	appendFn(0)

	// new cdc executor with stubs
	stub1, stub2, stub3, stub4, stub5, stub6, mockWatermarkUpdater, errChan := StubCDCSinkAndWatermarkUpdater(disttaeEngine, accountId)
	defer stub1.Reset()
	defer stub2.Reset()
	defer stub3.Reset()
	defer stub4.Reset()
	defer stub5.Reset()
	defer stub6.Reset()
	// Define tables to track
	tables := cdc.PatternTuples{
		Pts: []*cdc.PatternTuple{
			{
				Source: cdc.PatternTable{
					Database: databaseName,
					Table:    tableName,
				},
				Sink: cdc.PatternTable{
					Database: sinkDatabaseName,
					Table:    sinkTableName,
				},
			},
		},
	}

	fault.Enable()
	defer fault.Disable()

	executor := NewMockCDCExecutor(
		t,
		disttaeEngine,
		ctxWithTimeout,
		accountId,
		taskId,
		taskName,
		tables,
	)

	go executor.Start(ctxWithTimeout)

	// Test watermark updater directly
	wmKey := cdc.WatermarkKey{
		AccountId: uint64(accountId),
		TaskId:    taskId,
		DBName:    databaseName,
		TableName: tableName,
	}

	// Verify watermark
	checkWatermarkFn := func(expectedWM types.TS, waitTime int) {
		testutils.WaitExpect(
			waitTime,
			func() bool {
				wm, ok := mockWatermarkUpdater.GetWatermark(wmKey)
				return ok && wm.GE(&expectedWM)
			},
		)
		wm, ok := mockWatermarkUpdater.GetWatermark(wmKey)
		t.Logf("watermark check: wm: %v, expected: %v, ok: %v", wm.ToString(), expectedWM.ToString(), ok)
		require.True(t, ok)
		require.True(t, wm.GE(&expectedWM))
	}

	assert.Equal(t, 0, len(errChan))

	// snapshot1: check watermark after append 1
	wm1 := taeHandler.GetDB().TxnMgr.Now()
	checkWatermarkFn(wm1, 1000)

	// tail1: append2 and update watermark
	appendFn(1)
	wm2 := taeHandler.GetDB().TxnMgr.Now()
	checkWatermarkFn(wm2, 4000)
	t.Log("tail1: watermark updated after second append")

	// inject stale read (simulating error scenario)
	rmFn2, err := objectio.InjectCDCExecutor("stale read")
	require.NoError(t, err)
	defer rmFn2()

	// append more data
	appendFn(2)

	// snapshot2: check watermark (simulating retry from zero after error)
	// In real scenario, CDC would detect stale read and restart from a valid timestamp
	// Here we test that watermark updater can still work correctly
	wm3 := taeHandler.GetDB().TxnMgr.Now()
	checkWatermarkFn(wm3, 1000)

	CheckTableDataWithName(t, disttaeEngine, ctxWithTimeout, sinkDatabaseName, sinkTableName, databaseName, tableName)
}

func TestRetryRollback(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		taskId       = uuid.New().String()
		taskName     = "test_retry_rollback"
		databaseName = "test_db"
		tableName    = "test_table"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	// Create an additional table to serve as sink dest
	sinkDatabaseName := "sink_db"
	sinkTableName := "sink_table"

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)

	// Create test data
	bat := CreateDBAndTableForCDC(t, disttaeEngine, ctxWithTimeout, databaseName, tableName, 10)
	bats := bat.Split(10)
	defer bat.Close()

	// Get table info
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, databaseName, tableName)
	require.NoError(t, err)
	_ = rel.GetTableID(ctxWithTimeout)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	// append test data
	appendFn := func(idx int) {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		require.Nil(t, rel.Append(ctxWithTimeout, bats[idx]))
		require.Nil(t, txn.Commit(ctxWithTimeout))
	}

	appendFn(0)

	// new cdc executor with stubs
	stub1, stub2, stub3, stub4, stub5, stub6, mockWatermarkUpdater, _ := StubCDCSinkAndWatermarkUpdater(disttaeEngine, accountId)
	defer stub1.Reset()
	defer stub2.Reset()
	defer stub3.Reset()
	defer stub4.Reset()
	defer stub5.Reset()
	defer stub6.Reset()

	// Define tables to track
	tables := cdc.PatternTuples{
		Pts: []*cdc.PatternTuple{
			{
				Source: cdc.PatternTable{
					Database: databaseName,
					Table:    tableName,
				},
				Sink: cdc.PatternTable{
					Database: sinkDatabaseName,
					Table:    sinkTableName,
				},
			},
		},
	}

	fault.Enable()
	defer fault.Disable()

	// Inject commit error using InjectCDCExecutor
	rmFn, err := objectio.InjectCDCExecutor("commit error")
	require.NoError(t, err)
	defer rmFn()

	executor := NewMockCDCExecutor(
		t,
		disttaeEngine,
		ctxWithTimeout,
		accountId,
		taskId,
		taskName,
		tables,
	)

	go executor.Start(ctxWithTimeout)

	// Test watermark updater
	wmKey := cdc.WatermarkKey{
		AccountId: uint64(accountId),
		TaskId:    taskId,
		DBName:    databaseName,
		TableName: tableName,
	}

	// Verify watermark after initial append
	checkWatermarkFn := func(expectedWM types.TS, waitTime int, expectOk bool) {
		testutils.WaitExpect(
			waitTime,
			func() bool {
				wm, ok := mockWatermarkUpdater.GetWatermark(wmKey)
				return ok && wm.GE(&expectedWM)
			},
		)
		wm, ok := mockWatermarkUpdater.GetWatermark(wmKey)
		t.Logf("watermark check: wm: %v, expected: %v, ok: %v", wm.ToString(), expectedWM.ToString(), ok)
		require.True(t, ok)
		require.Equal(t, expectOk, wm.GE(&expectedWM))
	}

	// Wait for snapshot to complete (first commit will fail, then retry and succeed)
	wm1 := taeHandler.GetDB().TxnMgr.Now()
	// Give more time for retry to happen (1s sleep + retry time)
	checkWatermarkFn(wm1, 5000, false)

	rmFn()
	checkWatermarkFn(wm1, 5000, true)

	CheckTableDataWithName(t, disttaeEngine, ctxWithTimeout, sinkDatabaseName, sinkTableName, databaseName, tableName)
}

func TestTruncateTable(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		taskId       = uuid.New().String()
		taskName     = "test_truncate_table"
		databaseName = "test_db"
		tableName    = "test_table"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	// Create sink database and table
	sinkDatabaseName := "sink_db"
	sinkTableName := "sink_table"

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)

	// Create test data
	bat := CreateDBAndTableForCDC(t, disttaeEngine, ctxWithTimeout, databaseName, tableName, 10)
	bats := bat.Split(10)
	defer bat.Close()

	// Get table info
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, databaseName, tableName)
	require.NoError(t, err)
	_ = rel.GetTableID(ctxWithTimeout)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	// append function
	appendFn := func(idx int) {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		require.Nil(t, rel.Append(ctxWithTimeout, bats[idx]))
		require.Nil(t, txn.Commit(ctxWithTimeout))
	}

	// Append initial data
	appendFn(0)
	appendFn(1)

	// Setup CDC executor with stubs
	stub1, stub2, stub3, stub4, stub5, stub6, mockWatermarkUpdater, errChan := StubCDCSinkAndWatermarkUpdater(disttaeEngine, accountId)
	defer stub1.Reset()
	defer stub2.Reset()
	defer stub3.Reset()
	defer stub4.Reset()
	defer stub5.Reset()
	defer stub6.Reset()

	// Define tables to track
	tables := cdc.PatternTuples{
		Pts: []*cdc.PatternTuple{
			{
				Source: cdc.PatternTable{
					Database: databaseName,
					Table:    tableName,
				},
				Sink: cdc.PatternTable{
					Database: sinkDatabaseName,
					Table:    sinkTableName,
				},
			},
		},
	}

	fault.Enable()
	defer fault.Disable()

	executor := NewMockCDCExecutor(
		t,
		disttaeEngine,
		ctxWithTimeout,
		accountId,
		taskId,
		taskName,
		tables,
	)

	go executor.Start(ctxWithTimeout)

	// Test watermark updater
	wmKey := cdc.WatermarkKey{
		AccountId: uint64(accountId),
		TaskId:    taskId,
		DBName:    databaseName,
		TableName: tableName,
	}

	// Verify watermark function
	checkWatermarkFn := func(expectedWM types.TS, waitTime int) {
		testutils.WaitExpect(
			waitTime,
			func() bool {
				wm, ok := mockWatermarkUpdater.GetWatermark(wmKey)
				return ok && wm.GE(&expectedWM)
			},
		)
		wm, ok := mockWatermarkUpdater.GetWatermark(wmKey)
		t.Logf("watermark check: wm: %v, expected: %v, ok: %v", wm.ToString(), expectedWM.ToString(), ok)
		require.True(t, ok)
		require.True(t, wm.GE(&expectedWM))
	}

	assert.Equal(t, 0, len(errChan))

	// Wait for initial sync to complete
	wm1 := taeHandler.GetDB().TxnMgr.Now()
	checkWatermarkFn(wm1, 5000)
	t.Log("initial data synced successfully")

	// Drop the table using disttaeEngine
	dropTxn, err := disttaeEngine.NewTxnOperator(ctxWithTimeout, disttaeEngine.Now())
	require.NoError(t, err)
	db, err := disttaeEngine.Engine.Database(ctxWithTimeout, databaseName, dropTxn)
	require.NoError(t, err)
	err = db.Delete(ctxWithTimeout, tableName)
	require.NoError(t, err)
	err = dropTxn.Commit(ctxWithTimeout)
	require.NoError(t, err)
	t.Log("table dropped")

	// Recreate the table with the same structure
	bat2 := CreateDBAndTableForCDC(t, disttaeEngine, ctxWithTimeout, databaseName, tableName, 10)
	bats2 := bat2.Split(10)
	defer bat2.Close()

	// Redefine append function for new table
	appendFn = func(idx int) {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		require.Nil(t, rel.Append(ctxWithTimeout, bats2[idx]))
		require.Nil(t, txn.Commit(ctxWithTimeout))
	}

	// Append new data to recreated table
	appendFn(0)
	appendFn(1)
	appendFn(2)
	t.Log("new data appended to recreated table")

	// Wait for new data to sync
	wm2 := taeHandler.GetDB().TxnMgr.Now()
	checkWatermarkFn(wm2, 4000)
	t.Log("new data synced successfully")

	// Verify data consistency
	CheckTableDataWithName(t, disttaeEngine, ctxWithTimeout, sinkDatabaseName, sinkTableName, databaseName, tableName)
	t.Log("data consistency verified after table recreation")
}

func TestRestartCDC(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		taskId1      = uuid.New().String()
		taskId2      = uuid.New().String()
		taskName     = "test_restart_cdc"
		databaseName = "test_db"
		tableName    = "test_table"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)
	ctxWithTimeout, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	// Create sink database and table
	sinkDatabaseName := "sink_db"
	sinkTableName := "sink_table"

	err := mock_mo_indexes(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)
	err = mock_mo_foreign_keys(disttaeEngine, ctxWithTimeout)
	require.NoError(t, err)

	// Create test data
	bat := CreateDBAndTableForCDC(t, disttaeEngine, ctxWithTimeout, databaseName, tableName, 10)
	bats := bat.Split(10)
	defer bat.Close()

	// Get table info
	_, rel, txn, err := disttaeEngine.GetTable(ctxWithTimeout, databaseName, tableName)
	require.NoError(t, err)
	_ = rel.GetTableID(ctxWithTimeout)
	require.NoError(t, txn.Commit(ctxWithTimeout))

	// append function
	appendFn := func(idx int) {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		require.Nil(t, rel.Append(ctxWithTimeout, bats[idx]))
		require.Nil(t, txn.Commit(ctxWithTimeout))
	}

	// Setup CDC executor with stubs
	stub1, stub2, stub3, stub4, stub5, stub6, mockWatermarkUpdater, errChan := StubCDCSinkAndWatermarkUpdater(disttaeEngine, accountId)
	defer stub1.Reset()
	defer stub2.Reset()
	defer stub3.Reset()
	defer stub4.Reset()
	defer stub5.Reset()
	defer stub6.Reset()

	// Define tables to track
	tables := cdc.PatternTuples{
		Pts: []*cdc.PatternTuple{
			{
				Source: cdc.PatternTable{
					Database: databaseName,
					Table:    tableName,
				},
				Sink: cdc.PatternTable{
					Database: sinkDatabaseName,
					Table:    sinkTableName,
				},
			},
		},
	}

	fault.Enable()
	defer fault.Disable()

	// Create and start first CDC executor
	executor1 := NewMockCDCExecutor(
		t,
		disttaeEngine,
		ctxWithTimeout,
		accountId,
		taskId1,
		taskName,
		tables,
	)

	go executor1.Start(ctxWithTimeout)

	// Test watermark updater
	wmKey1 := cdc.WatermarkKey{
		AccountId: uint64(accountId),
		TaskId:    taskId1,
		DBName:    databaseName,
		TableName: tableName,
	}
	wmKey2 := cdc.WatermarkKey{
		AccountId: uint64(accountId),
		TaskId:    taskId2,
		DBName:    databaseName,
		TableName: tableName,
	}

	// Verify watermark function
	checkWatermarkFn := func(expectedWM types.TS, key cdc.WatermarkKey, waitTime int) {
		testutils.WaitExpect(
			waitTime,
			func() bool {
				wm, ok := mockWatermarkUpdater.GetWatermark(key)
				return ok && wm.GE(&expectedWM)
			},
		)
		wm, ok := mockWatermarkUpdater.GetWatermark(key)
		t.Logf("watermark check: wm: %v, expected: %v, ok: %v", wm.ToString(), expectedWM.ToString(), ok)
		require.True(t, ok)
		require.True(t, wm.GE(&expectedWM))
	}

	assert.Equal(t, 0, len(errChan))

	// Append initial data
	appendFn(0)
	appendFn(1)

	// Wait for first sync to complete
	wm1 := taeHandler.GetDB().TxnMgr.Now()
	checkWatermarkFn(wm1, wmKey1, 4000)
	t.Log("first sync completed with executor1")

	// Stop the first executor
	err = executor1.Cancel()
	require.NoError(t, err)
	t.Log("executor1 cancelled")

	// Create and start second CDC executor with same task ID
	executor2 := NewMockCDCExecutor(
		t,
		disttaeEngine,
		ctxWithTimeout,
		accountId,
		taskId2,
		taskName,
		tables,
	)

	go executor2.Start(ctxWithTimeout)
	t.Log("executor2 started")

	// Append more data
	appendFn(2)
	appendFn(3)

	// Wait for second sync to complete
	wm2 := taeHandler.GetDB().TxnMgr.Now()
	checkWatermarkFn(wm2, wmKey2, 4000)
	t.Log("second sync completed with executor2")

	// Verify final data consistency
	CheckTableDataWithName(t, disttaeEngine, ctxWithTimeout, sinkDatabaseName, sinkTableName, databaseName, tableName)
	t.Log("data consistency verified after CDC restart")

	// Cleanup second executor
	err = executor2.Cancel()
	require.NoError(t, err)
}
