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
	stub1, stub2, stub3, stub4, mockWatermarkUpdater, errChan := StubCDCSinkAndWatermarkUpdater(disttaeEngine, accountId)
	defer stub1.Reset()
	defer stub2.Reset()
	defer stub3.Reset()
	defer stub4.Reset()

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

	rmFn, err := objectio.InjectCDCScanTable("fast scan")
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
