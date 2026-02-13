// Copyright 2025 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbtxn "github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae/logtailreplay"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/prashantv/gostub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCollectObjectListFromCheckpointStaleRead tests the checkpoint reading logic in collectObjectListFromPartition
// This test specifically covers lines 142-220 of object_list.go:
// - RequestSnapshotRead to get checkpoint entries
// - Processing checkpoint entries to calculate minTS and maxTS
// - Stale read error when nextFrom is outside checkpoint range
// - GetObjectListFromCKP to read object list from checkpoint
func TestCollectObjectListFromCheckpointStaleRead(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		tableName    = "test_object_list_ckp"
		databaseName = "db_object_list_ckp"
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, accountId)

	disttaeEngine, taeHandler, rpcAgent, _ := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeHandler.Close(true)
		rpcAgent.Close()
	}()

	schema := catalog2.MockSchemaAll(20, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 30)
	defer bat.Close()
	bats := bat.Split(3)

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	// Create database and table
	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	// Insert first batch
	txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[0]))
	require.Nil(t, txn.Commit(ctx))
	t1 := txn.GetCommitTS()

	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := rel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.Nil(t, txn.Commit(ctx))

	// Force checkpoint
	now := taeHandler.GetDB().TxnMgr.Now()
	taeHandler.GetDB().ForceCheckpoint(ctx, now)

	// Insert second batch
	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[1]))
	require.Nil(t, txn.Commit(ctx))
	t2 := txn.GetCommitTS()

	// Force another checkpoint
	now = taeHandler.GetDB().TxnMgr.Now()
	taeHandler.GetDB().ForceCheckpoint(ctx, now)

	// Insert third batch
	txn, rel = testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	require.Nil(t, rel.Append(ctx, bats[2]))
	require.Nil(t, txn.Commit(ctx))
	t3 := txn.GetCommitTS()

	err = disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false)
	require.Nil(t, err)

	mp := common.DebugAllocator

	// Force GC to clean up old partition state, making t1 fall outside the current partition state range
	// This forces the code to go through the checkpoint reading path (lines 142-220)
	disttaeEngine.Engine.ForceGC(ctx, t2.Next())

	// Setup stub for RequestSnapshotRead to return checkpoint entries that don't include t1
	// This will trigger the stale read error at lines 174-176:
	//   if nextFrom.LT(&minTS) || nextFrom.GT(&maxTS) {
	//       return moerr.NewErrStaleReadNoCtx(minTS.ToString(), nextFrom.ToString())
	//   }
	ssStub := gostub.Stub(
		&disttae.RequestSnapshotRead,
		disttae.GetSnapshotReadFnWithHandler(
			func(ctx context.Context, meta pbtxn.TxnMeta, req *cmd_util.SnapshotReadReq, resp *cmd_util.SnapshotReadResp) (func(), error) {
				// Create checkpoint entry with time range [t2, t3]
				// When we try to read from t1 (which is < t2), it will be less than minTS
				// This triggers the stale read error
				t2Timestamp := t2.ToTimestamp()
				t3Timestamp := t3.ToTimestamp()

				resp.Succeed = true
				resp.Entries = []*cmd_util.CheckpointEntryResp{
					{
						Start:     &t2Timestamp,
						End:       &t3Timestamp,
						Location1: []byte("fake_location1"),
						Location2: []byte("fake_location2"),
						EntryType: 0,
						Version:   1,
					},
				}
				return func() {}, nil
			},
		),
	)
	defer ssStub.Reset()

	// Test: CollectObjectList should return stale read error
	// This exercises the checkpoint reading logic in lines 142-220
	{
		_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
		require.Nil(t, err)

		resultBat := logtailreplay.CreateObjectListBatch()
		defer resultBat.Clean(mp)

		// Try to collect object list from t1.Prev() (which is before checkpoint range)
		// This will:
		// 1. Call RequestSnapshotRead (line 143)
		// 2. Process checkpoint entries (lines 154-172)
		// 3. Check if nextFrom is in range (lines 174-177)
		// 4. Return stale read error because t1.Prev() < t2 (minTS)
		err = rel.CollectObjectList(ctx, t1.Prev(), t3.Next(), resultBat, mp)

		// Expect stale read error from line 176
		assert.Error(t, err)
		assert.True(t, moerr.IsMoErrCode(err, moerr.ErrStaleRead), "Expected stale read error, got: %v", err)
		t.Logf("Got expected stale read error: %v", err)
	}
}
