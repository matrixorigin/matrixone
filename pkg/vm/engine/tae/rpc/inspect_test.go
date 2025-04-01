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

package rpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
)

var wasmPath = "../../../../../test/distributed/resources/plugin/filterStats.wasm"

func Test_storageUsageDetails(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer tae.Close()

	storage := &storageUsageHistoryArg{
		ctx: &inspectContext{
			db:   tae,
			resp: &cmd_util.InspectResp{},
		},
	}
	err := storageUsageDetails(storage)
	require.NoError(t, err)
}

// TestPreparePlugin tests the plugin preparation with different URL schemes
func TestPreparePlugin(t *testing.T) {

	// Test with parse error
	_, err := preparePlugin("\x00\x00\x00\x00")
	require.Error(t, err)

	// Test with file path
	plugin, err := preparePlugin(wasmPath)
	require.NoError(t, err)
	require.NotNil(t, plugin)

	_, err = preparePlugin("http://example.com/no.wasm")
	require.Error(t, err) // Expected to fail since URL doesn't exist
}

func TestWasmArgRun(t *testing.T) {
	handle := mockTAEHandle(context.Background(), t, &options.Options{})
	schema := catalog.MockSchema(2, 1)
	schema.Name = "test1"
	bats := catalog.MockBatch(schema, 20).Split(2)
	ctx := context.Background()
	{
		asyncTxn, err := handle.db.StartTxn(nil)
		require.NoError(t, err)
		database, err := testutil.CreateDatabase2(ctx, asyncTxn, "db1")
		require.NoError(t, err)
		tbl, err := testutil.CreateRelation2(ctx, asyncTxn, database, schema)
		require.NoError(t, err)

		tbl.Append(ctx, bats[0])
		require.NoError(t, asyncTxn.Commit(context.Background()))
		require.NoError(t, handle.db.ForceFlush(ctx, handle.db.TxnMgr.Now()))
	}
	{
		asyncTxn, err := handle.db.StartTxn(nil)
		require.NoError(t, err)
		database, err := asyncTxn.GetDatabase("db1")
		require.NoError(t, err)
		tbl, err := database.GetRelationByName("test1")
		require.NoError(t, err)
		tbl.Append(ctx, bats[1])
		require.NoError(t, asyncTxn.Commit(context.Background()))
		require.NoError(t, handle.db.ForceFlush(ctx, handle.db.TxnMgr.Now()))
	}

	resp, _ := handle.runInspectCmd(`merge wasm  -w "./filterStats.wasm" -d`) // no target table
	require.Contains(t, resp.Message, "invalid input")

	resp, _ = handle.runInspectCmd("merge wasm -t db1.test1 -d") // no wasm file
	require.Contains(t, resp.Message, "invalid input")

	resp, _ = handle.runInspectCmd(fmt.Sprintf(`merge wasm -w "%s" -t db1.test1 -d`, wasmPath))
	require.Contains(t, string(resp.Payload), "dryrun(2):", string(resp.Payload))

	handle.runInspectCmd(fmt.Sprintf(`merge wasm -w "%s" -t db1.test1`, wasmPath))

	{
		asyncTxn, err := handle.db.StartTxn(nil)
		require.NoError(t, err)
		database, err := asyncTxn.GetDatabase("db1")
		require.NoError(t, err)
		tbl, err := database.GetRelationByName("test1")
		require.NoError(t, err)
		it := tbl.MakeObjectItOnSnap(false)
		cnt := 0
		for it.Next() {
			cnt++
		}
		require.Equal(t, 1, cnt)
	}
}

func TestMergeShowAndSwitch(t *testing.T) {
	handle := mockTAEHandle(context.Background(), t, &options.Options{})
	asyncTxn, err := handle.db.StartTxn(nil)
	require.NoError(t, err)

	ctx := context.Background()
	database, err := testutil.CreateDatabase2(ctx, asyncTxn, "db1")
	require.NoError(t, err)
	schema := catalog.MockSchema(2, 1)
	schema.Name = "test1"
	_, err = testutil.CreateRelation2(ctx, asyncTxn, database, schema)
	require.NoError(t, err)
	require.NoError(t, asyncTxn.Commit(context.Background()))

	resp, err := handle.runInspectCmd("merge show")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: true")

	handle.runInspectCmd("merge switch off")
	resp, err = handle.runInspectCmd("merge show")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: false")

	handle.runInspectCmd("merge switch off -t db1.test1")
	resp, err = handle.runInspectCmd("merge show -t db1.test1")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: false")
	require.Contains(t, string(resp.Payload), "auto merge for table 1000-test1: false")

	handle.runInspectCmd("merge switch on -t db1.test1")
	resp, err = handle.runInspectCmd("merge show -t db1.test1")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: false")
	require.Contains(t, string(resp.Payload), "auto merge for table 1000-test1: true")

	handle.runInspectCmd("merge switch on")
	resp, err = handle.runInspectCmd("merge show -t db1.test1")
	require.NoError(t, err)
	require.Contains(t, string(resp.Payload), "auto merge for all: true")
	require.Contains(t, string(resp.Payload), "auto merge for table 1000-test1: true")

	resp, err = handle.runInspectCmd("merge switch notOnOrOff")
	require.Contains(t, resp.Message, "invalid input")

	resp, err = handle.runInspectCmd("merge switch on off")
	require.Contains(t, resp.Message, "invalid input")
}
