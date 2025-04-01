// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
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

	"github.com/matrixorigin/matrixone/pkg/pb/txn"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var wasmPath = "../../../../../test/distributed/resources/plugin/filterStats.wasm"

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

// TestWasmArg tests the wasmArg struct and its methods
func TestWasmArg(t *testing.T) {
	// Create a test wasmArg
	wa := &wasmArg{
		wasm:     "./filterStats.wasm",
		function: "testFunction",
		dryrun:   true,
	}

	// Test String method
	str := wa.String()
	assert.Contains(t, str, "./filterStats.wasm")
	assert.Contains(t, str, "dryrun: true")
}

// TestPrepareCommand tests the PrepareCommand method of wasmArg
func TestPrepareCommand(t *testing.T) {
	wa := &wasmArg{}
	cmd := wa.PrepareCommand()

	assert.Equal(t, "wasm", cmd.Use)
	assert.Contains(t, cmd.Short, "run wasm policy")

	// Verify flags
	assert.NotNil(t, cmd.Flags().Lookup("target"))
	assert.NotNil(t, cmd.Flags().Lookup("wasm"))
	assert.NotNil(t, cmd.Flags().Lookup("function"))
	assert.NotNil(t, cmd.Flags().Lookup("dryrun"))
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

	runCmd := func(cmd string) (resp *cmd_util.InspectResp, err error) {
		resp = &cmd_util.InspectResp{}
		_, err = handle.HandleInspectTN(context.Background(), txn.TxnMeta{}, &cmd_util.InspectTN{
			AccessInfo: cmd_util.AccessInfo{},
			Operation:  cmd,
		}, resp)
		return
	}

	resp, _ := runCmd(`policy wasm  -w "./filterStats.wasm" -d`) // no target table
	require.Contains(t, resp.Message, "invalid input")

	resp, _ = runCmd("policy wasm -t db1.test1 -d") // no wasm file
	require.Contains(t, resp.Message, "invalid input")

	resp, _ = runCmd(fmt.Sprintf(`policy wasm -w "%s" -t db1.test1 -d`, wasmPath))
	require.Contains(t, string(resp.Payload), "dryrun(2):")

	runCmd(fmt.Sprintf(`policy wasm -w "%s" -t db1.test1`, wasmPath))

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
