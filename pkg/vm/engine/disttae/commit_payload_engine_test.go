// Copyright 2026 Matrix Origin
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

package disttae_test

import (
	"context"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/disttae"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
	"github.com/stretchr/testify/require"
)

// Commits through an in-process TN: every commit marshals its payload into
// one recycled off-heap buffer, every terminal hook gives it back, and the
// data TN committed is intact after later commits reused (and poisoned) the
// buffer.
func TestCommitPayloadThroughEngine(t *testing.T) {
	mpool.EnableDebugPoisonOnFree()
	defer mpool.DisableDebugPoisonOnFree()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = context.WithValue(ctx, defines.TenantIDKey{}, catalog.System_Account)

	payloadPool := mpool.MustNewZero()
	restore := disttae.SwapCommitPayloadPoolForTest(payloadPool)
	defer func() {
		restore()
		require.Zero(t, payloadPool.CurrNB(), "a commit payload buffer leaked")
	}()

	const primaryKeyIdx = 3
	schema := catalog2.MockSchemaAll(4, primaryKeyIdx)
	schema.Name = "payload_table"
	disttaeEngine, taeEngine, rpcAgent, mp := testutil.CreateEngines(ctx, testutil.TestOptions{}, t)
	defer func() {
		disttaeEngine.Close(ctx)
		taeEngine.Close(true)
		rpcAgent.Close()
	}()
	ctx, cancel = context.WithTimeout(ctx, time.Minute)
	defer cancel()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, "payload_db", schema.Name, schema)
	require.NoError(t, err)
	require.Equal(t, 1, disttae.IdleCommitPayloadsForTest(), "the DDL commit gave its buffer back")

	// Several commits, each inserting fresh rows: one buffer serves them all.
	rows := catalog2.MockBatch(schema, 40)
	defer rows.Close()
	allocated := int64(-1)
	for i := 0; i < 4; i++ {
		_, relation, txn, err := disttaeEngine.GetTable(ctx, "payload_db", schema.Name)
		require.NoError(t, err)
		window := rows.Window(i*10, 10)
		cnBat := containers.ToCNBatch(window)
		require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, cnBat, window.Attrs, false, true))
		require.NoError(t, txn.Commit(ctx))
		require.False(t, disttae.HoldsCommitPayloadForTest(txn.GetWorkspace().(*disttae.Transaction)))
		require.Equal(t, 1, disttae.IdleCommitPayloadsForTest())
		if allocated < 0 {
			allocated = payloadPool.Stats().NumAlloc.Load()
		}
	}
	require.LessOrEqual(t, payloadPool.Stats().NumAlloc.Load(), allocated+1,
		"later commits reuse the recycled buffer")

	// Prepare a commit and settle it through each terminal hook directly.
	for name, settle := range map[string]func(*disttae.Transaction){
		"rollback":       func(ws *disttae.Transaction) { require.NoError(t, ws.Rollback(ctx)) },
		"unknown-result": func(ws *disttae.Transaction) { ws.FinalizeCommitWithUnknownResult(ctx) },
	} {
		t.Run(name, func(t *testing.T) {
			_, relation, txn, err := disttaeEngine.GetTable(ctx, "payload_db", schema.Name)
			require.NoError(t, err)
			extra := catalog2.MockBatch(schema, 50)
			defer extra.Close()
			window := extra.Window(45, 5)
			require.NoError(t, testutil.WriteToRelation(ctx, txn, relation, containers.ToCNBatch(window), window.Attrs, false, true))
			ws := txn.GetWorkspace().(*disttae.Transaction)
			reqs, err := ws.Commit(ctx)
			require.NoError(t, err)
			require.NotEmpty(t, reqs)
			require.True(t, disttae.HoldsCommitPayloadForTest(ws))
			settle(ws)
			require.False(t, disttae.HoldsCommitPayloadForTest(ws))
			// The operator's own rollback settles the workspace again: the
			// release is idempotent.
			_ = txn.Rollback(ctx)
			require.False(t, disttae.HoldsCommitPayloadForTest(ws))
		})
	}

	// The 40 committed rows are intact although their payload buffer was
	// reused, and freed with poisoning, since.
	disttae.FreeIdleCommitPayloadsForTest()
	require.Zero(t, payloadPool.CurrNB())
	_, relation, txn, err := disttaeEngine.GetTable(ctx, "payload_db", schema.Name)
	require.NoError(t, err)
	reader, err := testutil.GetRelationReader(ctx, disttaeEngine, txn, relation, nil, mp, t)
	require.NoError(t, err)
	ret := testutil.EmptyBatchFromSchema(schema, mp, primaryKeyIdx)
	_, err = reader.Read(ctx, testutil.SchemaAttrs(schema, primaryKeyIdx), nil, mp, ret)
	require.NoError(t, err)
	require.Equal(t, 40, ret.RowCount())
	require.NoError(t, txn.Commit(ctx))
}
