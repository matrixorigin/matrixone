// Copyright 2026 Matrix Origin
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

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	catalog2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	testutil2 "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/test/testutil"
)

// TestCollectChangesSeedRangeDeliversInheritedDelete is the deterministic
// collection-boundary regression for #28847. It drives the real engine read that
// iscp.CollectChanges performs (skipDeletes=false) over the exact ranges an ISCP
// consumer of a COPIED index sees.
//
// RestoreTable / CREATE TABLE ... CLONE / DATA BRANCH copy an index's built state
// and re-arm its CDC at the DDL transaction's PRE-commit snapshot TS, so the
// clone's own row append commits INSIDE the first range the copy is maintained
// from. A DELETE of that inherited row in the same range is the
// insert-then-delete shape filterBatch Case 2.2 cancels as "no net change", which
// loses the tombstone the copied entry needs to be shadowed.
// iscp.collectChangesCtxForJobs opts exactly that window into
// engine.WithCollectChangesKeepTrailingDeletes.
//
// The steady-state controls are the bound the append-only cuVS sinks rely on:
// once a range is durable, an insert-then-delete pair cancels again, so ordinary
// churn emits no extra durable delete, while a delete whose row committed before
// the range is delivered either way.
func TestCollectChangesSeedRangeDeliversInheritedDelete(t *testing.T) {
	catalog.SetupDefines("")

	var (
		accountId    = catalog.System_Account
		databaseName = "db1"
		tableName    = "seed_range"
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

	ctx, cancel = context.WithTimeout(ctx, time.Minute*5)
	defer cancel()

	schema := catalog2.MockSchemaAll(3, 0)
	schema.Name = tableName
	bat := catalog2.MockBatch(schema, 1)
	defer bat.Close()

	_, _, err := disttaeEngine.CreateDatabaseAndTable(ctx, databaseName, tableName, schema)
	require.NoError(t, err)

	txn, taeRel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
	id := taeRel.GetMeta().(*catalog2.TableEntry).AsCommonID()
	require.NoError(t, txn.Commit(ctx))

	appendFn := func() {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		require.NoError(t, rel.Append(ctx, bat))
		require.NoError(t, txn.Commit(ctx))
	}
	deleteFn := func() {
		txn, rel := testutil2.GetRelation(t, accountId, taeHandler.GetDB(), databaseName, tableName)
		require.NoError(t, rel.DeleteByFilter(ctx, handle.NewEQFilter(bat.Vecs[0].Get(0))))
		require.NoError(t, txn.Commit(ctx))
	}
	now := func() types.TS { return taeHandler.GetDB().TxnMgr.Now() }

	// The seed watermark precedes the clone's own append, so that append and the
	// branch's delete of the inherited row share one range.
	seedTS := now()
	appendFn()
	tsAfterCloneAppend := now()
	deleteFn()
	tsAfterInheritedDelete := now()

	// Steady state: a key created and destroyed inside one later range.
	appendFn()
	deleteFn()
	tsAfterChurn := now()

	// Steady state: a row committed before the range and deleted inside it.
	appendFn()
	tsAfterReappend := now()
	deleteFn()
	tsAfterLaterDelete := now()

	require.NoError(t, disttaeEngine.SubscribeTable(ctx, id.DbID, id.TableID, databaseName, tableName, false))
	// A dedicated read pool makes the fixture's own ownership checkable: every
	// byte the reads allocate must be back before the test ends.
	readMP := mpool.MustNew("iscp-seed-range-read")
	readBaseline := readMP.Stats().NumCurrBytes.Load()
	defer mpool.DeleteMPool(readMP)

	_, rel, _, err := disttaeEngine.GetTable(ctx, databaseName, tableName)
	require.NoError(t, err)

	// countAndRelease inspects one Next result and frees it. Next transfers both
	// batches to the caller and changes.Close releases only the handle's own
	// resources, so the free runs in a defer: checkInsertBatch aborts the loop on
	// failure and must not leave the batches allocated.
	countAndRelease := func(data, tombstone *batch.Batch, err error) (inserts, deletes int) {
		defer func() {
			if data != nil {
				data.Clean(readMP)
			}
			if tombstone != nil {
				tombstone.Clean(readMP)
			}
		}()
		require.NoError(t, err)
		if data != nil {
			checkInsertBatch(bat, data, t)
			inserts = data.Vecs[0].Length()
		}
		if tombstone != nil {
			checkTombstoneBatch(tombstone, schema.GetPrimaryKey().Type, t)
			deletes = tombstone.Vecs[0].Length()
		}
		return
	}

	// readRange drains one range and counts the surviving rows of each side.
	readRange := func(from, to types.TS, keepTrailingDeletes bool) (inserts, deletes int) {
		readCtx := ctx
		if keepTrailingDeletes {
			readCtx = engine.WithCollectChangesKeepTrailingDeletes(readCtx)
		}
		changes, err := rel.CollectChanges(readCtx, from, to, false, readMP)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, changes.Close())
		}()
		for {
			data, tombstone, _, err := changes.Next(readCtx, readMP)
			ins, del := countAndRelease(data, tombstone, err)
			inserts += ins
			deletes += del
			if data == nil && tombstone == nil {
				return
			}
		}
	}

	// The seed range: the copied index already holds the key, so the tombstone is
	// the only thing that can shadow it. Cancelling the pair leaves the deleted
	// row queryable forever.
	inserts, deletes := readRange(seedTS, tsAfterInheritedDelete, false)
	require.Zero(t, inserts)
	require.Zero(t, deletes, "the default net effect cancels the clone's append with the inherited delete")

	inserts, deletes = readRange(seedTS, tsAfterInheritedDelete, true)
	require.Zero(t, inserts, "the copied insert stays redundant; only the delete is new information")
	require.Equal(t, 1, deletes, "the seed range must deliver the inherited row's tombstone")

	// A later range that starts after the clone's append never needed the option:
	// the delete is the only operation on that key inside the range.
	for _, keepTrailingDeletes := range []bool{false, true} {
		inserts, deletes = readRange(tsAfterCloneAppend, tsAfterInheritedDelete, keepTrailingDeletes)
		require.Zero(t, inserts)
		require.Equal(t, 1, deletes, "a delete-only range is delivered with the option off and on")
	}

	// Steady-state churn stays bounded: the pair cancels again once the seed range
	// is durable, so no extra durable delete reaches an append-only sink.
	inserts, deletes = readRange(tsAfterInheritedDelete, tsAfterChurn, false)
	require.Zero(t, inserts)
	require.Zero(t, deletes, "steady-state insert-then-delete churn must keep cancelling out")

	inserts, deletes = readRange(tsAfterReappend, tsAfterLaterDelete, false)
	require.Zero(t, inserts)
	require.Equal(t, 1, deletes, "a steady-state delete of an earlier row is still delivered")

	require.Equal(t, readBaseline, readMP.Stats().NumCurrBytes.Load(),
		"every batch the reads returned must be released before the test ends")
}
