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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnbase"
)

func TestGenerateManifest_Basic(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer tae.Close()

	// Create a test table and insert some data
	schema := catalog.MockSchemaAll(5, 0) // 5 columns, PK at col 0
	schema.Name = "test_manifest"
	txn, _ := tae.StartTxn(nil)
	db, err := txn.CreateDatabase("test_db", "", "")
	require.NoError(t, err)
	rel, err := db.CreateRelation(schema)
	require.NoError(t, err)
	_ = rel
	require.NoError(t, txn.Commit(ctx))

	// Look up the table entry from catalog
	txn2, _ := tae.StartTxn(nil)
	dbHdl, err := txn2.GetDatabase("test_db")
	require.NoError(t, err)
	relHdl, err := dbHdl.GetRelationByName("test_manifest")
	require.NoError(t, err)
	tableEntry := relHdl.GetMeta().(*catalog.TableEntry)
	require.NoError(t, txn2.Commit(ctx))

	// Generate manifest
	data, err := GenerateManifest(tableEntry, "/test/data/dir", tae.Runtime.Now())
	require.NoError(t, err)

	var m Manifest
	require.NoError(t, json.Unmarshal(data, &m))

	assert.Equal(t, 1, m.Version)
	assert.Equal(t, "test_db", m.Database)
	assert.Equal(t, "test_manifest", m.Table)
	assert.Equal(t, "/test/data/dir", m.DataDir)
	// MockSchemaAll creates 5 user columns + hidden columns; we skip hidden ones
	assert.True(t, len(m.Columns) > 0, "should have at least one column")
	// No data inserted, so no objects
	assert.Equal(t, 0, len(m.Objects))
	assert.NotNil(t, m.Stats)
	assert.Equal(t, int64(0), m.Stats.TotalRows)

	t.Logf("Manifest JSON:\n%s", string(data))
}

func TestGenerateManifest_WithData(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(5, 0)
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 2
	tae.BindSchema(schema)

	bat := catalog.MockBatch(schema, 20)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)
	tae.CompactBlocks(false)

	txn, rel := tae.GetRelation()
	tableEntry := rel.GetMeta().(*catalog.TableEntry)
	require.NoError(t, txn.Commit(ctx))

	data, err := GenerateManifest(tableEntry, "/test/data", tae.Runtime.Now())
	require.NoError(t, err)

	var m Manifest
	require.NoError(t, json.Unmarshal(data, &m))

	assert.Equal(t, 1, m.Version)
	assert.True(t, len(m.Objects) > 0, "should have at least one object after compact")
	assert.Equal(t, len(m.Objects), m.Stats.TotalObjects)
	assert.True(t, m.Stats.TotalRows > 0, "should have rows")

	for _, obj := range m.Objects {
		assert.NotEmpty(t, obj.Path, "object path should not be empty")
		assert.True(t, obj.Rows > 0, "object rows should be > 0")
		assert.True(t, obj.Blocks > 0, "object blocks should be > 0")
	}

	// Sort key column should be detected (MockSchemaAll(5,0) sets PK at col 0)
	assert.NotEmpty(t, m.SortColumn, "sort column should be set for PK table")

	t.Logf("Manifest with data (%d objects, %d rows):\n%s",
		m.Stats.TotalObjects, m.Stats.TotalRows, string(data))
}

// TestGenerateManifest_EngineClockAheadOfWall reproduces the
// review-5789148073 counterexample on a real engine: the configured engine
// HLC stays ahead of wall time (the same state a wall-clock rollback
// produces), so every committed object carries a commit end above wall
// time and every engine allocation keeps landing above wall time. If the
// manifest snapshot bound came from wall time instead of the engine's own
// timestamp owner, the normal reader would exclude those committed objects
// and export a nonempty table as an empty manifest.
func TestGenerateManifest_EngineClockAheadOfWall(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	// Deterministic ahead-of-wall engine clock, the same fixture injection
	// used by the db/test replay fixtures: no wall dependency, no sleeps.
	opts.Clock = types.NewMockHLCClock(time.Now().Add(time.Hour).UnixNano())
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(5, 0)
	schema.Extra.BlockMaxRows = 10
	schema.Extra.ObjectMaxBlocks = 2
	tae.BindSchema(schema)

	bat := catalog.MockBatch(schema, 20)
	defer bat.Close()
	tae.CreateRelAndAppend(bat, true)
	tae.CompactBlocks(false)

	txn, rel := tae.GetRelation()
	tableEntry := rel.GetMeta().(*catalog.TableEntry)
	require.NoError(t, txn.Commit(ctx))

	// Fixed bound: the manifest takes its snapshot from the engine's own
	// timestamp owner (DB.Runtime.Now), which is the same allocator that
	// stamped the commits above and therefore sits at or above every
	// already-committed object end.
	engineNow := tae.Runtime.Now()
	data, err := GenerateManifest(tableEntry, "/test/data", engineNow)
	require.NoError(t, err)

	var m Manifest
	require.NoError(t, json.Unmarshal(data, &m))
	require.NotEmpty(t, m.Objects,
		"engine-HLC bound: committed objects must stay visible, manifest=%s", string(data))
	require.EqualValues(t, 20, m.Stats.TotalRows,
		"engine-HLC bound: committed rows must stay visible, manifest=%s", string(data))

	// Independent negation, kept as the permanent wall-bound oracle: with
	// the engine HLC ahead of wall time, a wall-time bound sits below the
	// committed object ends and excludes every committed object, which is
	// exactly the empty-manifest-for-a-nonempty-table defect the engine
	// bound exists to prevent.
	wallNow := types.BuildTS(time.Now().UTC().UnixNano(), 0)
	dataWall, err := GenerateManifest(tableEntry, "/test/data", wallNow)
	require.NoError(t, err)

	var mWall Manifest
	require.NoError(t, json.Unmarshal(dataWall, &mWall))
	require.Emptyf(t, mWall.Objects,
		"wall-time bound below ahead-of-wall commit ends hides the committed generation, manifest=%s", string(dataWall))
	require.EqualValues(t, 0, mWall.Stats.TotalRows)
}

func TestInspectManifest(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer tae.Close()

	// Create a test table
	schema := catalog.MockSchemaAll(5, 0)
	schema.Name = "manifest_tbl"
	txn, _ := tae.StartTxn(nil)
	db, err := txn.CreateDatabase("manifest_db", "", "")
	require.NoError(t, err)
	_, err = db.CreateRelation(schema)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctx))

	// Run inspect manifest command
	resp := &cmd_util.InspectResp{}
	inspectCtx := &inspectContext{
		db:   tae,
		args: []string{"manifest", "-t", "manifest_db.manifest_tbl"},
		out:  &strings.Builder{},
		resp: resp,
	}
	RunInspect(ctx, inspectCtx)

	// Verify response payload is valid manifest JSON
	require.NotEmpty(t, resp.Payload, "manifest payload should not be empty")
	var m Manifest
	require.NoError(t, json.Unmarshal(resp.Payload, &m))
	assert.Equal(t, 1, m.Version)
	assert.Equal(t, "manifest_db", m.Database)
	assert.Equal(t, "manifest_tbl", m.Table)
	assert.True(t, len(m.Columns) > 0)

	t.Logf("Inspect manifest response:\n%s", string(resp.Payload))
}

func TestManifestHTTP(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.InitTestDB(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(5, 0)
	schema.Name = "http_tbl"
	txn, _ := tae.StartTxn(nil)
	db, err := txn.CreateDatabase("http_db", "", "")
	require.NoError(t, err)
	_, err = db.CreateRelation(schema)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctx))

	handler := manifestHTTPHandler(tae)

	// Success case
	req := httptest.NewRequest("GET", "/debug/tae/manifest?table=http_db.http_tbl", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))

	var m Manifest
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &m))
	assert.Equal(t, "http_db", m.Database)
	assert.Equal(t, "http_tbl", m.Table)

	// Missing table parameter
	req = httptest.NewRequest("GET", "/debug/tae/manifest", nil)
	w = httptest.NewRecorder()
	handler(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)

	// Non-existent table
	req = httptest.NewRequest("GET", "/debug/tae/manifest?table=noexist.noexist", nil)
	w = httptest.NewRecorder()
	handler(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)

	// Non-existent database (distinct from non-existent table)
	req = httptest.NewRequest("GET", "/debug/tae/manifest?table=no_such_db.some_tbl", nil)
	w = httptest.NewRecorder()
	handler(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)

	// Existing database, non-existent table
	req = httptest.NewRequest("GET", "/debug/tae/manifest?table=http_db.no_such_tbl", nil)
	w = httptest.NewRecorder()
	handler(w, req)
	assert.Equal(t, http.StatusNotFound, w.Code)

	// Invalid table parameter format (no dot separator)
	req = httptest.NewRequest("GET", "/debug/tae/manifest?table=nodot", nil)
	w = httptest.NewRecorder()
	handler(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestManifestHTTPActiveGenerationLifecycle(t *testing.T) {
	activeManifestDB.Store(nil)
	ctx := context.Background()

	req := httptest.NewRequest("GET", "/debug/tae/manifest?table=gen_db.gen_tbl", nil)
	w := httptest.NewRecorder()
	activeManifestHTTPHandler(w, req)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)

	tae1 := testutil.InitTestDB(ctx, ModuleName, t, config.WithLongScanAndCKPOpts(nil))
	createManifestHTTPTable(t, ctx, tae1, "gen_db", "gen_tbl")
	RegisterManifestHTTP(tae1)

	w = httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	tae2 := testutil.InitTestDB(ctx, ModuleName, t, config.WithLongScanAndCKPOpts(nil))
	createManifestHTTPTable(t, ctx, tae2, "gen_db2", "gen_tbl2")
	RegisterManifestHTTP(tae2)
	require.NoError(t, tae1.Close())
	UnregisterManifestHTTP(tae1)

	req = httptest.NewRequest("GET", "/debug/tae/manifest?table=gen_db2.gen_tbl2", nil)
	w = httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	UnregisterManifestHTTP(tae2)
	req = httptest.NewRequest("GET", "/debug/tae/manifest?table=gen_db2.gen_tbl2", nil)
	w = httptest.NewRecorder()
	http.DefaultServeMux.ServeHTTP(w, req)
	require.Equal(t, http.StatusServiceUnavailable, w.Code)
	require.NoError(t, tae2.Close())
}

func TestManifestHTTPClosedDBDoesNotPanic(t *testing.T) {
	ctx := context.Background()

	tae := testutil.InitTestDB(ctx, ModuleName, t, config.WithLongScanAndCKPOpts(nil))
	createManifestHTTPTable(t, ctx, tae, "closed_db", "closed_tbl")
	handler := manifestHTTPHandler(tae)
	require.NoError(t, tae.Close())

	req := httptest.NewRequest("GET", "/debug/tae/manifest?table=closed_db.closed_tbl", nil)
	w := httptest.NewRecorder()
	require.NotPanics(t, func() {
		handler(w, req)
	})
	require.Equal(t, http.StatusInternalServerError, w.Code)
}

func createManifestHTTPTable(t *testing.T, ctx context.Context, tae *db.DB, dbName, tableName string) {
	schema := catalog.MockSchemaAll(5, 0)
	schema.Name = tableName
	txn, err := tae.StartTxn(nil)
	require.NoError(t, err)
	dbHdl, err := txn.CreateDatabase(dbName, "", "")
	require.NoError(t, err)
	_, err = dbHdl.CreateRelation(schema)
	require.NoError(t, err)
	require.NoError(t, txn.Commit(ctx))
}

func visibleDataObjects(t *testing.T, table *catalog.TableEntry) []*catalog.ObjectEntry {
	t.Helper()
	it := table.MakeDataVisibleObjectIt(txnbase.MockTxnReaderWithNow())
	defer it.Release()
	objs := make([]*catalog.ObjectEntry, 0)
	for it.Next() {
		objs = append(objs, it.Item())
	}
	return objs
}

// manifestReadOutcome is produced by reader goroutines; the test goroutine
// performs every assertion.
type manifestReadOutcome struct {
	manifest Manifest
	err      error
}

// readManifestOf runs the real manifest producer and decodes it without any
// testify dependency, so it is safe to call from reader goroutines.
func readManifestOf(table *catalog.TableEntry, dataDir string, now types.TS) manifestReadOutcome {
	data, err := GenerateManifestPretty(table, dataDir, now)
	if err != nil {
		return manifestReadOutcome{err: err}
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return manifestReadOutcome{err: err}
	}
	return manifestReadOutcome{manifest: m}
}

func manifestOf(t *testing.T, table *catalog.TableEntry, now types.TS) Manifest {
	t.Helper()
	outcome := readManifestOf(table, "/test/data", now)
	require.NoError(t, outcome.err)
	return outcome.manifest
}

// requirePKsReadable verifies the exact committed primary-key content behind
// the manifest through the real engine read path. CheckRowsByScan only
// counts rows over all columns; this narrow check pins the actual written
// key set.
func requirePKsReadable(t *testing.T, tae *testutil.TestEngine, expectedPKs []any) {
	t.Helper()
	ctx := context.Background()
	txn, rel := tae.GetRelation()
	defer func() {
		require.NoError(t, txn.Commit(ctx))
	}()
	for _, pk := range expectedPKs {
		value, isNull, err := rel.GetValueByFilter(ctx, handle.NewEQFilter(pk), 0)
		require.NoError(t, err, "expected committed primary key %v to stay readable", pk)
		require.False(t, isNull)
		require.Equal(t, pk, value)
	}
}

func manifestPaths(m Manifest) []string {
	paths := make([]string, 0, len(m.Objects))
	for _, o := range m.Objects {
		paths = append(paths, o.Path)
	}
	sort.Strings(paths)
	return paths
}

// TestGenerateManifest_DuringUncommittedMerge guards #32215: while a real
// physical merge is applied inside an uncommitted transaction — the source
// objects already carry their delete counterpart, the replacement is still
// creating — the manifest must still report one complete committed object
// generation. It can never succeed with an empty object list for a nonempty
// table. The manifest API and its real HTTP consumer must agree before the
// merge, in the merge window, after the merge commits, and after a rolled
// back merge. An empty table keeps its successful empty manifest
// (TestGenerateManifest_Basic).
func TestGenerateManifest_DuringUncommittedMerge(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(5, 0)
	schema.Name = "mf_merge_tbl"
	schema.Extra.BlockMaxRows = 2
	schema.Extra.ObjectMaxBlocks = 2
	tae.BindSchema(schema)

	// Two 2-row generations: each generation lands in its own appendable
	// object and each CompactBlocks call flushes one committed data
	// object, giving two committed objects with 4 rows in total.
	bat1 := catalog.MockBatch(schema, 2)
	defer bat1.Close()
	tae.CreateRelAndAppend(bat1, true)
	tae.CompactBlocks(true)

	bat2 := containers.MockBatchWithAttrsAndOffset(schema.Types(), schema.Attrs(), 2, 2)
	defer bat2.Close()
	tae.DoAppend(bat2)
	tae.CompactBlocks(true)

	// The exact committed primary-key set written by the two batches, used
	// for the narrow content checks below. The mock int8 sort key starts
	// unique rows at math.MinInt8 and the offset batch appends its shifted
	// keys; values are captured from the batches themselves.
	expectedPKs := []any{
		bat1.Vecs[0].Get(0), bat1.Vecs[0].Get(1),
		bat2.Vecs[0].Get(0), bat2.Vecs[0].Get(1),
	}

	rtxn, rrel := tae.GetRelation()
	tableEntry := rrel.GetMeta().(*catalog.TableEntry)
	require.NoError(t, rtxn.Commit(ctx))
	tae.CheckRowsByScan(4, true)

	sources := visibleDataObjects(t, tableEntry)
	require.Len(t, sources, 2, "fixture requires two committed data objects")

	before := manifestOf(t, tableEntry, tae.Runtime.Now())
	require.Len(t, before.Objects, 2)
	require.EqualValues(t, 4, before.Stats.TotalRows)

	// Apply a real merge over both objects inside a dedicated transaction
	// without committing it: this is the window of #32215.
	mtxn, _ := tae.GetRelation()
	task, err := jobs.NewMergeObjectsTask(nil, mtxn, sources, tae.Runtime, 0, false)
	require.NoError(t, err)
	require.NoError(t, task.OnExec(ctx))
	mergeCommitted := false
	defer func() {
		if !mergeCommitted {
			require.NoError(t, mtxn.Rollback(ctx))
		}
	}()

	// During the merge the manifest must carry one complete committed
	// generation: the two unmerged sources with their 4 rows. It must not
	// report a successful empty manifest.
	during := manifestOf(t, tableEntry, tae.Runtime.Now())
	require.NotEmpty(t, during.Objects,
		"manifest must not succeed empty for a nonempty table during a physical merge")
	require.EqualValues(t, 2, len(during.Objects))
	require.Equal(t, manifestPaths(before), manifestPaths(during))
	require.EqualValues(t, 4, during.Stats.TotalRows)

	// The real HTTP consumer must agree inside the merge window.
	handler := manifestHTTPHandler(tae.DB)
	req := httptest.NewRequest("GET", "/debug/tae/manifest?table=db.mf_merge_tbl", nil)
	w := httptest.NewRecorder()
	handler(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var httpManifest Manifest
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &httpManifest))
	require.EqualValues(t, 4, httpManifest.Stats.TotalRows)
	require.Equal(t, manifestPaths(during), manifestPaths(httpManifest))

	// CheckRowsByScan only counts rows over all columns; the narrow PK
	// check pins the exact committed content inside the merge window.
	requirePKsReadable(t, tae, expectedPKs)
	tae.CheckRowsByScan(4, true)

	// Committing the merge publishes exactly one replacement object.
	mergeCommitted = true
	require.NoError(t, mtxn.Commit(ctx))

	after := manifestOf(t, tableEntry, tae.Runtime.Now())
	require.Len(t, after.Objects, 1)
	require.EqualValues(t, 4, after.Stats.TotalRows)
	requirePKsReadable(t, tae, expectedPKs)

	// A second, rolled back merge must not tear the committed generation.
	committed := visibleDataObjects(t, tableEntry)
	require.Len(t, committed, 1)
	mtxn2, _ := tae.GetRelation()
	task2, err := jobs.NewMergeObjectsTask(nil, mtxn2, committed, tae.Runtime, 0, false)
	require.NoError(t, err)
	require.NoError(t, task2.OnExec(ctx))
	require.NoError(t, mtxn2.Rollback(ctx))

	afterRollback := manifestOf(t, tableEntry, tae.Runtime.Now())
	require.Len(t, afterRollback.Objects, 1)
	require.EqualValues(t, 4, afterRollback.Stats.TotalRows)
	require.Equal(t, manifestPaths(after), manifestPaths(afterRollback))
	tae.CheckRowsByScan(4, true)
}

// TestGenerateManifest_DuringCommittingMerge pins a real merge transaction
// inside its commit window using the established SetPrepareCommitFn txn hook.
// The hook delegates the real txn.GetStore().PrepareCommit first and then
// holds the pipeline on an event; when it runs, the commit pipeline has
// already left the active state with its prepare timestamp assigned
// (ToPreparingLocked), which is precisely the committing window the object
// iterator's normal MVCC branch waits on through NeedWaitCommitting ->
// GetTxnState(true). While the merge commit is pinned, both real consumers
// — the manifest API and the HTTP handler — start a read, and a
// whole-process goroutine-stack witness verifies that BOTH paths are
// durably blocked inside GenerateManifest -> GetTxnState(true) ->
// resolveTxnState -> DoneCond.Wait (actual waiting frames; a truncated
// snapshot fails the bound) before the pin is released by event; no sleep,
// fake catalog state, or repetition can satisfy the witness. The readers then resolve at or after the window and
// must report one complete committed generation: the single published
// replacement object with all 4 rows and a path distinct from both sources —
// never an empty, partial, or doubled manifest. A release-once, bounded-join
// cleanup owner installed before any commit/reader goroutine guards every
// failure and timeout path (rolling the still-active owned merge
// transaction back on pre-launch failures and always releasing and joining
// started workers before the engine closes); worker goroutines make no
// testify calls.
func TestGenerateManifest_DuringCommittingMerge(t *testing.T) {
	ctx := context.Background()

	opts := config.WithLongScanAndCKPOpts(nil)
	tae := testutil.NewTestEngine(ctx, ModuleName, t, opts)
	defer tae.Close()

	schema := catalog.MockSchemaAll(5, 0)
	schema.Name = "mf_committing_tbl"
	schema.Extra.BlockMaxRows = 2
	schema.Extra.ObjectMaxBlocks = 2
	tae.BindSchema(schema)

	bat1 := catalog.MockBatch(schema, 2)
	defer bat1.Close()
	tae.CreateRelAndAppend(bat1, true)
	tae.CompactBlocks(true)

	bat2 := containers.MockBatchWithAttrsAndOffset(schema.Types(), schema.Attrs(), 2, 2)
	defer bat2.Close()
	tae.DoAppend(bat2)
	tae.CompactBlocks(true)

	expectedPKs := []any{
		bat1.Vecs[0].Get(0), bat1.Vecs[0].Get(1),
		bat2.Vecs[0].Get(0), bat2.Vecs[0].Get(1),
	}

	rtxn, rrel := tae.GetRelation()
	tableEntry := rrel.GetMeta().(*catalog.TableEntry)
	require.NoError(t, rtxn.Commit(ctx))
	tae.CheckRowsByScan(4, true)

	sources := visibleDataObjects(t, tableEntry)
	require.Len(t, sources, 2, "fixture requires two committed data objects")
	before := manifestOf(t, tableEntry, tae.Runtime.Now())
	require.Len(t, before.Objects, 2)
	require.EqualValues(t, 4, before.Stats.TotalRows)

	// Apply the real merge fully inside its own transaction.
	mtxn, _ := tae.GetRelation()
	task, err := jobs.NewMergeObjectsTask(nil, mtxn, sources, tae.Runtime, 0, false)
	require.NoError(t, err)
	require.NoError(t, task.OnExec(ctx))

	// T1: prepare every channel and install the single release-once,
	// bounded-join cleanup owner BEFORE starting the commit or any reader
	// goroutine. On every failure path it releases the pinned commit
	// exactly once (even if the hook reaches the pin late or never),
	// bound-joins every started worker before the engine closes, and —
	// when nothing async was launched yet — rolls the still-active owned
	// merge transaction back instead of abandoning it to the engine close.
	pinOpened := make(chan struct{})
	release := make(chan struct{})
	commitDone := make(chan error, 1)
	readerStarted := make(chan struct{})
	readerDone := make(chan struct{})
	readerCh := make(chan manifestReadOutcome, 1)
	httpStarted := make(chan struct{})
	httpDone := make(chan struct{})
	httpCh := make(chan manifestReadOutcome, 1)

	var releaseOnce sync.Once
	doRelease := func() { releaseOnce.Do(func() { close(release) }) }

	commitLaunched := false
	commitObserved := false
	readersLaunched := 0
	defer func() {
		if !commitLaunched {
			// Nothing async was started against the merge txn: it is
			// still active, and its owner rolls it back here — never
			// concurrently against an active commit.
			_ = mtxn.Rollback(ctx)
			return
		}
		// Always release the pinned commit, then bound-join every started
		// worker before the engine closes. The commit result is only
		// awaited when the main flow has not consumed it itself.
		doRelease()
		if !commitObserved {
			select {
			case <-commitDone:
			case <-time.After(31 * time.Second):
				t.Errorf("pinned merge commit did not resolve within bound")
			}
		}
		if readersLaunched >= 1 {
			select {
			case <-readerDone:
			case <-time.After(31 * time.Second):
				t.Errorf("manifest reader did not resolve within bound")
			}
		}
		if readersLaunched >= 2 {
			select {
			case <-httpDone:
			case <-time.After(31 * time.Second):
				t.Errorf("manifest HTTP reader did not resolve within bound")
			}
		}
	}()

	// Pin the merge inside its commit window: delegate the real store
	// preparation, then hold the pipeline on the release event. When this
	// callback runs, the commit pipeline has already left the active state
	// with its prepare timestamp assigned.
	mtxn.SetPrepareCommitFn(func(txn txnif.AsyncTxn) error {
		if err := txn.GetStore().PrepareCommit(); err != nil {
			return err
		}
		close(pinOpened)
		<-release
		return nil
	})

	go func() { commitDone <- mtxn.Commit(ctx) }()
	commitLaunched = true
	select {
	case <-pinOpened:
	case <-time.After(30 * time.Second):
		t.Fatal("merge commit never reached its pinned prepare-commit window")
	}

	// Both real consumers run inside the pinned window, launched only after
	// the pin is observed. The goroutines only produce outcomes; every
	// assertion stays on the test goroutine.
	go func() {
		defer close(readerDone)
		close(readerStarted)
		readerCh <- readManifestOf(tableEntry, "/test/data", tae.Runtime.Now())
	}()
	readersLaunched = 1
	go func() {
		defer close(httpDone)
		close(httpStarted)
		req := httptest.NewRequest("GET", "/debug/tae/manifest?table=db.mf_committing_tbl", nil)
		rec := httptest.NewRecorder()
		manifestHTTPHandler(tae.DB)(rec, req)
		if rec.Code != http.StatusOK {
			httpCh <- manifestReadOutcome{err: fmt.Errorf("manifest HTTP status = %d, want %d", rec.Code, http.StatusOK)}
			return
		}
		var m Manifest
		if err := json.Unmarshal(rec.Body.Bytes(), &m); err != nil {
			httpCh <- manifestReadOutcome{err: err}
			return
		}
		httpCh <- manifestReadOutcome{manifest: m}
	}()
	readersLaunched = 2

	select {
	case <-readerStarted:
	case <-time.After(30 * time.Second):
		t.Fatal("manifest reader never started within bound")
	}
	select {
	case <-httpStarted:
	case <-time.After(30 * time.Second):
		t.Fatal("manifest HTTP reader never started within bound")
	}

	// T2: hold the pin until BOTH real reader paths are durably observed
	// blocked inside the normal-branch MVCC wait —
	// GenerateManifest -> iterator -> ObjectEntry.IsVisible ->
	// NeedWaitCommitting -> GetTxnState(true) -> resolveTxnState ->
	// DoneCond.Wait. The predicate matches the actual waiting frames in a
	// whole-process snapshot while the prepare pin is still held; a
	// snapshot that fills the entire fixed 4MiB buffer is truncated and
	// fails toward the bound — never a partial PASS; no sleep, fake
	// catalog state, or repetition can satisfy the witness.
	stackBuf := make([]byte, 4<<20)
	require.Eventually(t, func() bool {
		n := runtime.Stack(stackBuf, true)
		if n == len(stackBuf) {
			// runtime.Stack filled the fixed buffer: this snapshot is
			// truncated and must never count as a witness. The buffer
			// stays at 4MiB (no unbounded growth); persistent truncation
			// fails the bound instead of passing a partial observation.
			return false
		}
		return countReadersBlockedInMVCCWait(string(stackBuf[:n])) == 2
	}, 10*time.Second, 2*time.Millisecond,
		"both real consumers must be observed durably blocked in GenerateManifest->GetTxnState(true)->resolveTxnState->Cond.Wait while the merge commit is pinned")

	// Witness satisfied: release the pinned commit by event.
	doRelease()

	select {
	case err := <-commitDone:
		commitObserved = true
		require.NoError(t, err)
	case <-time.After(30 * time.Second):
		t.Fatal("pinned merge commit did not resolve within bound")
	}
	select {
	case <-readerDone:
	case <-time.After(30 * time.Second):
		t.Fatal("manifest reader did not resolve within bound")
	}
	select {
	case <-httpDone:
	case <-time.After(30 * time.Second):
		t.Fatal("manifest HTTP reader did not resolve within bound")
	}

	res := <-readerCh
	httpRes := <-httpCh
	require.NoError(t, res.err)
	require.NoError(t, httpRes.err)

	// Reads resolving at or after the pinned window must report exactly the
	// published generation, and both consumers must agree.
	for name, m := range map[string]Manifest{"manifest": res.manifest, "http": httpRes.manifest} {
		require.EqualValues(t, 1, len(m.Objects), "%s: exactly one published object", name)
		require.EqualValues(t, 4, m.Stats.TotalRows, "%s: all four rows", name)
		require.EqualValues(t, 4, m.Objects[0].Rows, "%s: all four rows on the single object", name)
	}
	require.Equal(t, manifestPaths(res.manifest), manifestPaths(httpRes.manifest))
	require.NotEqual(t, manifestPaths(before), manifestPaths(res.manifest))

	// CheckRowsByScan only counts rows; the narrow PK check pins the exact
	// committed content of the published generation.
	requirePKsReadable(t, tae, expectedPKs)
	tae.CheckRowsByScan(4, true)

	after := manifestOf(t, tableEntry, tae.Runtime.Now())
	require.EqualValues(t, 1, len(after.Objects))
	require.EqualValues(t, 4, after.Stats.TotalRows)
	require.Equal(t, manifestPaths(res.manifest), manifestPaths(after))
}

// countReadersBlockedInMVCCWait counts goroutines durably blocked inside the
// real manifest read path's normal-branch MVCC WAIT:
// rpc.GenerateManifest -> iterator -> ObjectEntry.IsVisible ->
// NeedWaitCommitting -> GetTxnState(true) -> resolveTxnState ->
// sync.(*Cond).Wait on the txn DoneCond. A goroutine section counts only
// when the manifest frame, the txnbase GetTxnState frame, the true-wait-only
// resolveTxnState frame, and the actually parked cond marker (a
// sync.(*Cond).Wait frame or a "[sync.Cond.Wait" goroutine state) are ALL
// present. The reachable non-waiting fast path
// (NeedWaitCommitting -> IsCommitting -> GetTxnState(false), mvccnode.go
// IsCommitting) and unrelated cond waits therefore never match. This is a
// direct observation of durable waiting, not a timing guess and not a
// production-code hook; callers fail toward their bound whenever
// runtime.Stack reports a truncated snapshot.
func countReadersBlockedInMVCCWait(dump string) int {
	count := 0
	for _, g := range strings.Split(dump, "\n\ngoroutine ") {
		if !strings.Contains(g, "rpc.GenerateManifest") ||
			!strings.Contains(g, "GetTxnState") ||
			!strings.Contains(g, "resolveTxnState") ||
			!(strings.Contains(g, "sync.(*Cond).Wait") || strings.Contains(g, "[sync.Cond.Wait")) {
			continue
		}
		count++
	}
	return count
}

// TestCountReadersBlockedInMVCCWaitOracle pins the tightened witness
// predicate on synthetic runtime.Stack-shaped sections without any engine
// or goroutine orchestration: the non-waiting
// NeedWaitCommitting -> IsCommitting -> GetTxnState(false) fast path inside
// a real manifest read (the leak shape of the previous matcher) must not
// count, an unrelated sync.Cond wait must not count, and only the durable
// true-wait shape (manifest -> GetTxnState -> resolveTxnState ->
// sync.Cond.Wait) counts. The real positive stays in the engine case
// TestGenerateManifest_DuringCommittingMerge, which still requires exactly
// two durably blocked readers before releasing the pinned commit.
func TestCountReadersBlockedInMVCCWaitOracle(t *testing.T) {
	pkg := "github.com/matrixorigin/matrixone/pkg/vm/engine/tae/"

	nonWaitingFastPath := "goroutine 11 [runnable]:\n" +
		pkg + "rpc.GenerateManifest(0x40)\n" +
		pkg + "catalog.(*ObjectEntry).IsVisible(0x41)\n" +
		pkg + "txn/txnbase.(*TxnMVCCNode).NeedWaitCommitting(0x42)\n" +
		pkg + "txn/txnbase.(*TxnMVCCNode).IsCommitting(0x43)\n" +
		pkg + "txn/txnbase.(*TxnCtx).GetTxnState(0x44)\n"

	unrelatedCondWait := "goroutine 12 [sync.Cond.Wait, 0 minutes]:\n" +
		"github.com/matrixorigin/matrixone/pkg/other.(*Watcher).waitForSomething(0x50)\n" +
		"sync.(*Cond).Wait(0x51)\n"

	trueWait := "goroutine 13 [sync.Cond.Wait, 0 minutes]:\n" +
		pkg + "rpc.GenerateManifest(0x60)\n" +
		pkg + "catalog.(*ObjectEntry).IsVisible(0x61)\n" +
		pkg + "txn/txnbase.(*TxnMVCCNode).NeedWaitCommitting(0x62)\n" +
		pkg + "txn/txnbase.(*TxnCtx).GetTxnState(0x63)\n" +
		pkg + "txn/txnbase.(*TxnCtx).resolveTxnState(0x64)\n" +
		"sync.(*Cond).Wait(0x65)\n"

	assert.Equal(t, 0, countReadersBlockedInMVCCWait(nonWaitingFastPath),
		"the non-waiting IsCommitting->GetTxnState(false) fast path inside a manifest read must not count")
	assert.Equal(t, 0, countReadersBlockedInMVCCWait(unrelatedCondWait),
		"an unrelated sync.Cond wait must not count")
	assert.Equal(t, 1, countReadersBlockedInMVCCWait(trueWait),
		"the durable true-wait shape must count")
	assert.Equal(t, 1, countReadersBlockedInMVCCWait(
		nonWaitingFastPath+"\n\n"+unrelatedCondWait+"\n\n"+trueWait),
		"a combined whole-process dump counts exactly the one durable true-wait reader")
}
