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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/vm/engine/cmd_util"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/testutils/config"
)

func TestGenerateManifest_Basic(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
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
	data, err := GenerateManifest(tableEntry, "/test/data/dir")
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
	assert.Equal(t, 0, m.Stats.TotalRows)

	t.Logf("Manifest JSON:\n%s", string(data))
}

func TestGenerateManifest_WithData(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
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

	data, err := GenerateManifest(tableEntry, "/test/data")
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

func TestInspectManifest(t *testing.T) {
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
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
	defer testutils.AfterTest(t)()
	testutils.EnsureNoLeak(t)
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
}
