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

// Unit coverage for handleCreate's sync/async branches and the two SQL generators.
// recordingCtx extends the smoke stubCtx with SQL/CDC bookkeeping.
package compile

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type recordingCtx struct {
	stubCtx

	experimental bool
	expErr       error

	sqls        []string
	dropped     int
	created     int
	startFromTS []bool
}

func newRecordingCtx() *recordingCtx {
	return &recordingCtx{experimental: true}
}

func (r *recordingCtx) QryDatabase() string { return "db" }

func (r *recordingCtx) OriginalTableDef() *plan.TableDef {
	return &plan.TableDef{
		Name:  "src",
		TblId: 42,
		Pkey:  &plan.PrimaryKeyDef{PkeyColName: "id"},
	}
}

func (r *recordingCtx) IsExperimentalEnabled(_ string) (bool, error) {
	return r.experimental, r.expErr
}

func (r *recordingCtx) RunSql(sql string) error {
	r.sqls = append(r.sqls, sql)
	return nil
}

func (r *recordingCtx) DropIndexCdcTask(_ *plan.TableDef, _, _, _ string) error {
	r.dropped++
	return nil
}

func (r *recordingCtx) CreateIndexCdcTask(_, _ string, _ uint64, _ string, _ int8, startFromNow bool, _ string, _ *plan.TableDef) error {
	r.created++
	r.startFromTS = append(r.startFromTS, startFromNow)
	return nil
}

// hnswDefs returns the metadata+storage pair handleCreate expects; async=="" is
// synchronous.
func hnswDefs(async string) map[string]*plan.IndexDef {
	params := ""
	if async != "" {
		params = `{"` + catalog.Async + `":"` + async + `"}`
	}
	return map[string]*plan.IndexDef{
		catalog.Hnsw_TblType_Metadata: {
			IndexName:       "idx",
			IndexTableName:  "__mo_meta",
			IndexAlgoParams: params,
			Parts:           []string{"vec"},
		},
		catalog.Hnsw_TblType_Storage: {
			IndexName:      "idx",
			IndexTableName: "__mo_store",
			Parts:          []string{"vec"},
		},
	}
}

// --- handleCreate branches -------------------------------------------------

// A synchronous index builds inline, then re-registers CDC from now.
func TestHandleCreateIndex_Sync(t *testing.T) {
	ctx := newRecordingCtx()
	ctx.isFrontend = true
	require.NoError(t, Hooks{}.HandleCreateIndex(ctx, hnswDefs("")))
	require.Len(t, ctx.sqls, 1)
	require.Contains(t, ctx.sqls[0], "hnsw_create(")
	require.Equal(t, 1, ctx.dropped, "the prior task is dropped before recreating")
	require.Equal(t, []bool{true}, ctx.startFromTS, "sync builds consume from now")
}

// An async index issues the DELETEs itself and registers CDC from the table's
// creation timestamp.
func TestHandleCreateIndex_Async(t *testing.T) {
	ctx := newRecordingCtx()
	ctx.isFrontend = true
	require.NoError(t, Hooks{}.HandleCreateIndex(ctx, hnswDefs("true")))
	require.Len(t, ctx.sqls, 2, "one DELETE per hidden table")
	for _, sql := range ctx.sqls {
		require.True(t, strings.HasPrefix(sql, "DELETE FROM "), sql)
	}
	require.Equal(t, []bool{false}, ctx.startFromTS, "async consumes the full log")
}

// FORCE_SYNC rebuilds an always-async index inline.
func TestHandleReindex_ForceSyncOverridesAsync(t *testing.T) {
	ctx := newRecordingCtx()
	ctx.isFrontend = true
	require.NoError(t, Hooks{}.HandleReindex(ctx, hnswDefs("true"), true, false))
	require.Len(t, ctx.sqls, 1)
	require.Contains(t, ctx.sqls[0], "hnsw_create(")
	require.Equal(t, []bool{true}, ctx.startFromTS)
}

// The experimental gate is frontend-only; a background re-entry skips it.
func TestHandleCreateIndex_ExperimentalGate(t *testing.T) {
	off := newRecordingCtx()
	off.isFrontend = true
	off.experimental = false
	require.Error(t, Hooks{}.HandleCreateIndex(off, hnswDefs("")))

	bg := newRecordingCtx()
	bg.isFrontend = false
	bg.experimental = false
	require.NoError(t, Hooks{}.HandleCreateIndex(bg, hnswDefs("")))
}

func TestHandleCreateIndex_BadDefs(t *testing.T) {
	t.Run("wrong count", func(t *testing.T) {
		defs := hnswDefs("")
		delete(defs, catalog.Hnsw_TblType_Storage)
		require.Error(t, Hooks{}.HandleCreateIndex(newRecordingCtx(), defs))
	})
	t.Run("metadata missing", func(t *testing.T) {
		defs := hnswDefs("")
		defs["other"] = defs[catalog.Hnsw_TblType_Metadata]
		delete(defs, catalog.Hnsw_TblType_Metadata)
		require.Error(t, Hooks{}.HandleCreateIndex(newRecordingCtx(), defs))
	})
	t.Run("storage missing", func(t *testing.T) {
		defs := hnswDefs("")
		defs["other"] = defs[catalog.Hnsw_TblType_Storage]
		delete(defs, catalog.Hnsw_TblType_Storage)
		require.Error(t, Hooks{}.HandleCreateIndex(newRecordingCtx(), defs))
	})
	t.Run("multi-column metadata", func(t *testing.T) {
		defs := hnswDefs("")
		defs[catalog.Hnsw_TblType_Metadata].Parts = []string{"a", "b"}
		require.Error(t, Hooks{}.HandleCreateIndex(newRecordingCtx(), defs))
	})
	// A non-string async value errors; a missing key defaults to synchronous.
	t.Run("non-string async param", func(t *testing.T) {
		defs := hnswDefs("")
		defs[catalog.Hnsw_TblType_Metadata].IndexAlgoParams = `{"` + catalog.Async + `":1}`
		require.Error(t, Hooks{}.HandleCreateIndex(newRecordingCtx(), defs))
	})
}

// --- RestoreInitSQL --------------------------------------------------------

// RestoreInitSQL emits an ALTER REINDEX ... FORCE_SYNC.
func TestRestoreInitSQL(t *testing.T) {
	ok, sql, err := Hooks{}.RestoreInitSQL(newRecordingCtx(), hnswDefs(""))
	require.NoError(t, err)
	require.True(t, ok)
	require.Contains(t, sql, "ALTER REINDEX")
	require.Contains(t, sql, "FORCE_SYNC")

	defs := hnswDefs("")
	delete(defs, catalog.Hnsw_TblType_Metadata)
	_, _, err = Hooks{}.RestoreInitSQL(newRecordingCtx(), defs)
	require.Error(t, err)
}

// --- SQL generators --------------------------------------------------------

func TestGenDeleteSQL(t *testing.T) {
	sqls, err := genDeleteSQL(hnswDefs(""), "db")
	require.NoError(t, err)
	require.Equal(t, []string{
		"DELETE FROM `db`.`__mo_meta`",
		"DELETE FROM `db`.`__mo_store`",
	}, sqls)

	for _, missing := range []string{catalog.Hnsw_TblType_Metadata, catalog.Hnsw_TblType_Storage} {
		defs := hnswDefs("")
		delete(defs, missing)
		_, err := genDeleteSQL(defs, "db")
		require.Error(t, err)
	}
}

// The build statement CROSS APPLYs hnsw_create over the source table with the quoted
// pk and vector columns.
func TestGenBuildSQL(t *testing.T) {
	sqls, err := genBuildSQL(newRecordingCtx(), hnswDefs(""))
	require.NoError(t, err)
	require.Len(t, sqls, 1)
	require.Contains(t, sqls[0], "hnsw_create(")
	require.Contains(t, sqls[0], "`db`.`src`")
	require.Contains(t, sqls[0], "`src`.`id`")
	require.Contains(t, sqls[0], "`src`.`vec`")
	require.Contains(t, sqls[0], `"metadata":"__mo_meta"`)
	require.Contains(t, sqls[0], `"index":"__mo_store"`)

	for _, missing := range []string{catalog.Hnsw_TblType_Metadata, catalog.Hnsw_TblType_Storage} {
		defs := hnswDefs("")
		delete(defs, missing)
		_, err := genBuildSQL(newRecordingCtx(), defs)
		require.Error(t, err)
	}
}
