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

package compile

import (
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/util/executor"
	"github.com/matrixorigin/matrixone/pkg/vm/engine"
	"github.com/stretchr/testify/require"
)

// stubCompileContext implements compileplugin.CompileContext for genBuildSQL
// + the trivial hook tests. Methods not needed by the tests panic when called.
type stubCompileContext struct {
	originalTableDef *plan.TableDef
	qryDatabase      string
	vars             map[string]any
	isFrontend       bool

	// lastCdcTask records the args of the most recent
	// CreateIndexCdcTask call. Used by HandleCreateIndex_Async{True,
	// False} to assert the right branch (startFromNow / sql InitSQL)
	// fired.
	lastCdcTask struct {
		called       bool
		startFromNow bool
		sql          string
	}

	// lastIdxcronUpdate records the args of the most recent
	// RegisterIdxcronUpdate call — pins that handleCreate writes the
	// cron metadata row after CreateIndexCdcTask succeeds, and that
	// background re-entry skips the call.
	lastIdxcronUpdate struct {
		called      bool
		tableID     uint64
		dbName      string
		tableName   string
		indexName   string
		action      string
		metadataLen int
	}
}

func (s *stubCompileContext) Ctx() compileplugin.Context       { return nil }
func (s *stubCompileContext) Database() engine.Database        { return nil }
func (s *stubCompileContext) QryDatabase() string              { return s.qryDatabase }
func (s *stubCompileContext) OriginalTableDef() *plan.TableDef { return s.originalTableDef }
func (s *stubCompileContext) IndexInfo() *plan.CreateTable     { return nil }
func (s *stubCompileContext) MainTableID() uint64              { return 0 }
func (s *stubCompileContext) MainExtra() *api.SchemaExtra      { return nil }
func (s *stubCompileContext) RunSql(_ string) error            { return nil }
func (s *stubCompileContext) BuildIndexTable(_ *plan.TableDef) error {
	return nil
}
func (s *stubCompileContext) ResolveVariable(name string, _, _ bool) (any, error) {
	if v, ok := s.vars[name]; ok {
		return v, nil
	}
	return int64(0), nil
}
func (s *stubCompileContext) IsExperimentalEnabled(_ string) (bool, error) { return true, nil }
func (s *stubCompileContext) IsFrontend() bool                             { return s.isFrontend }
func (s *stubCompileContext) IsCCPRTaskTransaction() bool                  { return false }
func (s *stubCompileContext) IsTableFromPublication(_ *plan.TableDef) bool { return false }
func (s *stubCompileContext) SinkerTypeFromAlgo(_ string) int8             { return 0 }
func (s *stubCompileContext) CreateIndexCdcTask(_, _ string, _ uint64, _ string, _ int8, startFromNow bool, sql string, _ *plan.TableDef) error {
	s.lastCdcTask.called = true
	s.lastCdcTask.startFromNow = startFromNow
	s.lastCdcTask.sql = sql
	return nil
}
func (s *stubCompileContext) DropIndexCdcTask(_ *plan.TableDef, _, _, _ string) error {
	return nil
}
func (s *stubCompileContext) RunSqlWithResult(_ string) (executor.Result, error) {
	return executor.Result{}, nil
}
func (s *stubCompileContext) RegisterIdxcronUpdate(tableID uint64, dbName, tableName, indexName, action string, metadata []byte) error {
	s.lastIdxcronUpdate.called = true
	s.lastIdxcronUpdate.tableID = tableID
	s.lastIdxcronUpdate.dbName = dbName
	s.lastIdxcronUpdate.tableName = tableName
	s.lastIdxcronUpdate.indexName = indexName
	s.lastIdxcronUpdate.action = action
	s.lastIdxcronUpdate.metadataLen = len(metadata)
	return nil
}

func ivfpqIndexDefs() map[string]*plan.IndexDef {
	return map[string]*plan.IndexDef{
		catalog.Ivfpq_TblType_Metadata: {
			IndexName:      "ix",
			IndexTableName: "__mo_meta_001",
			Parts:          []string{"v"},
		},
		catalog.Ivfpq_TblType_Storage: {
			IndexName:       "ix",
			IndexTableName:  "__mo_idx_001",
			Parts:           []string{"v"},
			IndexAlgoParams: `{"op_type":"vector_l2_ops"}`,
		},
	}
}

func TestIvfpqGenDeleteSQL(t *testing.T) {
	defs := ivfpqIndexDefs()
	sqls, err := genDeleteSQL(defs, "db1")
	require.NoError(t, err)
	require.Len(t, sqls, 2)
	require.Contains(t, sqls[0], "DELETE FROM `db1`.`__mo_meta_001`")
	require.Contains(t, sqls[1], "DELETE FROM `db1`.`__mo_idx_001`")
}

func TestIvfpqGenDeleteSQL_MissingMeta(t *testing.T) {
	defs := ivfpqIndexDefs()
	delete(defs, catalog.Ivfpq_TblType_Metadata)
	_, err := genDeleteSQL(defs, "db1")
	require.Error(t, err)
}

func TestIvfpqGenDeleteSQL_MissingStorage(t *testing.T) {
	defs := ivfpqIndexDefs()
	delete(defs, catalog.Ivfpq_TblType_Storage)
	_, err := genDeleteSQL(defs, "db1")
	require.Error(t, err)
}

func TestIvfpqFilterColumnsFromParams_Empty(t *testing.T) {
	require.Equal(t, "", filterColumnsFromParams("", "src"))
	require.Equal(t, "", filterColumnsFromParams(`{"op_type":"vector_l2_ops"}`, "src"))
	require.Equal(t, "", filterColumnsFromParams(`not-json`, "src"))
}

func TestIvfpqFilterColumnsFromParams_OK(t *testing.T) {
	got := filterColumnsFromParams(`{"included_columns":"price, name"}`, "src")
	require.Equal(t, ", src.price, src.name", got)
}

func TestIvfpqFilterColumnsFromParams_SkipsBlank(t *testing.T) {
	got := filterColumnsFromParams(`{"included_columns":"price, ,name"}`, "src")
	require.Equal(t, ", src.price, src.name", got)
}

func TestIvfpqGenBuildSQL_OK(t *testing.T) {
	ctx := &stubCompileContext{
		qryDatabase: "db1",
		originalTableDef: &plan.TableDef{
			Name: "t",
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
		},
		vars: map[string]any{
			"ivfpq_threads_build":      int64(4),
			"ivfpq_max_index_capacity": int64(1024),
		},
	}
	sqls, err := genBuildSQL(ctx, ivfpqIndexDefs())
	require.NoError(t, err)
	require.Len(t, sqls, 1)
	require.True(t, strings.Contains(sqls[0], "ivfpq_create"))
	require.True(t, strings.Contains(sqls[0], "`db1`.`t`"))
}

func TestIvfpqGenBuildSQL_MissingMeta(t *testing.T) {
	defs := ivfpqIndexDefs()
	delete(defs, catalog.Ivfpq_TblType_Metadata)
	ctx := &stubCompileContext{
		qryDatabase: "db1",
		originalTableDef: &plan.TableDef{
			Name: "t",
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
		},
	}
	_, err := genBuildSQL(ctx, defs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ivfpq_meta")
}

func TestIvfpqGenBuildSQL_MissingStorage(t *testing.T) {
	defs := ivfpqIndexDefs()
	delete(defs, catalog.Ivfpq_TblType_Storage)
	ctx := &stubCompileContext{
		qryDatabase: "db1",
		originalTableDef: &plan.TableDef{
			Name: "t",
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
		},
	}
	_, err := genBuildSQL(ctx, defs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ivfpq_index")
}

func TestIvfpqValidateReindexParams(t *testing.T) {
	// no IndexAlgoParamList → passthrough
	old := map[string]string{"a": "1"}
	got, err := Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{})
	require.NoError(t, err)
	require.Equal(t, old, got)
}

// TestIvfpqValidateReindexParams_ListsMerge: ALTER … REINDEX … IVFPQ
// LISTS=N updates the algo params (mirrors IVF-FLAT).
func TestIvfpqValidateReindexParams_ListsMerge(t *testing.T) {
	old := map[string]string{
		catalog.IndexAlgoParamLists:  "4",
		catalog.IndexAlgoParamOpType: "vector_l2_ops",
	}
	got, err := Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{IndexAlgoParamList: 16})
	require.NoError(t, err)
	require.Equal(t, "16", got[catalog.IndexAlgoParamLists])
	require.Equal(t, "vector_l2_ops", got[catalog.IndexAlgoParamOpType])
	// Original map untouched.
	require.Equal(t, "4", old[catalog.IndexAlgoParamLists])
}

func TestIvfpqHandleDropIndex(t *testing.T) {
	require.NoError(t, Hooks{}.HandleDropIndex(nil, nil))
}

func TestIvfpqIdxcronMetadata_Frontend(t *testing.T) {
	// Frontend context → metadata is captured.
	ctx := &stubCompileContext{
		isFrontend: true,
		vars: map[string]any{
			"ivfpq_threads_search":     int64(4),
			"ivfpq_threads_build":      int64(8),
			"ivfpq_max_index_capacity": int64(1000000),
			"lower_case_table_names":   int64(1),
			"experimental_ivfpq_index": int8(1),
		},
	}
	got, err := Hooks{}.IdxcronMetadata(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got, "frontend session should produce a metadata blob")
	require.Contains(t, string(got), "ivfpq_threads_build")
	require.Contains(t, string(got), "ivfpq_max_index_capacity")
}

func TestIvfpqIdxcronMetadata_Background(t *testing.T) {
	// ctx.IsFrontend() reports false → BuildIdxcronMetadata bails out
	// without resolving any variables.
	ctx := &stubCompileContext{}
	got, err := Hooks{}.IdxcronMetadata(ctx)
	require.NoError(t, err)
	require.Nil(t, got, "background invocation should yield nil metadata")
}

func TestIvfpqIndexFlagConst(t *testing.T) {
	// Sanity-check the gate constant matches the catalog string the
	// HandleCreateIndex body checks against.
	require.Equal(t, "experimental_ivfpq_index", "experimental_ivfpq_index")
}

// experimentalFlagCtx wraps the stub to toggle IsExperimentalEnabled.
type experimentalFlagCtx struct {
	*stubCompileContext
	enabled bool
	flagErr error
}

func (e *experimentalFlagCtx) IsExperimentalEnabled(_ string) (bool, error) {
	return e.enabled, e.flagErr
}

func newHandleCtx(enabled bool) *experimentalFlagCtx {
	return &experimentalFlagCtx{
		stubCompileContext: &stubCompileContext{
			// Frontend context — the experimental-flag gate is
			// skipped when !IsFrontend (background re-entry).
			isFrontend:  true,
			qryDatabase: "db1",
			originalTableDef: &plan.TableDef{
				Name: "t",
				Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
			},
			vars: map[string]any{
				"ivfpq_threads_build":      int64(4),
				"ivfpq_max_index_capacity": int64(1024),
			},
		},
		enabled: enabled,
	}
}

func TestIvfpqHandleCreateIndex_GateDisabled(t *testing.T) {
	err := Hooks{}.HandleCreateIndex(newHandleCtx(false), ivfpqIndexDefs())
	require.Error(t, err)
	require.Contains(t, err.Error(), "experimental_ivfpq_index")
}

func TestIvfpqHandleCreateIndex_InvalidDefCount(t *testing.T) {
	defs := ivfpqIndexDefs()
	delete(defs, catalog.Ivfpq_TblType_Metadata)
	err := Hooks{}.HandleCreateIndex(newHandleCtx(true), defs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid ivfpq index table definition")
}

func TestIvfpqHandleCreateIndex_OK(t *testing.T) {
	// Default (no async key in metadata IndexAlgoParams) takes the
	// sync branch: ctx.CreateIndexCdcTask called with
	// startFromNow=true and empty InitSQL.
	ctx := newHandleCtx(true)
	err := Hooks{}.HandleCreateIndex(ctx, ivfpqIndexDefs())
	require.NoError(t, err)
	require.True(t, ctx.stubCompileContext.lastCdcTask.called)
	require.True(t, ctx.stubCompileContext.lastCdcTask.startFromNow, "sync default → startFromNow=true")
	require.Empty(t, ctx.stubCompileContext.lastCdcTask.sql, "sync default → no InitSQL")
	require.True(t, ctx.stubCompileContext.lastIdxcronUpdate.called, "frontend create must register cron metadata row")
	require.Equal(t, actionIvfpqReindex, ctx.stubCompileContext.lastIdxcronUpdate.action)
	require.Equal(t, "ix", ctx.stubCompileContext.lastIdxcronUpdate.indexName)
	require.Greater(t, ctx.stubCompileContext.lastIdxcronUpdate.metadataLen, 0)
}

func TestIvfpqHandleCreateIndex_AsyncTrue(t *testing.T) {
	// Explicit async="true" on metadata params takes the async-via-
	// InitSQL branch: startFromNow=false, sql carries the build SQL.
	defs := ivfpqIndexDefs()
	defs[catalog.Ivfpq_TblType_Metadata].IndexAlgoParams = `{"async":"true"}`
	ctx := newHandleCtx(true)
	err := Hooks{}.HandleCreateIndex(ctx, defs)
	require.NoError(t, err)
	require.True(t, ctx.stubCompileContext.lastCdcTask.called)
	require.False(t, ctx.stubCompileContext.lastCdcTask.startFromNow, "async=true → startFromNow=false")
	require.NotEmpty(t, ctx.stubCompileContext.lastCdcTask.sql, "async=true → InitSQL carries the build")
	require.True(t, ctx.stubCompileContext.lastIdxcronUpdate.called, "async branch must also register cron metadata row")
	require.Equal(t, actionIvfpqReindex, ctx.stubCompileContext.lastIdxcronUpdate.action)
}

func TestIvfpqHandleCreateIndex_AsyncFalseExplicit(t *testing.T) {
	// Explicit async="false" behaves identically to the default.
	defs := ivfpqIndexDefs()
	defs[catalog.Ivfpq_TblType_Metadata].IndexAlgoParams = `{"async":"false"}`
	ctx := newHandleCtx(true)
	err := Hooks{}.HandleCreateIndex(ctx, defs)
	require.NoError(t, err)
	require.True(t, ctx.stubCompileContext.lastCdcTask.called)
	require.True(t, ctx.stubCompileContext.lastCdcTask.startFromNow)
	require.Empty(t, ctx.stubCompileContext.lastCdcTask.sql)
	require.True(t, ctx.stubCompileContext.lastIdxcronUpdate.called)
}

// TestIvfpqHandleCreateIndex_BackgroundReentry: HandleReindex invoked
// from a non-frontend (background cron) context must NOT re-write the
// mo_index_update row — IdxcronMetadata returns nil for non-frontend,
// so registerIdxcronUpdate takes the skip branch.
func TestIvfpqHandleCreateIndex_BackgroundReentry(t *testing.T) {
	ctx := newHandleCtx(true)
	ctx.stubCompileContext.isFrontend = false
	err := Hooks{}.HandleReindex(ctx, ivfpqIndexDefs(), true)
	require.NoError(t, err)
	require.True(t, ctx.stubCompileContext.lastCdcTask.called, "background re-entry still drives the CDC task")
	require.False(t, ctx.stubCompileContext.lastIdxcronUpdate.called, "background re-entry must NOT rewrite mo_index_update")
}

func TestIvfpqHandleReindex_DelegatesToCreate(t *testing.T) {
	// HandleReindex is a thin pass-through to handleCreate; honors
	// the forceSync arg directly (unlike HandleCreateIndex, which
	// now reads catalog.IsIndexAsync).
	err := Hooks{}.HandleReindex(newHandleCtx(true), ivfpqIndexDefs(), false)
	require.NoError(t, err)
}
