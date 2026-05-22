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

func (s *stubCompileContext) Ctx() compileplugin.Context             { return nil }
func (s *stubCompileContext) Database() engine.Database              { return nil }
func (s *stubCompileContext) QryDatabase() string                    { return s.qryDatabase }
func (s *stubCompileContext) OriginalTableDef() *plan.TableDef       { return s.originalTableDef }
func (s *stubCompileContext) IndexInfo() *plan.CreateTable           { return nil }
func (s *stubCompileContext) MainTableID() uint64                    { return 0 }
func (s *stubCompileContext) MainExtra() *api.SchemaExtra            { return nil }
func (s *stubCompileContext) RunSql(_ string) error                  { return nil }
func (s *stubCompileContext) BuildIndexTable(_ *plan.TableDef) error { return nil }
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

func cagraIndexDefs() map[string]*plan.IndexDef {
	return map[string]*plan.IndexDef{
		catalog.Cagra_TblType_Metadata: {
			IndexName:      "ix",
			IndexTableName: "__mo_cagra_meta_001",
			Parts:          []string{"v"},
		},
		catalog.Cagra_TblType_Storage: {
			IndexName:       "ix",
			IndexTableName:  "__mo_cagra_idx_001",
			Parts:           []string{"v"},
			IndexAlgoParams: `{"op_type":"vector_l2_ops"}`,
		},
	}
}

func TestCagraGenDeleteSQL(t *testing.T) {
	defs := cagraIndexDefs()
	sqls, err := genDeleteSQL(defs, "db1")
	require.NoError(t, err)
	require.Len(t, sqls, 2)
	require.Contains(t, sqls[0], "DELETE FROM `db1`.`__mo_cagra_meta_001`")
	require.Contains(t, sqls[1], "DELETE FROM `db1`.`__mo_cagra_idx_001`")
}

func TestCagraGenDeleteSQL_MissingMeta(t *testing.T) {
	defs := cagraIndexDefs()
	delete(defs, catalog.Cagra_TblType_Metadata)
	_, err := genDeleteSQL(defs, "db1")
	require.Error(t, err)
}

func TestCagraGenDeleteSQL_MissingStorage(t *testing.T) {
	defs := cagraIndexDefs()
	delete(defs, catalog.Cagra_TblType_Storage)
	_, err := genDeleteSQL(defs, "db1")
	require.Error(t, err)
}

func TestCagraFilterColumnsFromParams_Empty(t *testing.T) {
	require.Equal(t, "", filterColumnsFromParams("", "src"))
	require.Equal(t, "", filterColumnsFromParams(`{"op_type":"vector_l2_ops"}`, "src"))
	require.Equal(t, "", filterColumnsFromParams(`not-json`, "src"))
}

func TestCagraFilterColumnsFromParams_OK(t *testing.T) {
	got := filterColumnsFromParams(`{"included_columns":"price, name"}`, "src")
	require.Equal(t, ", src.price, src.name", got)
}

func TestCagraFilterColumnsFromParams_SkipsBlank(t *testing.T) {
	got := filterColumnsFromParams(`{"included_columns":"price, ,name"}`, "src")
	require.Equal(t, ", src.price, src.name", got)
}

func TestCagraGenBuildSQL_OK(t *testing.T) {
	ctx := &stubCompileContext{
		qryDatabase: "db1",
		originalTableDef: &plan.TableDef{
			Name: "t",
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
		},
		vars: map[string]any{
			"cagra_threads_build":      int64(4),
			"cagra_max_index_capacity": int64(1024),
		},
	}
	sqls, err := genBuildSQL(ctx, cagraIndexDefs())
	require.NoError(t, err)
	require.Len(t, sqls, 1)
	require.True(t, strings.Contains(sqls[0], "cagra_create"))
	require.True(t, strings.Contains(sqls[0], "`db1`.`t`"))
}

func TestCagraGenBuildSQL_MissingMeta(t *testing.T) {
	defs := cagraIndexDefs()
	delete(defs, catalog.Cagra_TblType_Metadata)
	ctx := &stubCompileContext{
		qryDatabase: "db1",
		originalTableDef: &plan.TableDef{
			Name: "t",
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
		},
	}
	_, err := genBuildSQL(ctx, defs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cagra_meta")
}

func TestCagraGenBuildSQL_MissingStorage(t *testing.T) {
	defs := cagraIndexDefs()
	delete(defs, catalog.Cagra_TblType_Storage)
	ctx := &stubCompileContext{
		qryDatabase: "db1",
		originalTableDef: &plan.TableDef{
			Name: "t",
			Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
		},
	}
	_, err := genBuildSQL(ctx, defs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cagra_index")
}

func TestCagraValidateReindexParams(t *testing.T) {
	old := map[string]string{"a": "1"}
	got, err := Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{})
	require.NoError(t, err)
	require.Equal(t, old, got)
}

func TestCagraHandleDropIndex(t *testing.T) {
	require.NoError(t, Hooks{}.HandleDropIndex(nil, nil))
}

func TestCagraIdxcronMetadata_Frontend(t *testing.T) {
	ctx := &stubCompileContext{
		isFrontend: true,
		vars: map[string]any{
			"cagra_threads_search":     int64(4),
			"cagra_threads_build":      int64(8),
			"cagra_max_index_capacity": int64(1000000),
			"lower_case_table_names":   int64(1),
			"experimental_cagra_index": int8(1),
		},
	}
	got, err := Hooks{}.IdxcronMetadata(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, got, "frontend session should produce a metadata blob")
	require.Contains(t, string(got), "cagra_threads_build")
	require.Contains(t, string(got), "cagra_max_index_capacity")
}

func TestCagraIdxcronMetadata_Background(t *testing.T) {
	// ctx.IsFrontend() reports false → BuildIdxcronMetadata bails out
	// without resolving any variables.
	ctx := &stubCompileContext{}
	got, err := Hooks{}.IdxcronMetadata(ctx)
	require.NoError(t, err)
	require.Nil(t, got, "background invocation should yield nil metadata")
}

// TestCagraIdxcronMetadata_ProbeFail: a sub-Compile context where the
// frontend probe sysvar resolves to nil (the CREATE TABLE CLONE
// scenario — IsFrontend=true but the inherited session resolver is
// partial) must defer to background semantics. The whole metadata
// blob is nil, not partially captured. Reproduces the BVT regression
// seen on `create table db1.t9_copy clone db1.t9;`.
func TestCagraIdxcronMetadata_ProbeFail(t *testing.T) {
	ctx := &stubCompileContext{
		isFrontend: true,
		vars: map[string]any{
			// FrontendProbeVar ("cagra_threads_search") set to nil
			// simulates the sub-Compile resolver returning (nil, nil).
			"cagra_threads_search":     nil,
			"cagra_threads_build":      int64(8),
			"cagra_max_index_capacity": int64(1000000),
			"lower_case_table_names":   int64(1),
			"experimental_cagra_index": int8(1),
		},
	}
	got, err := Hooks{}.IdxcronMetadata(ctx)
	require.NoError(t, err)
	require.Nil(t, got, "probe-fail should defer to background, not partial capture")
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
				"cagra_threads_build":      int64(4),
				"cagra_max_index_capacity": int64(1024),
			},
		},
		enabled: enabled,
	}
}

func TestCagraHandleCreateIndex_GateDisabled(t *testing.T) {
	err := Hooks{}.HandleCreateIndex(newHandleCtx(false), cagraIndexDefs())
	require.Error(t, err)
	require.Contains(t, err.Error(), "experimental_cagra_index")
}

func TestCagraHandleCreateIndex_InvalidDefCount(t *testing.T) {
	defs := cagraIndexDefs()
	delete(defs, catalog.Cagra_TblType_Metadata)
	err := Hooks{}.HandleCreateIndex(newHandleCtx(true), defs)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid cagra index table definition")
}

func TestCagraHandleCreateIndex_OK(t *testing.T) {
	// Default (no async key in metadata IndexAlgoParams) takes the
	// sync branch: ctx.CreateIndexCdcTask called with
	// startFromNow=true and empty InitSQL.
	ctx := newHandleCtx(true)
	err := Hooks{}.HandleCreateIndex(ctx, cagraIndexDefs())
	require.NoError(t, err)
	require.True(t, ctx.stubCompileContext.lastCdcTask.called)
	require.True(t, ctx.stubCompileContext.lastCdcTask.startFromNow, "sync default → startFromNow=true")
	require.Empty(t, ctx.stubCompileContext.lastCdcTask.sql, "sync default → no InitSQL")
	require.True(t, ctx.stubCompileContext.lastIdxcronUpdate.called, "frontend create must register cron metadata row")
	require.Equal(t, actionCagraReindex, ctx.stubCompileContext.lastIdxcronUpdate.action)
	require.Equal(t, "ix", ctx.stubCompileContext.lastIdxcronUpdate.indexName)
	require.Greater(t, ctx.stubCompileContext.lastIdxcronUpdate.metadataLen, 0)
}

func TestCagraHandleCreateIndex_AsyncTrue(t *testing.T) {
	// Explicit async="true" on metadata params takes the async-via-
	// InitSQL branch: startFromNow=false, sql carries the build SQL.
	defs := cagraIndexDefs()
	defs[catalog.Cagra_TblType_Metadata].IndexAlgoParams = `{"async":"true"}`
	ctx := newHandleCtx(true)
	err := Hooks{}.HandleCreateIndex(ctx, defs)
	require.NoError(t, err)
	require.True(t, ctx.stubCompileContext.lastCdcTask.called)
	require.False(t, ctx.stubCompileContext.lastCdcTask.startFromNow, "async=true → startFromNow=false")
	require.NotEmpty(t, ctx.stubCompileContext.lastCdcTask.sql, "async=true → InitSQL carries the build")
	require.True(t, ctx.stubCompileContext.lastIdxcronUpdate.called, "async branch must also register cron metadata row")
	require.Equal(t, actionCagraReindex, ctx.stubCompileContext.lastIdxcronUpdate.action)
}

func TestCagraHandleCreateIndex_AsyncFalseExplicit(t *testing.T) {
	// Explicit async="false" behaves identically to the default.
	defs := cagraIndexDefs()
	defs[catalog.Cagra_TblType_Metadata].IndexAlgoParams = `{"async":"false"}`
	ctx := newHandleCtx(true)
	err := Hooks{}.HandleCreateIndex(ctx, defs)
	require.NoError(t, err)
	require.True(t, ctx.stubCompileContext.lastCdcTask.called)
	require.True(t, ctx.stubCompileContext.lastCdcTask.startFromNow)
	require.Empty(t, ctx.stubCompileContext.lastCdcTask.sql)
	require.True(t, ctx.stubCompileContext.lastIdxcronUpdate.called)
}

// TestCagraHandleCreateIndex_BackgroundReentry: HandleReindex invoked
// from a non-frontend (background cron) context must NOT re-write the
// mo_index_update row — IdxcronMetadata returns nil for non-frontend,
// so registerIdxcronUpdate takes the skip branch.
func TestCagraHandleCreateIndex_BackgroundReentry(t *testing.T) {
	ctx := newHandleCtx(true)
	ctx.stubCompileContext.isFrontend = false
	err := Hooks{}.HandleReindex(ctx, cagraIndexDefs(), true)
	require.NoError(t, err)
	require.True(t, ctx.stubCompileContext.lastCdcTask.called, "background re-entry still drives the CDC task")
	require.False(t, ctx.stubCompileContext.lastIdxcronUpdate.called, "background re-entry must NOT rewrite mo_index_update")
}

func TestCagraHandleReindex_DelegatesToCreate(t *testing.T) {
	// HandleReindex is a thin pass-through to handleCreate; honors
	// the forceSync arg directly (unlike HandleCreateIndex, which
	// now reads catalog.IsIndexAsync).
	err := Hooks{}.HandleReindex(newHandleCtx(true), cagraIndexDefs(), false)
	require.NoError(t, err)
}

// TestCagraValidateReindexParams_IgnoresLists: CAGRA has no `lists`
// param — a non-zero IndexAlgoParamList from the parser must be
// dropped, not merged into the algo params.
func TestCagraValidateReindexParams_IgnoresLists(t *testing.T) {
	old := map[string]string{
		catalog.IndexAlgoParamOpType: "vector_l2_ops",
	}
	got, err := Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{IndexAlgoParamList: 16})
	require.NoError(t, err)
	require.Equal(t, old, got, "CAGRA must ignore IndexAlgoParamList")
	_, hasLists := got[catalog.IndexAlgoParamLists]
	require.False(t, hasLists, "CAGRA params must not gain a lists key")
}
