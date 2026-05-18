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
}

func (s *stubCompileContext) Ctx() compileplugin.Context        { return nil }
func (s *stubCompileContext) Database() engine.Database         { return nil }
func (s *stubCompileContext) QryDatabase() string               { return s.qryDatabase }
func (s *stubCompileContext) OriginalTableDef() *plan.TableDef  { return s.originalTableDef }
func (s *stubCompileContext) IndexInfo() *plan.CreateTable      { return nil }
func (s *stubCompileContext) MainTableID() uint64               { return 0 }
func (s *stubCompileContext) MainExtra() *api.SchemaExtra       { return nil }
func (s *stubCompileContext) RunSql(_ string) error             { return nil }
func (s *stubCompileContext) BuildIndexTable(_ *plan.TableDef) error { return nil }
func (s *stubCompileContext) ResolveVariable(name string, _, _ bool) (any, error) {
	if v, ok := s.vars[name]; ok {
		return v, nil
	}
	return int64(0), nil
}
func (s *stubCompileContext) IsExperimentalEnabled(_ string) (bool, error) { return true, nil }
func (s *stubCompileContext) IsCCPRTaskTransaction() bool                  { return false }
func (s *stubCompileContext) IsTableFromPublication(_ *plan.TableDef) bool { return false }
func (s *stubCompileContext) SinkerTypeFromAlgo(_ string) int8             { return 0 }
func (s *stubCompileContext) CreateIndexCdcTask(_, _ string, _ uint64, _ string, _ int8, _ bool, _ string, _ *plan.TableDef) error {
	return nil
}
func (s *stubCompileContext) DropIndexCdcTask(_ *plan.TableDef, _, _, _ string) error {
	return nil
}
func (s *stubCompileContext) RunSqlWithResult(_ string) (executor.Result, error) {
	return executor.Result{}, nil
}
func (s *stubCompileContext) RegisterIdxcronUpdate(_ uint64, _, _, _, _ string, _ []byte) error {
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

func TestCagraIdxcronMetadata(t *testing.T) {
	got, err := Hooks{}.IdxcronMetadata(nil)
	require.NoError(t, err)
	require.Nil(t, got)
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
	err := Hooks{}.HandleCreateIndex(newHandleCtx(true), cagraIndexDefs())
	require.NoError(t, err)
}

func TestCagraHandleReindex_DelegatesToCreate(t *testing.T) {
	// HandleReindex is a thin pass-through to HandleCreateIndex.
	err := Hooks{}.HandleReindex(newHandleCtx(true), cagraIndexDefs(), false)
	require.NoError(t, err)
}
