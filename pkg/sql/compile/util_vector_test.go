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
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

// filterColumnsFromParams ---------------------------------------------------

func TestFilterColumnsFromParams_Empty(t *testing.T) {
	require.Equal(t, "", filterColumnsFromParams("", "src"))
}

func TestFilterColumnsFromParams_BadJSON(t *testing.T) {
	require.Equal(t, "", filterColumnsFromParams("not json", "src"))
}

func TestFilterColumnsFromParams_KeyMissing(t *testing.T) {
	require.Equal(t, "", filterColumnsFromParams(`{"m":"32"}`, "src"))
}

func TestFilterColumnsFromParams_NotString(t *testing.T) {
	// included_columns present but not a string → StrictString fails.
	require.Equal(t, "", filterColumnsFromParams(`{"included_columns": 42}`, "src"))
}

func TestFilterColumnsFromParams_EmptyString(t *testing.T) {
	require.Equal(t, "", filterColumnsFromParams(`{"included_columns": ""}`, "src"))
}

func TestFilterColumnsFromParams_Single(t *testing.T) {
	require.Equal(t, ", src.price",
		filterColumnsFromParams(`{"included_columns":"price"}`, "src"))
}

func TestFilterColumnsFromParams_MultipleAndTrim(t *testing.T) {
	out := filterColumnsFromParams(`{"included_columns":" price , category , "}`, "src")
	require.Equal(t, ", src.price, src.category", out)
}

// genDelete*Index -----------------------------------------------------------

func mustProcWithVars(t *testing.T, vars map[string]int64) *process.Process {
	proc := testutil.NewProc(t)
	proc.SetResolveVariableFunc(func(name string, _ bool, _ bool) (interface{}, error) {
		v, ok := vars[name]
		if !ok {
			return nil, fmt.Errorf("unknown variable %s", name)
		}
		return v, nil
	})
	return proc
}

func TestGenDeleteCagraIndex_MissingMeta(t *testing.T) {
	proc := mustProcWithVars(t, nil)
	defs := map[string]*plan.IndexDef{
		catalog.Cagra_TblType_Storage: {IndexTableName: "idx"},
	}
	_, err := genDeleteCagraIndex(proc, defs, "db", &plan.TableDef{Name: "t"})
	require.Error(t, err)
}

func TestGenDeleteCagraIndex_MissingIndex(t *testing.T) {
	proc := mustProcWithVars(t, nil)
	defs := map[string]*plan.IndexDef{
		catalog.Cagra_TblType_Metadata: {IndexTableName: "meta"},
	}
	_, err := genDeleteCagraIndex(proc, defs, "db", &plan.TableDef{Name: "t"})
	require.Error(t, err)
}

func TestGenDeleteCagraIndex_OK(t *testing.T) {
	proc := mustProcWithVars(t, nil)
	defs := map[string]*plan.IndexDef{
		catalog.Cagra_TblType_Metadata: {IndexTableName: "meta_tbl"},
		catalog.Cagra_TblType_Storage:  {IndexTableName: "idx_tbl"},
	}
	sqls, err := genDeleteCagraIndex(proc, defs, "db", &plan.TableDef{Name: "t"})
	require.NoError(t, err)
	require.Len(t, sqls, 2)
	require.Contains(t, sqls[0], "`db`.`meta_tbl`")
	require.Contains(t, sqls[1], "`db`.`idx_tbl`")
}

func TestGenDeleteIvfpqIndex_MissingMeta(t *testing.T) {
	proc := mustProcWithVars(t, nil)
	defs := map[string]*plan.IndexDef{
		catalog.Ivfpq_TblType_Storage: {IndexTableName: "idx"},
	}
	_, err := genDeleteIvfpqIndex(proc, defs, "db", &plan.TableDef{Name: "t"})
	require.Error(t, err)
}

func TestGenDeleteIvfpqIndex_MissingIndex(t *testing.T) {
	proc := mustProcWithVars(t, nil)
	defs := map[string]*plan.IndexDef{
		catalog.Ivfpq_TblType_Metadata: {IndexTableName: "meta"},
	}
	_, err := genDeleteIvfpqIndex(proc, defs, "db", &plan.TableDef{Name: "t"})
	require.Error(t, err)
}

func TestGenDeleteIvfpqIndex_OK(t *testing.T) {
	proc := mustProcWithVars(t, nil)
	defs := map[string]*plan.IndexDef{
		catalog.Ivfpq_TblType_Metadata: {IndexTableName: "ivfpq_meta"},
		catalog.Ivfpq_TblType_Storage:  {IndexTableName: "ivfpq_idx"},
	}
	sqls, err := genDeleteIvfpqIndex(proc, defs, "db", &plan.TableDef{Name: "t"})
	require.NoError(t, err)
	require.Len(t, sqls, 2)
	require.Contains(t, sqls[0], "`db`.`ivfpq_meta`")
	require.Contains(t, sqls[1], "`db`.`ivfpq_idx`")
}

// genBuild*Index ------------------------------------------------------------

func cagraDefs() map[string]*plan.IndexDef {
	return map[string]*plan.IndexDef{
		catalog.Cagra_TblType_Metadata: {IndexTableName: "cagra_meta"},
		catalog.Cagra_TblType_Storage: {
			IndexTableName:  "cagra_idx",
			Parts:           []string{"v"},
			IndexAlgoParams: `{"m":"32","included_columns":"price"}`,
		},
	}
}

func ivfpqDefs() map[string]*plan.IndexDef {
	return map[string]*plan.IndexDef{
		catalog.Ivfpq_TblType_Metadata: {IndexTableName: "ivfpq_meta"},
		catalog.Ivfpq_TblType_Storage: {
			IndexTableName:  "ivfpq_idx",
			Parts:           []string{"v"},
			IndexAlgoParams: `{"lists":"4","included_columns":"price"}`,
		},
	}
}

func srcTable() *plan.TableDef {
	return &plan.TableDef{
		Name: "t",
		Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"},
	}
}

func TestGenBuildCagraIndex_MissingMeta(t *testing.T) {
	proc := mustProcWithVars(t, nil)
	defs := cagraDefs()
	delete(defs, catalog.Cagra_TblType_Metadata)
	_, err := genBuildCagraIndex(proc, defs, "db", srcTable())
	require.Error(t, err)
}

func TestGenBuildCagraIndex_MissingIndex(t *testing.T) {
	proc := mustProcWithVars(t, nil)
	defs := cagraDefs()
	delete(defs, catalog.Cagra_TblType_Storage)
	_, err := genBuildCagraIndex(proc, defs, "db", srcTable())
	require.Error(t, err)
}

func TestGenBuildCagraIndex_ResolveThreadsBuildErr(t *testing.T) {
	// no vars → ResolveVariable returns an error for every name.
	proc := mustProcWithVars(t, nil)
	_, err := genBuildCagraIndex(proc, cagraDefs(), "db", srcTable())
	require.Error(t, err)
}

func TestGenBuildCagraIndex_ResolveCapacityErr(t *testing.T) {
	proc := mustProcWithVars(t, map[string]int64{"cagra_threads_build": 4})
	_, err := genBuildCagraIndex(proc, cagraDefs(), "db", srcTable())
	require.Error(t, err)
}

func TestGenBuildCagraIndex_OK(t *testing.T) {
	proc := mustProcWithVars(t, map[string]int64{
		"cagra_threads_build":      8,
		"cagra_max_index_capacity": 100000,
	})
	sqls, err := genBuildCagraIndex(proc, cagraDefs(), "db", srcTable())
	require.NoError(t, err)
	require.Len(t, sqls, 1)
	// The included_columns suffix must be present in the SQL.
	require.Contains(t, sqls[0], "src.price")
	require.Contains(t, sqls[0], "cagra_create")
}

func TestGenBuildIvfpqIndex_MissingMeta(t *testing.T) {
	proc := mustProcWithVars(t, nil)
	defs := ivfpqDefs()
	delete(defs, catalog.Ivfpq_TblType_Metadata)
	_, err := genBuildIvfpqIndex(proc, defs, "db", srcTable())
	require.Error(t, err)
}

func TestGenBuildIvfpqIndex_MissingIndex(t *testing.T) {
	proc := mustProcWithVars(t, nil)
	defs := ivfpqDefs()
	delete(defs, catalog.Ivfpq_TblType_Storage)
	_, err := genBuildIvfpqIndex(proc, defs, "db", srcTable())
	require.Error(t, err)
}

func TestGenBuildIvfpqIndex_ResolveThreadsBuildErr(t *testing.T) {
	proc := mustProcWithVars(t, nil)
	_, err := genBuildIvfpqIndex(proc, ivfpqDefs(), "db", srcTable())
	require.Error(t, err)
}

func TestGenBuildIvfpqIndex_ResolveCapacityErr(t *testing.T) {
	proc := mustProcWithVars(t, map[string]int64{"ivfpq_threads_build": 4})
	_, err := genBuildIvfpqIndex(proc, ivfpqDefs(), "db", srcTable())
	require.Error(t, err)
}

func TestGenBuildIvfpqIndex_OK(t *testing.T) {
	proc := mustProcWithVars(t, map[string]int64{
		"ivfpq_threads_build":      4,
		"ivfpq_max_index_capacity": 50000,
	})
	sqls, err := genBuildIvfpqIndex(proc, ivfpqDefs(), "db", srcTable())
	require.NoError(t, err)
	require.Len(t, sqls, 1)
	require.Contains(t, sqls[0], "src.price")
	require.Contains(t, sqls[0], "ivfpq_create")
}
