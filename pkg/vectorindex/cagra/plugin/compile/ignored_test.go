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
// WITHOUT WARRANTIES OR CONDITIONS OF ANY Cagra, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/catalog"
	compileplugin "github.com/matrixorigin/matrixone/pkg/indexplugin/compile"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func withIgnored(defs map[string]*plan.IndexDef, params string) map[string]*plan.IndexDef {
	defs[catalog.Cagra_TblType_Storage].IndexAlgoParams = params
	defs[catalog.IndexIgnored_TblType] = &plan.IndexDef{
		IndexName:       "ix",
		IndexTableName:  "__mo_ignored_001",
		Parts:           []string{"v"},
		IndexAlgoParams: params,
	}
	return defs
}

func ignoredBuildCtx() *stubCompileContext {
	return &stubCompileContext{
		qryDatabase:      "db1",
		originalTableDef: &plan.TableDef{Name: "t", Pkey: &plan.PrimaryKeyDef{PkeyColName: "id"}},
		vars:             map[string]any{"cagra_threads_build": int64(4)},
	}
}

// REINDEX and the build start the ignored rows over.
func TestCagraGenDeleteSQL_Ignored(t *testing.T) {
	sqls, err := genDeleteSQL(withIgnored(cagraIndexDefs(), `{"op_type":"vector_l2_ops"}`), "db1")
	require.NoError(t, err)
	require.Len(t, sqls, 3)
	require.Equal(t, "DELETE FROM `db1`.`__mo_ignored_001`", sqls[2])
}

// The build learns the ignored table and the limit of the index.
func TestCagraGenBuildSQL_Ignored(t *testing.T) {
	sqls, err := genBuildSQL(ignoredBuildCtx(), cagraIndexDefs())
	require.NoError(t, err)
	require.NotContains(t, sqls[0], `"ignored"`)
	require.NotContains(t, sqls[0], `"max_bad_records"`)

	sqls, err = genBuildSQL(ignoredBuildCtx(), withIgnored(cagraIndexDefs(), `{"op_type":"vector_l2_ops","max_bad_records":"5"}`))
	require.NoError(t, err)
	require.Contains(t, sqls[0], `"ignored":"__mo_ignored_001"`)
	require.Contains(t, sqls[0], `"max_bad_records":5`)

	_, err = genBuildSQL(ignoredBuildCtx(), withIgnored(cagraIndexDefs(), `{"max_bad_records":"x"}`))
	require.Error(t, err)
}

// ALTER ... REINDEX MAX_BAD_RECORDS changes the limit of the index.
func TestCagraValidateReindexParams_MaxBadRecords(t *testing.T) {
	old := map[string]string{catalog.IndexAlgoParamOpType: "vector_l2_ops", catalog.IndexAlgoParamMaxBadRecords: "0"}
	got, err := Hooks{}.ValidateReindexParams(old, compileplugin.ReindexParamUpdate{Params: map[string]string{catalog.IndexAlgoParamMaxBadRecords: "100"}})
	require.NoError(t, err)
	require.Equal(t, "100", got[catalog.IndexAlgoParamMaxBadRecords])
	require.Equal(t, "0", old[catalog.IndexAlgoParamMaxBadRecords])
}
