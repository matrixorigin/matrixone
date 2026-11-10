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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func withIgnored(defs map[string]*plan.IndexDef, maxBadRecords string) map[string]*plan.IndexDef {
	defs[catalog.IndexIgnored_TblType] = &plan.IndexDef{
		IndexName:          "idx",
		IndexTableName:     "__mo_index_ignored",
		IndexAlgoTableType: catalog.IndexIgnored_TblType,
		Parts:              []string{"embedding"},
		IndexAlgoParams:    params(catalog.IndexAlgoParamMaxBadRecords, maxBadRecords),
	}
	return defs
}

// A build of an index with an ignored table starts its rows over and tells product_l2 where to
// record the rows it leaves out.
func TestRunCreateOrReindex_IgnoredTable(t *testing.T) {
	ctx := &scriptedCtx{db: "db", tblDef: srcTableDef()}
	defs := withIgnored(ivfIndexDefs(params(
		catalog.IndexAlgoParamLists, "1",
		catalog.IndexAlgoParamOpType, "vector_l2_ops")), "3")
	require.NoError(t, runCreateOrReindex(ctx, defs, false))
	joined := strings.Join(ctx.sqls, "\n")
	require.Contains(t, joined, "DELETE FROM `db`.`__mo_index_ignored` WHERE TRUE")
	require.Contains(t, joined, "CENTROIDX ('vector_l2_ops', ")
	require.Contains(t, joined, `"ignored":"__mo_index_ignored"`)
	require.Contains(t, joined, `"max_bad_records":3`)
	require.Contains(t, joined, `"pkey":"id"`)
	require.Contains(t, joined, "SELECT COUNT(*) FROM `db`.`__mo_index_ignored`", "the build checks the limit")

	// an asynchronous build leaves the rows to the CDC replay, which starts from an empty table
	ctx = &scriptedCtx{db: "db", tblDef: srcTableDef()}
	defs = withIgnored(ivfIndexDefs(params(
		catalog.IndexAlgoParamLists, "1",
		catalog.IndexAlgoParamOpType, "vector_l2_ops",
		catalog.Async, "true")), "3")
	require.NoError(t, runCreateOrReindex(ctx, defs, false))
	joined = strings.Join(ctx.sqls, "\n")
	require.Contains(t, joined, "DELETE FROM `db`.`__mo_index_ignored` WHERE TRUE")
	require.NotContains(t, joined, "SELECT COUNT(*) FROM `db`.`__mo_index_ignored`")

	defs = withIgnored(ivfIndexDefs(params(
		catalog.IndexAlgoParamLists, "1",
		catalog.IndexAlgoParamOpType, "vector_l2_ops")), "x")
	require.Error(t, runCreateOrReindex(&scriptedCtx{db: "db", tblDef: srcTableDef()}, defs, false))
}

func TestValidateReindexParams_MaxBadRecords(t *testing.T) {
	merged, err := Hooks{}.ValidateReindexParams(map[string]string{catalog.IndexAlgoParamOpType: "vector_l2_ops"},
		compileplugin.ReindexParamUpdate{Params: map[string]string{catalog.IndexAlgoParamMaxBadRecords: "9"}})
	require.NoError(t, err)
	require.Equal(t, "9", merged[catalog.IndexAlgoParamMaxBadRecords])
}
