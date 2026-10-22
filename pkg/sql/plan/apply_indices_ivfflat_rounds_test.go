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

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	ivfflatplan "github.com/matrixorigin/matrixone/pkg/vectorindex/ivfflat/plugin/plan"
	"github.com/stretchr/testify/require"
)

func ivfRoundsPlanFixture(t *testing.T, sql string) *plan.Query {
	t.Helper()
	ctx := newVectorJoinMockCtx(t)
	table := newVectorJoinTableDef(false, true)
	table.Name, table.TblId = "ivf_rounds", 951
	table.Cols[0].Typ = plan.Type{Id: int32(types.T_int64), NotNullable: true}
	table.Cols[1].Typ.Width = 2
	table.Cols = append(table.Cols, &plan.ColDef{Name: "category", Typ: plan.Type{Id: int32(types.T_int32)}})
	table.Name2ColIndex["category"] = 2
	for _, def := range newVectorJoinIvfIndex().IndexDefs {
		def.TableExist = true
		def.IndexAlgoParams = `{"op_type":"vector_l2_ops","lists":"4"}`
		table.Indexes = append(table.Indexes, def)
		ctx.tables[def.IndexTableName] = &plan.TableDef{Name: def.IndexTableName, TblId: uint64(960 + len(table.Indexes))}
		ctx.objects[def.IndexTableName] = &plan.ObjectRef{SchemaName: "tpch", ObjName: def.IndexTableName}
	}
	ctx.tables[table.Name] = table
	ctx.objects[table.Name] = &plan.ObjectRef{SchemaName: "tpch", ObjName: table.Name, Obj: int64(table.TblId)}
	statements, err := mysql.Parse(ctx.GetContext(), sql, 1)
	require.NoError(t, err)
	require.Len(t, statements, 1)
	defer statements[0].Free()
	p, err := BuildPlan(ctx, statements[0], false)
	require.NoError(t, err)
	return p.GetQuery()
}

func TestIvfflatIterativeWidens(t *testing.T) {
	for _, tc := range []struct {
		name      string
		sql       string
		rounds    bool
		iterative bool
	}{
		{"iterative, key filter", `select id from ivf_rounds where id between 3 and 9
			order by l2_distance(v,'[0,0]') limit 2 by rank with option 'mode=iterative'`, true, true},
		{"iterative, residual filter", `select id from ivf_rounds where category = 1
			order by l2_distance(v,'[0,0]') limit 2 by rank with option 'mode=iterative'`, false, true},
		{"pre, key filter", `select id from ivf_rounds where id between 3 and 9
			order by l2_distance(v,'[0,0]') limit 2 by rank with option 'mode=pre'`, false, false},
		{"pre, residual filter", `select id from ivf_rounds where category = 1
			order by l2_distance(v,'[0,0]') limit 2 by rank with option 'mode=pre'`, false, false},
		{"post, key filter", `select id from ivf_rounds where id between 3 and 9
			order by l2_distance(v,'[0,0]') limit 2 by rank with option 'mode=post'`, false, false},
		{"no mode, key filter", `select id from ivf_rounds where id between 3 and 9
			order by l2_distance(v,'[0,0]') limit 2`, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			query := ivfRoundsPlanFixture(t, tc.sql)
			search := findNodeOfType(query, plan.Node_INDEX_SEARCH_SCAN)
			require.NotNil(t, search)
			opts, err := ivfflatplan.DecodeScanOptions(search.IndexSearchScan.AlgoOptions)
			require.NoError(t, err)
			require.Equal(t, tc.rounds, opts.BucketExpandStep > 0)
			require.Equal(t, tc.iterative, opts.Iterative)
		})
	}
}
