// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

const pointUpdateRoleRule = "/*+ {\"rewrites\":{\"tpch.nation\":\"select n_nationkey, n_name from tpch.nation where n_regionkey = 1\"}} */ "

func TestRoleRuleDMLKeepsSinglePointScan(t *testing.T) {
	for _, tc := range []struct {
		statement string
		prepared  bool
	}{
		{statement: "update nation set n_name = 'updated' where n_nationkey = 5"},
		{statement: "update nation set n_name = 'updated' where n_nationkey = ?", prepared: true},
		{statement: "delete from nation where n_nationkey = 5"},
	} {
		t.Run(tc.statement, func(t *testing.T) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
			p := buildRoleRuleDMLTestPlan(t, ctx, pointUpdateRoleRule+tc.statement, tc.prepared)
			query := p.GetQuery()
			_, def, err := ctx.Resolve("tpch", "nation", nil, nil)
			require.NoError(t, err)
			seen := make(map[int32]bool)
			var scans, barriers int
			var visit func(int32)
			visit = func(id int32) {
				if seen[id] {
					return
				}
				seen[id] = true
				node := query.Nodes[id]
				if node.NodeType == plan.Node_TABLE_SCAN && node.GetTableDef().GetTblId() == def.TblId {
					scans++
					require.NotEmpty(t, node.FilterList, "the primary-key predicate must reach the scan")
				}
				if node.NodeType == plan.Node_FILTER && node.FilterIsBarrier {
					barriers++
				}
				for _, child := range node.Children {
					visit(child)
				}
			}
			for _, step := range query.Steps {
				visit(step)
			}
			require.Equal(t, 1, scans)
			require.Positive(t, barriers, "the role predicate must remain a row-selection boundary")
		})
	}
}

// Compare planning with a role rewrite to explicitly filtering the same rows.
func BenchmarkRoleRulePointUpdatePlan(b *testing.B) {
	const statement = "update nation set n_name = 'updated' where n_nationkey = 5"
	for _, tc := range []struct {
		name string
		sql  string
	}{
		{"explicit", statement + " and n_regionkey = 1"},
		{"policy", pointUpdateRoleRule + statement},
	} {
		b.Run(tc.name, func(b *testing.B) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(b))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				buildRoleRuleDMLTestPlan(b, ctx, tc.sql, false)
			}
		})
	}
}

func buildRoleRuleDMLTestPlan(t testing.TB, ctx CompilerContext, sql string, prepared bool) *Plan {
	t.Helper()
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	defer stmt.Free()
	require.NoError(t, parsers.AddRewriteHints(ctx.GetContext(), []tree.Statement{stmt}, sql))
	p, err := BuildPlan(ctx, stmt, prepared, nil)
	require.NoError(t, err)
	return p
}
