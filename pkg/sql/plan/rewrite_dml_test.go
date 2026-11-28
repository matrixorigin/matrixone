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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/stretchr/testify/require"
)

const pointUpdateRoleRule = "/*+ {\"rewrites\":{\"tpch.nation\":\"select n_nationkey, n_name from tpch.nation where n_regionkey = 1\"}} */ "

func TestWritableRewriteSelectChecksEveryWrapper(t *testing.T) {
	for _, tc := range []struct {
		sql      string
		writable bool
	}{
		{"((select id from db.items))", true},
		{"((select id from db.items limit 1))", false},
		{"(select id from db.items) limit 1", false},
	} {
		t.Run(tc.sql, func(t *testing.T) {
			stmt, err := parsers.ParseOne(context.Background(), dialect.MYSQL, tc.sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			require.NotNil(t, rewriteSelectClause(stmt, false))
			require.Equal(t, tc.writable, rewriteSelectClause(stmt, true) != nil)
		})
	}
}

func TestDMLRewriteExplainPreservesPolicyOnPlanCopy(t *testing.T) {
	query := &plan.Query{StmtType: plan.Query_UPDATE, HasTableRewrite: true}
	require.Error(t, CheckDMLRewriteExplain(context.Background(), DeepCopyQuery(query)))
}

func TestRoleRuleDMLKeepsSinglePointScan(t *testing.T) {
	const update = "update nation set n_name = 'updated' where n_nationkey = "
	for _, tc := range []struct {
		statement string
		prepared  bool
		parameter string
		wantPoint bool
	}{
		{statement: update + "5", wantPoint: true},
		{statement: "delete from nation where n_nationkey = 5", wantPoint: true},
		{statement: update + "?", prepared: true},
		{statement: update + "?", parameter: "5", wantPoint: true},
	} {
		t.Run(tc.statement+tc.parameter, func(t *testing.T) {
			ctx := NewMockCompilerContext(true, newPlanTestProcess(t))
			sql := pointUpdateRoleRule + tc.statement
			var p *Plan
			if tc.parameter == "" {
				p = buildRoleRuleDMLTestPlan(t, ctx, sql, tc.prepared)
			} else {
				proc := ctx.GetProcess()
				params := vector.NewVec(types.T_text.ToType(), proc.Mp())
				t.Cleanup(func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) })
				require.NoError(t, vector.AppendBytes(params, []byte(tc.parameter), false, proc.Mp()))
				proc.SetPrepareParams(params)
				stmt := parseRoleRuleDMLTestStatement(t, ctx, sql)
				defer stmt.Free()
				source := types.T_int64.ToType()
				bound, err := BuildPreparedExecutionPlan(ctx, stmt,
					[]PreparedSourceBinding{{Position: 0, Type: source}},
					[]any{ParamValue{Value: tc.parameter, IsBinaryProtocol: true, SourceType: source, HasSourceType: true}})
				require.NoError(t, err)
				require.True(t, bound.DiagnosticFree)
				p = bound.Plan
			}
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
					require.Equal(t, tc.wantPoint, len(node.FilterList) != 0,
						"only the proven primary-key predicate can reach the scan")
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
	stmt := parseRoleRuleDMLTestStatement(t, ctx, sql)
	defer stmt.Free()
	p, err := BuildPlan(ctx, stmt, prepared, nil)
	require.NoError(t, err)
	return p
}

func parseRoleRuleDMLTestStatement(t testing.TB, ctx CompilerContext, sql string) tree.Statement {
	t.Helper()
	stmt, err := parsers.ParseOne(ctx.GetContext(), dialect.MYSQL, sql, 1)
	require.NoError(t, err)
	require.NoError(t, parsers.AddRewriteHints(ctx.GetContext(), []tree.Statement{stmt}, sql))
	return stmt
}
