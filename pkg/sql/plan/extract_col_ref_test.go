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

	"github.com/matrixorigin/matrixone/pkg/catalog"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

func TestExtractColRefInFilterNested(t *testing.T) {
	col := func(rel, pos int32) *pb.Expr {
		return &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: rel, ColPos: pos}}}
	}
	fn := func(args ...*pb.Expr) *pb.Expr { return &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Args: args}}} }
	literal := &pb.Expr{Expr: &pb.Expr_Lit{Lit: &pb.Literal{}}}
	param := &pb.Expr{Expr: &pb.Expr_P{P: &pb.ParamRef{}}}
	list := func(items ...*pb.Expr) *pb.Expr {
		return &pb.Expr{Expr: &pb.Expr_List{List: &pb.ExprList{List: items}}}
	}
	a, b, c := col(1, 1), col(1, 2), col(2, 1)
	cases := []struct {
		name string
		expr *pb.Expr
		ok   bool
	}{
		{"single", fn(a, literal), true},
		{"same_nested", fn(a, fn(a, param)), true},
		{"multi_nested", fn(a, fn(b, c)), false},
		{"deep_multi", fn(a, fn(a, fn(b, c))), false},
		{"swapped_multi", fn(fn(b, c), a), false},
		{"different_tag", fn(a, c), false},
		{"different_position", fn(a, b), false},
		{"empty_list", fn(a, list()), false},
		{"parameter_list", fn(a, list(param, literal)), true},
		{"list_with_other_column", fn(a, list(param, list(b))), false},
		{"list_with_same_column", fn(a, list(param, a)), false},
		{"constant_first", fn(literal, a), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := extractColRefInFilter(tc.expr)
			if tc.ok {
				require.Equal(t, a.GetCol(), got)
			} else {
				require.Nil(t, got)
			}
		})
	}
}

func BenchmarkExtractColRefInFilter(b *testing.B) {
	col := &pb.Expr{Expr: &pb.Expr_Col{Col: &pb.ColRef{RelPos: 1, ColPos: 1}}}
	lit := &pb.Expr{Expr: &pb.Expr_Lit{Lit: &pb.Literal{}}}
	wide := &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Args: append([]*pb.Expr{col}, make([]*pb.Expr, 63)...)}}}
	for i := 1; i < len(wide.GetF().Args); i++ {
		wide.GetF().Args[i] = lit
	}
	deep := col
	for i := 0; i < 64; i++ {
		deep = &pb.Expr{Expr: &pb.Expr_F{F: &pb.Function{Args: []*pb.Expr{deep, lit}}}}
	}
	for _, tc := range []struct {
		name string
		expr *pb.Expr
	}{{"wide", wide}, {"deep", deep}} {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if extractColRefInFilter(tc.expr) == nil {
					b.Fatal("missing column")
				}
			}
		})
	}
}

// The index predicate for a prepared IN retains the parameter list until
// execution. Losing its block selectivity makes a point lookup ineligible
// for prepare-time physical compilation.
func TestPreparedInIndexLookupKeepsTPPlan(t *testing.T) {
	mock := NewMockOptimizer(true)
	addIndexHintChoiceTableForTest(mock)
	table := mock.ctxt.tables["index_hint_t"]
	table.Indexes = table.Indexes[:1] // non-covering secondary index on a
	ctx := &backfillCachedStatsContext{
		backfillTopStatsContext: &backfillTopStatsContext{MockCompilerContext: &mock.ctxt, rows: 10_000_000},
		cache:                   NewStatsCache(),
	}
	stats := NewStatsInfo()
	stats.TableCnt = ctx.rows
	stats.NdvMap["a"] = 1_000_000
	stats.NdvMap["id"] = ctx.rows
	stats.NdvMap[catalog.IndexTableIndexColName] = ctx.rows
	ctx.cache.Set(table.TblId, stats)
	ctx.cache.Set(mock.ctxt.tables[table.Indexes[0].IndexTableName].TblId, stats)

	for _, rhs := range []string{"?,?,?,?,?,?,?,?,?,?", "1,2,3,4,5,6,7,8,9,10"} {
		stmt, err := mysql.Parse(ctx.GetContext(), "select id,a,b from index_hint_t where a in ("+rhs+")", 1)
		require.NoError(t, err)
		p, err := BuildPlan(ctx, stmt[0], true)
		require.NoError(t, err)
		require.Equal(t, []string{"idx_a"}, reachableIndexScanNames(p.GetQuery()))
		require.Equal(t, ExecTypeTP, GetExecType(p.GetQuery(), false, true), rhs)
	}
}
