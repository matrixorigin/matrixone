// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect/mysql"
	"github.com/stretchr/testify/require"
)

// Inputs are recreated outside the timed section: normalization mutates its
// owned expressions, and reusing a normalized tree would measure a cache hit.
func BenchmarkInDomainORNormalization(b *testing.B) {
	for _, n := range []int{512, 1024, 2048, 4096, 8192} {
		for _, shape := range []string{"flat", "balanced", "in"} {
			for _, outer := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/and=%t/n=%d", shape, outer, n), func(b *testing.B) {
					ctx := NewMockCompilerContext(true, newPlanTestProcess(b))
					col := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_int64)},
						Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 1, Name: "fact_id"}}}
					terms := make([]*planpb.Expr, n)
					for i := range terms {
						var err error
						terms[i], err = BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
							DeepCopyExpr(col), MakePlan2Int64ConstExprWithType(int64(i + 1))})
						require.NoError(b, err)
					}
					var input *planpb.Expr
					var err error
					switch shape {
					case "in":
						values := make([]*planpb.Expr, n)
						for i := range values {
							values[i] = MakePlan2Int64ConstExprWithType(int64(i + 1))
						}
						input, err = buildColumnDomainExpr(ctx.GetContext(), col, values)
					case "balanced":
						input, err = combinePlanExprsBalanced(ctx.GetContext(), "or", terms)
					default:
						input, err = combinePlanDisjunction(ctx.GetContext(), terms)
					}
					require.NoError(b, err)
					scenario := DeepCopyExpr(col)
					scenario.GetCol().ColPos = 0
					scenario.GetCol().Name = "scenario_id"
					equality, err := BindFuncExprImplByPlanExpr(ctx.GetContext(), "=", []*planpb.Expr{
						scenario, MakePlan2Int64ConstExprWithType(191)})
					require.NoError(b, err)
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
						filters := []*planpb.Expr{DeepCopyExpr(input)}
						if outer {
							filters = append([]*planpb.Expr{DeepCopyExpr(equality)}, filters...)
						}
						b.StartTimer()
						result := builder.normalizeColumnDomain(filters)
						if len(result) == 0 {
							b.Fatal("normalization dropped every filter")
						}
					}
				})
			}
		}
	}
}

func largeORPredicate(n int, shape string) string {
	terms := make([]string, n)
	for i := range terms {
		terms[i] = fmt.Sprintf("fact_id=%d", i+1)
	}
	if shape == "in" {
		for i := range terms {
			terms[i] = fmt.Sprint(i + 1)
		}
		return "fact_id IN (" + strings.Join(terms, ",") + ")"
	}
	if shape == "balanced" {
		var join func([]string) string
		join = func(xs []string) string {
			if len(xs) == 1 {
				return xs[0]
			}
			mid := len(xs) / 2
			return "(" + join(xs[:mid]) + " OR " + join(xs[mid:]) + ")"
		}
		return join(terms)
	}
	return "(" + strings.Join(terms, " OR ") + ")"
}

func newORPlanContext(tb testing.TB) *MockCompilerContext {
	tb.Helper()
	ctx := NewMockCompilerContext(true, newPlanTestProcess(tb))
	stmt, err := mysql.ParseOne(tb.Context(),
		"CREATE TABLE or_and_probe (scenario_id INT, fact_id BIGINT, amount DECIMAL(24,6))", 1)
	require.NoError(tb, err)
	defer stmt.Free()
	ddl, err := BuildPlan(ctx, stmt, false)
	require.NoError(tb, err)
	def := ddl.GetDdl().GetCreateTable().GetTableDef()
	require.NotNil(tb, def)
	def.TblId = 900001
	ctx.tables["or_and_probe"] = def
	ctx.objects["or_and_probe"] = &planpb.ObjectRef{SchemaName: "tpch", ObjName: "or_and_probe", Obj: int64(def.TblId)}
	return ctx
}

// Parse and complete planning are timed; a new compiler context on every
// iteration prevents identical SQL or a previous mutable plan hiding cold cost.
func BenchmarkColdPlanOR(b *testing.B) {
	for _, n := range []int{512, 1024, 2048, 4096, 8192} {
		for _, shape := range []string{"flat", "balanced", "in"} {
			for _, outer := range []bool{false, true} {
				b.Run(fmt.Sprintf("%s/and=%t/n=%d", shape, outer, n), func(b *testing.B) {
					predicate := largeORPredicate(n, shape)
					if outer {
						predicate = "scenario_id=191 AND " + predicate
					}
					sql := "SELECT COUNT(*),SUM(amount) FROM tpch.or_and_probe WHERE " + predicate
					b.ReportAllocs()
					b.ResetTimer()
					var parseTime, planTime time.Duration
					for i := 0; i < b.N; i++ {
						b.StopTimer()
						ctx := newORPlanContext(b)
						b.StartTimer()
						started := time.Now()
						stmt, err := mysql.ParseOne(b.Context(), sql, 1)
						parseTime += time.Since(started)
						require.NoError(b, err)
						started = time.Now()
						_, err = BuildPlan(ctx, stmt, false)
						planTime += time.Since(started)
						stmt.Free()
						require.NoError(b, err)
					}
					b.ReportMetric(float64(parseTime.Nanoseconds())/float64(b.N), "parse-ns/op")
					b.ReportMetric(float64(planTime.Nanoseconds())/float64(b.N), "plan-ns/op")
				})
			}
		}
	}
}
