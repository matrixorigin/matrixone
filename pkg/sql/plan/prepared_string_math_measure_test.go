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
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
)

func benchmarkStringMathParam(pos int32) *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}},
	}
}

func benchmarkStringMathDecimalColumn() *planpb.Expr {
	typ := types.New(types.T_decimal128, 20, 4)
	return &planpb.Expr{
		Typ:  makePlan2Type(&typ),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: 0, ColPos: 0}},
	}
}

func benchmarkStringMathIntLiteral(value int64) *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_int64)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_I64Val{I64Val: value}}},
	}
}

func benchmarkStringMathTextLiteral(value string) *planpb.Expr {
	return &planpb.Expr{
		Typ:  planpb.Type{Id: int32(types.T_text)},
		Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Value: &planpb.Literal_Sval{Sval: value}}},
	}
}

func benchmarkStringMathBind(name string, args []*planpb.Expr) *planpb.Expr {
	expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
	if err != nil {
		panic(err)
	}
	return expr
}

func benchmarkStringMathPlan(params, depth, noise int, eligible, noMatch bool) *planpb.Plan {
	projects := make([]*planpb.Expr, 0, noise+params)
	for i := 0; i < noise; i++ {
		projects = append(projects, benchmarkStringMathIntLiteral(int64(i)))
	}
	for i := 0; i < params; i++ {
		expr := benchmarkStringMathParam(int32(i))
		if eligible {
			if depth == 0 {
				expr = benchmarkStringMathBind("=", []*planpb.Expr{benchmarkStringMathDecimalColumn(), expr})
			} else {
				for j := 0; j < depth; j++ {
					expr = benchmarkStringMathBind("coalesce", []*planpb.Expr{expr, benchmarkStringMathDecimalColumn()})
				}
			}
		} else if noMatch {
			// CONCAT deliberately keeps the runtime parameter in a text-only
			// context. It still creates a deep expression tree, so the benchmark
			// measures the complete no-match traversal rather than the cheap
			// no-enabled-parameter fast path.
			for j := 0; j < depth; j++ {
				expr = benchmarkStringMathBind("concat", []*planpb.Expr{expr, benchmarkStringMathTextLiteral("x")})
			}
		}
		projects = append(projects, expr)
	}
	return &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Nodes:    []*planpb.Node{{NodeType: planpb.Node_VALUE_SCAN, ProjectList: projects}},
	}}}
}

func benchmarkStringMathOwnerPlan(projects ...*planpb.Expr) *planpb.Plan {
	return &planpb.Plan{Plan: &planpb.Plan_Query{Query: &planpb.Query{
		StmtType: planpb.Query_SELECT,
		Steps:    []int32{0},
		Nodes:    []*planpb.Node{{NodeType: planpb.Node_VALUE_SCAN, ProjectList: projects}},
	}}}
}

func benchmarkStringMathMixedRolePlan() *planpb.Plan {
	projects := make([]*planpb.Expr, 128)
	for i := range projects {
		projects[i] = benchmarkStringMathIntLiteral(int64(i))
	}
	decimal := benchmarkStringMathDecimalColumn()
	projects = append(projects,
		benchmarkStringMathBind("abs", []*planpb.Expr{benchmarkStringMathParam(0)}),
		benchmarkStringMathBind("round", []*planpb.Expr{decimal, benchmarkStringMathParam(1)}),
		benchmarkStringMathBind("abs", []*planpb.Expr{benchmarkStringMathBind("round", []*planpb.Expr{
			decimal, benchmarkStringMathParam(2),
		})}),
		benchmarkStringMathBind("round", []*planpb.Expr{decimal, benchmarkStringMathBind("abs", []*planpb.Expr{
			benchmarkStringMathParam(3),
		})}),
		benchmarkStringMathBind("concat", []*planpb.Expr{benchmarkStringMathParam(4), benchmarkStringMathTextLiteral("x")}),
	)
	return benchmarkStringMathOwnerPlan(projects...)
}

func benchmarkStringMathRoleCases() []struct {
	name string
	plan *planpb.Plan
	want []bool
} {
	return []struct {
		name string
		plan *planpb.Plan
		want []bool
	}{
		{
			name: "value-abs-p1",
			plan: benchmarkStringMathOwnerPlan(benchmarkStringMathBind("abs", []*planpb.Expr{benchmarkStringMathParam(0)})),
			want: []bool{true},
		},
		{
			name: "control-round-p1",
			plan: benchmarkStringMathOwnerPlan(benchmarkStringMathBind("round", []*planpb.Expr{
				benchmarkStringMathDecimalColumn(), benchmarkStringMathParam(0),
			})),
			want: []bool{false},
		},
		{
			name: "nested-outer-abs-inner-round-precision",
			plan: benchmarkStringMathOwnerPlan(benchmarkStringMathBind("abs", []*planpb.Expr{
				benchmarkStringMathBind("round", []*planpb.Expr{benchmarkStringMathDecimalColumn(), benchmarkStringMathParam(0)}),
			})),
			want: []bool{false},
		},
		{
			name: "nested-round-inner-abs-value",
			plan: benchmarkStringMathOwnerPlan(benchmarkStringMathBind("round", []*planpb.Expr{
				benchmarkStringMathDecimalColumn(), benchmarkStringMathBind("abs", []*planpb.Expr{benchmarkStringMathParam(0)}),
			})),
			want: []bool{true},
		},
		{
			name: "no-match-concat-p1",
			plan: benchmarkStringMathOwnerPlan(benchmarkStringMathBind("concat", []*planpb.Expr{
				benchmarkStringMathParam(0), benchmarkStringMathTextLiteral("x"),
			})),
			want: []bool{false},
		},
		{
			name: "no-match-deep-p8-d8-n128",
			plan: benchmarkStringMathPlan(8, 8, 128, false, true),
			want: []bool{false, false, false, false, false, false, false, false},
		},
		{
			name: "mixed-roles-p5-n128",
			plan: benchmarkStringMathMixedRolePlan(),
			want: []bool{true, false, false, true, false},
		},
	}
}

func TestPreparedStringMathRoleDiscoveryAcrossExpressionContainers(t *testing.T) {
	stringMath := func(name string, args ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_F{F: &planpb.Function{
			Func: &planpb.ObjectRef{ObjName: name},
			Args: args,
		}}}
	}
	position := func(pos int32) *planpb.Expr {
		return benchmarkStringMathParam(pos)
	}
	list := func(items ...*planpb.Expr) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{List: items}}}
	}

	tests := []struct {
		name string
		expr *planpb.Expr
		want map[int]bool
	}{
		{
			name: "scalar-subquery",
			expr: stringMath("abs", &planpb.Expr{Expr: &planpb.Expr_Sub{Sub: &planpb.SubqueryRef{
				Child: stringMath("coalesce",
					stringMath("abs", position(0)),
					stringMath("round", benchmarkStringMathDecimalColumn(), position(1)),
				),
			}}}),
			// The inner ABS owns position 0 as a value; the inner ROUND owns
			// position 1 as a control argument. The outer ABS must not override
			// either role across the scalar-subquery boundary.
			want: map[int]bool{0: true, 1: false, -1: false},
		},
		{
			name: "list",
			expr: stringMath("round", benchmarkStringMathDecimalColumn(), list(
				stringMath("abs", position(2)),
				position(3),
			)),
			// A nested value function retains its value role while a bare
			// parameter in ROUND's list-shaped precision argument stays control.
			want: map[int]bool{2: true, 3: false, -1: false},
		},
		{
			name: "literal-source",
			expr: stringMath("round", benchmarkStringMathDecimalColumn(), &planpb.Expr{Expr: &planpb.Expr_Lit{Lit: &planpb.Literal{Src: stringMath("coalesce",
				stringMath("abs", position(11)),
				stringMath("round", benchmarkStringMathDecimalColumn(), position(12)),
			)}}}),
			want: map[int]bool{11: true, 12: false, -1: false},
		},
		{
			name: "window",
			expr: stringMath("round", benchmarkStringMathDecimalColumn(), &planpb.Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
				WindowFunc: stringMath("abs", position(4)),
				PartitionBy: []*planpb.Expr{
					stringMath("round", benchmarkStringMathDecimalColumn(), position(5)),
					stringMath("abs", position(9)),
				},
				OrderBy: []*planpb.OrderBySpec{{Expr: stringMath("abs", position(6))}},
				Frame: &planpb.FrameClause{
					Type: planpb.FrameClause_ROWS,
					Start: &planpb.FrameBound{
						Type: planpb.FrameBound_PRECEDING,
						Val: stringMath("coalesce",
							stringMath("round", benchmarkStringMathDecimalColumn(), position(7)),
							stringMath("abs", position(10)),
						),
					},
					End: &planpb.FrameBound{
						Type: planpb.FrameBound_FOLLOWING,
						Val:  stringMath("abs", position(8)),
					},
				},
			}}}),
			// Window values/order keys and frame/partition controls must be
			// traversed independently while preserving each function's role. The
			// window is nested under ROUND's control argument so both containment
			// and role-discovery walkers must cross every window container.
			want: map[int]bool{4: true, 5: false, 6: true, 7: false, 8: true, 9: true, 10: true, -1: false},
		},
	}

	if preparedParamUsesStringMathFunction(nil, 0) {
		t.Fatal("nil plan unexpectedly reports a string-math value parameter")
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			owner := benchmarkStringMathOwnerPlan(test.expr)
			for position, want := range test.want {
				if got := preparedParamUsesStringMathFunction(owner, position); got != want {
					t.Errorf("position %d: want value-role=%v, got %v", position, want, got)
				}
			}
		})
	}
}

func benchmarkStringMathValues(params int, enabled bool) []any {
	values := make([]any, params)
	for i := range values {
		values[i] = ParamValue{
			Value:               "9007199254740992.0001tail",
			PrepareParamKind:    vector.PrepareParamDecimal,
			EnableNumericPrefix: enabled,
			IsBinaryProtocol:    true,
		}
	}
	return values
}

func benchmarkStringMathCases() []struct {
	name          string
	params        int
	depth         int
	noise         int
	eligible      bool
	valueCount    int
	numericPrefix bool
	noMatch       bool
} {
	return []struct {
		name          string
		params        int
		depth         int
		noise         int
		eligible      bool
		valueCount    int
		numericPrefix bool
		noMatch       bool
	}{
		{name: "p1-d0", params: 1, eligible: true, valueCount: 1, numericPrefix: true},
		{name: "p8-d0", params: 8, eligible: true, valueCount: 8, numericPrefix: true},
		{name: "p32-d0", params: 32, eligible: true, valueCount: 32, numericPrefix: true},
		{name: "p128-d0", params: 128, eligible: true, valueCount: 128, numericPrefix: true},
		{name: "p1-d1", params: 1, depth: 1, eligible: true, valueCount: 1, numericPrefix: true},
		{name: "p1-d8", params: 1, depth: 8, eligible: true, valueCount: 1, numericPrefix: true},
		{name: "p1-d32", params: 1, depth: 32, eligible: true, valueCount: 1, numericPrefix: true},
		{name: "p1-d64", params: 1, depth: 64, eligible: true, valueCount: 1, numericPrefix: true},
		{name: "p1-d8-n32", params: 1, depth: 8, noise: 32, eligible: true, valueCount: 1, numericPrefix: true},
		{name: "p1-d8-n128", params: 1, depth: 8, noise: 128, eligible: true, valueCount: 1, numericPrefix: true},
		{name: "no-match-p8-d8-n128", params: 8, depth: 8, noise: 128, valueCount: 8, numericPrefix: true, noMatch: true},
	}
}

func BenchmarkPreparedStringMathEligibility(b *testing.B) {
	for _, test := range benchmarkStringMathCases() {
		b.Run(test.name, func(b *testing.B) {
			query := benchmarkStringMathPlan(test.params, test.depth, test.noise, test.eligible, test.noMatch)
			values := benchmarkStringMathValues(test.valueCount, test.numericPrefix)
			want := test.eligible
			if got := PreparedPlanNeedsNumericPrefixSpecialization(query, values); got != want {
				b.Fatalf("eligible=%v, got=%v", want, got)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if got := PreparedPlanNeedsNumericPrefixSpecialization(query, values); got != want {
					b.Fatalf("eligible=%v, got=%v", want, got)
				}
			}
		})
	}
}

func BenchmarkPreparedStringMathRoleDiscovery(b *testing.B) {
	for _, test := range benchmarkStringMathRoleCases() {
		b.Run(test.name, func(b *testing.B) {
			for position, want := range test.want {
				if got := preparedParamUsesStringMathFunction(test.plan, position); got != want {
					b.Fatalf("position=%d, want=%v, got=%v", position, want, got)
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for position, want := range test.want {
					if got := preparedParamUsesStringMathFunction(test.plan, position); got != want {
						b.Fatalf("position=%d, want=%v, got=%v", position, want, got)
					}
				}
			}
		})
	}
}

func BenchmarkPreparedStringMathSpecialization(b *testing.B) {
	ctx := context.Background()
	for _, test := range benchmarkStringMathCases()[:10] {
		b.Run(test.name, func(b *testing.B) {
			query := benchmarkStringMathPlan(test.params, test.depth, test.noise, true, false)
			values := benchmarkStringMathValues(test.valueCount, true)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, err := FillValuesOfParamsInPlanWithSpecialization(ctx, DeepCopyPlan(query), values); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
