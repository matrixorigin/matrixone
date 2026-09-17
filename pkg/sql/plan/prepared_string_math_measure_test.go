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
			name: "outer-abs-does-not-own-length-argument",
			plan: benchmarkStringMathOwnerPlan(benchmarkStringMathBind("abs", []*planpb.Expr{
				benchmarkStringMathBind("length", []*planpb.Expr{benchmarkStringMathParam(0)}),
			})),
			want: []bool{false},
		},
		{
			name: "nested-abs-owner-through-length",
			plan: benchmarkStringMathOwnerPlan(benchmarkStringMathBind("length", []*planpb.Expr{
				benchmarkStringMathBind("abs", []*planpb.Expr{benchmarkStringMathParam(0)}),
			})),
			want: []bool{true},
		},
		{
			name: "outer-round-does-not-own-concat-argument",
			plan: benchmarkStringMathOwnerPlan(benchmarkStringMathBind("round", []*planpb.Expr{
				benchmarkStringMathBind("concat", []*planpb.Expr{
					benchmarkStringMathTextLiteral("1"), benchmarkStringMathParam(0),
				}), benchmarkStringMathIntLiteral(0),
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
			name: "outer-math-does-not-own-through-length",
			expr: stringMath("abs", stringMath("length", position(13))),
			want: map[int]bool{13: false},
		},
		{
			name: "nested-owner-through-length",
			expr: stringMath("length", stringMath("abs", position(14))),
			want: map[int]bool{14: true},
		},
		{
			name: "outer-math-does-not-own-through-concat",
			expr: stringMath("round",
				stringMath("concat", benchmarkStringMathTextLiteral("1"), position(15)),
				benchmarkStringMathIntLiteral(0)),
			want: map[int]bool{15: false},
		},
		{
			name: "outer-math-does-not-own-list-members",
			expr: stringMath("abs", list(position(16))),
			want: map[int]bool{16: false},
		},
		{
			name: "nested-owner-in-list-remains-discoverable",
			expr: stringMath("abs", list(stringMath("abs", position(17)))),
			want: map[int]bool{17: true},
		},
		{
			name: "window-value-and-controls-have-separate-roles",
			expr: stringMath("abs", &planpb.Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
				WindowFunc: stringMath("abs", position(18)),
				PartitionBy: []*planpb.Expr{
					position(19),
					stringMath("abs", position(20)),
				},
				OrderBy: []*planpb.OrderBySpec{{Expr: position(21)}},
				Frame: &planpb.FrameClause{Start: &planpb.FrameBound{
					Type: planpb.FrameBound_PRECEDING,
					Val:  position(22),
				}},
			}}}),
			want: map[int]bool{18: true, 19: false, 20: true, 21: false, 22: false},
		},
		{
			name: "planner-bound-implicit-cast",
			expr: benchmarkStringMathBind("abs", []*planpb.Expr{position(23)}),
			want: map[int]bool{23: true},
		},
		{
			name: "planner-bound-inner-owner-through-length",
			expr: benchmarkStringMathBind("length", []*planpb.Expr{
				benchmarkStringMathBind("abs", []*planpb.Expr{position(14)}),
			}),
			want: map[int]bool{14: true},
		},
		{
			name: "planner-bound-length-domain-boundary",
			expr: benchmarkStringMathBind("abs", []*planpb.Expr{
				benchmarkStringMathBind("length", []*planpb.Expr{position(25)}),
			}),
			want: map[int]bool{25: false},
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

func TestPreparedNumericRebindingStopsAtNonNumericFunctionArguments(t *testing.T) {
	param := benchmarkStringMathParam(0)
	positions := map[int32]struct{}{0: {}}
	rule := NewResetParamRefRule(context.Background(), []*planpb.Expr{
		benchmarkStringMathTextLiteral("01"),
	})
	// The outer deferred numeric owner has an eligible position and has already
	// produced this text-preserving CONCAT occurrence. Neither a permissive
	// string-math value role nor role=None may rewrite the marker inside CONCAT.
	rule.sqlExecuteStringMathParams = []*planpb.Expr{makePlan2Float64ConstExprWithType(1)}

	for _, role := range []struct {
		name string
		role preparedStringMathRole
	}{
		{name: "outer value role", role: preparedStringMathRoleValue},
		{name: "fallback role none", role: preparedStringMathRoleNone},
	} {
		t.Run(role.name, func(t *testing.T) {
			expr := benchmarkStringMathBind("concat", []*planpb.Expr{
				benchmarkStringMathTextLiteral("1"), param,
			})
			bound := benchmarkStringMathBind("concat", []*planpb.Expr{
				benchmarkStringMathTextLiteral("1"), benchmarkStringMathTextLiteral("01"),
			})
			got, changed, err := rule.rebindPreparedNumericExprWithRole(
				expr, bound, positions, role.role)
			if err != nil {
				t.Fatalf("rebind returned an error across CONCAT: %v", err)
			}
			if changed {
				t.Fatalf("rebind crossed CONCAT argument boundary; got %v", got)
			}
			if got != bound {
				t.Fatal("unchanged nonnumeric occurrence did not preserve its bound wrapper")
			}
		})
	}
	t.Run("list wrapper is preserved", func(t *testing.T) {
		expr := &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{
			List: []*planpb.Expr{param},
		}}}
		bound := &planpb.Expr{Expr: &planpb.Expr_List{List: &planpb.ExprList{
			List: []*planpb.Expr{benchmarkStringMathTextLiteral("01")},
		}}}
		got, changed, err := rule.rebindPreparedNumericExprWithRole(
			expr, bound, positions, preparedStringMathRoleValue)
		if err != nil {
			t.Fatalf("rebind returned an error across a list wrapper: %v", err)
		}
		if changed || got != bound {
			t.Fatalf("rebind changed the non-scalar list occurrence: changed=%v got=%v", changed, got)
		}
	})
	t.Run("window wrapper preserves bound controls", func(t *testing.T) {
		boundParam := benchmarkStringMathTextLiteral("01")
		expr := &planpb.Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
			WindowFunc:  benchmarkStringMathBind("abs", []*planpb.Expr{param}),
			PartitionBy: []*planpb.Expr{param},
			OrderBy:     []*planpb.OrderBySpec{{Expr: param}},
			Frame: &planpb.FrameClause{Start: &planpb.FrameBound{
				Type: planpb.FrameBound_PRECEDING,
				Val:  param,
			}},
		}}}
		bound := &planpb.Expr{Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
			WindowFunc:  benchmarkStringMathBind("abs", []*planpb.Expr{boundParam}),
			PartitionBy: []*planpb.Expr{boundParam},
			OrderBy:     []*planpb.OrderBySpec{{Expr: boundParam}},
			Frame: &planpb.FrameClause{Start: &planpb.FrameBound{
				Type: planpb.FrameBound_PRECEDING,
				Val:  boundParam,
			}},
		}}}
		got, changed, err := rule.rebindPreparedNumericExprWithRole(
			expr, bound, positions, preparedStringMathRoleValue)
		if err != nil {
			t.Fatalf("rebind returned an error across a window wrapper: %v", err)
		}
		if changed || got != bound {
			t.Fatalf("rebind changed the bound window/control subtree: changed=%v got=%v", changed, got)
		}
		window := got.GetW()
		if window.PartitionBy[0] != boundParam || window.OrderBy[0].Expr != boundParam ||
			window.Frame.Start.Val != boundParam {
			t.Fatal("window partition/order/frame did not retain the already-bound text marker")
		}
	})
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
