// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package plan

import (
	"context"
	"fmt"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestIntegerArgumentMySQLSourceEvaluation(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	for _, tc := range []struct{ input, want string }{
		{"1.5e0", "a.b"}, {"2.5e0", "a.b"}, {"1.5", "a.b"}, {"2.5", "a.b.c"},
		{"2.5E0", "a.b"},
		{"-2.5e0", "c.d"}, {"-2.5", "b.c.d"},
		{"cast(1.5 as double)", "a"}, {"cast(-1.5 as double)", "d"},
		{"cast(1.5 as double)+0e0", "a.b"}, {"abs(cast(1.5 as double))", "a.b"},
		{"if(true,cast(1.5 as double),1.5e0)", "a"}, {"if(false,cast(1.5 as double),1.5e0)", "a.b"},
		{"if(true,2.5,2.5e0)", "a.b.c"}, {"if(false,2.5,2.5e0)", "a.b"},
		{"case when true then 2.5 else 2.5e0 end", "a.b.c"},
		{"case 1 when 1 then cast(1.5 as double) else 1.5e0 end", "a"},
		{"coalesce(cast(1.5 as double),1.5e0)", "a.b"},
		{"ifnull(cast(1.5 as double),0e0)", "a.b"},
		{"nullif(cast(1.5 as double),0e0)", "a"},
		{"if(true,ifnull(cast(1.5 as double),0e0),0)", "a.b"},
		{"'1.9tail'", "a"}, {"'bad'", ""}, {"true", "a"},
		{"cast('9007199254740993' as decimal(20,0))", "a.b.c.d"},
		{"case when false then cast('9223372036854775808' as unsigned) else 2 end", "a.b"},
	} {
		t.Run(tc.input, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select substring_index('a.b.c.d','.',"+tc.input+")", 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			require.Equal(t, int32(types.T_int64), bound.GetF().Args[2].Typ.Id)
			_, overload := function.DecodeOverloadID(bound.GetF().Func.Obj)
			require.Equal(t, int32(2), overload)
			folded, err := ConstantFold(batch.EmptyForConstFoldBatch, bound, proc, false, true)
			require.NoError(t, err)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, folded, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.False(t, result.IsConstNull())
			require.Equal(t, tc.want, result.GetStringAt(0))
		})
	}
}

func TestIntegerArgumentRejectedTemporalType(t *testing.T) {
	ctx := context.Background()
	stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, "select left('abc',cast('00:00:01.5' as time(6)))", 1)
	require.NoError(t, err)
	defer stmt.Free()
	ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
	_, err = NewDefaultBinder(ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
	require.Error(t, err, "source type is rejected before execution, not merely on a non-NULL row")
}

func TestIntegerArgumentSourceContextScope(t *testing.T) {
	ctx := context.Background()
	for _, sql := range []string{
		"select substring_index('a.b.c','.',2.5)",
		"select substring_index('a.b.c','.',no_such_integer_source())",
	} {
		t.Run(sql, func(t *testing.T) {
			binder := NewDefaultBinder(ctx, nil, nil, planpb.Type{Id: int32(types.T_varchar), Width: 64}, nil)
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, sql, 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			_, err = binder.BindExpr(ast, 0, false)
			if sql == "select substring_index('a.b.c','.',2.5)" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.False(t, binder.integerArgumentSourceContext)
			stmt2, err := parsers.ParseOne(ctx, dialect.MYSQL, "select 2.5", 1)
			require.NoError(t, err)
			defer stmt2.Free()
			ast2 := stmt2.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			ordinary, err := binder.BindExpr(ast2, 0, false)
			require.NoError(t, err)
			require.Equal(t, int32(types.T_varchar), ordinary.Typ.Id, "ordinary assignment context must be restored")
		})
	}
}

func TestIntegerArgumentBoundSources(t *testing.T) {
	ctx := context.Background()
	for _, source := range []types.T{types.T_float32, types.T_float64, types.T_decimal64, types.T_decimal128, types.T_decimal256, types.T_uint64, types.T_bit, types.T_varchar} {
		t.Run(source.String(), func(t *testing.T) {
			column := &planpb.Expr{Typ: planpb.Type{Id: int32(source), Scale: 1}, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
			args := []*Expr{makePlan2StringConstExprWithType("a.b.c"), makePlan2StringConstExprWithType("."), column}
			original := DeepCopyExpr(column)
			bound, err := BindFuncExprImplByPlanExpr(ctx, "substring_index", args)
			require.NoError(t, err)
			cast := bound.GetF().Args[2]
			require.Equal(t, int32(types.T_int64), cast.Typ.Id)
			_, overload := function.DecodeOverloadID(cast.GetF().Func.Obj)
			require.Equal(t, function.IntegerArgumentCastOverload, overload)
			require.Equal(t, original, column)
			require.Same(t, column, args[2])
		})
	}
}

func TestIntegerArgumentBoundSelection(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	for _, selected := range []bool{true, false} {
		t.Run(fmt.Sprint(selected), func(t *testing.T) {
			decimal, err := makePlan2DecimalExprWithType(ctx, "2.5")
			require.NoError(t, err)
			common, err := BindFuncExprImplByPlanExpr(ctx, "case", []*Expr{makePlan2BoolConstExprWithType(selected), decimal, makePlan2Float64ConstExprWithType(2.5)})
			require.NoError(t, err)
			original := DeepCopyExpr(common)
			bound, err := BindFuncExprImplByPlanExpr(ctx, "substring_index", []*Expr{makePlan2StringConstExprWithType("a.b.c.d"), makePlan2StringConstExprWithType("."), common})
			require.NoError(t, err)
			require.Equal(t, original, common)
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, bound, []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			want := "a.b"
			if selected {
				want = "a.b.c"
			}
			defer free()
			require.Equal(t, want, result.GetStringAt(0))
		})
	}
}

func TestIntegerArgumentPreparedRuntimeCandidates(t *testing.T) {
	for _, tc := range []struct {
		query     string
		positions []int32
	}{
		{`select substring_index(?,".",?)`, []int32{1}},
		{`select period_add(?,?)`, []int32{0, 1}},
		{`select period_diff(?,?)`, []int32{0, 1}},
		{`select ceil(1.25,?)`, []int32{0}},
		{`select floor(1.25,?)`, []int32{0}},
		{`select round(1.25,?)`, []int32{0}},
		{`select truncate(1.25,?)`, []int32{0}},
		{`select from_days(?)`, []int32{0}},
		{`select week(cast("2026-09-20" as date),?)`, []int32{0}},
		{`select yearweek(cast("2026-09-20" as date),?)`, []int32{0}},
		{`select timestampadd(day,?,cast("2026-09-20" as date))`, []int32{0}},
		{`select subvector(cast("[1,2,3]" as vecf32(3)),?)`, []int32{0}},
		{`select last_query_id(?)`, []int32{0}},
		{`select random_bytes(?)`, []int32{0}},
		{`select sha2("x",?)`, []int32{0}},
		{`select split_part("a.b.c",".",?)`, []int32{0}},
		{`select regexp_instr("abc","b",?)`, []int32{0}},
		{`select regexp_replace("abc","b","x",?,?)`, []int32{0, 1}},
		{`select regexp_substr("abc","b",?,?)`, []int32{0, 1}},
		{`select substring_index("a.b.c",".",cast(? as double))`, []int32{0}},
		{`select substring_index("a.b.c",".",if(?,?,?))`, []int32{1, 2}},
		{`select substring_index("a.b.c",".",coalesce(?,0e0))`, []int32{0}},
		{`select substring_index("a.b.c",".",ifnull(?,0e0))`, []int32{0}},
		{`select substring_index("a.b.c",".",(select ? where true))`, []int32{0}},
		{`select substring_index("a.b.c",".",(select ? group by 1))`, []int32{0}},
		{`select substring_index("a.b.c",".",(select coalesce(?,0e0) where ?))`, []int32{0}},
		{`select substring_index("a.b.c",".",(select 0e0 where ? union all select ?))`, []int32{1}},
		{`select substring_index("a.b.c",".",(select ? where false union select ?))`, []int32{0, 1}},
		{`select substring_index("a.b.c",".",(select max(coalesce(?,0e0)) where ?))`, []int32{0}},
		{`select substring_index("a.b.c",".",(select first_value(coalesce(?,0e0)) over (order by ?) where true))`, []int32{0}},
		{`select substring_index("a.b.c",".",(select ? union all select cast(0 as double) limit 1))`, []int32{0}},
		{`select substring_index(?,".",2)`, nil},
	} {
		t.Run(tc.query, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, "prepare integer_source from '"+tc.query+"'")
			require.NoError(t, err)
			require.Equal(t, tc.positions, PreparedPlanNumericFallbackParamPositions(prepared.GetDcl().GetPrepare().Plan))
		})
	}
}

func TestIntegerArgumentMixedPreparedSources(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		query string
		value any
		typ   types.T
		want  string
		null  bool
	}{
		{`select substring_index("a.b.c.d",".",?)`, "1.5", types.T_float64, "a.b", false},
		{`select substring_index("a.b.c.d",".",?)`, "1.9tail", types.T_any, "a", false},
		{`select substring_index("a.b.c.d",".",?)`, "2", types.T_uint64, "a.b", false},
		{`select substring_index("a.b.c.d",".",cast(? as double))`, "1.5", types.T_float64, "a", false},
		{`select substring_index("a.b.c.d",".",?)`, nil, types.T_any, "", true},
	} {
		t.Run(tc.query+"/"+tc.typ.String(), func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, "prepare mixed_integer from '"+tc.query+"'")
			require.NoError(t, err)
			original := prepared.GetDcl().GetPrepare().Plan
			require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(original))
			bound, changed, err := FillValuesOfParamsInPlanWithPreparedNumericOverload(proc.Ctx, original, []any{ParamValue{Value: tc.value, IsBinaryProtocol: true, RuntimeType: tc.typ.ToType(), HasRuntimeType: tc.typ != types.T_any}})
			require.NoError(t, err)
			require.True(t, changed)
			params := vector.NewVec(types.T_text.ToType())
			defer func() { proc.SetPrepareParams(nil); params.Free(proc.Mp()) }()
			require.NoError(t, vector.AppendBytes(params, []byte(fmt.Sprint(tc.value)), tc.value == nil, proc.Mp()))
			proc.SetPrepareParams(params)
			query := bound.GetQuery()
			root := query.Nodes[query.Steps[0]]
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, root.ProjectList[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			if tc.null {
				require.True(t, result.IsConstNull() || result.IsNull(0))
			} else {
				require.Equal(t, tc.want, result.GetStringAt(0))
			}
		})
	}
}

func TestIntegerArgumentPreparedSelectors(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, tc := range []struct {
		query  string
		params []any
		want   string
	}{
		{
			`select substring_index("a.b.c.d",".",if(true,?,?))`,
			[]any{
				ParamValue{Value: float64(1.5), IsBinaryProtocol: true, HasRuntimeType: true, RuntimeType: types.T_float64.ToType()},
				ParamValue{Value: "2.5", SourceType: types.New(types.T_decimal64, 2, 1), HasSourceType: true},
			},
			"a.b",
		},
		{
			`select substring_index("a.b.c.d",".",case when false then ? else ? end)`,
			[]any{
				ParamValue{Value: "1.5", SourceType: types.New(types.T_decimal64, 2, 1), HasSourceType: true},
				ParamValue{Value: "2.5", SourceType: types.New(types.T_decimal64, 2, 1), HasSourceType: true},
			},
			"a.b.c",
		},
		{
			`select substring_index("a.b.c.d",".",nullif(?,?))`,
			[]any{
				ParamValue{Value: float64(1.5), IsBinaryProtocol: true, HasRuntimeType: true, RuntimeType: types.T_float64.ToType()},
				ParamValue{Value: float64(2.5), IsBinaryProtocol: true, HasRuntimeType: true, RuntimeType: types.T_float64.ToType()},
			},
			"a.b",
		},
		{
			`select substring_index("a.b.c.d",".",coalesce(?,0e0))`,
			[]any{ParamValue{Value: float64(1.5), IsBinaryProtocol: true, HasRuntimeType: true, RuntimeType: types.T_float64.ToType()}},
			"a.b",
		},
		{
			`select substring_index("a.b.c.d",".",ifnull(?,0e0))`,
			[]any{ParamValue{Value: float64(1.5), IsBinaryProtocol: true, HasRuntimeType: true, RuntimeType: types.T_float64.ToType()}},
			"a.b",
		},
		{
			`select substring_index("a.b.c.d",".",coalesce(?,0e0))`,
			[]any{ParamValue{Value: "1.5", IsBinaryProtocol: true, HasRuntimeType: true, RuntimeType: types.T_varchar.ToType()}},
			"a",
		},
		{
			`select substring_index("a.b.c.d",".",ifnull(?,0e0))`,
			[]any{ParamValue{Value: "1.5", IsBinaryProtocol: true, HasRuntimeType: true, RuntimeType: types.T_varchar.ToType()}},
			"a",
		},
		{
			`select substring_index("a.b.c.d",".",coalesce(?,"0"))`,
			[]any{ParamValue{Value: float64(1.5), IsBinaryProtocol: true, HasRuntimeType: true, RuntimeType: types.T_float64.ToType()}},
			"a",
		},
	} {
		t.Run(tc.query, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, "prepare integer_selector from '"+tc.query+"'")
			require.NoError(t, err)
			bound, changed, err := FillValuesOfParamsInPlanWithPreparedNumericOverload(proc.Ctx, prepared.GetDcl().GetPrepare().Plan, tc.params)
			require.NoError(t, err)
			require.True(t, changed)
			query := bound.GetQuery()
			root := query.Nodes[query.Steps[0]]
			result, free, err := colexec.GetReadonlyResultFromExpression(proc, root.ProjectList[0], []*batch.Batch{batch.EmptyForConstFoldBatch})
			require.NoError(t, err)
			defer free()
			require.Equal(t, tc.want, result.GetStringAt(0))
		})
	}
}

func TestIntegerArgumentPreparedScalarSubquery(t *testing.T) {
	proc := testutil.NewProcess(t)
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		`prepare integer_scalar from 'select substring_index("a.b.c.d",".",(select ? where true))'`)
	require.NoError(t, err)
	original := prepared.GetDcl().GetPrepare().Plan
	require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(original))

	bound, changed, err := FillValuesOfParamsInPlanWithPreparedNumericOverload(proc.Ctx, original, []any{
		ParamValue{Value: float64(1.5), IsBinaryProtocol: true, HasRuntimeType: true, RuntimeType: types.T_float64.ToType()},
	})
	require.NoError(t, err)
	require.True(t, changed)
	query := bound.GetQuery()
	root := query.Nodes[query.Steps[0]]
	privateCast := root.ProjectList[0].GetF().Args[2]
	require.True(t, isIntegerArgumentCast(privateCast))
	require.Equal(t, int32(types.T_float64), privateCast.GetF().Args[0].Typ.Id)

	var scalarProjection *Expr
	for _, node := range query.Nodes {
		if node == nil || node.NodeType != planpb.Node_PROJECT || len(node.ProjectList) != 1 {
			continue
		}
		if literal := node.ProjectList[0].GetLit(); literal != nil && literal.GetDval() == 1.5 {
			scalarProjection = node.ProjectList[0]
			break
		}
	}
	require.NotNil(t, scalarProjection)
	inner, freeInner, err := colexec.GetReadonlyResultFromExpression(
		proc, scalarProjection, []*batch.Batch{batch.EmptyForConstFoldBatch})
	require.NoError(t, err)
	defer freeInner()
	input := batch.NewWithSize(1)
	input.Vecs[0] = inner
	input.SetRowCount(1)
	result, freeResult, err := colexec.GetReadonlyResultFromExpression(proc, root.ProjectList[0], []*batch.Batch{input})
	require.NoError(t, err)
	defer freeResult()
	require.Equal(t, "a.b", result.GetStringAt(0))
}

func TestIntegerArgumentPreparedGroupedAndSetScalarSubqueries(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, sql := range []string{
		`select substring_index("a.b.c.d",".",(select ? group by 1))`,
		`select substring_index("a.b.c.d",".",(select ? union all select cast(0 as double) limit 1))`,
	} {
		t.Run(sql, func(t *testing.T) {
			prepared, err := runOneStmt(NewMockOptimizer(false), t, "prepare integer_scalar_shape from '"+sql+"'")
			require.NoError(t, err)
			original := prepared.GetDcl().GetPrepare().Plan
			require.Equal(t, []int32{0}, PreparedPlanNumericFallbackParamPositions(original))

			bound, changed, err := FillValuesOfParamsInPlanWithPreparedNumericOverload(proc.Ctx, original, []any{
				ParamValue{Value: float64(1.5), IsBinaryProtocol: true, HasRuntimeType: true, RuntimeType: types.T_float64.ToType()},
			})
			require.NoError(t, err)
			require.True(t, changed)
			query := bound.GetQuery()
			root := query.Nodes[query.Steps[0]]
			privateCast := root.ProjectList[0].GetF().Args[2]
			require.True(t, isIntegerArgumentCast(privateCast))
			require.Equal(t, int32(types.T_float64), privateCast.GetF().Args[0].Typ.Id)

			input := batch.NewWithSize(1)
			input.Vecs[0], err = vector.NewConstFixed(types.T_float64.ToType(), 1.5, 1, proc.Mp())
			require.NoError(t, err)
			input.SetRowCount(1)
			defer input.Clean(proc.Mp())
			result, freeResult, err := colexec.GetReadonlyResultFromExpression(
				proc, root.ProjectList[0], []*batch.Batch{input})
			require.NoError(t, err)
			defer freeResult()
			require.Equal(t, "a.b", result.GetStringAt(0))
		})
	}
}

func TestIntegerArgumentPreparedSourceEvaluation(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	for _, explicitReal := range []bool{false, true} {
		marker := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_text)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
		var source *Expr = marker
		if explicitReal {
			var err error
			typ := types.T_float64.ToType()
			source, err = appendExplicitCastBeforeExpr(ctx, source, makePlan2Type(&typ))
			require.NoError(t, err)
		}
		prepared, err := BindFuncExprImplByPlanExpr(ctx, "substring_index", []*Expr{makePlan2StringConstExprWithType("a.b.c.d"), makePlan2StringConstExprWithType("."), source})
		require.NoError(t, err)
		original := DeepCopyExpr(prepared)
		for _, tc := range []struct {
			name           string
			param          ParamValue
			want, castWant string
		}{
			{"SQL DOUBLE", ParamValue{Value: "1.5", SourceType: types.T_float64.ToType(), HasSourceType: true}, "a.b", "a"},
			{"SQL DECIMAL", ParamValue{Value: "2.5", SourceType: types.New(types.T_decimal64, 2, 1), HasSourceType: true}, "a.b.c", "a.b"},
			{"SQL text", ParamValue{Value: "1.9", SourceType: types.T_varchar.ToType(), HasSourceType: true}, "a", "a"},
			{"binary DOUBLE", ParamValue{Value: float64(1.5), RuntimeType: types.T_float64.ToType(), HasRuntimeType: true, IsBinaryProtocol: true}, "a.b", "a"},
			{"binary numeric spelling is text", ParamValue{Value: "1.5", IsBinaryProtocol: true}, "a", "a"},
		} {
			t.Run(fmt.Sprintf("explicit=%v/%s", explicitReal, tc.name), func(t *testing.T) {
				rule := NewResetParamRefRule(ctx, []*Expr{makePlan2StringConstExprWithType("placeholder")})
				rule.SetParamValues([]any{tc.param})
				rewritten, err := rule.ApplyExpr(DeepCopyExpr(prepared))
				require.NoError(t, err)
				result, free, err := colexec.GetReadonlyResultFromExpression(proc, rewritten, []*batch.Batch{batch.EmptyForConstFoldBatch})
				require.NoError(t, err)
				defer free()
				want := tc.want
				if explicitReal {
					want = tc.castWant
				}
				require.Equal(t, want, result.GetStringAt(0))
				require.Equal(t, original, prepared)
			})
		}
	}
}

func TestIntegerArgumentRuntimeSourcePreservesNestedPrivateCast(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	marker := &Expr{Typ: planpb.Type{Id: int32(types.T_text)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	logical, err := appendIntegerArgument(ctx, marker, types.T_int64, false)
	require.NoError(t, err)
	physical := types.T_int8.ToType()
	adapter, err := appendCastBeforeExprWithOverload(ctx, logical, makePlan2Type(&physical), 0)
	require.NoError(t, err)
	rule := NewResetParamRefRule(ctx, []*Expr{makePlan2StringConstExprWithType("placeholder")})
	rule.SetParamValues([]any{ParamValue{Value: float64(1.5), IsBinaryProtocol: true, HasRuntimeType: true, RuntimeType: types.T_float64.ToType()}})
	rebound, err := rule.integerArgumentRuntimeSource(adapter)
	require.NoError(t, err)
	require.True(t, isIntegerArgumentCast(rebound.GetF().Args[0]), "a physical adapter must not turn private coercion into ordinary CAST")
	result, free, err := colexec.GetReadonlyResultFromExpression(proc, rebound, []*batch.Batch{batch.EmptyForConstFoldBatch})
	require.NoError(t, err)
	defer free()
	require.Equal(t, int8(2), vector.GetFixedAtNoTypeCheck[int8](result, 0))
}

func TestIntegerArgumentOverflowIsNotSaturation(t *testing.T) {
	ctx := context.Background()
	proc := testutil.NewProcess(t)
	for _, input := range []string{"9223372036854775808", "-9223372036854775809", "cast('18446744073709551615' as unsigned)", "'9223372036854775808'", "1e30"} {
		t.Run(input, func(t *testing.T) {
			stmt, err := parsers.ParseOne(ctx, dialect.MYSQL, fmt.Sprintf("select substring_index('a.b','.',%s)", input), 1)
			require.NoError(t, err)
			defer stmt.Free()
			ast := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr
			bound, err := NewDefaultBinder(ctx, nil, nil, planpb.Type{}, nil).BindExpr(ast, 0, false)
			require.NoError(t, err)
			_, err = ConstantFold(batch.EmptyForConstFoldBatch, bound, proc, false, true)
			require.Error(t, err)
		})
	}
}
