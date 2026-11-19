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
	"math"
	"math/big"
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/stretchr/testify/require"
)

func TestDecimalRevenuePrecisionRetainsIntegerDomain(t *testing.T) {
	ctx := context.Background()
	decimalType := types.New(types.T_decimal64, 15, 2)
	literal := makePlan2Int64ConstExprWithType(1)
	before := proto.Clone(literal)
	discount, err := BindFuncExprImplByPlanExpr(ctx, "-", []*planpb.Expr{literal, decimalArithmeticTestColumn(decimalType, 0)})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), discount.Typ.Id)
	require.Equal(t, int32(22), discount.Typ.Width)
	require.True(t, proto.Equal(before, literal), "lookup refinement mutated a shared literal")
	revenue, err := BindFuncExprImplByPlanExpr(ctx, "*", []*planpb.Expr{decimalArithmeticTestColumn(decimalType, 1), discount})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), revenue.Typ.Id)
	require.Equal(t, int32(37), revenue.Typ.Width)
	require.Equal(t, int32(4), revenue.Typ.Scale)
}

func TestCTASDecimalArithmeticPublishesDerivedPrecision(t *testing.T) {
	for _, tc := range []struct {
		name, expr   string
		width, scale int32
	}{
		{"discounted revenue", "l_extendedprice * (1 - l_discount)", 37, 4},
		{"direct product", "l_extendedprice * l_discount", 30, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			built, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t, "create table derived_precision as select "+tc.expr+" as amount from lineitem")
			require.NoError(t, err)
			col := built.GetDdl().GetCreateTable().GetTableDef().Cols[0]
			require.Equal(t, int32(types.T_decimal128), col.Typ.Id)
			require.Equal(t, tc.width, col.Typ.Width)
			require.Equal(t, tc.scale, col.Typ.Scale)
		})
	}
}

func TestDecimalPrecisionRetainsFullIntegerValues(t *testing.T) {
	for _, tc := range []struct{ name, sql, want string }{
		{"signed bigint", "select cast(9223372036854775807 as signed) * cast('9999999999999.99' as decimal(15,2))", "92233720368547665836279631452241.93"},
		{"unsigned bigint", "select cast(18446744073709551615 as unsigned) * cast('1.00' as decimal(15,2))", "18446744073709551615.00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := newPlanTestProcess(t)
			built, err := runOneExprStmt(NewMockOptimizer(false, proc), t, tc.sql)
			require.NoError(t, err)
			expr := built.GetQuery().Nodes[1].ProjectList[0]
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)
			value := vector.GetFixedAtWithTypeCheck[types.Decimal128](result, 0)
			require.Equal(t, tc.want, value.Format(expr.Typ.Scale))
		})
	}
}

func TestCTASDecimalAggregatePrecisionUsesColumnDomains(t *testing.T) {
	for _, tc := range []struct {
		name, aggregate string
		id              types.T
		width, scale    int32
	}{
		{"sum", "sum", types.T_decimal256, 52, 4},
		{"average", "avg", types.T_decimal128, 34, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			built, err := buildSingleStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
				"create table derived_aggregate as select "+tc.aggregate+"(l_extendedprice * l_discount) as amount from lineitem")
			require.NoError(t, err)
			col := built.GetDdl().GetCreateTable().GetTableDef().Cols[0]
			require.Equal(t, int32(tc.id), col.Typ.Id)
			require.Equal(t, tc.width, col.Typ.Width)
			require.Equal(t, tc.scale, col.Typ.Scale)
		})
	}
}

func TestPreparedDecimalArithmeticRetainsDeclaredRuntimeDomains(t *testing.T) {
	for _, tc := range []struct {
		name           string
		values         []any
		runtime        types.Type
		widths, scales []int32
	}{
		{"bigint", []any{"1", "9223372036854775807", nil, "1"}, types.T_int64.ToType(), []int32{22, 22, 34}, []int32{2, 2, 2}},
		{"decimal", []any{"1.25", "999999999999999999.99", nil, "1.25"}, types.New(types.T_decimal128, 20, 2), []int32{21, 21, 35}, []int32{2, 2, 4}},
	} {
		for i, op := range []string{"+", "-", "*"} {
			for _, expression := range []string{
				"cast(n_nationkey as decimal(15,2)) " + op + " ?",
				"? " + op + " cast(n_nationkey as decimal(15,2))",
				"abs(cast(n_nationkey as decimal(15,2)) " + op + " ?)",
			} {
				t.Run(tc.name+expression, func(t *testing.T) {
					prepared, err := runOneStmt(NewMockOptimizer(false, newPlanTestProcess(t)), t,
						"prepare arithmetic_domain from 'select "+expression+" as amount from nation'")
					require.NoError(t, err)
					cached := prepared.GetDcl().GetPrepare().Plan
					before := proto.Clone(cached)
					for _, value := range tc.values {
						filled, _, err := FillValuesOfParamsInPlanWithSpecialization(context.Background(), cached, []any{
							ParamValue{Value: value, RuntimeType: tc.runtime, HasRuntimeType: true, IsBinaryProtocol: true},
						})
						require.NoError(t, err)
						query := filled.GetQuery()
						expr := query.Nodes[len(query.Nodes)-1].ProjectList[0]
						require.Equal(t, int32(types.T_decimal128), expr.Typ.Id)
						require.Equal(t, tc.widths[i], expr.Typ.Width)
						require.Equal(t, tc.scales[i], expr.Typ.Scale)
						require.True(t, proto.Equal(before, cached), "EXECUTE must not mutate the prepared domain")
					}
				})
			}
		}
	}
}

func TestPreparedDecimalArithmeticRecursiveRebindRetainsDomain(t *testing.T) {
	ctx := t.Context()
	runtime := types.New(types.T_decimal128, 20, 2)
	for _, op := range []string{"+", "-", "*"} {
		t.Run(op, func(t *testing.T) {
			marker := &Expr{Typ: makePlan2Type(&runtime), Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
			source, err := BindFuncExprImplByPlanExpr(ctx, op, []*Expr{
				decimalArithmeticTestColumn(types.New(types.T_decimal64, 15, 2), 0), marker,
			})
			require.NoError(t, err)
			before := proto.Clone(source)
			value, err := preparedRuntimeParamExpr(ctx, "1.25", false, runtime)
			require.NoError(t, err)
			rule := NewResetParamRefRule(ctx, []*Expr{value})
			rule.SetParamValues([]any{ParamValue{Value: "1.25", RuntimeType: runtime, HasRuntimeType: true, IsBinaryProtocol: true}})
			bound, changed, err := rule.rebindPreparedNumericExpr(source, map[int32]struct{}{0: {}})
			require.NoError(t, err)
			require.True(t, changed)
			require.Equal(t, source.Typ, bound.Typ, "a runtime value must not narrow the declared domain")
			require.True(t, proto.Equal(before, source), "recursive rebinding must preserve its source")
		})
	}
}

func TestDecimalCheckedCarrierProductRetainsFollowingDomain(t *testing.T) {
	proc := newPlanTestProcess(t)
	leftType, rightType, tailType := types.New(types.T_decimal128, 20, 0), types.New(types.T_decimal64, 18, 0), types.New(types.T_decimal64, 17, 0)
	first, err := BindFuncExprImplByPlanExpr(t.Context(), "*", []*planpb.Expr{decimalArithmeticTestColumn(leftType, 0), decimalArithmeticTestColumn(rightType, 1)})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal128), first.Typ.Id)
	require.Equal(t, int32(38), first.Typ.Width)
	expr, err := BindFuncExprImplByPlanExpr(t.Context(), "*", []*planpb.Expr{first, decimalArithmeticTestColumn(tailType, 2)})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), expr.Typ.Id)
	require.Equal(t, int32(56), expr.Typ.Width)
	input := batch.NewWithSize(3)
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	for i, typ := range []types.Type{leftType, rightType, tailType} {
		input.Vecs[i] = vector.NewVec(typ)
	}
	left, err := types.ParseDecimal128("18000000000000000000", 20, 0)
	require.NoError(t, err)
	values := []int64{math.MaxInt64, math.MinInt64, 0}
	for row, v := range values {
		require.NoError(t, vector.AppendFixed(input.Vecs[0], left, false, proc.Mp()))
		// A released TIME/integer producer can supply this full signed
		// coefficient in its precision-18 D64 envelope.
		require.NoError(t, vector.AppendFixed(input.Vecs[1], types.Decimal64(v), row == 2, proc.Mp()))
		require.NoError(t, vector.AppendFixed(input.Vecs[2], types.Decimal64(99999999999999999), false, proc.Mp()))
	}
	input.SetRowCount(3)
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	for row := range 2 {
		want, _ := new(big.Int).SetString("18000000000000000000", 10)
		want.Mul(want, big.NewInt(values[row]))
		want.Mul(want, big.NewInt(99999999999999999))
		require.Equal(t, want.String(), vector.GetFixedAtWithTypeCheck[types.Decimal256](result, row).Format(0))
	}
	require.True(t, result.IsNull(2))
}
