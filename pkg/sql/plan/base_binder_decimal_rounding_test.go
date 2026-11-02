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
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/stretchr/testify/require"
)

func TestDecimalRoundingReturnMetadata(t *testing.T) {
	decimalArg := func(typ types.Type) *planpb.Expr {
		return &planpb.Expr{
			Typ:  makePlan2Type(&typ),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
		}
	}
	digitsArg := func(value int64) *planpb.Expr {
		return makePlan2Int64ConstExprWithType(value)
	}

	for _, test := range []struct {
		name     string
		function string
		input    types.Type
		digits   *int64
		want     types.Type
	}{
		{name: "round decimal64", function: "round", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(2)), want: types.New(types.T_decimal64, 9, 2)},
		{name: "round decimal128", function: "round", input: types.New(types.T_decimal128, 38, 10), digits: ptrTo(int64(5)), want: types.New(types.T_decimal128, 34, 5)},
		{name: "round decimal256", function: "round", input: types.New(types.T_decimal256, 50, 10), digits: ptrTo(int64(5)), want: types.New(types.T_decimal256, 46, 5)},
		{name: "round keeps input when digits exceeds scale", function: "round", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(8)), want: types.New(types.T_decimal64, 10, 4)},
		{name: "round negative digits reserves carry", function: "round", input: types.New(types.T_decimal128, 30, 0), digits: ptrTo(int64(-30)), want: types.New(types.T_decimal128, 31, 0)},
		{name: "round decimal64 negative digits reserves carry", function: "round", input: types.New(types.T_decimal64, 15, 0), digits: ptrTo(int64(-15)), want: types.New(types.T_decimal64, 16, 0)},
		{name: "round decimal256 negative digits reserves carry", function: "round", input: types.New(types.T_decimal256, 60, 0), digits: ptrTo(int64(-60)), want: types.New(types.T_decimal256, 61, 0)},
		{name: "round negative digits retains conservative fractional envelope", function: "round", input: types.New(types.T_decimal128, 30, 2), digits: ptrTo(int64(-28)), want: types.New(types.T_decimal128, 30, 0)},
		{name: "truncate negative digits has no carry", function: "truncate", input: types.New(types.T_decimal128, 30, 0), digits: ptrTo(int64(-30)), want: types.New(types.T_decimal128, 30, 0)},
		{name: "truncate decimal64", function: "truncate", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(2)), want: types.New(types.T_decimal64, 8, 2)},
		{name: "truncate to integer", function: "truncate", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(0)), want: types.New(types.T_decimal64, 6, 0)},
		{name: "truncate decimal128", function: "truncate", input: types.New(types.T_decimal128, 38, 10), digits: ptrTo(int64(5)), want: types.New(types.T_decimal128, 33, 5)},
		{name: "truncate decimal256", function: "truncate", input: types.New(types.T_decimal256, 50, 10), digits: ptrTo(int64(5)), want: types.New(types.T_decimal256, 45, 5)},
		{name: "ceil decimal64", function: "ceil", input: types.New(types.T_decimal64, 10, 4), want: types.New(types.T_decimal64, 7, 0)},
		{name: "floor decimal64", function: "floor", input: types.New(types.T_decimal64, 10, 4), want: types.New(types.T_decimal64, 7, 0)},
		{name: "ceil decimal19 stays decimal", function: "ceil", input: types.New(types.T_decimal128, 19, 0), want: types.New(types.T_decimal128, 19, 0)},
		{name: "floor decimal19 stays decimal", function: "floor", input: types.New(types.T_decimal128, 19, 0), want: types.New(types.T_decimal128, 19, 0)},
		{name: "ceil with digits", function: "ceil", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(2)), want: types.New(types.T_decimal64, 9, 2)},
		{name: "ceiling with digits", function: "ceiling", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(2)), want: types.New(types.T_decimal64, 9, 2)},
		{name: "floor with digits", function: "floor", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(2)), want: types.New(types.T_decimal64, 9, 2)},
		{name: "ceil keeps input when digits exceeds scale", function: "ceil", input: types.New(types.T_decimal64, 10, 4), digits: ptrTo(int64(8)), want: types.New(types.T_decimal64, 10, 4)},
		{name: "ceil decimal256", function: "ceil", input: types.New(types.T_decimal256, 50, 10), want: types.New(types.T_decimal256, 41, 0)},
		{name: "floor decimal256", function: "floor", input: types.New(types.T_decimal256, 50, 10), want: types.New(types.T_decimal256, 41, 0)},
		{name: "round coarse position needs only carry", function: "round", input: types.New(types.T_decimal128, 30, 0), digits: ptrTo(int64(-35)), want: types.New(types.T_decimal128, 31, 0)},
		{name: "ceil past input precision", function: "ceil", input: types.New(types.T_decimal128, 30, 0), digits: ptrTo(int64(-35)), want: types.New(types.T_decimal128, 36, 0)},
		{name: "ceiling past input precision", function: "ceiling", input: types.New(types.T_decimal128, 30, 0), digits: ptrTo(int64(-35)), want: types.New(types.T_decimal128, 36, 0)},
		{name: "floor past input precision", function: "floor", input: types.New(types.T_decimal128, 30, 0), digits: ptrTo(int64(-35)), want: types.New(types.T_decimal128, 36, 0)},
		{name: "ceil decimal64 coarse position", function: "ceil", input: types.New(types.T_decimal64, 9, 0), digits: ptrTo(int64(-15)), want: types.New(types.T_decimal64, 16, 0)},
		{name: "floor decimal256 coarse position", function: "floor", input: types.New(types.T_decimal256, 40, 0), digits: ptrTo(int64(-50)), want: types.New(types.T_decimal256, 51, 0)},
		{name: "ceil extreme negative digits", function: "ceil", input: types.New(types.T_decimal128, 30, 0), digits: ptrTo(int64(math.MinInt64)), want: types.New(types.T_decimal128, 38, 0)},
		{name: "ceil decimal64 max precision with negative digits", function: "ceil", input: types.New(types.T_decimal64, 18, 0), digits: ptrTo(int64(-1)), want: types.New(types.T_decimal64, 18, 0)},
		{name: "ceiling decimal128 max precision with negative digits", function: "ceiling", input: types.New(types.T_decimal128, 38, 0), digits: ptrTo(int64(-1)), want: types.New(types.T_decimal128, 38, 0)},
		{name: "floor decimal256 max precision with negative digits", function: "floor", input: types.New(types.T_decimal256, 65, 0), digits: ptrTo(int64(-1)), want: types.New(types.T_decimal256, 65, 0)},
		{name: "ceil decimal256 reserves carry within family", function: "ceil", input: types.New(types.T_decimal256, 50, 0), digits: ptrTo(int64(-1)), want: types.New(types.T_decimal256, 51, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			args := []*planpb.Expr{decimalArg(test.input)}
			if test.digits != nil {
				args = append(args, digitsArg(*test.digits))
			}
			got, err := BindFuncExprImplByPlanExpr(context.Background(), test.function, args)
			require.NoError(t, err)
			require.Equal(t, makePlan2Type(&test.want), got.Typ)
		})
	}
}

func TestCTASDecimalRoundingMetadataAtMaxPrecision(t *testing.T) {
	for _, function := range []string{"ceil", "ceiling", "floor", "round", "truncate"} {
		t.Run(function, func(t *testing.T) {
			stmt, err := parsers.ParseOne(
				t.Context(), dialect.MYSQL,
				"create table rounded as select "+function+"(cast(1 as decimal(65,0)), -1) as x",
				1,
			)
			require.NoError(t, err)
			defer stmt.Free()

			logicPlan, err := BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), stmt, false)
			require.NoError(t, err)
			cols := logicPlan.GetDdl().GetCreateTable().GetTableDef().GetCols()
			require.NotEmpty(t, cols)
			resultType := cols[0].Typ
			require.Equal(t, int32(types.T_decimal256), resultType.Id)
			require.Equal(t, int32(65), resultType.Width)
			require.Zero(t, resultType.Scale)

			// The CTAS result type must be expressible as a regular table schema so
			// dump/restore and SHOW CREATE TABLE can round-trip it.
			roundTripSQL := "create table rounded_copy (x " +
				makeTypeByPlan2Type(resultType).DescString() + ")"
			roundTripStmt, err := parsers.ParseOne(t.Context(), dialect.MYSQL, roundTripSQL, 1)
			require.NoError(t, err)
			defer roundTripStmt.Free()
			_, err = BuildPlan(NewMockCompilerContext(false, newPlanTestProcess(t)), roundTripStmt, false)
			require.NoError(t, err)
		})
	}
}

func TestDecimalRoundingVariableDigitsReserveCarry(t *testing.T) {
	for _, name := range []string{"round", "ceil", "floor", "truncate"} {
		t.Run(name, func(t *testing.T) {
			input := types.New(types.T_decimal128, 30, 2)
			got, err := BindFuncExprImplByPlanExpr(t.Context(), name, []*planpb.Expr{
				decimalArithmeticTestColumn(input, 0), decimalArithmeticTestColumn(types.T_int64.ToType(), 1),
			})
			require.NoError(t, err)
			want := int32(31)
			if name == "truncate" {
				want = 30
			} else if name == "ceil" || name == "floor" {
				want = 38
			}
			require.Equal(t, want, got.Typ.Width)
			require.Equal(t, int32(2), got.Typ.Scale)
		})
	}
}

func TestDecimalRoundCarryFeedsArithmetic(t *testing.T) {
	for _, variableDigits := range []bool{false, true} {
		t.Run(map[bool]string{false: "constant_digits", true: "variable_digits"}[variableDigits], func(t *testing.T) {
			proc := newPlanTestProcess(t)
			inputType := types.New(types.T_decimal64, 15, 0)
			column := decimalArithmeticTestColumn(inputType, 0)
			square, err := BindFuncExprImplByPlanExpr(t.Context(), "*", []*planpb.Expr{column, column})
			require.NoError(t, err)
			digits := makePlan2Int64ConstExprWithType(-30)
			if variableDigits {
				digits = decimalArithmeticTestColumn(types.T_int64.ToType(), 1)
			}
			rounded, err := BindFuncExprImplByPlanExpr(t.Context(), "round", []*planpb.Expr{square, digits})
			require.NoError(t, err)
			product, err := BindFuncExprImplByPlanExpr(t.Context(), "*", []*planpb.Expr{rounded, rounded})
			require.NoError(t, err)
			executor, err := colexec.NewExpressionExecutor(proc, product)
			require.NoError(t, err)
			defer executor.Free()
			input := batch.NewWithSize(2)
			defer input.Clean(proc.Mp())
			input.Vecs[0] = vector.NewVec(inputType)
			input.Vecs[1] = vector.NewVec(types.T_int64.ToType())
			for i, v := range []int64{999999999999999, -999999999999999, 0} {
				require.NoError(t, vector.AppendFixed(input.Vecs[0], types.Decimal64(v), i == 2, proc.Mp()))
				require.NoError(t, vector.AppendFixed(input.Vecs[1], int64(-30), false, proc.Mp()))
			}
			input.SetRowCount(3)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			for row := range 2 {
				require.Equal(t, "1"+strings.Repeat("0", 60), vector.GetFixedAtWithTypeCheck[types.Decimal256](result, row).Format(result.GetType().Scale))
			}
			require.True(t, result.IsNull(2))
			require.Equal(t, int32(62), product.Typ.Width)
		})
	}
}

// A directed rounding quantum can exceed the input domain, unlike ROUND's
// nearest-value carry. Exercise the real consumer with independent exact values.
func TestDecimalDirectedRoundingPositionFeedsArithmetic(t *testing.T) {
	for _, name := range []string{"ceil", "floor"} {
		for _, variable := range []bool{false, true} {
			for _, tc := range []struct {
				digits   int64
				exponent int
				rejected bool
			}{
				{-29, 30, false}, {-30, 30, false}, {-31, 31, false}, {-35, 35, false}, {-37, 37, false}, {-38, 0, true},
			} {
				t.Run(fmt.Sprintf("%s/variable_%t/digits_%d", name, variable, tc.digits), func(t *testing.T) {
					proc := newPlanTestProcess(t)
					valueType := types.New(types.T_decimal128, 30, 0)
					factorType := types.New(types.T_decimal64, 10, 0)
					input := batch.NewWithSize(3)
					t.Cleanup(func() { input.Clean(proc.Mp()) })
					input.Vecs[0] = vector.NewVec(valueType)
					input.Vecs[1] = vector.NewVec(factorType)
					digitsVector, err := vector.NewConstFixed(types.T_int64.ToType(), tc.digits, 4, proc.Mp())
					require.NoError(t, err)
					input.Vecs[2] = digitsVector
					for i, text := range []string{strings.Repeat("9", 30), "-" + strings.Repeat("9", 30), "0", "0"} {
						value, err := types.ParseDecimal128(text, 30, 0)
						require.NoError(t, err)
						require.NoError(t, vector.AppendFixed(input.Vecs[0], value, i == 3, proc.Mp()))
						require.NoError(t, vector.AppendFixed(input.Vecs[1], types.Decimal64(1000000000), false, proc.Mp()))
					}
					input.SetRowCount(4)
					digits := makePlan2Int64ConstExprWithType(tc.digits)
					if variable {
						digits = decimalArithmeticTestColumn(types.T_int64.ToType(), 2)
					}
					rounded, err := BindFuncExprImplByPlanExpr(t.Context(), name, []*planpb.Expr{decimalArithmeticTestColumn(valueType, 0), digits})
					require.NoError(t, err)
					product, err := BindFuncExprImplByPlanExpr(t.Context(), "*", []*planpb.Expr{rounded, decimalArithmeticTestColumn(factorType, 1)})
					require.NoError(t, err)
					executor, err := colexec.NewExpressionExecutor(proc, product)
					require.NoError(t, err)
					t.Cleanup(executor.Free)
					result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
					if tc.rejected {
						require.ErrorContains(t, err, name+"(decimal128")
						return
					}
					require.NoError(t, err)
					positive, negative := "1"+strings.Repeat("0", tc.exponent+9), "0"
					if name == "floor" {
						positive, negative = "0", "-1"+strings.Repeat("0", tc.exponent+9)
					}
					if tc.digits == -29 {
						if name == "ceil" {
							negative = "-9" + strings.Repeat("0", 38)
						} else {
							positive = "9" + strings.Repeat("0", 38)
						}
					}
					for row, want := range []string{positive, negative, "0"} {
						require.Equal(t, want, vector.GetFixedAtWithTypeCheck[types.Decimal256](result, row).Format(0))
					}
					require.True(t, result.IsNull(3))
				})
			}
		}
	}
}
