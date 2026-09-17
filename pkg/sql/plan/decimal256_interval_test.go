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
	"strings"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	planfunction "github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func makeWideDecimalIntervalExpr(t *testing.T, value string) *plan.Expr {
	t.Helper()
	expr, err := makePlan2DecimalExprWithType(context.Background(), value)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), expr.Typ.Id, expr.String())
	return expr
}

func makeDecimalIntervalCastExpr(
	t *testing.T, source *plan.Expr, width, scale int32,
) *plan.Expr {
	t.Helper()
	expr, err := appendCastBeforeExpr(context.Background(), source, plan.Type{
		Id:          int32(types.T_decimal256),
		Width:       width,
		Scale:       scale,
		NotNullable: true,
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), expr.Typ.Id, expr.String())
	return expr
}

func makeExplicitDecimalIntervalCastExpr(
	t *testing.T, source *plan.Expr, width, scale int32,
) *plan.Expr {
	t.Helper()
	expr, err := appendExplicitCastBeforeExpr(context.Background(), source, plan.Type{
		Id:          int32(types.T_decimal256),
		Width:       width,
		Scale:       scale,
		NotNullable: true,
	})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_decimal256), expr.Typ.Id, expr.String())
	return expr
}

func makeWideDecimalCastIntervalExpr(
	t *testing.T, value string, width, scale int32,
) *plan.Expr {
	t.Helper()
	return makeDecimalIntervalCastExpr(t, makeStringConst(value), width, scale)
}

func TestDecimal256IntervalPreservesFractionalSeconds(t *testing.T) {
	ctx := context.Background()
	dateExpr := makeDatetimeConst("2026-01-01 00:00:00")
	value := "1.25000000000000000000000000000000000000"

	for _, tc := range []struct {
		unit string
		want int64
	}{
		{unit: "SECOND", want: 1250000},
		{unit: "MINUTE", want: 75000000},
		{unit: "HOUR", want: 4500000000},
		{unit: "DAY", want: 108000000000},
	} {
		t.Run("date arithmetic "+tc.unit, func(t *testing.T) {
			args, err := resetDateFunctionArgs(ctx, dateExpr, makeIntervalExpr(
				makeWideDecimalIntervalExpr(t, value), tc.unit))
			require.NoError(t, err)
			require.Equal(t, tc.want, extractInt64Value(args[1]))
			require.Equal(t, int64(types.MicroSecond), extractInt64Value(args[2]))
			require.True(t, args[1].GetLit().GetDecimalLiteralRequiresV82())
		})
	}

	t.Run("interval function", func(t *testing.T) {
		args, err := resetIntervalFunctionArgs(ctx, makeIntervalExpr(
			makeWideDecimalIntervalExpr(t, value), "SECOND"))
		require.NoError(t, err)
		require.Equal(t, int64(1250000), extractInt64FromExpr(args[0]))
		require.Equal(t, int64(types.MicroSecond), extractInt64FromExpr(args[1]))
		require.True(t, args[0].GetLit().GetDecimalLiteralRequiresV82())
	})
}

func TestDecimal256IntervalPreparedConstantKeepsMicroseconds(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		"prepare decimal256_interval from select date_add(cast('2026-01-01 00:00:00' as datetime(6)), interval 1.25000000000000000000000000000000000000 second)")
	require.NoError(t, err)
	plan := prepared.GetDcl().GetPrepare().GetPlan()
	root := plan.GetQuery().Nodes[plan.GetQuery().Steps[len(plan.GetQuery().Steps)-1]]
	require.Len(t, root.ProjectList, 1, prepared.String())
	require.Equal(t, int32(types.T_datetime), root.ProjectList[0].Typ.Id)
	want, err := types.ParseDatetime("2026-01-01 00:00:01.250000", 6)
	require.NoError(t, err)
	require.Equal(t, int64(want),
		root.ProjectList[0].GetLit().GetDatetimeval())
	require.True(t, root.ProjectList[0].GetLit().GetDecimalLiteralRequiresV82())
}

func TestDecimalIntervalUsesCastTargetScale(t *testing.T) {
	args, err := resetIntervalFunctionArgs(context.Background(), makeIntervalExpr(
		makeWideDecimalCastIntervalExpr(t, "1.255", 10, 2), "SECOND"))
	require.NoError(t, err)
	require.Equal(t, int64(1260000), extractInt64FromExpr(args[0]))
	require.Equal(t, int64(types.MicroSecond), extractInt64FromExpr(args[1]))
}

func TestDecimal256IntervalHandlesFloatAndNestedCasts(t *testing.T) {
	ctx := context.Background()
	floatCast := makeDecimalIntervalCastExpr(t, makeFloat64Const(1.25), 40, 2)
	args, err := resetIntervalFunctionArgs(ctx, makeIntervalExpr(floatCast, "SECOND"))
	require.NoError(t, err)
	require.Equal(t, int64(1250000), extractInt64FromExpr(args[0]))
	require.Equal(t, int64(types.MicroSecond), extractInt64FromExpr(args[1]))
	require.True(t, args[0].GetLit().GetDecimalLiteralRequiresV82())

	inner := makeWideDecimalCastIntervalExpr(t, "1.25", 40, 2)
	nested := makeDecimalIntervalCastExpr(t, inner, 40, 4)
	args, err = resetIntervalFunctionArgs(ctx, makeIntervalExpr(nested, "SECOND"))
	require.NoError(t, err)
	require.Equal(t, int64(1250000), extractInt64FromExpr(args[0]))
	require.Equal(t, int64(types.MicroSecond), extractInt64FromExpr(args[1]))
	require.True(t, args[0].GetLit().GetDecimalLiteralRequiresV82())
}

func TestDecimal256IntervalMatchesExplicitDecimalCastToken(t *testing.T) {
	ctx := context.Background()
	value, err := planfunction.ParseExplicitDecimal256CastString("0b10", 40, 2)
	require.NoError(t, err)
	require.Equal(t, "2.00", value.Format(2))

	cast := makeExplicitDecimalIntervalCastExpr(t, makeStringConst("0b10"), 40, 2)
	args, err := resetIntervalFunctionArgs(ctx, makeIntervalExpr(cast, "SECOND"))
	require.NoError(t, err)
	require.Equal(t, int64(2000000), extractInt64FromExpr(args[0]))
	require.Equal(t, int64(types.MicroSecond), extractInt64FromExpr(args[1]))
	require.True(t, args[0].GetLit().GetDecimalLiteralRequiresV82())
}

func TestDecimal256IntervalRejectsInt64OverflowAfterRounding(t *testing.T) {
	ctx := context.Background()
	dateExpr := makeDatetimeConst("2026-01-01 00:00:00")
	_, err := resetDateFunctionArgs(ctx, dateExpr, makeIntervalExpr(
		makeWideDecimalIntervalExpr(t, "9223372036854.77580800000000000000000000000000000000"),
		"SECOND"))
	require.Error(t, err)
	require.ErrorContains(t, err, "out of range")
}

func TestDecimal256IntervalRoundsExactlyBeforeWindowValidation(t *testing.T) {
	proc := testutil.NewProc(t)
	ctx := context.Background()

	positive := makeIntervalExpr(
		makeWideDecimalIntervalExpr(t, "1.00000050000000000000000000000000000000"),
		"SECOND")
	got, err := resetWindowIntervalExpr(ctx, proc, positive)
	require.NoError(t, err)
	require.Equal(t, int64(1000001), extractInt64Value(got.GetList().List[0]))
	require.Equal(t, int64(types.MicroSecond), extractInt64Value(got.GetList().List[1]))
	require.True(t, got.GetList().List[0].GetLit().GetDecimalLiteralRequiresV82())

	negative := makeIntervalExpr(
		makeWideDecimalIntervalExpr(t, "-0."+strings.Repeat("0", 38)+"1"),
		"SECOND")
	got, err = resetWindowIntervalExpr(ctx, proc, negative)
	require.Nil(t, got)
	require.ErrorContains(t, err, "frame start or end is negative")
}
