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

package colexec_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestEvaluateFilterByZoneMapDatetimeTimestampComparison(t *testing.T) {
	parseDatetime := func(t *testing.T, value string) types.Datetime {
		t.Helper()
		datetime, err := types.ParseDatetime(value, 6)
		require.NoError(t, err)
		return datetime
	}
	makeExpr := func(t *testing.T, timestamp types.Timestamp, scale int32) *plan.Expr {
		t.Helper()
		column := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_datetime), Scale: 6},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: "request_at"},
			},
		}
		constant := plan2.MakePlan2TimestampConstExprWithType(int64(timestamp))
		constant.Typ.Scale = scale
		expr, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), ">", []*plan.Expr{column, constant})
		require.NoError(t, err)
		require.Equal(t, int32(types.T_datetime), expr.GetF().Args[0].Typ.Id)
		require.Equal(t, int32(types.T_timestamp), expr.GetF().Args[1].Typ.Id)
		return expr
	}

	t.Run("fixed offset prunes", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		defer proc.Free()
		zone := time.FixedZone("UTC+08", 8*3600)
		proc.GetSessionInfo().TimeZone = zone
		threshold := parseDatetime(t, "2026-08-10 12:00:00").ToTimestamp(zone)
		expr := makeExpr(t, threshold, 6)
		meta := makeDatetimeBlockMeta(
			parseDatetime(t, "2026-08-10 10:00:00"),
			parseDatetime(t, "2026-08-10 11:00:00"),
		)
		zms, vecs := makeZoneMapEvalScratch(expr)

		selected := colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
		require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
	})

	t.Run("ordinary named-zone range prunes", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		defer proc.Free()
		zone, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)
		proc.GetSessionInfo().TimeZone = zone
		threshold := parseDatetime(t, "2024-01-10 12:00:00").ToTimestamp(zone)
		expr := makeExpr(t, threshold, 6)
		meta := makeDatetimeBlockMeta(
			parseDatetime(t, "2024-01-10 10:00:00"),
			parseDatetime(t, "2024-01-10 11:00:00"),
		)
		zms, vecs := makeZoneMapEvalScratch(expr)

		selected := colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
		require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
	})

	t.Run("DST fold is conservative", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		defer proc.Free()
		zone, err := time.LoadLocation("America/New_York")
		require.NoError(t, err)
		proc.GetSessionInfo().TimeZone = zone
		threshold, err := types.ParseTimestamp(time.UTC, "2024-11-03 06:15:00", 6)
		require.NoError(t, err)
		expr := makeExpr(t, threshold, 6)
		meta := makeDatetimeBlockMeta(
			parseDatetime(t, "2024-11-03 01:00:00"),
			parseDatetime(t, "2024-11-03 01:59:59"),
		)
		zms, vecs := makeZoneMapEvalScratch(expr)

		selected := colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
		require.True(t, selected, "ambiguous local-time ranges must remain for residual evaluation")
	})

	t.Run("timestamp scale is applied before pruning", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		defer proc.Free()
		proc.GetSessionInfo().TimeZone = time.UTC
		value := parseDatetime(t, "2026-08-10 12:00:00.123456")
		threshold := value.ToTimestamp(time.UTC).TruncateToScale(3)
		expr := makeExpr(t, threshold, 3)
		meta := makeDatetimeBlockMeta(value, value)
		zms, vecs := makeZoneMapEvalScratch(expr)

		selected := colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
		require.False(t, selected, "comparison must retain the prior TIMESTAMP(3) cast precision")
	})

	t.Run("between prunes", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		defer proc.Free()
		zone := time.FixedZone("UTC+08", 8*3600)
		proc.GetSessionInfo().TimeZone = zone
		column := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_datetime), Scale: 6},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: "request_at"},
			},
		}
		lower := plan2.MakePlan2TimestampConstExprWithType(int64(parseDatetime(t, "2026-08-10 12:00:00").ToTimestamp(zone)))
		upper := plan2.MakePlan2TimestampConstExprWithType(int64(parseDatetime(t, "2026-08-10 13:00:00").ToTimestamp(zone)))
		expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "between", []*plan.Expr{column, lower, upper})
		require.NoError(t, err)
		meta := makeDatetimeBlockMeta(
			parseDatetime(t, "2026-08-10 10:00:00"),
			parseDatetime(t, "2026-08-10 11:00:00"),
		)
		zms, vecs := makeZoneMapEvalScratch(expr)

		selected := colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
		require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
	})

	t.Run("between preserves common value scale", func(t *testing.T) {
		proc := testutil.NewProcess(t)
		defer proc.Free()
		proc.GetSessionInfo().TimeZone = time.UTC
		value := parseDatetime(t, "2026-08-10 12:00:00.123456")
		column := &plan.Expr{
			Typ: plan.Type{Id: int32(types.T_datetime), Scale: 6},
			Expr: &plan.Expr_Col{
				Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: "request_at"},
			},
		}
		lower := plan2.MakePlan2TimestampConstExprWithType(int64(value.ToTimestamp(time.UTC).TruncateToScale(3)))
		lower.Typ.Scale = 3
		upperValue, err := types.ParseTimestamp(time.UTC, "2026-08-10 12:00:00.123100", 6)
		require.NoError(t, err)
		upper := plan2.MakePlan2TimestampConstExprWithType(int64(upperValue))
		upper.Typ.Scale = 6
		expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "between", []*plan.Expr{column, lower, upper})
		require.NoError(t, err)
		zms, vecs := makeZoneMapEvalScratch(expr)

		selected := colexec.EvaluateFilterByZoneMap(
			proc.Ctx, proc, expr, makeDatetimeBlockMeta(value), map[int]int{0: 0}, zms, vecs,
		)
		require.True(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
	})
}

func TestEvaluateFilterByZoneMapNullableInListIsConservative(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr := makeVarcharInExpr(t, ctx, "keep", true)
	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.True(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapNullableInVecIsConservative(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr := makeVarcharInVecExpr(t, ctx, proc, "keep", true)
	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.True(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestFoldedNullableInExprKeepsMatchAndNullsMiss(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr := makeVarcharInExpr(t, ctx, "keep", true)
	folded, err := plan2.ConstantFold(batch.EmptyForConstFoldBatch, plan2.DeepCopyExpr(expr), proc, true, true)
	require.NoError(t, err)

	result := evalVarcharPredicate(t, proc, folded, "keep", "key", "")
	requireBoolValue(t, result, 0, true, false)
	requireBoolValue(t, result, 1, false, true)
	requireBoolValue(t, result, 2, false, true)
}

func TestFoldedNullableNotInExprNullsMiss(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr := makeVarcharNotInExpr(t, ctx, "keep", true)
	folded, err := plan2.ConstantFold(batch.EmptyForConstFoldBatch, plan2.DeepCopyExpr(expr), proc, true, true)
	require.NoError(t, err)

	result := evalVarcharPredicate(t, proc, folded, "keep", "key", "")
	requireBoolValue(t, result, 0, false, false)
	requireBoolValue(t, result, 1, false, true)
	requireBoolValue(t, result, 2, false, true)
}

func TestEvaluateFilterByZoneMapMathPrecision(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	meta := objectio.BuildMetaData(1, 2).GetBlockMeta(0)
	value := float64(149)
	valueZM := index.NewZM(types.T_float64, 0)
	index.UpdateZM(valueZM, types.EncodeFloat64(&value))
	value = -149
	index.UpdateZM(valueZM, types.EncodeFloat64(&value))
	meta.MustGetColumn(0).SetZoneMap(valueZM)
	digitsZM := index.NewZM(types.T_int64, 0)
	for _, d := range []int64{-2, -1, 0} {
		index.UpdateZM(digitsZM, types.EncodeInt64(&d))
	}
	meta.MustGetColumn(1).SetZoneMap(digitsZM)
	col := func(oid types.T, pos int32) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(oid)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
	}
	for _, tc := range []struct {
		name      string
		fn        string
		op        string
		threshold int64
		digits    *plan.Expr
		selected  bool
	}{
		{"dynamic keeps interior maximum", "round", ">", 149, col(types.T_int64, 1), true},
		{"constant matching", "round", ">", 149, plan2.MakePlan2Int64ConstExprWithType(-1), true},
		{"constant disjoint", "round", ">", 149, plan2.MakePlan2Int64ConstExprWithType(-2), false},
		{"default precision", "round", ">", 149, nil, false},
		{"truncate keeps negative minimum", "truncate", "<", -140, col(types.T_int64, 1), true},
		{"truncate constant disjoint", "truncate", "<", -140, plan2.MakePlan2Int64ConstExprWithType(-2), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			args := []*plan.Expr{col(types.T_float64, 0)}
			if tc.digits != nil {
				args = append(args, tc.digits)
			}
			round, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, tc.fn, args)
			require.NoError(t, err)
			expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, tc.op, []*plan.Expr{round, plan2.MakePlan2Int64ConstExprWithType(tc.threshold)})
			require.NoError(t, err)
			zms, vecs := makeZoneMapEvalScratch(expr)
			defer func() {
				for _, v := range vecs {
					if v != nil {
						v.Free(proc.Mp())
					}
				}
			}()
			require.Equal(t, tc.selected, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0, 1: 1}, zms, vecs))
		})
	}
}

func TestEvaluateFilterByZoneMapStillPrunesFalseEquality(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
		makeVarcharColExpr(),
		plan2.MakePlan2StringConstExprWithType("zzz"),
	})
	require.NoError(t, err)

	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapAndKeepsKnownFalsePruning(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	falseEq, err := plan2.BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
		makeVarcharColExpr(),
		plan2.MakePlan2StringConstExprWithType("zzz"),
	})
	require.NoError(t, err)

	nullExpr := plan2.MakePlan2StringConstExprWithType("")
	nullExpr.Expr.(*plan.Expr_Lit).Lit.Isnull = true
	nullEq, err := plan2.BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
		makeVarcharColExpr(),
		nullExpr,
	})
	require.NoError(t, err)

	expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{falseEq, nullEq})
	require.NoError(t, err)

	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapAndKeepsPossibleTrueBoolRange(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	eq, err := plan2.BindFuncExprImplByPlanExpr(ctx, "=", []*plan.Expr{
		makeVarcharColExprAt(1),
		plan2.MakePlan2StringConstExprWithType("keep"),
	})
	require.NoError(t, err)
	expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{makeBoolColExpr(), eq})
	require.NoError(t, err)

	meta := makeBoolAndVarcharBlockMeta(false, true, "key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0, 1: 1}, zms, vecs)
	require.True(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapNullableInListWithoutMatchPrunes(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr := makeVarcharInExpr(t, ctx, "zzz", true)
	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapNullableNotInListPrunes(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr := makeVarcharNotInExpr(t, ctx, "zzz", true)
	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapNotInExpandedWithBareNullPrunes(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	neqValue, err := plan2.BindFuncExprImplByPlanExpr(ctx, "!=", []*plan.Expr{
		makeVarcharColExpr(),
		plan2.MakePlan2StringConstExprWithType("zzz"),
	})
	require.NoError(t, err)
	neqNull, err := plan2.BindFuncExprImplByPlanExpr(ctx, "!=", []*plan.Expr{
		makeVarcharColExpr(),
		makeBareNullExpr(),
	})
	require.NoError(t, err)
	expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "and", []*plan.Expr{neqValue, neqNull})
	require.NoError(t, err)

	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapNotEqualBareNullPrunes(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	for _, op := range []string{"!=", "<>"} {
		t.Run(op, func(t *testing.T) {
			expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, op, []*plan.Expr{
				makeVarcharColExpr(),
				makeBareNullExpr(),
			})
			require.NoError(t, err)

			meta := makeVarcharBlockMeta("key", "keep")
			zms, vecs := makeZoneMapEvalScratch(expr)

			selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
			require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
		})
	}
}

func TestEvaluateFilterByZoneMapUnknownResultIsConservative(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr := makeVarcharColExpr()
	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.True(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapNullComparisonsPrune(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	for _, op := range []string{">", "<", ">=", "<=", "=", "!=", "<>"} {
		t.Run(op, func(t *testing.T) {
			expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, op, []*plan.Expr{
				makeVarcharColExpr(),
				makeBareNullExpr(),
			})
			require.NoError(t, err)

			meta := makeVarcharBlockMeta("key", "keep")
			zms, vecs := makeZoneMapEvalScratch(expr)

			selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
			require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
		})
	}

	t.Run("between", func(t *testing.T) {
		expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "between", []*plan.Expr{
			makeVarcharColExpr(),
			makeBareNullExpr(),
			plan2.MakePlan2StringConstExprWithType("zzz"),
		})
		require.NoError(t, err)

		meta := makeVarcharBlockMeta("key", "keep")
		zms, vecs := makeZoneMapEvalScratch(expr)

		selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
		require.False(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
	})
}

func TestEvaluateFilterByZoneMapInListWithUnknownMemberIsConservative(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	listExpr := &plan.Expr{
		Typ: makeVarcharColExpr().Typ,
		Expr: &plan.Expr_List{
			List: &plan.ExprList{List: []*plan.Expr{
				// Unknown no-column item covers the conservative native IN fallback.
				{Typ: makeVarcharColExpr().Typ},
				makeBareNullExpr(),
			}},
		},
	}
	expr := makeNativeVarcharInExpr(t, ctx, []*plan.Expr{
		makeVarcharColExprAt(0),
		listExpr,
	})

	meta := makeVarcharBlockMeta("key", "keep")
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.True(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func TestEvaluateFilterByZoneMapInListWithUninitializedLHSIsConservative(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := proc.Ctx

	expr := makeVarcharInExpr(t, ctx, "zzz", true)
	dataMeta := objectio.BuildMetaData(1, 1)
	meta := dataMeta.GetBlockMeta(0)
	zms, vecs := makeZoneMapEvalScratch(expr)

	selected := colexec.EvaluateFilterByZoneMap(ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
	require.True(t, selected, plan2.FormatExpr(expr, plan2.FormatOption{}))
}

func makeVarcharInExpr(t *testing.T, ctx context.Context, value string, withNull bool) *plan.Expr {
	t.Helper()

	listValues := []*plan.Expr{plan2.MakePlan2StringConstExprWithType(value)}
	if withNull {
		nullExpr := plan2.MakePlan2StringConstExprWithType("")
		nullExpr.Expr.(*plan.Expr_Lit).Lit.Isnull = true
		listValues = append(listValues, nullExpr)
	}

	listExpr := &plan.Expr{
		Typ: makeVarcharColExpr().Typ,
		Expr: &plan.Expr_List{
			List: &plan.ExprList{List: listValues},
		},
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "in", []*plan.Expr{
		makeVarcharColExpr(),
		listExpr,
	})
	require.NoError(t, err)
	return expr
}

func makeVarcharInVecExpr(t *testing.T, ctx context.Context, proc *process.Process, value string, withNull bool) *plan.Expr {
	t.Helper()

	vec := vector.NewVec(types.T_varchar.ToType())
	require.NoError(t, vector.AppendBytes(vec, []byte(value), false, proc.Mp()))
	if withNull {
		require.NoError(t, vector.AppendBytes(vec, nil, true, proc.Mp()))
	}
	data, err := vec.MarshalBinary()
	require.NoError(t, err)
	vec.Free(proc.Mp())

	vecLen := int32(1)
	if withNull {
		vecLen = 2
	}
	vecExpr := &plan.Expr{
		Typ:  makeVarcharColExpr().Typ,
		Expr: &plan.Expr_Vec{Vec: &plan.LiteralVec{Len: vecLen, Data: data}},
	}
	return makeNativeVarcharInExpr(t, ctx, []*plan.Expr{
		makeVarcharColExpr(),
		vecExpr,
	})
}

func makeNativeVarcharInExpr(t *testing.T, ctx context.Context, args []*plan.Expr) *plan.Expr {
	t.Helper()

	varcharType := types.T_varchar.ToType()
	fGet, err := function.GetFunctionByName(ctx, "in", []types.Type{varcharType, varcharType})
	require.NoError(t, err)
	returnType := fGet.GetReturnType()
	return &plan.Expr{
		Typ: plan.Type{
			Id:    int32(returnType.Oid),
			Width: returnType.Width,
			Scale: returnType.Scale,
		},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     fGet.GetEncodedOverloadID(),
					ObjName: "in",
				},
				Args: args,
			},
		},
	}
}

func makeVarcharNotInExpr(t *testing.T, ctx context.Context, value string, withNull bool) *plan.Expr {
	t.Helper()

	listValues := []*plan.Expr{plan2.MakePlan2StringConstExprWithType(value)}
	if withNull {
		nullExpr := plan2.MakePlan2StringConstExprWithType("")
		nullExpr.Expr.(*plan.Expr_Lit).Lit.Isnull = true
		listValues = append(listValues, nullExpr)
	}

	listExpr := &plan.Expr{
		Typ: makeVarcharColExpr().Typ,
		Expr: &plan.Expr_List{
			List: &plan.ExprList{List: listValues},
		},
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(ctx, "not_in", []*plan.Expr{
		makeVarcharColExpr(),
		listExpr,
	})
	require.NoError(t, err)
	return expr
}

func evalVarcharPredicate(t *testing.T, proc *process.Process, expr *plan.Expr, values ...string) *vector.Vector {
	t.Helper()

	input := batch.NewWithSize(1)
	defer input.Clean(proc.Mp())
	vec := vector.NewVec(types.T_varchar.ToType())
	input.Vecs[0] = vec
	for _, value := range values {
		require.NoError(t, vector.AppendBytes(vec, []byte(value), value == "", proc.Mp()))
	}
	input.SetRowCount(len(values))

	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer executor.Free()

	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	dup, err := result.Dup(proc.Mp())
	require.NoError(t, err)
	t.Cleanup(func() {
		dup.Free(proc.Mp())
	})
	return dup
}

func requireBoolValue(t *testing.T, vec *vector.Vector, row uint64, expected bool, expectedNull bool) {
	t.Helper()

	param := vector.GenerateFunctionFixedTypeParameter[bool](vec)
	actual, isNull := param.GetValue(row)
	require.Equal(t, expectedNull, isNull)
	if !expectedNull {
		require.Equal(t, expected, actual)
	}
}

func makeVarcharColExpr() *plan.Expr {
	return makeVarcharColExprAt(0)
}

func makeVarcharColExprAt(pos int32) *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_varchar), Width: 16},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: pos, Name: "k"},
		},
	}
}

func makeBoolColExpr() *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_bool)},
		Expr: &plan.Expr_Col{
			Col: &plan.ColRef{RelPos: 0, ColPos: 0, Name: "flag"},
		},
	}
}

func makeBareNullExpr() *plan.Expr {
	return &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_any)},
		Expr: &plan.Expr_Lit{
			Lit: &plan.Literal{Isnull: true},
		},
	}
}

func TestEvaluateFilterByZoneMapRoundOverflowCleanup(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { proc.Free(); require.Zero(t, proc.Mp().CurrNB()) })
	column := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	rounded, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "round", []*plan.Expr{column, plan2.MakePlan2Int64ConstExprWithType(-19)})
	require.NoError(t, err)
	predicate, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*plan.Expr{rounded, plan2.MakePlan2Int64ConstExprWithType(1)})
	require.NoError(t, err)
	meta := objectio.BuildMetaData(1, 1).GetBlockMeta(0)
	zms, vecs := makeZoneMapEvalScratch(predicate)
	t.Cleanup(func() {
		for _, vec := range vecs {
			if vec != nil {
				vec.Free(proc.Mp())
			}
		}
	})
	for i, value := range []int64{4999999999999999999, 5000000000000000000, 4999999999999999999} {
		zm := index.NewZM(types.T_int64, 0)
		index.UpdateZM(zm, types.EncodeInt64(&value))
		meta.MustGetColumn(0).SetZoneMap(zm)
		var escaped any
		selected := false
		func() {
			defer func() { escaped = recover() }()
			selected = colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, predicate, meta, map[int]int{0: 0}, zms, vecs)
		}()
		require.Nil(t, escaped, "metadata failure must defer to row execution; native bytes=%d", proc.Mp().CurrNB())
		require.Equal(t, i == 1, selected)
		require.Zero(t, proc.Mp().CurrNB(), "speculative result and operand vectors must be freed")
	}
}

func makeVarcharBlockMeta(values ...string) objectio.BlockObject {
	dataMeta := objectio.BuildMetaData(1, 1)
	meta := dataMeta.GetBlockMeta(0)

	zm := index.NewZM(types.T_varchar, 0)
	for _, value := range values {
		index.UpdateZM(zm, []byte(value))
	}
	meta.MustGetColumn(0).SetZoneMap(zm)
	return meta
}

func makeDatetimeBlockMeta(values ...types.Datetime) objectio.BlockObject {
	dataMeta := objectio.BuildMetaData(1, 1)
	meta := dataMeta.GetBlockMeta(0)

	zm := index.NewZM(types.T_datetime, 6)
	for _, value := range values {
		encoded := int64(value)
		index.UpdateZM(zm, types.EncodeInt64(&encoded))
	}
	meta.MustGetColumn(0).SetZoneMap(zm)
	return meta
}

func makeBoolAndVarcharBlockMeta(minBool, maxBool bool, values ...string) objectio.BlockObject {
	dataMeta := objectio.BuildMetaData(1, 2)
	meta := dataMeta.GetBlockMeta(0)

	boolZM := index.NewZM(types.T_bool, 0)
	index.UpdateZM(boolZM, types.EncodeBool(&minBool))
	index.UpdateZM(boolZM, types.EncodeBool(&maxBool))
	meta.MustGetColumn(0).SetZoneMap(boolZM)

	varcharZM := index.NewZM(types.T_varchar, 0)
	for _, value := range values {
		index.UpdateZM(varcharZM, []byte(value))
	}
	meta.MustGetColumn(1).SetZoneMap(varcharZM)
	return meta
}

func makeZoneMapEvalScratch(expr *plan.Expr) ([]objectio.ZoneMap, []*vector.Vector) {
	need := plan2.AssignAuxIdForExpr(expr, 0)
	return make([]objectio.ZoneMap, need), make([]*vector.Vector, need)
}

// Exercise the compiler's materialization boundary, not just literal predicates.
func TestEvaluateFilterByZoneMapMaterializedFunctions(t *testing.T) {
	for _, tc := range []struct {
		name                string
		function            string
		argument, threshold int64
	}{
		{"round", "round", -1, -10}, {"identity round", "round", 0, -10},
		{"truncate", "truncate", -1, -10}, {"addition", "+", 1, -9},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
			col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			value := bindZoneMapFunction(t, proc, tc.function, col, plan2.MakePlan2Int64ConstExprWithType(tc.argument))
			expr := bindZoneMapFunction(t, proc, "=", value, plan2.MakePlan2Int64ConstExprWithType(tc.threshold))
			expr = materializeZoneMapFilter(t, proc, expr)
			zms, vecs := makeZoneMapEvalScratch(expr)
			// Reuse the range scratch over distinct metadata; constant caches stay query-local.
			for _, block := range []struct {
				min, max int64
				selected bool
			}{{-1024, -513, false}, {-512, -1, true}, {-1024, -513, false}} {
				meta := objectio.BuildMetaData(1, 1).GetBlockMeta(0)
				zm := index.NewZM(types.T_int64, 0)
				index.UpdateZM(zm, types.EncodeInt64(&block.min))
				index.UpdateZM(zm, types.EncodeInt64(&block.max))
				meta.MustGetColumn(0).SetZoneMap(zm)
				require.Equal(t, block.selected, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs))
			}
		})
	}
}

func TestEvaluateFilterByZoneMapMaterializedFallback(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	precision := plan2.MakePlan2Int64ConstExprWithType(-19)
	expr := materializeZoneMapFilter(t, proc, bindZoneMapFunction(t, proc, "=", bindZoneMapFunction(t, proc, "round", col, precision), plan2.MakePlan2Int64ConstExprWithType(1)))
	zms, vecs := makeZoneMapEvalScratch(expr)
	for _, v := range []int64{4999999999999999999, 5000000000000000000, 4999999999999999999} {
		meta := objectio.BuildMetaData(1, 1).GetBlockMeta(0)
		zm := index.NewZM(types.T_int64, 0)
		index.UpdateZM(zm, types.EncodeInt64(&v))
		meta.MustGetColumn(0).SetZoneMap(zm)
		require.Equal(t, v == 5000000000000000000, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs))
	}
	// The same compiler shape with NULL precision must never use zero precision.
	nullPrecision := plan2.MakePlan2Int64ConstExprWithType(0)
	nullPrecision.GetLit().Isnull = true
	expr = materializeZoneMapFilter(t, proc, bindZoneMapFunction(t, proc, "=", bindZoneMapFunction(t, proc, "round", col, nullPrecision), plan2.MakePlan2Int64ConstExprWithType(1)))
	zms, vecs = makeZoneMapEvalScratch(expr)
	meta := objectio.BuildMetaData(1, 1).GetBlockMeta(0)
	v := int64(512)
	zm := index.NewZM(types.T_int64, 0)
	index.UpdateZM(zm, types.EncodeInt64(&v))
	meta.MustGetColumn(0).SetZoneMap(zm)
	require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs))
	// Unknown alternatives retain blocks; a false conjunct can still prove absence.
	unknown := &plan.Expr{Typ: col.Typ, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true}}}
	missing := bindZoneMapFunction(t, proc, "=", col, unknown)
	falsePredicate := bindZoneMapFunction(t, proc, "=", col, plan2.MakePlan2Int64ConstExprWithType(-10))
	for _, name := range []string{"and", "or"} {
		combined := bindZoneMapFunction(t, proc, name, falsePredicate, missing)
		zms, vecs := makeZoneMapEvalScratch(combined)
		require.Equal(t, name == "or", colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, combined, meta, map[int]int{0: 0}, zms, vecs))
	}
}

func TestGetExprZoneMapMaterializedScalarEncoding(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, tc := range []struct {
		name  string
		oid   types.T
		scale int32
		data  []byte
		valid bool
	}{
		{"signed", types.T_int64, 0, types.EncodeValue(int64(-10), types.T_int64), true},
		{"narrow", types.T_int8, 0, types.EncodeValue(int8(-10), types.T_int8), true},
		{"unsigned", types.T_uint64, 0, types.EncodeValue(uint64(math.MaxUint64), types.T_uint64), true},
		{"float", types.T_float64, 0, types.EncodeValue(float64(1.25), types.T_float64), true},
		{"decimal", types.T_decimal64, 2, types.EncodeValue(types.Decimal64(125), types.T_decimal64), true},
		{"timestamp", types.T_timestamp, 6, types.EncodeValue(types.Timestamp(123), types.T_timestamp), true},
		{"empty bytes", types.T_varchar, 0, []byte{}, true},
		{"long bytes", types.T_varchar, 0, []byte(strings.Repeat("z", 100)), true},
		{"nil unknown", types.T_int64, 0, nil, false},
		{"short", types.T_int64, 0, []byte{1}, false},
		{"oversized", types.T_int64, 0, make([]byte, 9), false},
		{"invalid bool", types.T_bool, 0, []byte{2}, false},
		{"NaN", types.T_float64, 0, types.EncodeValue(math.NaN(), types.T_float64), false},
		{"infinite", types.T_float64, 0, types.EncodeValue(math.Inf(1), types.T_float64), false},
		{"timestamp scale", types.T_timestamp, 7, types.EncodeValue(types.Timestamp(123), types.T_timestamp), false},
		{"unsupported JSON", types.T_json, 0, []byte("null"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expr := &plan.Expr{Typ: plan.Type{Id: int32(tc.oid), Scale: tc.scale}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: tc.data}}}
			zms, vecs := makeZoneMapEvalScratch(expr)
			zm := colexec.GetExprZoneMap(proc.Ctx, proc, expr, nil, nil, zms, vecs)
			require.Equal(t, tc.valid, zm.IsInited())
			if tc.valid {
				require.Equal(t, tc.oid, zm.GetType())
				require.Equal(t, tc.scale, zm.GetScale())
				require.True(t, zm.ContainsKey(tc.data), "published scalar bounds must contain the producer value")
			}
		})
	}
}

func TestEvaluateFilterByZoneMapMaterializedPrefixAndIn(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
	varcharConst := func(value string) *plan.Expr {
		expr := plan2.MakePlan2StringConstExprWithType(value)
		expr.Typ.Id = int32(types.T_varchar)
		return expr
	}
	for _, value := range []string{"ke", "zz", "", strings.Repeat("z", 100)} {
		expr := materializeZoneMapFilter(t, proc, bindZoneMapFunction(t, proc, "prefix_eq", makeVarcharColExpr(), varcharConst(value)))
		zms, vecs := makeZoneMapEvalScratch(expr)
		require.Equal(t, value == "ke" || value == "", colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, makeVarcharBlockMeta("keep"), map[int]int{0: 0}, zms, vecs))
	}
	expr := bindZoneMapFunction(t, proc, "prefix_between", makeVarcharColExpr(), varcharConst("ka"), varcharConst("kf"))
	expr = materializeZoneMapFilter(t, proc, expr)
	zms, vecs := makeZoneMapEvalScratch(expr)
	require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, makeVarcharBlockMeta("keep"), map[int]int{0: 0}, zms, vecs))
	expr.GetF().Args[1].GetFold().Data = nil
	zms, vecs = makeZoneMapEvalScratch(expr)
	require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, makeVarcharBlockMeta("keep"), map[int]int{0: 0}, zms, vecs))
	for _, value := range []string{"keep", "zzz"} {
		expr := materializeZoneMapFilter(t, proc, makeVarcharInExpr(t, proc.Ctx, value, true))
		zms, vecs := makeZoneMapEvalScratch(expr)
		require.Equal(t, value == "keep", colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, makeVarcharBlockMeta("keep"), map[int]int{0: 0}, zms, vecs))
		// A payload type mismatch and a malformed vector must both fail open.
		fold := expr.GetF().Args[1].GetFold()
		require.NotNil(t, fold)
		expr.GetF().Args[1].Typ.Id = int32(types.T_int64)
		zms, vecs = makeZoneMapEvalScratch(expr)
		require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, makeVarcharBlockMeta("keep"), map[int]int{0: 0}, zms, vecs))
		fold.Data = []byte{1, 2, 3}
		zms, vecs = makeZoneMapEvalScratch(expr)
		require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, makeVarcharBlockMeta("keep"), map[int]int{0: 0}, zms, vecs))
	}
}

func bindZoneMapFunction(t *testing.T, proc *process.Process, name string, args ...*plan.Expr) *plan.Expr {
	t.Helper()
	expr, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, name, args)
	require.NoError(t, err)
	return expr
}

func materializeZoneMapFilter(t *testing.T, proc *process.Process, expr *plan.Expr) *plan.Expr {
	t.Helper()
	expr = plan2.DeepCopyExpr(expr)
	var executors []colexec.ExpressionExecutor
	t.Cleanup(func() {
		for _, executor := range executors {
			executor.Free()
		}
	})
	_, err := plan2.ReplaceFoldExpr(proc, expr, &executors)
	require.NoError(t, err)
	require.NoError(t, plan2.EvalFoldExpr(proc, expr, &executors))
	return expr
}

func TestEvaluateFilterByZoneMapCharComparisonKeepsPadSpaceMatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_char), Width: 8}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	rhs := plan2.MakePlan2StringConstExprWithType("MO ")
	rhs.Typ.Width = 8
	expr := bindZoneMapFunction(t, proc, "=", col, rhs)
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.New(types.T_char, 8, 0))
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("MO"), false, proc.Mp()))
	input.SetRowCount(1)
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	rows, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	requireBoolValue(t, rows, 0, true, false)
	expr = materializeZoneMapFilter(t, proc, expr)
	meta := objectio.BuildMetaData(1, 1).GetBlockMeta(0)
	zm := index.NewZM(types.T_char, 0)
	index.UpdateZM(zm, []byte("MO"))
	meta.MustGetColumn(0).SetZoneMap(zm)
	zms, vecs := makeZoneMapEvalScratch(expr)
	require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs), "byte ordering cannot disprove a CHAR PAD SPACE match")
}

func TestEvaluateFilterByZoneMapVaryingFunctionArguments(t *testing.T) {
	for _, tc := range []struct {
		name              string
		values, precision []int64
		quotient          bool
		threshold         int64
	}{
		{name: "division", values: []int64{-2, 1, 2}, quotient: true, threshold: 1},
		{name: "integer division", values: []int64{-2, 1, 2}, quotient: true, threshold: 1},
		{name: "ceil singleton precision", values: []int64{-1, 1, 1}, precision: []int64{-1, -1, -1}, threshold: 10},
		{name: "ceil", values: []int64{-1, 1, 1}, precision: []int64{-1, -1, 1}, threshold: 10},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			meta := objectio.BuildMetaData(1, 2).GetBlockMeta(0)
			col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			var value *plan.Expr
			if tc.quotient {
				name := "/"
				oid := types.T_float64
				if tc.name == "integer division" {
					name = "div"
					oid = types.T_int64
				}
				col.Typ.Id = int32(oid)
				input.Vecs[0] = vector.NewVec(oid.ToType())
				zm := index.NewZM(oid, 0)
				for _, v := range tc.values {
					var n any = v
					if oid == types.T_float64 {
						n = float64(v)
					}
					require.NoError(t, vector.AppendAny(input.Vecs[0], n, false, proc.Mp()))
					index.UpdateZMAny(zm, n)
				}
				meta.MustGetColumn(0).SetZoneMap(zm)
				one := plan2.MakePlan2Int64ConstExprWithType(1)
				if oid == types.T_float64 {
					one = &plan.Expr{Typ: plan.Type{Id: int32(oid)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: 1}}}}
				}
				value = bindZoneMapFunction(t, proc, name, one, col)
			} else {
				input.Vecs[0] = vector.NewVec(types.T_int64.ToType())
				input.Vecs[1] = vector.NewVec(types.T_int64.ToType())
				for i, values := range [][]int64{tc.values, tc.precision} {
					zm := index.NewZM(types.T_int64, 0)
					for _, v := range values {
						require.NoError(t, vector.AppendFixed(input.Vecs[i], v, false, proc.Mp()))
						index.UpdateZM(zm, types.EncodeInt64(&v))
					}
					meta.MustGetColumn(uint16(i)).SetZoneMap(zm)
				}
				digit := &plan.Expr{Typ: col.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
				value = bindZoneMapFunction(t, proc, "ceil", col, digit)
			}
			input.SetRowCount(len(tc.values))
			expr := bindZoneMapFunction(t, proc, "=", value, plan2.MakePlan2Int64ConstExprWithType(tc.threshold))
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			if !tc.quotient {
				require.ErrorContains(t, err, "not const")
			} else {
				require.NoError(t, err)
				requireBoolValue(t, result, 1, true, false)
			}
			expr = materializeZoneMapFilter(t, proc, expr)
			zms, vecs := makeZoneMapEvalScratch(expr)
			require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0, 1: 1}, zms, vecs), "paired endpoints cannot disprove the actual matching row or suppress its diagnostic")
			// Fixed arguments retain useful exclusion proofs.
			constant := plan2.MakePlan2Int64ConstExprWithType(1)
			name := "div"
			match, miss := int64(1), int64(10)
			if tc.name == "division" {
				name = "/"
				constant = &plan.Expr{Typ: col.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: 1}}}}
			} else if !tc.quotient {
				name, match, miss = "ceil", 1, 20
				constant = plan2.MakePlan2Int64ConstExprWithType(-1)
			}
			for _, bound := range []int64{match, miss} {
				controlArgs := []*plan.Expr{col, constant}
				if !tc.quotient {
					controlArgs = []*plan.Expr{col}
				}
				control := materializeZoneMapFilter(t, proc, bindZoneMapFunction(t, proc, "=", bindZoneMapFunction(t, proc, name, controlArgs...), plan2.MakePlan2Int64ConstExprWithType(bound)))
				controlZMs, controlVecs := makeZoneMapEvalScratch(control)
				require.Equal(t, bound == match, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, control, meta, map[int]int{0: 0, 1: 1}, controlZMs, controlVecs))
			}
		})
	}
}

func TestEvaluateFilterByZoneMapCharInKeepsPadSpaceMatch(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
	col := &plan.Expr{Typ: plan.Type{Id: int32(types.T_char), Width: 8}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	list := &plan.Expr{Typ: col.Typ, Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{plan2.MakePlan2StringConstExprWithType("MO "), plan2.MakePlan2StringConstExprWithType("ZZ")}}}}
	for _, item := range list.GetList().List {
		item.Typ = col.Typ
	}
	expr := bindZoneMapFunction(t, proc, "in", col, list)
	input := batch.NewWithSize(1)
	input.Vecs[0] = vector.NewVec(types.New(types.T_char, 8, 0))
	t.Cleanup(func() { input.Clean(proc.Mp()) })
	require.NoError(t, vector.AppendBytes(input.Vecs[0], []byte("MO"), false, proc.Mp()))
	input.SetRowCount(1)
	executor, err := colexec.NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	t.Cleanup(executor.Free)
	result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	requireBoolValue(t, result, 0, true, false)
	expr = materializeZoneMapFilter(t, proc, expr)
	meta := objectio.BuildMetaData(1, 1).GetBlockMeta(0)
	zm := index.NewZM(types.T_char, 0)
	index.UpdateZM(zm, []byte("MO"))
	meta.MustGetColumn(0).SetZoneMap(zm)
	zms, vecs := makeZoneMapEvalScratch(expr)
	require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs), "CHAR tuple membership trims its keys and probes")
}

// A static family flag cannot justify paired endpoint proofs in every overload.
func TestEvaluateFilterByZoneMapEndpointContract(t *testing.T) {
	date := func(s string) types.Date { d, err := types.ParseDateCast(s); require.NoError(t, err); return d }
	dateLit := func(d types.Date) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_date)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dateval{Dateval: int32(d)}}}}
	}
	dt, err := types.ParseDatetime("1970-01-01 00:00:01", 6)
	require.NoError(t, err)
	newYork, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	dstTarget, err := types.ParseDatetime("2026-11-01 01:59:00", 6)
	require.NoError(t, err)
	datetimeLit := func(v types.Datetime) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_datetime), Scale: 6}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Datetimeval{Datetimeval: int64(v)}}}}
	}
	for _, tc := range []struct {
		name    string
		zone    *time.Location
		format  string
		oid     types.T
		values  []any
		amounts []int64
		row     int
		target  *plan.Expr
	}{
		{name: "in_range", oid: types.T_int64, values: []any{int64(0), int64(5), int64(10)}, row: 1},
		{name: "date_sub", oid: types.T_date, values: []any{date("2000-01-10"), date("2000-01-20"), date("2000-01-10")}, amounts: []int64{0, 10, 10}, row: 2, target: dateLit(date("1999-12-31"))},
		{name: "date", oid: types.T_varchar, values: []any{[]byte(" 2099-01-01"), []byte("2000-01-01"), []byte("2001-01-01")}, row: 1, target: dateLit(date("2000-01-01"))},
		{name: "year", oid: types.T_varchar, values: []any{[]byte(" 2099-01-01"), []byte("2000-01-01"), []byte("2001-01-01")}, row: 1, target: plan2.MakePlan2Int64ConstExprWithType(2000)},
		{name: "from_unixtime", oid: types.T_int64, values: []any{int64(0), int64(1), int64(32536771200)}, row: 1, target: &plan.Expr{Typ: plan.Type{Id: int32(types.T_datetime), Scale: 6}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Datetimeval{Datetimeval: int64(dt)}}}}},
		{name: "from_unixtime DST", zone: newYork, oid: types.T_int64, values: []any{time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC).Unix(), time.Date(2026, 11, 1, 5, 59, 0, 0, time.UTC).Unix(), time.Date(2026, 11, 1, 6, 40, 0, 0, time.UTC).Unix()}, row: 1, target: datetimeLit(dstTarget)},
		{name: "from_unixtime format", format: "%d-%m-%Y", oid: types.T_int64, values: []any{time.Date(2020, 12, 31, 0, 0, 0, 0, time.UTC).Unix(), time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC).Unix(), time.Date(2021, 1, 2, 0, 0, 0, 0, time.UTC).Unix()}, row: 1, target: plan2.MakePlan2StringConstExprWithType("01-01-2021")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.GetSessionInfo().TimeZone = time.UTC
			if tc.zone != nil {
				proc.GetSessionInfo().TimeZone = tc.zone
			}
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			meta := objectio.BuildMetaData(1, 2).GetBlockMeta(0)
			col := &plan.Expr{Typ: plan.Type{Id: int32(tc.oid)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			input.Vecs[0] = vector.NewVec(tc.oid.ToType())
			zm := index.NewZM(tc.oid, 0)
			for _, v := range tc.values {
				require.NoError(t, vector.AppendAny(input.Vecs[0], v, false, proc.Mp()))
				index.UpdateZMAny(zm, v)
			}
			meta.MustGetColumn(0).SetZoneMap(zm)
			var expr *plan.Expr
			switch tc.name {
			case "in_range":
				expr = bindZoneMapFunction(t, proc, "in_range", col, plan2.MakePlan2Int64ConstExprWithType(3), plan2.MakePlan2Int64ConstExprWithType(7), plan2.MakePlan2Uint8ConstExprWithType(3))
			case "date_sub":
				input.Vecs[1] = vector.NewVec(types.T_int64.ToType())
				amountZM := index.NewZM(types.T_int64, 0)
				for _, v := range tc.amounts {
					require.NoError(t, vector.AppendFixed(input.Vecs[1], v, false, proc.Mp()))
					index.UpdateZM(amountZM, types.EncodeInt64(&v))
				}
				meta.MustGetColumn(1).SetZoneMap(amountZM)
				amount := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 1}}}
				expr = bindZoneMapFunction(t, proc, "=", bindZoneMapFunction(t, proc, "date_sub", col, amount), tc.target)
			case "from_unixtime DST", "from_unixtime format":
				args := []*plan.Expr{col}
				if tc.format != "" {
					args = append(args, plan2.MakePlan2StringConstExprWithType(tc.format))
				}
				expr = bindZoneMapFunction(t, proc, "=", bindZoneMapFunction(t, proc, "from_unixtime", args...), tc.target)
			default:
				expr = bindZoneMapFunction(t, proc, "=", bindZoneMapFunction(t, proc, tc.name, col), tc.target)
			}
			input.SetRowCount(len(tc.values))
			executor, err := colexec.NewExpressionExecutor(proc, expr)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			require.NoError(t, err)
			requireBoolValue(t, result, uint64(tc.row), true, false)
			expr = materializeZoneMapFilter(t, proc, expr)
			zms, vecs := makeZoneMapEvalScratch(expr)
			require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0, 1: 1}, zms, vecs), "endpoint contract must retain a proven matching row")
		})
	}
}

func TestEvaluateFilterByZoneMapDecimalPrecisionControl(t *testing.T) {
	for _, oid := range []types.T{types.T_decimal64, types.T_decimal128} {
		t.Run(oid.String(), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
			width := int32(18)
			if oid == types.T_decimal128 {
				width = 38
			}
			typ := plan.Type{Id: int32(oid), Width: width, Scale: 2}
			col := &plan.Expr{Typ: typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			input := batch.NewWithSize(1)
			input.Vecs[0] = vector.NewVec(types.New(oid, width, 2))
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			meta := objectio.BuildMetaData(1, 1).GetBlockMeta(0)
			zm := index.NewZM(oid, 2)
			for _, n := range []int64{-1000, -500} {
				var d any = types.Decimal64(uint64(n))
				if oid == types.T_decimal128 {
					d = types.Decimal128{B0_63: uint64(n), B64_127: math.MaxUint64}
				}
				require.NoError(t, vector.AppendAny(input.Vecs[0], d, false, proc.Mp()))
				index.UpdateZMAny(zm, d)
			}
			input.SetRowCount(2)
			meta.MustGetColumn(0).SetZoneMap(zm)
			for _, fn := range []string{"round", "ceil", "floor"} {
				for _, bound := range []int64{-1000, -100000} {
					transform := bindZoneMapFunction(t, proc, fn, col, plan2.MakePlan2Int64ConstExprWithType(-1))
					target := plan2.MakePlan2Decimal64ExprWithType(types.Decimal64(uint64(bound/100)), &transform.Typ)
					if oid == types.T_decimal128 {
						target = plan2.MakePlan2Decimal128ExprWithType(types.Decimal128{B0_63: uint64(bound / 100), B64_127: math.MaxUint64}, &transform.Typ)
					}
					expr := bindZoneMapFunction(t, proc, "=", transform, target)
					executor, err := colexec.NewExpressionExecutor(proc, expr)
					require.NoError(t, err)
					t.Cleanup(executor.Free)
					result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
					require.NoError(t, err)
					requireBoolValue(t, result, 0, bound == -1000, false)
					expr = materializeZoneMapFilter(t, proc, expr)
					zms, vecs := makeZoneMapEvalScratch(expr)
					got := colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs)
					require.Equal(t, bound == -1000, got, "fixed decimal precision retains matching rows and excludes disjoint ranges")
				}
			}
		})
	}
}

func TestEvaluateFilterByZoneMapPositiveEndpointControls(t *testing.T) {
	date := func(s string) types.Date { d, err := types.ParseDateCast(s); require.NoError(t, err); return d }
	dates := []any{date("2000-01-01"), date("2000-01-02"), date("2000-01-03")}
	dateLit := func(d types.Date) *plan.Expr { return plan2.MakePlan2DateConstExprWithType(int32(d)) }
	datetimeLit := func(s string) *plan.Expr {
		d, err := types.ParseDatetime(s, 6)
		require.NoError(t, err)
		return plan2.MakePlan2DateTimeConstExprWithType(int64(d))
	}
	for _, tc := range []struct {
		name         string
		oid          types.T
		values       []any
		control      *plan.Expr
		controlFirst bool
		match, miss  *plan.Expr
	}{
		{"date", types.T_date, dates, nil, false, dateLit(date("2000-01-02")), dateLit(date("2000-01-04"))},
		{"year", types.T_date, dates, nil, false, plan2.MakePlan2Int64ConstExprWithType(2000), plan2.MakePlan2Int64ConstExprWithType(2001)},
		{"date_sub", types.T_date, dates, plan2.MakePlan2Int64ConstExprWithType(1), false, dateLit(date("2000-01-01")), dateLit(date("2000-01-03"))},
		{"date_trunc", types.T_date, dates, plan2.MakePlan2StringConstExprWithType("year"), true, dateLit(date("2000-01-01")), dateLit(date("2000-01-02"))},
		{"from_unixtime", types.T_int64, []any{int64(0), int64(1), int64(2)}, nil, false, datetimeLit("1970-01-01 00:00:01"), datetimeLit("1970-01-01 00:00:03")},
		{"ceil", types.T_int64, []any{int64(-10), int64(-5), int64(0)}, plan2.MakePlan2Int64ConstExprWithType(-1), false, plan2.MakePlan2Int64ConstExprWithType(0), plan2.MakePlan2Int64ConstExprWithType(10)},
		{"floor", types.T_int64, []any{int64(-10), int64(-5), int64(0)}, plan2.MakePlan2Int64ConstExprWithType(-1), false, plan2.MakePlan2Int64ConstExprWithType(-10), plan2.MakePlan2Int64ConstExprWithType(-20)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			proc.GetSessionInfo().TimeZone = time.UTC
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
			input := batch.NewWithSize(1)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			input.Vecs[0] = vector.NewVec(tc.oid.ToType())
			zm := index.NewZM(tc.oid, 0)
			for _, value := range tc.values {
				require.NoError(t, vector.AppendAny(input.Vecs[0], value, false, proc.Mp()))
				index.UpdateZMAny(zm, value)
			}
			input.SetRowCount(len(tc.values))
			meta := objectio.BuildMetaData(1, 1).GetBlockMeta(0)
			meta.MustGetColumn(0).SetZoneMap(zm)
			col := &plan.Expr{Typ: plan.Type{Id: int32(tc.oid)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			for i, target := range []*plan.Expr{tc.match, tc.miss} {
				args := []*plan.Expr{col}
				if tc.control != nil {
					if tc.controlFirst {
						args = []*plan.Expr{tc.control, col}
					} else {
						args = append(args, tc.control)
					}
				}
				expr := bindZoneMapFunction(t, proc, "=", bindZoneMapFunction(t, proc, tc.name, args...), target)
				executor, err := colexec.NewExpressionExecutor(proc, expr)
				require.NoError(t, err)
				t.Cleanup(executor.Free)
				result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
				require.NoError(t, err)
				requireBoolValue(t, result, 1, i == 0, false)
				expr = materializeZoneMapFilter(t, proc, expr)
				zms, vecs := makeZoneMapEvalScratch(expr)
				require.Equal(t, i == 0, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, meta, map[int]int{0: 0}, zms, vecs), "safe transforms must retain useful exclusion")
			}
		})
	}
}

type zoneMapTestWarnings struct{ count int }

func (s *zoneMapTestWarnings) AppendWarningDiagnostic(uint16, string) { s.count++ }

func TestEvaluateFilterByZoneMapConstantDiagnostics(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
	sink := &zoneMapTestWarnings{}
	proc.WarningSink = sink
	addTime := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar), Width: 29, Scale: 6}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.ADDTIME, 9), ObjName: "addtime"},
		Args: []*plan.Expr{plan2.MakePlan2StringConstExprWithType("838:59:59"), plan2.MakePlan2StringConstExprWithType("00:00:01")},
	}}}
	expr := bindZoneMapFunction(t, proc, "=", addTime, plan2.MakePlan2StringConstExprWithType("other"))
	func() {
		vec, free, err := colexec.GetReadonlyResultFromNoColumnExpression(proc, expr)
		require.NoError(t, err)
		defer free()
		requireBoolValue(t, vec, 0, false, true)
		require.Positive(t, sink.count, "the actual residual expression produces a warning")
	}()
	sink.count = 0
	require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, nil, nil, nil, nil), "a warning producer must remain executable")
	require.Zero(t, sink.count)
	require.Same(t, sink, proc.GetWarningSink())
	col := makeVarcharColExpr()
	mixed := bindZoneMapFunction(t, proc, "=", col, addTime)
	meta := objectio.BuildMetaData(1, 1).GetBlockMeta(0)
	zm := index.NewZM(types.T_varchar, 0)
	index.UpdateZM(zm, []byte("other"))
	meta.MustGetColumn(0).SetZoneMap(zm)
	zms, vecs := makeZoneMapEvalScratch(mixed)
	require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, mixed, meta, map[int]int{0: 0}, zms, vecs))
	require.Zero(t, sink.count)
	require.Same(t, sink, proc.GetWarningSink())
	overflow := bindZoneMapFunction(t, proc, "=", bindZoneMapFunction(t, proc, "round", plan2.MakePlan2Int64ConstExprWithType(math.MaxInt64), plan2.MakePlan2Int64ConstExprWithType(-1)), plan2.MakePlan2Int64ConstExprWithType(0))
	require.NotPanics(t, func() { require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, overflow, nil, nil, nil, nil)) })
	for _, data := range [][]byte{{0}, {1}, nil, {2}} {
		fold := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{Id: 999, IsConst: true, Data: data}}}
		require.Equal(t, !bytes.Equal(data, []byte{0}), colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, fold, nil, nil, nil, nil))
	}
}

// The production list owner serializes ordinary tuple carriers as LiteralVec;
// the vector codec, rather than the outer tuple type, owns their physical type.
func TestEvaluateFilterByZoneMapMaterializedTupleList(t *testing.T) {
	proc := testutil.NewProcess(t)
	t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
	for _, member := range []string{"m", "z"} {
		t.Run(member, func(t *testing.T) {
			list := &plan.Expr{Typ: plan.Type{Id: int32(types.T_tuple)}, Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{
				plan2.MakePlan2StringConstExprWithType(member), plan2.MakePlan2StringConstExprWithType("zz"),
			}}}}
			expr := makeNativeVarcharInExpr(t, proc.Ctx, []*plan.Expr{makeVarcharColExpr(), list})
			row := evalVarcharPredicate(t, proc, expr, "m")
			requireBoolValue(t, row, 0, member == "m", false)
			expr = materializeZoneMapFilter(t, proc, expr)
			require.NotNil(t, expr.GetF().Args[1].GetVec(), "ordinary tuple lists use the existing LiteralVec owner")
			zms, vecs := makeZoneMapEvalScratch(expr)
			require.Equal(t, member == "m", colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, expr, makeVarcharBlockMeta("m", "n"), map[int]int{0: 0}, zms, vecs))
		})
	}
}

func TestEvaluateFilterByZoneMapCheckedArithmetic(t *testing.T) {
	for _, tc := range []struct {
		name, op string
		value    int64
		selected bool
	}{
		{"addition overflow retained", "+", math.MaxInt64, true},
		{"multiplication overflow retained", "*", math.MaxInt64, true},
		{"safe addition prunes", "+", 10, false},
		{"safe subtraction prunes", "-", 10, false},
		{"safe multiplication prunes", "*", 10, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			column := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			arithmetic, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, tc.op, []*plan.Expr{column, column})
			require.NoError(t, err)
			predicate, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*plan.Expr{arithmetic, column})
			require.NoError(t, err)
			require.True(t, plan2.ExprIsZonemappable(proc.Ctx, predicate))
			meta := objectio.BuildMetaData(1, 1).GetBlockMeta(0)
			zms, vecs := makeZoneMapEvalScratch(predicate)
			for i, value := range []int64{tc.value, 10, tc.value} {
				zm := index.NewZM(types.T_int64, 0)
				index.UpdateZM(zm, types.EncodeInt64(&value))
				meta.MustGetColumn(0).SetZoneMap(zm)
				selected := tc.selected && i != 1
				require.Equal(t, selected, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, predicate, meta, map[int]int{0: 0}, zms, vecs), "scratch must not retain previous block's proof")
			}
		})
	}
}

func TestEvaluateFilterByZoneMapConstantArithmetic(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	for _, tc := range []struct {
		name, op         string
		constant, result int64
		constantOnLeft   bool
		overflow         int64
	}{
		{"addition", "+", 2, 5, false, math.MaxInt64},
		{"subtraction", "-", 2, 5, false, math.MinInt64},
		{"constant minus column", "-", 14, 5, true, math.MinInt64},
		{"multiplication", "*", 2, 6, false, math.MaxInt64},
	} {
		t.Run(tc.name, func(t *testing.T) {
			column := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
			constant := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeInt64(&tc.constant)}}}
			args := []*plan.Expr{column, constant}
			if tc.constantOnLeft {
				args[0], args[1] = args[1], args[0]
			}
			arithmetic, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, tc.op, args)
			require.NoError(t, err)
			result := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeInt64(&tc.result)}}}
			predicate, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*plan.Expr{arithmetic, result})
			require.NoError(t, err)
			require.True(t, plan2.ExprIsZonemappable(proc.Ctx, predicate))
			meta := objectio.BuildMetaData(1, 1).GetBlockMeta(0)
			zms, vecs := makeZoneMapEvalScratch(predicate)
			for _, block := range []struct {
				min, max int64
				selected bool
			}{
				{1, 10, true},
				{100, 110, false},
				{tc.overflow, tc.overflow, true},
				{1, 10, true},
			} {
				zm := index.NewZM(types.T_int64, 0)
				index.UpdateZM(zm, types.EncodeInt64(&block.min))
				index.UpdateZM(zm, types.EncodeInt64(&block.max))
				meta.MustGetColumn(0).SetZoneMap(zm)
				require.Equal(t, block.selected, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, predicate, meta, map[int]int{0: 0}, zms, vecs))
			}
			// Reuse scratch while the statement's Fold value changes. Unknown
			// payloads must erase the previous exclusion proof.
			fold := constant.GetFold()
			for _, value := range []struct {
				data   []byte
				scalar bool
			}{
				{nil, true},
				{[]byte{1}, true},
				{types.EncodeInt64(&tc.constant), false},
				{types.EncodeInt64(&tc.constant), true},
			} {
				fold.Data, fold.IsConst = value.data, value.scalar
				require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, predicate, meta, map[int]int{0: 0}, zms, vecs))
			}
			min, max := int64(100), int64(110)
			zm := index.NewZM(types.T_int64, 0)
			index.UpdateZM(zm, types.EncodeInt64(&min))
			index.UpdateZM(zm, types.EncodeInt64(&max))
			meta.MustGetColumn(0).SetZoneMap(zm)
			require.False(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, predicate, meta, map[int]int{0: 0}, zms, vecs))
			bound := min + tc.constant
			if tc.op == "-" {
				bound = min - tc.constant
				if tc.constantOnLeft {
					bound = tc.constant - min
				}
			} else if tc.op == "*" {
				bound = min * tc.constant
			}
			result.GetFold().Data = types.EncodeInt64(&bound)
			require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, predicate, meta, map[int]int{0: 0}, zms, vecs))
			result.GetFold().Data = types.EncodeInt64(&tc.result)
			require.False(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, predicate, meta, map[int]int{0: 0}, zms, vecs))

			fold.Data = nil
			require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, predicate, meta, map[int]int{0: 0}, zms, vecs))

		})
	}
}

func TestEvaluateFilterByZoneMapVaryingDenominator(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	one, two := float64(1), float64(2)
	constant := &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeFloat64(&one)}}}
	column := &plan.Expr{Typ: constant.Typ, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
	quotient, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "/", []*plan.Expr{constant, column})
	require.NoError(t, err)
	bound := &plan.Expr{Typ: constant.Typ, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeFloat64(&two)}}}
	predicate, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, ">", []*plan.Expr{quotient, bound})
	require.NoError(t, err)
	meta := objectio.BuildMetaData(1, 1).GetBlockMeta(0)
	zms, vecs := makeZoneMapEvalScratch(predicate)
	for _, tc := range []struct {
		min, max float64
		selected bool
	}{
		{-1, 1, true}, // 0.1 matches although neither endpoint does.
		{1, 2, true},  // Varying denominators have no endpoint-pair proof.
		{1, 1, false}, // Preserve a safe singleton exclusion after unknown.
	} {
		zm := index.NewZM(types.T_float64, 0)
		index.UpdateZM(zm, types.EncodeFloat64(&tc.min))
		index.UpdateZM(zm, types.EncodeFloat64(&tc.max))
		meta.MustGetColumn(0).SetZoneMap(zm)
		require.Equal(t, tc.selected, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, predicate, meta, map[int]int{0: 0}, zms, vecs))
	}
}

func TestEvaluateFilterByZoneMapScalarOverflowCleanup(t *testing.T) {
	for _, typeID := range []types.T{types.T_int64, types.T_uint64} {
		t.Run(typeID.String(), func(t *testing.T) {
			proc := testutil.NewProcess(t)
			defer proc.Free()
			col := func(pos int32) *plan.Expr {
				return &plan.Expr{Typ: plan.Type{Id: int32(typeID)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
			}
			sum, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "+", []*plan.Expr{col(0), col(1)})
			require.NoError(t, err)
			digits := int64(-1)
			precision := &plan.Expr{Typ: plan.Type{Id: int32(types.T_int64)}, Expr: &plan.Expr_Fold{Fold: &plan.FoldVal{IsConst: true, Data: types.EncodeInt64(&digits)}}}
			rounded, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "round", []*plan.Expr{sum, precision})
			require.NoError(t, err)
			target := plan2.MakePlan2Int64ConstExprWithType(100)
			upper := uint64(math.MaxInt64)
			if typeID == types.T_uint64 {
				target = plan2.MakePlan2Uint64ConstExprWithType(100)
				upper = math.MaxUint64
			}
			predicate, err := plan2.BindFuncExprImplByPlanExpr(proc.Ctx, "=", []*plan.Expr{rounded, target})
			require.NoError(t, err)
			meta := objectio.BuildMetaData(1, 2).GetBlockMeta(0)
			zms, vecs := makeZoneMapEvalScratch(predicate)
			baseline := proc.Mp().CurrNB()
			for _, tc := range []struct {
				vmin, vmax, wmin, wmax uint64
				selected               bool
				unknown                bool
			}{
				{1, 2, 1, 2, false, false},
				{0, upper - 100, 0, 100, true, true}, // Independent endpoints overflow ROUND; actual correlated rows do not.
				{1, 2, 1, 2, false, false},           // Reuse the same scratch after losing the proof.
				{90, 100, 0, 0, true, false},
				{0, upper - 110, 0, 100, true, false}, // Nearby endpoint remains within the ROUND domain.
			} {
				for i, bounds := range [][2]uint64{{tc.vmin, tc.vmax}, {tc.wmin, tc.wmax}} {
					zm := index.NewZM(typeID, 0)
					index.UpdateZM(zm, types.EncodeUint64(&bounds[0]))
					index.UpdateZM(zm, types.EncodeUint64(&bounds[1]))
					meta.MustGetColumn(uint16(i)).SetZoneMap(zm)
				}
				var selected bool
				var panicked any
				func() {
					defer func() { panicked = recover() }()
					selected = colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, predicate, meta, map[int]int{0: 0, 1: 1}, zms, vecs)
				}()
				for i, vec := range vecs {
					if vec != nil {
						vec.Free(proc.Mp())
						vecs[i] = nil
					}
				}
				if panicked != nil {
					t.Errorf("metadata scalar panic escaped: %v", panicked)
				}
				require.Equal(t, baseline, proc.Mp().CurrNB(), "temporary result must be released on every exit")
				require.Equal(t, tc.selected, selected)
				require.Equal(t, tc.unknown, !zms[rounded.AuxId].IsInited(), "only the invalid endpoint loses its proof")
			}
		})
	}
}

// Point metadata permits division without changing a column's FLAT provenance.
func TestEvaluateFilterByZoneMapPointDivisionControls(t *testing.T) {
	for _, tc := range []struct {
		name string
		oid  types.T
	}{
		{"/", types.T_float64}, {"div", types.T_int64}, {"div", types.T_uint64}, {"div", types.T_float64},
	} {
		for _, denominator := range []int64{2, -2} {
			if tc.oid == types.T_uint64 && denominator < 0 {
				continue
			}
			t.Run(fmt.Sprintf("%s/%s/%d", tc.name, tc.oid, denominator), func(t *testing.T) {
				proc := testutil.NewProcess(t)
				t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
				input := batch.NewWithSize(2)
				t.Cleanup(func() { input.Clean(proc.Mp()) })
				meta := objectio.BuildMetaData(1, 2).GetBlockMeta(0)
				for i, values := range [][]int64{{1, 5, 9}, {denominator, denominator, denominator}} {
					input.Vecs[i] = vector.NewVec(tc.oid.ToType())
					zm := index.NewZM(tc.oid, 0)
					for _, v := range values {
						var n any = v
						if tc.oid == types.T_uint64 {
							n = uint64(v)
						} else if tc.oid == types.T_float64 {
							n = float64(v)
						}
						require.NoError(t, vector.AppendAny(input.Vecs[i], n, false, proc.Mp()))
						index.UpdateZMAny(zm, n)
					}
					meta.MustGetColumn(uint16(i)).SetZoneMap(zm)
				}
				input.SetRowCount(3)
				require.False(t, input.Vecs[1].IsConst())
				col := func(pos int32) *plan.Expr {
					return &plan.Expr{Typ: plan.Type{Id: int32(tc.oid)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
				}
				quotient := bindZoneMapFunction(t, proc, tc.name, col(0), col(1))
				target := int64(5) / denominator
				literal := func(miss bool) *plan.Expr {
					v := target
					if miss {
						v = 100
					}
					if tc.name == "/" {
						f := float64(5) / float64(denominator)
						if miss {
							f = 100
						}
						return &plan.Expr{Typ: plan.Type{Id: int32(types.T_float64)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Dval{Dval: f}}}}
					}
					if quotient.Typ.Id == int32(types.T_uint64) {
						return plan2.MakePlan2Uint64ConstExprWithType(uint64(v))
					}
					return plan2.MakePlan2Int64ConstExprWithType(v)
				}
				for _, miss := range []bool{false, true} {
					predicate := bindZoneMapFunction(t, proc, "=", quotient, literal(miss))
					executor, err := colexec.NewExpressionExecutor(proc, predicate)
					require.NoError(t, err)
					t.Cleanup(executor.Free)
					result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
					require.NoError(t, err)
					requireBoolValue(t, result, 1, !miss, false)
					predicate = materializeZoneMapFilter(t, proc, predicate)
					zms, vecs := makeZoneMapEvalScratch(predicate)
					require.Equal(t, !miss, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, predicate, meta, map[int]int{0: 0, 1: 1}, zms, vecs))
					for _, v := range vecs {
						if v != nil {
							v.Free(proc.Mp())
						}
					}
				}
			})
		}
	}
}

func TestEvaluateFilterByZoneMapPointDivisionDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name                   string
		oid                    types.T
		numerator, denominator any
		null                   bool
		wantError              bool
	}{
		{"signed minimum positive", types.T_int64, int64(math.MinInt64), int64(1), false, true},
		{"signed minimum negative", types.T_int64, int64(math.MinInt64), int64(-1), false, true},
		{"float integer conversion", types.T_float64, float64(math.MaxFloat64), float64(1), false, true},
		{"zero", types.T_int64, int64(1), int64(0), false, false},
		{"null", types.T_int64, int64(1), int64(2), true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			proc := testutil.NewProcess(t)
			t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
			input := batch.NewWithSize(2)
			t.Cleanup(func() { input.Clean(proc.Mp()) })
			meta := objectio.BuildMetaData(1, 2).GetBlockMeta(0)
			for i, value := range []any{tc.numerator, tc.denominator} {
				input.Vecs[i] = vector.NewVec(tc.oid.ToType())
				isNull := i == 1 && tc.null
				require.NoError(t, vector.AppendAny(input.Vecs[i], value, isNull, proc.Mp()))
				zm := index.NewZM(tc.oid, 0)
				if !isNull {
					index.UpdateZMAny(zm, value)
				}
				meta.MustGetColumn(uint16(i)).SetZoneMap(zm)
			}
			input.SetRowCount(1)
			col := func(pos int32) *plan.Expr {
				return &plan.Expr{Typ: plan.Type{Id: int32(tc.oid)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: pos}}}
			}
			quotient := bindZoneMapFunction(t, proc, "div", col(0), col(1))
			predicate := bindZoneMapFunction(t, proc, "=", quotient, plan2.MakePlan2Int64ConstExprWithType(0))
			executor, err := colexec.NewExpressionExecutor(proc, predicate)
			require.NoError(t, err)
			t.Cleanup(executor.Free)
			result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.True(t, result.GetNulls().Contains(0))
			}
			predicate = materializeZoneMapFilter(t, proc, predicate)
			zms, vecs := makeZoneMapEvalScratch(predicate)
			t.Cleanup(func() {
				for _, v := range vecs {
					if v != nil {
						v.Free(proc.Mp())
					}
				}
			})
			require.True(t, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, predicate, meta, map[int]int{0: 0, 1: 1}, zms, vecs), "metadata must not suppress a row diagnostic or publish NULL endpoints")
			require.False(t, zms[quotient.AuxId].IsInited())
		})
	}
}

// Declared integer columns use Scale=-1, while ordinary literals use Scale=0.
// Neither annotation changes physical integer or floating-point arithmetic.
func TestEvaluateFilterByZoneMapArithmeticScaleAnnotations(t *testing.T) {
	for _, oid := range []types.T{types.T_int64, types.T_uint64, types.T_float64} {
		for _, op := range []string{"+", "-", "*"} {
			for _, reversed := range []bool{false, true} {
				if reversed && (op != "-" || oid == types.T_uint64) {
					continue
				}
				t.Run(fmt.Sprintf("%s/%s/reversed=%t", oid, op, reversed), func(t *testing.T) {
					proc := testutil.NewProcess(t)
					t.Cleanup(func() { require.Zero(t, proc.Mp().CurrNB()); proc.Free() })
					input := batch.NewWithSize(1)
					input.Vecs[0] = vector.NewVec(oid.ToType())
					t.Cleanup(func() { input.Clean(proc.Mp()) })
					meta := objectio.BuildMetaData(1, 1).GetBlockMeta(0)
					zm := index.NewZM(oid, -1)
					values := []int64{1, 5, 9}
					if oid == types.T_uint64 && op == "-" {
						values[0] = 3
					}
					for _, v := range values {
						var n any = v
						if oid == types.T_uint64 {
							n = uint64(v)
						} else if oid == types.T_float64 {
							n = float64(v)
						}
						require.NoError(t, vector.AppendAny(input.Vecs[0], n, false, proc.Mp()))
						index.UpdateZMAny(zm, n)
					}
					input.SetRowCount(3)
					meta.MustGetColumn(0).SetZoneMap(zm)
					col := &plan.Expr{Typ: plan.Type{Id: int32(oid), Scale: -1}, Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}}}
					literal := func(v int64) *plan.Expr {
						if oid == types.T_uint64 {
							return plan2.MakePlan2Uint64ConstExprWithType(uint64(v))
						}
						if oid == types.T_float64 {
							return plan2.MakePlan2Float64ConstExprWithType(float64(v))
						}
						return plan2.MakePlan2Int64ConstExprWithType(v)
					}
					args := []*plan.Expr{col, literal(2)}
					colIndex, literalIndex := 0, 1
					if reversed {
						args[0], args[1] = args[1], args[0]
						colIndex, literalIndex = 1, 0
					}
					arithmetic := bindZoneMapFunction(t, proc, op, args...)
					require.Equal(t, int32(-1), arithmetic.GetF().Args[colIndex].Typ.Scale)
					require.Zero(t, arithmetic.GetF().Args[literalIndex].Typ.Scale)
					target := int64(7)
					if op == "-" {
						target = 3
						if reversed {
							target = -3
						}
					} else if op == "*" {
						target = 10
					}
					for _, miss := range []bool{false, true} {
						value := target
						if miss {
							value = 100
						}
						predicate := bindZoneMapFunction(t, proc, "=", arithmetic, literal(value))
						executor, err := colexec.NewExpressionExecutor(proc, predicate)
						require.NoError(t, err)
						t.Cleanup(executor.Free)
						result, err := executor.Eval(proc, []*batch.Batch{input}, nil)
						require.NoError(t, err)
						requireBoolValue(t, result, 1, !miss, false)
						require.True(t, plan2.ExprIsZonemappable(proc.Ctx, predicate))
						predicate = materializeZoneMapFilter(t, proc, predicate)
						zms, vecs := makeZoneMapEvalScratch(predicate)
						t.Cleanup(func() {
							for _, v := range vecs {
								if v != nil {
									v.Free(proc.Mp())
								}
							}
						})
						require.Equal(t, !miss, colexec.EvaluateFilterByZoneMap(proc.Ctx, proc, predicate, meta, map[int]int{0: 0}, zms, vecs))
					}
				})
			}
		}
	}
}
