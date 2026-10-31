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
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDirectTemporalCandidateStartsUnadmitted(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	datetimeCol := GetColExpr(Type{Id: int32(types.T_datetime)}, 0, 0)
	timestampCol := GetColExpr(Type{Id: int32(types.T_timestamp), Scale: 3}, 0, 0)
	datetimeBound := &plan.Expr{Typ: datetimeCol.Typ, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Datetimeval{Datetimeval: 10}}}}
	for _, tc := range []struct {
		column, bound *plan.Expr
		scale         int32
	}{{datetimeCol, now, 0}, {timestampCol, datetimeBound, 3}} {
		candidate, guard, err := makeDirectTemporalCandidate(proc.Ctx, tc.column, tc.bound, types.TemporalLess, tc.scale)
		require.NoError(t, err)
		require.True(t, function.ContainsTemporalBound(candidate))
		require.False(t, candidate.TemporalCandidateFilter)
		require.False(t, guard.TemporalCandidateGuard)
		var checkPermission func(*plan.Expr)
		checkPermission = func(expr *plan.Expr) {
			if fn := expr.GetF(); fn != nil {
				id, _ := function.DecodeOverloadID(fn.Func.Obj)
				if id == function.TEMPORAL_DATETIME_BOUND || id == function.TEMPORAL_TIMESTAMP_BOUND {
					require.False(t, fn.Args[4].GetLit().GetBval(), "narrowing requires execution placement proof")
				}
				for _, arg := range fn.Args {
					checkPermission(arg)
				}
			}
		}
		checkPermission(candidate)
	}
}

func TestDirectTemporalCandidateWithNarrowingDisabledKeepsPhysicalDomain(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	col := GetColExpr(Type{Id: int32(types.T_datetime)}, 0, 0)
	candidate, _, err := makeDirectTemporalCandidate(proc.Ctx, col, now, types.TemporalLess, 0)
	require.NoError(t, err)
	executor, err := colexec.NewExpressionExecutor(proc, candidate)
	require.NoError(t, err)
	defer executor.Free()
	bat := batch.NewWithSize(1)
	defer bat.Clean(proc.Mp())
	bat.Vecs[0] = vector.NewVec(types.T_datetime.ToType())
	rows := []types.Datetime{math.MinInt64, -2, -1, 0, 1, types.DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999), math.MaxInt64}
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], rows, nil, proc.Mp()))
	bat.SetRowCount(len(rows))
	result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	for row := range rows {
		require.True(t, vector.GetFixedAtWithTypeCheck[bool](result, row), "physical row %d", row)
	}
}

func TestDirectTemporalCandidateCoversScalarMatches(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	proc.Base.UnixTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano()
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	col := GetColExpr(Type{Id: int32(types.T_datetime)}, 0, 0)
	candidate, _, err := makeDirectTemporalCandidate(proc.Ctx, col, now, types.TemporalLess, 0)
	require.NoError(t, err)
	allowTemporalCandidateNarrowingForTest(candidate)
	executor, err := colexec.NewExpressionExecutor(proc, candidate)
	require.NoError(t, err)
	defer executor.Free()
	bat := batch.NewWithSize(1)
	defer bat.Clean(proc.Mp())
	bat.Vecs[0] = vector.NewVec(types.T_datetime.ToType())
	center := types.DatetimeFromClock(2026, 6, 1, 12, 0, 0, 0)
	rows := []types.Datetime{-1, center - 1000000, center, center + 1000000}
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], rows, nil, proc.Mp()))
	bat.SetRowCount(len(rows))
	result, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	bound := types.UnixNanoToTimestamp(proc.Base.UnixTime).TruncateToScale(0)
	for i, row := range rows {
		if row.ToTimestamp(time.UTC).TruncateToScale(0) < bound {
			require.True(t, vector.GetFixedAtWithTypeCheck[bool](result, i), "matching row %d", i)
		}
	}
	require.False(t, vector.GetFixedAtWithTypeCheck[bool](result, len(rows)-1), "selective candidate should prune the distant later row")
}

func TestTemporalRowCandidateRefreshesClockAfterExecutorReset(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	proc.Base.UnixTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano()
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	col := GetColExpr(Type{Id: int32(types.T_datetime)}, 0, 0)
	candidate, _, err := makeDirectTemporalCandidate(proc.Ctx, col, now, types.TemporalLess, 0)
	require.NoError(t, err)
	allowTemporalCandidateNarrowingForTest(candidate)
	executor, err := colexec.NewExpressionExecutor(proc, candidate)
	require.NoError(t, err)
	defer executor.Free()
	bat := batch.NewWithSize(1)
	defer bat.Clean(proc.Mp())
	bat.Vecs[0] = vector.NewVec(types.T_datetime.ToType())
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.DatetimeFromClock(2026, 6, 1, 13, 0, 0, 0), false, proc.Mp()))
	bat.SetRowCount(1)
	first, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	require.False(t, vector.GetFixedAtWithTypeCheck[bool](first, 0))
	executor.ResetForNextQuery()
	proc.Base.UnixTime += int64(2 * time.Hour)
	second, err := executor.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	require.True(t, vector.GetFixedAtWithTypeCheck[bool](second, 0), "row filtering must use this execution's clock")
}

func TestEncodedTemporalCandidateCoversScalarMatches(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	proc.Base.UnixTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano()
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	component := GetColExpr(Type{Id: int32(types.T_datetime)}, 0, 0)
	key := GetColExpr(Type{Id: int32(types.T_varchar)}, 0, 0)
	candidate, _, err := makeEncodedTemporalCandidate(proc.Ctx, key, component, now, nil, false, types.TemporalLess, 0)
	require.NoError(t, err)
	allowTemporalCandidateNarrowingForTest(candidate)
	serial, err := BindFuncExprImplByPlanExpr(proc.Ctx, indexTableComparisonSerialFunc(), []*plan.Expr{component})
	require.NoError(t, err)
	encoder, err := colexec.NewExpressionExecutor(proc, serial)
	require.NoError(t, err)
	defer encoder.Free()
	bat := batch.NewWithSize(1)
	defer bat.Clean(proc.Mp())
	bat.Vecs[0] = vector.NewVec(types.T_datetime.ToType())
	center := types.DatetimeFromClock(2026, 6, 1, 12, 0, 0, 0)
	rows := []types.Datetime{-1, center - 1000000, center, center + 1000000}
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], rows, nil, proc.Mp()))
	bat.SetRowCount(len(rows))
	encoded, err := encoder.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	encodedBatch := batch.NewWithSize(1)
	encodedBatch.Vecs[0] = encoded // owned by encoder
	encodedBatch.SetRowCount(len(rows))
	executor, err := colexec.NewExpressionExecutor(proc, candidate)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, []*batch.Batch{encodedBatch}, nil)
	require.NoError(t, err)
	bound := types.UnixNanoToTimestamp(proc.Base.UnixTime).TruncateToScale(0)
	for i, row := range rows {
		if row.ToTimestamp(time.UTC).TruncateToScale(0) < bound {
			require.True(t, vector.GetFixedAtWithTypeCheck[bool](result, i), "matching row %d", i)
		}
	}
	require.False(t, vector.GetFixedAtWithTypeCheck[bool](result, len(rows)-1))
}

func TestEncodedTemporalCandidateWithPrefixAndSuffix(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	proc.Base.UnixTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano()
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	integer := Type{Id: int32(types.T_int64)}
	prefixCol := GetColExpr(integer, 0, 0)
	component := GetColExpr(Type{Id: int32(types.T_datetime)}, 0, 1)
	suffixCol := GetColExpr(integer, 0, 2)
	key := GetColExpr(Type{Id: int32(types.T_varchar)}, 0, 0)
	prefix := &plan.Expr{Typ: integer, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I64Val{I64Val: 7}}}}
	candidate, _, err := makeEncodedTemporalCandidate(proc.Ctx, key, component, now, []*plan.Expr{prefix}, true, types.TemporalLess, 0)
	require.NoError(t, err)
	allowTemporalCandidateNarrowingForTest(candidate)
	serial, err := BindFuncExprImplByPlanExpr(proc.Ctx, indexTableComparisonSerialFunc(), []*plan.Expr{prefixCol, component, suffixCol})
	require.NoError(t, err)
	encoder, err := colexec.NewExpressionExecutor(proc, serial)
	require.NoError(t, err)
	defer encoder.Free()
	bat := batch.NewWithSize(3)
	defer bat.Clean(proc.Mp())
	bat.Vecs[0] = vector.NewVec(types.T_int64.ToType())
	bat.Vecs[1] = vector.NewVec(types.T_datetime.ToType())
	bat.Vecs[2] = vector.NewVec(types.T_int64.ToType())
	center := types.DatetimeFromClock(2026, 6, 1, 12, 0, 0, 0)
	rows := []types.Datetime{-1, center - 1000000, center + 1000000, center - 1000000, math.MinInt64, math.MaxInt64}
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], []int64{7, 7, 7, 8, 7, 7}, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[1], rows, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[2], []int64{1, 2, 3, 4, 5, 6}, nil, proc.Mp()))
	bat.SetRowCount(len(rows))
	encoded, err := encoder.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	encodedBatch := batch.NewWithSize(1)
	encodedBatch.Vecs[0] = encoded // owned by encoder
	encodedBatch.SetRowCount(len(rows))
	executor, err := colexec.NewExpressionExecutor(proc, candidate)
	require.NoError(t, err)
	defer executor.Free()
	result, err := executor.Eval(proc, []*batch.Batch{encodedBatch}, nil)
	require.NoError(t, err)
	require.True(t, vector.GetFixedAtWithTypeCheck[bool](result, 0))
	require.True(t, vector.GetFixedAtWithTypeCheck[bool](result, 1))
	require.False(t, vector.GetFixedAtWithTypeCheck[bool](result, 2))
	require.False(t, vector.GetFixedAtWithTypeCheck[bool](result, 3))
	require.True(t, vector.GetFixedAtWithTypeCheck[bool](result, 4), "raw lower physical tail must remain visible")
	require.True(t, vector.GetFixedAtWithTypeCheck[bool](result, 5), "raw upper physical tail must remain visible")
}

func allowTemporalCandidateNarrowingForTest(expr *plan.Expr) {
	if fn := expr.GetF(); fn != nil {
		id, _ := function.DecodeOverloadID(fn.Func.Obj)
		if id == function.TEMPORAL_DATETIME_BOUND || id == function.TEMPORAL_TIMESTAMP_BOUND {
			fn.Args[4] = MakePlan2BoolConstExprWithType(true)
		}
		for _, arg := range fn.Args {
			allowTemporalCandidateNarrowingForTest(arg)
		}
	}
}
