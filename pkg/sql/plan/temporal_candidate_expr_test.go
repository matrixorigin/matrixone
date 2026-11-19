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
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestDirectTemporalCandidateCoversScalarMatches(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	proc.GetSessionInfo().TimeZone = time.UTC
	proc.Base.UnixTime = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC).UnixNano()
	now, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", nil)
	require.NoError(t, err)
	col := GetColExpr(Type{Id: int32(types.T_datetime)}, 0, 0)
	candidate, err := buildTemporalCandidate(proc.Ctx, col, now, nil, types.ComparisonLess, 0)
	require.NoError(t, err)
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
	candidate, err := buildTemporalCandidate(proc.Ctx, col, now, nil, types.ComparisonLess, 0)
	require.NoError(t, err)
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
	candidate, err := buildTemporalCandidate(proc.Ctx, component, now, key, types.ComparisonLess, 0)
	require.NoError(t, err)
	serial, err := BindFuncExprImplByPlanExpr(proc.Ctx, indexTableComparisonSerialFunc(), []*plan.Expr{component, MakePlan2Int64ConstExprWithType(7)})
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
