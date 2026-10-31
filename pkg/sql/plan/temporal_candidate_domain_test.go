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
	"math"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func TestTemporalInteriorDomainUsesActualCurrentTimestamp(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	maximum := int64(types.DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999))
	for _, name := range []string{"now", "current_timestamp"} {
		for fsp := int32(0); fsp <= 6; fsp++ {
			bound, err := BindFuncExprImplByPlanExpr(proc.Ctx, name, []*pb.Expr{MakePlan2Int64ConstExprWithType(int64(fsp))})
			require.NoError(t, err)
			require.True(t, temporalCandidateInteriorTimestampBound(bound))
			for _, clock := range []int64{math.MinInt64, -1, 0, 1, math.MaxInt64} {
				proc.Base.UnixTime = clock
				func() {
					exe, err := colexec.NewExpressionExecutor(proc, bound)
					require.NoError(t, err)
					defer exe.Free()
					got, err := exe.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
					require.NoError(t, err)
					require.False(t, got.IsNull(0))
					value := int64(vector.GetFixedAtWithTypeCheck[types.Timestamp](got, 0))
					require.GreaterOrEqual(t, value, temporalInteriorTimestampMargin)
					require.LessOrEqual(t, value, maximum-temporalInteriorTimestampMargin)
				}()
			}
		}
	}
	literal := func(v int64) *pb.Expr {
		return &pb.Expr{Typ: Type{Id: int32(types.T_timestamp), Scale: 6}, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Timestampval{Timestampval: v}}}}
	}
	for _, v := range []int64{math.MinInt64, -1, 0, temporalInteriorTimestampMargin - 1, maximum - temporalInteriorTimestampMargin + 1, math.MaxInt64} {
		require.False(t, temporalCandidateInteriorTimestampBound(literal(v)))
	}
	for _, v := range []int64{temporalInteriorTimestampMargin, maximum - temporalInteriorTimestampMargin} {
		require.True(t, temporalCandidateInteriorTimestampBound(literal(v)))
	}
	safe := literal(int64(types.UnixNanoToTimestamp(0)))
	for _, alter := range []string{"source", "memo", "unknown", "parameter", "typed nil", "wrong oneof", "null"} {
		bad := proto.Clone(safe).(*pb.Expr)
		switch alter {
		case "source":
			bad.GetLit().Src = &pb.Expr{Expr: &pb.Expr_P{P: &pb.ParamRef{}}}
		case "null":
			bad.GetLit().Isnull = true
			bad.GetLit().Value = nil
		case "memo":
			bad.AuxId = -1
		case "unknown":
			bad.XXX_unrecognized = []byte{8, 1}
		case "parameter":
			bad.Expr = &pb.Expr_P{P: &pb.ParamRef{}}
		case "typed nil":
			bad.Expr = (*pb.Expr_Lit)(nil)
		case "wrong oneof":
			bad.GetLit().Value = &pb.Literal_I64Val{I64Val: int64(types.UnixNanoToTimestamp(0))}
		}
		require.False(t, temporalCandidateInteriorTimestampBound(bad), alter)
	}
}

// The oracle is the old four-arm set, derived independently from the scalar
// bound primitive. It includes raw out-of-calendar int64 values, not only rows
// that a SQL INSERT normally accepts.
func checkInteriorTemporalUnion(t *testing.T, proc *process.Process, clock int64, scale int32, op types.TemporalOp, zone *time.Location, encoded, allow bool, literal *types.Timestamp, leadingPrefix bool) {
	t.Helper()
	proc.Base.UnixTime = clock
	proc.GetSessionInfo().TimeZone = zone
	bound, err := BindFuncExprImplByPlanExpr(proc.Ctx, "now", []*pb.Expr{MakePlan2Int64ConstExprWithType(6)})
	require.NoError(t, err)
	stamp := types.UnixNanoToTimestamp(clock).TruncateToScale(6)
	if literal != nil {
		stamp = *literal
		bound = &pb.Expr{Typ: Type{Id: int32(types.T_timestamp), Scale: 6}, Expr: &pb.Expr_Lit{Lit: &pb.Literal{Value: &pb.Literal_Timestampval{Timestampval: int64(stamp)}}}}
	}
	var prefix []*pb.Expr
	if leadingPrefix {
		prefix = []*pb.Expr{MakePlan2Int64ConstExprWithType(42)}
	}
	column := GetColExpr(Type{Id: int32(types.T_datetime), Scale: scale}, 0, 0)
	var candidate *pb.Expr
	if encoded {
		candidate, _, err = makeEncodedTemporalCandidate(proc.Ctx, GetColExpr(Type{Id: int32(types.T_varchar)}, 0, 0), column, bound, prefix, true, op, scale)
	} else {
		candidate, _, err = makeDirectTemporalCandidate(proc.Ctx, column, bound, op, scale)
	}
	require.NoError(t, err)
	if allow {
		allowTemporalCandidateNarrowingForTest(candidate)
	}
	interval, err := types.BuildTemporalCandidate(proc.Ctx, types.T_datetime, scale, int64(stamp), false, op, zone)
	require.NoError(t, err)
	if allow && literal == nil && clock == 1714067100123456000 && scale == 6 && (zone == time.UTC || zone.String() == "America/New_York") {
		require.False(t, interval.Full, "the oracle must actually cover selective fixed/named ranges")
	}
	if !allow {
		interval = types.TemporalCandidate{Lo: math.MinInt64, Hi: math.MaxInt64, Sentinel: math.MinInt64, Full: true}
	}
	maximum := int64(types.DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999))
	rows := []types.Datetime{math.MinInt64, math.MinInt64 + 1, -2, -1, 0, 1, types.Datetime(maximum), types.Datetime(maximum + 1), math.MaxInt64}
	for _, v := range []int64{interval.Lo, interval.Hi} {
		rows = append(rows, types.Datetime(v))
		if v > math.MinInt64 {
			rows = append(rows, types.Datetime(v-1))
		}
		if v < math.MaxInt64 {
			rows = append(rows, types.Datetime(v+1))
		}
	}
	bat := batch.NewWithSize(1)
	defer bat.Clean(proc.Mp())
	bat.Vecs[0] = vector.NewVec(types.New(types.T_datetime, 0, scale))
	require.NoError(t, vector.AppendFixedList(bat.Vecs[0], rows, nil, proc.Mp()))
	require.NoError(t, vector.AppendFixed(bat.Vecs[0], types.Datetime(0), true, proc.Mp()))
	bat.SetRowCount(len(rows) + 1)
	input := bat
	if encoded {
		parts := append(DeepCopyExprList(prefix), column, MakePlan2Int64ConstExprWithType(7))
		serial, err := BindFuncExprImplByPlanExpr(proc.Ctx, "serial", parts)
		require.NoError(t, err)
		encoder, err := colexec.NewExpressionExecutor(proc, serial)
		require.NoError(t, err)
		defer encoder.Free()
		values, err := encoder.Eval(proc, []*batch.Batch{bat}, nil)
		require.NoError(t, err)
		input = batch.NewWithSize(1)
		input.Vecs[0] = values // encoder owns the vector
		input.SetRowCount(bat.RowCount())
	}
	exe, err := colexec.NewExpressionExecutor(proc, candidate)
	require.NoError(t, err)
	defer exe.Free()
	result, err := exe.Eval(proc, []*batch.Batch{input}, nil)
	require.NoError(t, err)
	guard := function.TemporalTimestampBoundNotEmpty(stamp, int32(op), scale)
	for i, row := range rows {
		v := int64(row)
		old := guard && (interval.Full || v >= interval.Lo && v <= interval.Hi || v == interval.Sentinel || v < -1 || v > maximum)
		selected := !result.IsNull(uint64(i)) && vector.GetFixedAtWithTypeCheck[bool](result, i)
		require.Equal(t, old, selected, "clock=%d scale=%d op=%d zone=%s encoded=%v allow=%v row=%d", clock, scale, op, zone, encoded, allow, row)
		if row >= -1 && v <= maximum {
			scalar := row.ToTimestamp(zone).TruncateToScale(scale)
			match := false
			switch op {
			case types.TemporalEqual:
				match = scalar == stamp
			case types.TemporalLess:
				match = scalar < stamp
			case types.TemporalLessEqual:
				match = scalar <= stamp
			case types.TemporalGreater:
				match = scalar > stamp
			case types.TemporalGreaterEqual:
				match = scalar >= stamp
			}
			if match {
				require.True(t, selected, "candidate must retain every original scalar match")
			}
		}
	}
	require.False(t, !result.IsNull(uint64(len(rows))) && vector.GetFixedAtWithTypeCheck[bool](result, len(rows)), "NULL is never selected")
}

func TestTemporalInteriorUnionMatchesFourArmOracle(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	named, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	zones := []*time.Location{time.UTC, time.FixedZone("offset-min", math.MinInt32), time.FixedZone("offset-max", math.MaxInt32), named}
	for _, clock := range []int64{math.MinInt64, 0, 1714067100123456000, math.MaxInt64} {
		for _, scale := range []int32{0, 3, 6} {
			for op := types.TemporalEqual; op <= types.TemporalGreaterEqual; op++ {
				for _, zone := range zones {
					for _, encoded := range []bool{false, true} {
						for _, allow := range []bool{false, true} {
							checkInteriorTemporalUnion(t, proc, clock, scale, op, zone, encoded, allow, nil, false)
						}
					}
				}
			}
		}
	}
}

func TestTemporalInteriorLiteralUnionMatchesFourArmOracle(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	maximum := int64(types.DatetimeFromClock(9999, 12, 31, 23, 59, 59, 999999))
	for _, value := range []int64{-1, 0, temporalInteriorTimestampMargin - 1, temporalInteriorTimestampMargin, temporalInteriorTimestampMargin + 1, maximum - temporalInteriorTimestampMargin - 1, maximum - temporalInteriorTimestampMargin, maximum - temporalInteriorTimestampMargin + 1, math.MaxInt64} {
		bound := types.Timestamp(value)
		for _, scale := range []int32{0, 3, 6} {
			for op := types.TemporalEqual; op <= types.TemporalGreaterEqual; op++ {
				for _, zone := range []*time.Location{time.UTC, time.FixedZone("offset-min", math.MinInt32), time.FixedZone("offset-max", math.MaxInt32)} {
					for _, allow := range []bool{false, true} {
						for mode := 0; mode < 3; mode++ {
							checkInteriorTemporalUnion(t, proc, 0, scale, op, zone, mode != 0, allow, &bound, mode == 2)
						}
					}
				}
			}
		}
	}
}
