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

package function

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestTemporalBoundRuntimeValues(t *testing.T) {
	proc := fixedTruncProcess(t)
	proc.GetSessionInfo().TimeZone = time.UTC
	bound := types.Timestamp(types.DatetimeFromClock(2026, 6, 1, 12, 0, 0, 123000))
	values := vector.NewVec(types.T_timestamp.ToTypeWithScale(3))
	t.Cleanup(func() { values.Free(proc.Mp()) })
	require.NoError(t, vector.AppendFixedList(values, []types.Timestamp{bound, bound, bound + 1}, []bool{false, true, false}, proc.Mp()))
	control := func(value int32) *vector.Vector {
		t.Helper()
		v, err := vector.NewConstFixed(types.T_int32.ToType(), value, 3, proc.Mp())
		require.NoError(t, err)
		t.Cleanup(func() { v.Free(proc.Mp()) })
		return v
	}
	op, scale := control(int32(types.ComparisonEqual)), control(3)
	out := vector.NewFunctionResultWrapper(types.T_datetime.ToTypeWithScale(6), proc.Mp())
	t.Cleanup(out.Free)
	eval := func(op, scale, selector *vector.Vector) []types.Datetime {
		t.Helper()
		require.NoError(t, out.PreExtendAndReset(3))
		require.NoError(t, temporalDatetimeBound([]*vector.Vector{values, op, scale, selector}, out, proc, 3, nil))
		return vector.MustFixedColWithTypeCheck[types.Datetime](out.GetResultVector())
	}
	lo := eval(op, scale, control(0))
	require.Equal(t, types.Datetime(bound)-500, lo[0])
	require.Equal(t, types.Datetime(math.MinInt64), lo[1], "NULL must widen")
	require.Equal(t, types.Datetime(math.MinInt64), lo[2], "empty interval must widen")
	hi := eval(op, scale, control(1))
	require.Equal(t, types.Datetime(math.MaxInt64), hi[1])
	require.Equal(t, types.Datetime(math.MaxInt64), hi[2])
	nullControl := vector.NewConstNull(types.T_int32.ToType(), 3, proc.Mp())
	t.Cleanup(func() { nullControl.Free(proc.Mp()) })
	for _, controls := range [][3]*vector.Vector{{nullControl, scale, control(0)}, {op, nullControl, control(0)}, {op, scale, nullControl}, {control(256), scale, control(0)}, {op, scale, control(3)}} {
		got := eval(controls[0], controls[1], controls[2])
		require.Equal(t, types.Datetime(math.MinInt64), got[0])
	}
	zone, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	proc.GetSessionInfo().TimeZone = zone
	require.Equal(t, types.Datetime(math.MinInt64), eval(op, scale, control(0))[0], "named zones must widen")
	selector := control(0)
	ctx, cancel := context.WithCancel(proc.Ctx)
	proc.Ctx = ctx
	cancel()
	require.NoError(t, out.PreExtendAndReset(3))
	require.ErrorIs(t, temporalDatetimeBound([]*vector.Vector{values, op, scale, selector}, out, proc, 3, nil), context.Canceled)
}

func TestTemporalBoundIsRuntimeConstantOnly(t *testing.T) {
	intLiteral := func(value int32) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: value}}}}
	}
	bound := &plan.Expr{Typ: plan.Type{Id: int32(types.T_timestamp), Scale: 3}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Timestampval{Timestampval: 1000}}}}
	helper := &plan.Expr{Typ: plan.Type{Id: int32(types.T_datetime)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: EncodeOverloadID(TEMPORAL_DATETIME_BOUND, 0), ObjName: "__mo_temporal_datetime_bound"},
		Args: []*plan.Expr{bound, intLiteral(0), intLiteral(3), intLiteral(0)},
	}}}
	require.True(t, ContainsRuntimeBound(helper))
	require.False(t, IsConstant(helper, true))
	require.False(t, IsFoldCandidate(helper, true))
	require.True(t, IsRuntimeConstant(helper))
	require.Equal(t, ZoneMapConstant, GetZoneMapEvaluation(helper.GetF()))
	parent := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: EncodeOverloadID(SERIAL, 0), ObjName: "serial"}, Args: []*plan.Expr{helper},
	}}}
	require.True(t, ContainsRuntimeBound(parent))
	require.False(t, IsFoldCandidate(parent, true))
}

// A runtime evaluation boundary is independent of value repeatability and of
// ordinary statement-clock folding. Unknown families do not gain that policy.
func TestRuntimeEvaluationBoundariesKeepExistingFamilies(t *testing.T) {
	call := func(id, overload int32) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{Func: &plan.ObjectRef{Obj: EncodeOverloadID(id, overload)}}}}
	}
	// Freeze the admitted families across the complete registry. Another
	// annotation must not silently change existing expression-folding policy.
	for id := int32(0); id < FUNCTION_END_NUMBER; id++ {
		want := id == TEMPORAL_DATETIME_BOUND || id == TEMPORAL_TIMESTAMP_BOUND
		require.Equal(t, want, RequiresRuntimeEvaluation(call(id, 0)), "family %d", id)
	}
	require.False(t, RequiresRuntimeEvaluation(nil))
	require.False(t, ContainsRuntimeBound(nil))
	for _, id := range []int32{TEMPORAL_DATETIME_BOUND, TEMPORAL_TIMESTAMP_BOUND} {
		require.True(t, RequiresRuntimeEvaluation(call(id, 0)))
		malformed := call(id, math.MaxInt32)
		require.True(t, RequiresRuntimeEvaluation(malformed), "folding must preserve the function-family boundary")
		require.False(t, IsRuntimeConstant(malformed), "a boundary is not permission to execute an unknown overload")
		list := &plan.Expr{Expr: &plan.Expr_List{List: &plan.ExprList{List: []*plan.Expr{call(id, 0)}}}}
		require.False(t, RequiresRuntimeEvaluation(list))
		require.True(t, ContainsRuntimeBound(list))
	}
	for _, id := range []int32{-1, FUNCTION_END_NUMBER, math.MaxInt32, EWM_MEAN, CURRENT_TIMESTAMP} {
		require.False(t, RequiresRuntimeEvaluation(call(id, 0)))
		require.False(t, ContainsRuntimeBound(call(id, 0)))
	}
	now := call(CURRENT_TIMESTAMP, 0)
	require.True(t, IsConstant(now, true), "ordinary clock folding retains its current-execution policy")
	require.True(t, IsRuntimeConstant(now))
}
