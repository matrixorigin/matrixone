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
	require.NoError(t, vector.AppendFixedList(values, []types.Timestamp{bound, bound, bound, bound + 1}, []bool{false, false, true, false}, proc.Mp()))
	control := func(typ types.Type, value any) *vector.Vector {
		t.Helper()
		var v *vector.Vector
		var err error
		switch x := value.(type) {
		case int32:
			v, err = vector.NewConstFixed(typ, x, 4, proc.Mp())
		case bool:
			v, err = vector.NewConstFixed(typ, x, 4, proc.Mp())
		default:
			t.Fatal("invalid test control")
		}
		require.NoError(t, err)
		t.Cleanup(func() { v.Free(proc.Mp()) })
		return v
	}
	op := control(types.T_int32.ToType(), int32(types.TemporalEqual))
	scale := control(types.T_int32.ToType(), int32(3))
	selector := control(types.T_int32.ToType(), int32(0))
	permission := control(types.T_bool.ToType(), true)
	out := vector.NewFunctionResultWrapper(types.T_datetime.ToTypeWithScale(6), proc.Mp())
	t.Cleanup(out.Free)
	require.NoError(t, out.PreExtendAndReset(4))
	require.NoError(t, temporalDatetimeBound([]*vector.Vector{values, op, scale, selector, permission}, out, proc, 4, nil))
	got := out.GetResultVector()
	require.Equal(t, types.Datetime(bound)-500, vector.GetFixedAtWithTypeCheck[types.Datetime](got, 0))
	require.False(t, got.IsNull(2))
	guard := vector.NewFunctionResultWrapper(types.T_bool.ToType(), proc.Mp())
	t.Cleanup(guard.Free)
	require.NoError(t, guard.PreExtendAndReset(4))
	require.NoError(t, temporalTimestampBoundNotEmpty([]*vector.Vector{values, op, scale}, guard, proc, 4, nil))
	require.Equal(t, []bool{true, true, false, false}, vector.MustFixedColWithTypeCheck[bool](guard.GetResultVector()))
	// NULL/unknown controls historically keep the non-NULL bound guard open.
	// The shared scalar predicate must not turn them into an equality test.
	nullControl := vector.NewConstNull(types.T_int32.ToType(), 4, proc.Mp())
	t.Cleanup(func() { nullControl.Free(proc.Mp()) })
	for _, controls := range [][2]*vector.Vector{{nullControl, scale}, {op, nullControl}, {control(types.T_int32.ToType(), int32(256)), scale}} {
		require.NoError(t, guard.PreExtendAndReset(4))
		require.NoError(t, temporalTimestampBoundNotEmpty([]*vector.Vector{values, controls[0], controls[1]}, guard, proc, 4, nil))
		require.Equal(t, []bool{true, true, false, true}, vector.MustFixedColWithTypeCheck[bool](guard.GetResultVector()))
	}

	noPermission := control(types.T_bool.ToType(), false)
	require.NoError(t, out.PreExtendAndReset(4))
	require.NoError(t, temporalDatetimeBound([]*vector.Vector{values, op, scale, selector, noPermission}, out, proc, 4, nil))
	require.Equal(t, types.Datetime(math.MinInt64), vector.GetFixedAtWithTypeCheck[types.Datetime](out.GetResultVector(), 0))
}

func TestTemporalBoundIsRuntimeConstantOnly(t *testing.T) {
	intLiteral := func(value int32) *plan.Expr {
		return &plan.Expr{Typ: plan.Type{Id: int32(types.T_int32)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_I32Val{I32Val: value}}}}
	}
	bound := &plan.Expr{Typ: plan.Type{Id: int32(types.T_timestamp), Scale: 3}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Timestampval{Timestampval: 1000}}}}
	permission := &plan.Expr{Typ: plan.Type{Id: int32(types.T_bool)}, Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Bval{Bval: true}}}}
	helper := &plan.Expr{Typ: plan.Type{Id: int32(types.T_datetime)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: EncodeOverloadID(TEMPORAL_DATETIME_BOUND, 0), ObjName: "__mo_temporal_datetime_bound"},
		Args: []*plan.Expr{bound, intLiteral(0), intLiteral(3), intLiteral(0), permission},
	}}}
	require.True(t, ContainsTemporalBound(helper))
	require.False(t, IsConstant(helper, true))
	require.False(t, IsFoldCandidate(helper, true))
	require.True(t, IsRuntimeConstant(helper))
	require.Equal(t, ZoneMapConstant, GetZoneMapEvaluation(helper.GetF()))
	parent := &plan.Expr{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: EncodeOverloadID(SERIAL, 0), ObjName: "serial"}, Args: []*plan.Expr{helper},
	}}}
	require.True(t, ContainsTemporalBound(parent))
	require.False(t, IsFoldCandidate(parent, true))
}
