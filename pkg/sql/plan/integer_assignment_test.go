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

	moruntime "github.com/matrixorigin/matrixone/pkg/common/runtime"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestIntegerAssignmentSourceCastSelection(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	previous, exists := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	require.True(t, exists)
	defer rt.SetGlobalVariables(moruntime.MOProtocolVersion, previous)
	target := types.T_int64.ToType()
	dst := makePlan2Type(&target)
	param := &Expr{Typ: pb.Type{Id: int32(types.T_text)}, Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 0}}}
	for _, version := range []int64{defines.MORPCVersion4, defines.MORPCVersion5} {
		rt.SetGlobalVariables(moruntime.MOProtocolVersion, version)
		for _, source := range []*Expr{makePlan2Float64ConstExprWithType(2.5), param} {
			want := "cast"
			if version == defines.MORPCVersion5 {
				want = "cast_assign"
			}
			bound, err := forceAssignmentCastExprWithProcess(t.Context(), DeepCopyExpr(source), dst, false, proc)
			require.NoError(t, err)
			require.Equal(t, want, bound.GetF().Func.ObjName)
			targetExpr := &Expr{Typ: dst, Expr: &pb.Expr_T{T: &pb.TargetType{}}}
			bound2, err := forceCastExpr2WithProcess(t.Context(), DeepCopyExpr(source), target, targetExpr, false, proc)
			require.NoError(t, err)
			require.Equal(t, want, bound2.GetF().Func.ObjName)
		}
		require.Equal(t, "cast", assignmentCastFunctionNameForSource(makePlan2Int64ConstExprWithType(7), dst, false, proc))
		require.Equal(t, "cast", assignmentCastFunctionNameForSource(makePlan2StringConstExprWithType("2.5"), dst, false, proc))
	}
	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion5)
	assignment, err := forceAssignmentCastExprWithProcess(t.Context(), param, dst, false, proc)
	require.NoError(t, err)
	pos, ok := directIntegerAssignmentParam(assignment)
	require.True(t, ok)
	require.Zero(t, pos)
	_, ok = directIntegerAssignmentParam(nil)
	require.False(t, ok)
	explicit, err := forceCastExprWithName(t.Context(), DeepCopyExpr(param), dst, "cast")
	require.NoError(t, err)
	_, ok = directIntegerAssignmentParam(explicit)
	require.False(t, ok)
	wrapped, err := forceAssignmentCastExprWithProcess(t.Context(), explicit, dst, false, proc)
	require.NoError(t, err)
	_, ok = directIntegerAssignmentParam(wrapped)
	require.False(t, ok)
}

func TestMediumIntAssignmentRequiresClusterProtocolFence(t *testing.T) {
	proc := testutil.NewProcess(t)
	rt := moruntime.ServiceRuntime(proc.GetService())
	oldVersion, hadVersion := rt.GetGlobalVariables(moruntime.MOProtocolVersion)
	oldFloor, hadFloor := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor)
	t.Cleanup(func() {
		if hadVersion {
			rt.SetGlobalVariables(moruntime.MOProtocolVersion, oldVersion)
		}
		if hadFloor {
			rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, oldFloor)
		} else if current, ok := rt.GetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor); ok {
			rt.CompareAndDeleteGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, current)
		}
	})

	var mediumType types.Type = types.New(types.T_int32, 24, -1)
	mediumPlanType := makePlan2Type(&mediumType)
	source := MakePlan2Int32ConstExprWithType(1)
	targetExpr := &Expr{Typ: mediumPlanType, Expr: &pb.Expr_T{T: &pb.TargetType{}}}
	preparedMedium := &Expr{
		Typ:  mediumPlanType,
		Expr: &pb.Expr_P{P: &pb.ParamRef{Pos: 0}},
	}
	builder := NewQueryBuilder(pb.Query_SELECT, NewMockCompilerContext(true, proc), false, true)

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion109)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion110)
	_, err := forceAssignmentCastExprWithProcess(t.Context(), source, mediumPlanType, false, proc)
	require.ErrorContains(t, err, "protocol version 110")
	_, err = forceCastExpr2WithProcess(t.Context(), source, mediumType, targetExpr, false, proc)
	require.ErrorContains(t, err, "protocol version 110")
	_, err = builder.forceProjectedAssignmentCastExpr(preparedMedium, preparedMedium, mediumPlanType, false)
	require.ErrorContains(t, err, "protocol version 110")

	rt.SetGlobalVariables(moruntime.MOProtocolVersion, defines.MORPCVersion110)
	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion109)
	_, err = forceAssignmentCastExprWithProcess(t.Context(), source, mediumPlanType, false, proc)
	require.ErrorContains(t, err, "authoring floor=109")
	_, err = forceCastExpr2WithProcess(t.Context(), source, mediumType, targetExpr, false, proc)
	require.ErrorContains(t, err, "authoring floor=109")
	_, err = builder.forceProjectedAssignmentCastExpr(preparedMedium, preparedMedium, mediumPlanType, false)
	require.ErrorContains(t, err, "authoring floor=109")

	rt.SetGlobalVariables(moruntime.PersistedExpressionProtocolAuthoringFloor, defines.MORPCVersion110)
	_, err = forceAssignmentCastExprWithProcess(t.Context(), source, mediumPlanType, false, proc)
	require.NoError(t, err)
	_, err = forceCastExpr2WithProcess(t.Context(), source, mediumType, targetExpr, false, proc)
	require.NoError(t, err)
	preparedCast, err := builder.forceProjectedAssignmentCastExpr(preparedMedium, preparedMedium, mediumPlanType, false)
	require.NoError(t, err)
	require.Equal(t, "cast_assign", preparedCast.GetF().GetFunc().GetObjName())

	ignoreCast, err := forceAssignmentCastExprWithProcess(t.Context(), preparedMedium, mediumPlanType, true, proc)
	require.NoError(t, err)
	strictCast, err := forceAssignmentCastExprWithName(t.Context(), source, mediumPlanType, "cast_strict")
	require.NoError(t, err)
	for _, tc := range []struct {
		name string
		id   int32
		expr *Expr
	}{
		{name: "cast_strict", id: function.CAST_STRICT, expr: strictCast},
		{name: "cast_assign", id: function.CAST_ASSIGN, expr: preparedCast},
		{name: "cast_ignore", id: function.CAST_IGNORE, expr: ignoreCast},
	} {
		resolved := tc.expr
		require.Equal(t, tc.id, int32(resolved.GetF().GetFunc().GetObj()>>32))
		features, err := pb.RequiredRemoteExpressionFeatures(resolved)
		require.NoError(t, err)
		require.True(t, features.MediumIntAssignmentBounds, tc.name)
		version, err := RequiredPersistedExpressionProtocolVersion(resolved)
		require.NoError(t, err)
		require.Equal(t, int64(defines.MORPCVersion110), version, tc.name)

		// Set-operation alignment may shallow-widen the result metadata while
		// preserving the assignment CAST and its MEDIUMINT target argument.
		widenedResult := DeepCopyExpr(resolved)
		widenedResult.Typ.Width = 32
		features, err = pb.RequiredRemoteExpressionFeatures(widenedResult)
		require.NoError(t, err)
		require.True(t, features.MediumIntAssignmentBounds, tc.name+" after result widening")
		version, err = RequiredPersistedExpressionProtocolVersion(widenedResult)
		require.NoError(t, err)
		require.Equal(t, int64(defines.MORPCVersion110), version, tc.name+" after result widening")
	}

	ordinaryIntType := types.T_int32.ToType()
	ordinaryInt := makePlan2Type(&ordinaryIntType)
	ordinaryCast, err := forceAssignmentCastExprWithName(
		t.Context(), MakePlan2Int64ConstExprWithType(1), ordinaryInt, "cast_assign")
	require.NoError(t, err)
	features, err := pb.RequiredRemoteExpressionFeatures(ordinaryCast)
	require.NoError(t, err)
	require.False(t, features.MediumIntAssignmentBounds, "ordinary INT assignments keep the existing protocol floor")

	genericCast, err := forceCastExprWithName(t.Context(), source, mediumPlanType, "cast")
	require.NoError(t, err)
	features, err = pb.RequiredRemoteExpressionFeatures(genericCast)
	require.NoError(t, err)
	require.False(t, features.MediumIntAssignmentBounds, "generic CAST does not enforce assignment bounds")
}
