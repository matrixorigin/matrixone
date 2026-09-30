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
