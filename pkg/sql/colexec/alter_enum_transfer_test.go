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

package colexec

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestAlterEnumTransferCastOwnsTargetTypedVector(t *testing.T) {
	proc := testutil.NewProcess(t)
	sourceType := types.T_enum.ToType()
	targetType := types.T_enum.ToType()
	targetType.SetEnumEncoding(types.EnumEncodingFramed)
	targetType.SetEnumCollation(types.EnumCollationGeneralCI)
	source := vector.NewVec(sourceType)
	bat := batch.NewWithSize(1)
	bat.Vecs[0] = source
	defer bat.Clean(proc.Mp())
	for _, v := range []types.Enum{0, 1, 2} {
		require.NoError(t, vector.AppendFixed(source, v, false, proc.Mp()))
	}
	require.NoError(t, vector.AppendFixed(source, types.Enum(0), true, proc.Mp()))
	bat.SetRowCount(4)
	sourcePlan := vector.TypeToProtoType(sourceType)
	targetPlan := vector.TypeToProtoType(targetType)
	overload, err := function.GetFunctionByName(proc.Ctx, "cast", []types.Type{sourceType, targetType})
	require.NoError(t, err)
	expr := &plan.Expr{Typ: targetPlan, Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{Obj: overload.GetEncodedOverloadID(), ObjName: "cast"},
		Args: []*plan.Expr{
			{Typ: sourcePlan, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 0, ColPos: 0}}},
			{Typ: targetPlan, Expr: &plan.Expr_T{T: &plan.TargetType{}}},
		},
	}}}
	exec, err := NewExpressionExecutor(proc, expr)
	require.NoError(t, err)
	defer exec.Free()
	result, err := exec.Eval(proc, []*batch.Batch{bat}, nil)
	require.NoError(t, err)
	require.NotSame(t, source, result)
	require.Equal(t, targetType, *result.GetType())
	require.Equal(t, sourceType, *source.GetType())
	require.Equal(t, []types.Enum{0, 1, 2, 0}, vector.MustFixedColWithTypeCheck[types.Enum](result))
	require.True(t, result.IsNull(3))
	require.Equal(t, []types.Enum{0, 1, 2, 0}, vector.MustFixedColWithTypeCheck[types.Enum](source))
	require.True(t, source.IsNull(3))
}
