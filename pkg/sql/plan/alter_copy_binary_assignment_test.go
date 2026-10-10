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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestAlterCopyBinaryAssignmentBoundary(t *testing.T) {
	ordinary := t.Context()
	copyContext := context.WithValue(ordinary, defines.AlterCopyOpt{}, &pbplan.AlterCopyOpt{TargetTableName: "copy_target"})
	for _, oid := range []types.T{types.T_binary, types.T_varbinary} {
		t.Run(oid.String(), func(t *testing.T) {
			physicalType := types.New(oid, 4, 0)
			target := makePlan2Type(&physicalType)
			source := MakePlan2StringConstExprWithType("🧪")
			require.Equal(t, "cast", assignmentCastFunctionNameForSource(ordinary, source, target, false, nil))
			expression, err := forceAssignmentCastExprWithProcess(copyContext, source, target, false, nil)
			require.NoError(t, err)
			require.Equal(t, "cast_strict", expression.GetF().Func.ObjName)
			// Even an equal inferred type must validate the generated value.
			source.Typ = target
			expression, err = forceAssignmentCastExprWithProcess(copyContext, source, target, false, nil)
			require.NoError(t, err)
			require.NotSame(t, source, expression)
			require.Equal(t, "cast_strict", expression.GetF().Func.ObjName)
			targetExpr := &pbplan.Expr{Typ: target, Expr: &pbplan.Expr_T{T: &pbplan.TargetType{}}}
			expression, err = forceCastExpr2WithProcess(copyContext, source, makeTypeByPlan2Type(target), targetExpr, false, nil)
			require.NoError(t, err)
			require.Equal(t, "cast_strict", expression.GetF().Func.ObjName)
		})
	}
	target := Type{Id: int32(types.T_varbinary), Width: 4}
	for _, value := range []any{nil, "not a COPY option", (*pbplan.AlterCopyOpt)(nil), &pbplan.AlterCopyOpt{}} {
		ctx := ordinary
		if value != nil {
			ctx = context.WithValue(ctx, defines.AlterCopyOpt{}, value)
		}
		require.False(t, alterCopyBinaryAssignment(ctx, target))
	}
	require.False(t, alterCopyBinaryAssignment(copyContext, Type{Id: int32(types.T_varchar), Width: 4}))
}
