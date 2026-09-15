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

package plan

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/rule"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
)

func TestExactDivisionByZeroRemainsRuntimeChecked(t *testing.T) {
	proc := testutil.NewProcess(t)
	ctx := withIntegerAssignmentDomain(t.Context())
	divide := func(left, right *planpb.Expr) *planpb.Expr {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "/", []*planpb.Expr{left, right})
		require.NoError(t, err)
		return expr
	}
	floor := func(expr *planpb.Expr) *planpb.Expr {
		bound, err := BindFuncExprImplByPlanExpr(ctx, "floor", []*planpb.Expr{expr})
		require.NoError(t, err)
		return bound
	}

	zeroDivisor := divide(makePlan2Int64ConstExprWithType(0), makePlan2Int64ConstExprWithType(2))
	floorDivisor := floor(divide(makePlan2Int64ConstExprWithType(1), makePlan2Int64ConstExprWithType(2)))
	for _, tc := range []struct {
		name    string
		divisor *planpb.Expr
	}{
		{"literal", makePlan2Int64ConstExprWithType(0)},
		{"nested", zeroDivisor},
		{"floor", floorDivisor},
	} {
		t.Run(tc.name, func(t *testing.T) {
			divisor := tc.divisor
			division := divide(makePlan2Int64ConstExprWithType(10), DeepCopyExpr(divisor))
			require.True(t, rule.ShouldDeferDivisionConstantFold(
				division.GetF(), batch.EmptyForConstFoldBatch, proc, true))

			folded, err := ConstantFold(batch.EmptyForConstFoldBatch, DeepCopyExpr(division), proc, true, true)
			require.NoError(t, err)
			require.NotNil(t, folded.GetF(), "strict DML must retain division for runtime sql_mode handling")

			node := &planpb.Node{ProjectList: []*planpb.Expr{DeepCopyExpr(division)}}
			rule.NewConstantFold(true).Apply(node, nil, proc)
			require.NotNil(t, node.ProjectList[0].GetF())
		})
	}

	nonzero := divide(makePlan2Int64ConstExprWithType(2), makePlan2Int64ConstExprWithType(2))
	division := divide(makePlan2Int64ConstExprWithType(10), nonzero)
	require.False(t, rule.ShouldDeferDivisionConstantFold(
		division.GetF(), batch.EmptyForConstFoldBatch, proc, true))
}

func TestExactNumericSourceSurvivesFoldCopyAndWire(t *testing.T) {
	proc := testutil.NewProcess(t)
	for _, optimizerFold := range []bool{false, true} {
		division, err := BindFuncExprImplByPlanExpr(withIntegerAssignmentDomain(t.Context()), "/", []*planpb.Expr{
			makePlan2Int64ConstExprWithType(9007199254740993),
			makePlan2Int64ConstExprWithType(2),
		})
		require.NoError(t, err)
		wrapped, err := BindFuncExprImplByPlanExpr(t.Context(), "coalesce", []*planpb.Expr{
			division, makePlan2Int64ConstExprWithType(0),
		})
		require.NoError(t, err)

		var folded *planpb.Expr
		if optimizerFold {
			node := &planpb.Node{ProjectList: []*planpb.Expr{wrapped}}
			rule.NewConstantFold(false).Apply(node, nil, proc)
			folded = node.ProjectList[0]
		} else {
			folded, err = ConstantFold(batch.EmptyForConstFoldBatch, wrapped, proc, false, true)
			require.NoError(t, err)
		}
		// DECIMAL256 has no wire literal oneof; folding may retain the exact
		// expression. Verify the executed value after the wire round trip below.
		// Exactness now belongs to the execution type before folding, rather
		// than a source tree attached to an already rounded FLOAT literal.
		require.Equal(t, int32(types.T_decimal256), folded.Typ.Id)
		require.True(t, function.IsExactNumericExpression(folded, nil))

		wire, err := proto.Marshal(DeepCopyExpr(folded))
		require.NoError(t, err)
		var decoded planpb.Expr
		require.NoError(t, proto.Unmarshal(wire, &decoded))
		require.True(t, function.IsExactNumericExpression(&decoded, nil))

		target := types.T_int64.ToType()
		assignment, err := forceAssignmentCastExpr(t.Context(), &decoded, makePlan2Type(&target))
		require.NoError(t, err)
		require.Equal(t, "cast_assign", assignment.GetF().Func.GetObjName())
		require.Equal(t, "cast", assignment.GetF().Args[0].GetF().Func.GetObjName())
		require.Equal(t, "round", assignment.GetF().Args[0].GetF().Args[0].GetF().Func.GetObjName())
		require.Equal(t, int32(types.T_decimal256), assignment.GetF().Args[0].Typ.Id)
		require.Zero(t, assignment.GetF().Args[0].Typ.Scale)
		func() {
			executor, err := colexec.NewExpressionExecutor(proc, assignment)
			require.NoError(t, err)
			defer executor.Free()
			result, err := executor.Eval(proc, []*batch.Batch{batch.EmptyForConstFoldBatch}, nil)
			require.NoError(t, err)
			require.Equal(t, int64(4503599627370497), vector.GetFixedAtWithTypeCheck[int64](result, 0))
		}()
	}
}
