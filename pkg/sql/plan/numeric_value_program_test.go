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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestNumericValueProgramOutputsAndPassengerCast(t *testing.T) {
	ctx := context.Background()
	source := &planpb.Expr{
		Typ:  types.T_varchar.ToType().PlanType(),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 3}},
	}
	value, kind, err := makeNumericValueProgramOutputs(ctx, source)
	require.NoError(t, err)
	require.Equal(t, int32(function.SCALAR_PROGRAM_REF), numericProgramFunctionID(value))
	require.Equal(t, int32(function.SCALAR_PROGRAM_REF), numericProgramFunctionID(kind))
	definition := value.GetF().Args[0]
	require.Equal(t, int32(function.SCALAR_PROGRAM), numericProgramFunctionID(definition))
	require.Equal(t, source, definition.GetF().Args[1])
	valueScope := definition.GetF().Args[0].GetLit().GetI64Val()
	kindArgs := kind.GetF().Args
	require.Len(t, kindArgs, 2)
	kindScope := kindArgs[0].GetLit().GetI64Val()
	require.Equal(t, valueScope, kindScope)
	require.Equal(t, int64(0), scalarProgramSlotForTest(value))
	require.Equal(t, int64(1), scalarProgramSlotForTest(kind))

	rawColumn := &planpb.Expr{
		Typ:  types.T_varchar.ToType().PlanType(),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}},
	}
	kindColumn := &planpb.Expr{
		Typ:  types.T_bool.ToType().PlanType(),
		Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 1}},
	}
	cast, err := appendNumericValueCastWithKind(ctx, rawColumn, kindColumn,
		makeSimplePlan2Type(types.T_int64), function.NumericValueCastPolicyComparison)
	require.NoError(t, err)
	args := cast.GetF().Args
	require.Len(t, args, 4)
	require.Equal(t, rawColumn, args[0], "the consumer must read the producer value column directly")
	require.Equal(t, kindColumn, args[2], "the consumer must read the ordinary BOOL passenger directly")
	require.Nil(t, args[0].GetF(), "the consumer must not add a second producer program around a column")

	analysisSource, policy, ok := numericValueCastAnalysisSource(cast)
	require.True(t, ok, "CAST9 over ordinary producer columns is a valid source view")
	require.Equal(t, function.NumericValueCastPolicyComparison, policy)
	require.Equal(t, rawColumn, analysisSource, "analysis keeps the value ColRef, not its BOOL passenger")
}

func numericProgramFunctionID(expr *planpb.Expr) int32 {
	if expr == nil || expr.GetF() == nil || expr.GetF().Func == nil {
		return 0
	}
	id, _ := function.DecodeOverloadID(expr.GetF().Func.GetObj())
	return id
}

func scalarProgramSlotForTest(expr *planpb.Expr) int64 {
	args := expr.GetF().Args
	return args[len(args)-1].GetLit().GetI64Val()
}
