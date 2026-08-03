// Copyright 2021 - 2026 Matrix Origin
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

package rule

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func makeConstantCastExpr(t *testing.T, name string, sourceType, targetType types.Type, value string) *plan.Expr {
	t.Helper()
	f, err := function.GetFunctionByName(context.Background(), name, []types.Type{sourceType, targetType})
	require.NoError(t, err)

	targetPlanType := plan.Type{
		Id:    int32(targetType.Oid),
		Width: targetType.Width,
		Scale: targetType.Scale,
	}
	return &plan.Expr{
		Typ: targetPlanType,
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: f.GetEncodedOverloadID(), ObjName: name},
			Args: []*plan.Expr{
				{
					Typ:  plan.Type{Id: int32(sourceType.Oid), Width: sourceType.Width, Scale: sourceType.Scale},
					Expr: &plan.Expr_Lit{Lit: &plan.Literal{Value: &plan.Literal_Sval{Sval: value}}},
				},
				{
					Typ:  targetPlanType,
					Expr: &plan.Expr_T{T: &plan.TargetType{}},
				},
			},
		}},
	}
}

func TestPreparedConstantFoldKeepsSqlModeDependentTemporalCast(t *testing.T) {
	proc := testutil.NewProcess(t)
	stringType := types.New(types.T_varchar, 32, 0)

	for _, targetType := range []types.Type{
		types.T_date.ToType(),
		types.New(types.T_datetime, 0, 6),
		types.New(types.T_timestamp, 0, 6),
	} {
		t.Run(targetType.Oid.String(), func(t *testing.T) {
			expr := makeConstantCastExpr(t, "cast", stringType, targetType, "2024-01-02 03:04:05")
			folded := NewConstantFold(true).constantFold(expr, proc)
			require.NotNil(t, folded.GetF())
		})
	}
}

func TestConstantFoldStillFoldsUnaffectedCasts(t *testing.T) {
	proc := testutil.NewProcess(t)
	stringType := types.New(types.T_varchar, 32, 0)

	nonPreparedTemporal := makeConstantCastExpr(t, "cast", stringType, types.T_date.ToType(), "2024-01-02")
	require.NotNil(t, NewConstantFold(false).constantFold(nonPreparedTemporal, proc).GetLit())

	preparedNumeric := makeConstantCastExpr(t, "cast", stringType, types.T_int64.ToType(), "42")
	require.NotNil(t, NewConstantFold(true).constantFold(preparedNumeric, proc).GetLit())

	preparedStrictTemporal := makeConstantCastExpr(t, "cast_strict", stringType, types.T_date.ToType(), "2024-01-02")
	require.NotNil(t, NewConstantFold(true).constantFold(preparedStrictTemporal, proc).GetLit())
}
