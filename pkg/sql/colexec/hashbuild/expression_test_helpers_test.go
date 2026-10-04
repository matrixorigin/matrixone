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

package hashbuild

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

func makeIssue26454ConcatKey(t testing.TB, proc *process.Process) *plan.Expr {
	t.Helper()
	cast := func(colPos int32) *plan.Expr {
		col := &plan.Expr{
			Typ:  plan.Type{Id: int32(types.T_int32)},
			Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: colPos}},
		}
		targetType := plan.Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		}
		expr, err := plan2.BindFuncExprImplByPlanExpr(
			proc.Ctx,
			"cast",
			[]*plan.Expr{
				col,
				{
					Typ:  targetType,
					Expr: &plan.Expr_T{T: &plan.TargetType{}},
				},
			},
		)
		require.NoError(t, err)
		return expr
	}
	expr, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"concat",
		[]*plan.Expr{
			cast(0),
			plan2.MakePlan2StringConstExprWithType("-"),
			cast(1),
		},
	)
	require.NoError(t, err)
	return expr
}

func makeIssue26454CaseKey(t testing.TB, proc *process.Process) *plan.Expr {
	t.Helper()
	column := &plan.Expr{
		Typ: plan.Type{
			Id:    int32(types.T_varchar),
			Width: types.MaxVarcharLen,
		},
		Expr: &plan.Expr_Col{Col: &plan.ColRef{ColPos: 0}},
	}
	condition, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"=",
		[]*plan.Expr{
			column,
			plan2.MakePlan2StringConstExprWithType("ATM_CON"),
		},
	)
	require.NoError(t, err)
	expr, err := plan2.BindFuncExprImplByPlanExpr(
		proc.Ctx,
		"case",
		[]*plan.Expr{
			condition,
			plan2.MakePlan2StringConstExprWithType("CON_CONTRACT_HEADERS"),
			plan2.MakePlan2StringConstExprWithType("CON_CONTRACT_DOC"),
		},
	)
	require.NoError(t, err)
	return expr
}
