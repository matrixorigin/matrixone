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

type divPrecisionCompilerContext struct {
	CompilerContext
	increment int64
}

func (c divPrecisionCompilerContext) ResolveVariable(name string, system, global bool) (any, error) {
	if name == "div_precision_increment" && system && !global {
		return c.increment, nil
	}
	return c.CompilerContext.ResolveVariable(name, system, global)
}

func TestQueryBuilderCarriesDivPrecisionIncrementIntoBinding(t *testing.T) {
	decimalType := types.New(types.T_decimal64, 10, 2)
	newColumn := func(position int32) *Expr {
		return &Expr{
			Typ: makePlan2Type(&decimalType),
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{
				RelPos: 0,
				ColPos: position,
			}},
		}
	}

	for _, test := range []struct {
		increment int64
		want      types.Type
	}{
		{increment: 0, want: types.New(types.T_decimal128, 12, 2)},
		{increment: 4, want: types.New(types.T_decimal128, 16, 6)},
		{increment: 10, want: types.New(types.T_decimal128, 22, 12)},
		{increment: 30, want: types.New(types.T_decimal256, 42, 30)},
	} {
		t.Run(test.want.String(), func(t *testing.T) {
			compiler := divPrecisionCompilerContext{
				CompilerContext: NewMockCompilerContext(true),
				increment:       test.increment,
			}
			builder := NewQueryBuilder(planpb.Query_SELECT, compiler, false, true)
			expr, err := BindFuncExprImplByPlanExpr(
				builder.GetContext(), "/", []*Expr{newColumn(0), newColumn(1)})
			require.NoError(t, err)
			require.Equal(t, test.want, makeTypeByPlan2Expr(expr))
		})
	}
}

func TestPreparedDivisionSpecializationUsesPrecisionIncrementContext(t *testing.T) {
	prepared, err := runOneStmt(NewMockOptimizer(false), t,
		`prepare div_precision from 'select ? / 2 as q'`)
	require.NoError(t, err)

	ctx := function.WithDivPrecisionIncrement(context.Background(), 10)
	bound, specialized, err := FillValuesOfParamsInPlanWithSpecialization(
		ctx,
		prepared.GetDcl().GetPrepare().Plan,
		[]any{ParamValue{
			Value:         "1.00",
			SourceType:    types.New(types.T_decimal64, 10, 2),
			HasSourceType: true,
		}},
	)
	require.NoError(t, err)
	require.True(t, specialized)
	division := findPlanFunctionExpr(bound, "/")
	require.NotNil(t, division)
	require.Equal(t, types.New(types.T_decimal128, 20, 12), makeTypeByPlan2Expr(division))
}
