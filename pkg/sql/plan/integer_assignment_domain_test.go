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
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestIntegerAssignmentDomainIsScoped(t *testing.T) {
	cc := NewMockOptimizer(false).CurrentContext()
	builder := NewQueryBuilder(planpb.Query_SELECT, cc, false, false)
	original := cc.GetContext()
	require.False(t, inIntegerAssignmentDomain(builder.GetContext()))
	func() {
		restore := builder.enterIntegerAssignmentDomain(true)
		defer restore()
		require.True(t, inIntegerAssignmentDomain(builder.GetContext()))
		require.False(t, inIntegerAssignmentDomain(cc.GetContext()))
		nested := builder.enterIntegerAssignmentDomain(false)
		require.True(t, inIntegerAssignmentDomain(builder.GetContext()))
		nested()
		require.True(t, inIntegerAssignmentDomain(builder.GetContext()))
		_, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "/", nil)
		require.Error(t, err)
	}()
	require.False(t, inIntegerAssignmentDomain(builder.GetContext()))
	require.Equal(t, original, cc.GetContext())
}

func TestIntegerAssignmentDivisionDoesNotChangeStandaloneBinding(t *testing.T) {
	ctx := t.Context()
	bind := func(ctxIsAssignment bool) *Expr {
		bindCtx := ctx
		if ctxIsAssignment {
			bindCtx = withIntegerAssignmentDomain(bindCtx)
		}
		expr, err := BindFuncExprImplByPlanExpr(bindCtx, "/", []*Expr{
			makePlan2Int64ConstExprWithType(5), makePlan2Int64ConstExprWithType(2),
		})
		require.NoError(t, err)
		return expr
	}
	require.Equal(t, int32(types.T_float64), bind(false).Typ.Id)
	require.Equal(t, int32(types.T_decimal256), bind(true).Typ.Id)
	require.Equal(t, int32(types.T_float64), bind(false).Typ.Id)
	divisor := makePlan2Int64ConstExprWithType(2)
	divisor.Typ.Scale = -1
	expr, err := BindFuncExprImplByPlanExpr(withIntegerAssignmentDomain(ctx), "/", []*Expr{
		makePlan2Int64ConstExprWithType(5), divisor,
	})
	require.NoError(t, err)
	require.Zero(t, expr.GetF().Args[1].Typ.Scale, "integer metadata scale must not rescale the divisor to zero")
}
