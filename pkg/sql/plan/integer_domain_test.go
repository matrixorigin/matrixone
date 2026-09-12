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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestBoundUnsignedDomainSurvivesParentAndPlanTransport(t *testing.T) {
	proc := testutil.NewProcess(t)
	defer proc.Free()
	zero := makePlan2Int64ConstExprWithType(0)
	unsignedZero, err := appendCastBeforeExpr(context.Background(), DeepCopyExpr(zero), planpb.Type{Id: int32(types.T_uint64), Width: 64, Scale: -1})
	require.NoError(t, err)
	inner, err := BindFuncExprImplByPlanExpr(context.Background(), "+", []*Expr{unsignedZero, zero})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_uint64), inner.Typ.Id)
	outer, err := BindFuncExprImplByPlanExpr(context.Background(), "-", []*Expr{inner, makePlan2Int64ConstExprWithType(1)})
	require.NoError(t, err)
	_, err = ConstantFold(batch.EmptyForConstFoldBatch, outer, proc, true, true)
	require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)

	for _, enabled := range []bool{false, true} {
		ctx := NewMockCompilerContext(false)
		if enabled {
			ctx.SetSqlModeOverride("NO_UNSIGNED_SUBTRACTION")
		} else {
			ctx.SetSqlModeOverride("")
		}
		builder := NewQueryBuilder(planpb.Query_SELECT, ctx, false, false)
		expr, err := BindFuncExprImplByPlanExpr(builder.GetContext(), "-", []*Expr{DeepCopyExpr(unsignedZero), makePlan2Int64ConstExprWithType(1)})
		require.NoError(t, err)
		data, err := expr.Marshal()
		require.NoError(t, err)
		restored := new(planpb.Expr)
		require.NoError(t, restored.Unmarshal(data))
		proc.SetResolveVariableFunc(nil) // A remote executor has only the bound expression.
		got, err := ConstantFold(batch.EmptyForConstFoldBatch, restored, proc, true, true)
		if enabled {
			require.NoError(t, err)
			require.Equal(t, int32(types.T_int64), got.Typ.Id)
			require.Equal(t, int64(-1), got.GetLit().GetI64Val())
		} else {
			require.True(t, moerr.IsMoErrCode(err, moerr.ErrOutOfRange), "%v", err)
		}
	}
}

func TestPreparedUnsignedSubtractionRebindingKeepsBoundMode(t *testing.T) {
	ctx := function.WithNoUnsignedSubtraction(context.Background(), true)
	args := []*Expr{makePlan2Uint64ConstExprWithType(0), {Typ: planpb.Type{Id: int32(types.T_int64)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}}
	expr, err := BindFuncExprImplByPlanExpr(ctx, "-", args)
	require.NoError(t, err)
	rule := NewResetParamRefRule(ctx, []*Expr{makePlan2Int64ConstExprWithType(1)})
	bound, err := rule.ApplyExpr(expr)
	require.NoError(t, err)
	require.Equal(t, int32(types.T_int64), bound.Typ.Id)
}

func TestPreparedIntegerPeerRetainsOriginalDomain(t *testing.T) {
	ctx := context.Background()
	for _, explicit := range []bool{false, true} {
		peer := &Expr{Typ: planpb.Type{Id: int32(types.T_bit), Width: 64},
			Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: 0}}}
		if explicit {
			var err error
			peer, err = appendCastBeforeExpr(ctx, peer, planpb.Type{Id: int32(types.T_uint64), Width: 64})
			require.NoError(t, err)
			peer.GetF().SyntaxExplicitCast = true
		}
		marker := &Expr{Typ: planpb.Type{Id: int32(types.T_uint64)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
		prepared, err := BindFuncExprImplByPlanExpr(ctx, "+", []*Expr{DeepCopyExpr(peer), marker})
		require.NoError(t, err)
		direct, err := BindFuncExprImplByPlanExpr(ctx, "+", []*Expr{DeepCopyExpr(peer), makePlan2Int64ConstExprWithType(-1)})
		require.NoError(t, err)
		rule := NewResetParamRefRule(ctx, []*Expr{makePlan2Int64ConstExprWithType(-1)})
		rebound, err := rule.ApplyExpr(prepared)
		require.NoError(t, err)
		require.Equal(t, direct.Typ.Id, rebound.Typ.Id, "explicit cast: %v", explicit)
		require.Equal(t, direct.GetF().Func.Obj, rebound.GetF().Func.Obj)
	}
}
