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
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func TestBindVectorMatmulRequiresConstantConfig(t *testing.T) {
	ctx := context.Background()
	col := func(pos int32, typ planpb.Type) *planpb.Expr {
		return &planpb.Expr{Typ: typ, Expr: &planpb.Expr_Col{Col: &planpb.ColRef{ColPos: pos}}}
	}
	id := col(0, planpb.Type{Id: int32(types.T_int64)})
	vec := col(1, planpb.Type{Id: int32(types.T_array_float8), Width: 4})
	text := col(2, planpb.Type{Id: int32(types.T_varchar), Width: 100})
	params := makePlan2StringConstExprWithType(`{"limit":3}`)
	queries := makePlan2StringConstExprWithType(`[[1,0,0,0]]`)

	expr, err := BindFuncExprImplByPlanExpr(ctx, NameVectorMatmul, []*planpb.Expr{params, id, vec, queries})
	require.NoError(t, err)
	require.Equal(t, int32(types.T_json), expr.Typ.Id)

	param := &planpb.Expr{Typ: planpb.Type{Id: int32(types.T_varchar)}, Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 1}}}
	_, err = BindFuncExprImplByPlanExpr(ctx, NameVectorMatmul, []*planpb.Expr{params, id, vec, param})
	require.NoError(t, err)

	for _, args := range [][]*planpb.Expr{
		{text, id, vec, queries},
		{params, id, vec, text},
		{makePlan2NullConstExprWithType(), id, vec, queries},
		{params, id, vec, nil},
	} {
		_, err = BindFuncExprImplByPlanExpr(ctx, NameVectorMatmul, args)
		require.ErrorContains(t, err, "must be non-null constants or parameters")
	}
	_, err = BindFuncExprImplByPlanExpr(ctx, NameVectorMatmul, []*planpb.Expr{params, id, vec})
	require.ErrorContains(t, err, "requires 4 arguments")

	// the vector argument must be vecf8/vecf4
	f32 := col(1, planpb.Type{Id: int32(types.T_array_float32), Width: 4})
	_, err = BindFuncExprImplByPlanExpr(ctx, NameVectorMatmul, []*planpb.Expr{params, id, f32, queries})
	require.Error(t, err)
}
