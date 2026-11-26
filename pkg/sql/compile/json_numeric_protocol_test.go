// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compile

import (
	"context"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	plan2 "github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/stretchr/testify/require"
)

func TestJSONNumericJoinConditionRemainsResidual(t *testing.T) {
	col := func(typ types.T, rel, pos int32) *planpb.Expr {
		return &planpb.Expr{Typ: typ.ToType().PlanType(), Expr: &planpb.Expr_Col{Col: &planpb.ColRef{RelPos: rel, ColPos: pos}}}
	}
	bind := func(args ...*planpb.Expr) *planpb.Expr {
		e, err := plan2.BindFuncExprImplByPlanExpr(context.Background(), "=", args)
		require.NoError(t, err)
		return e
	}
	mixed := bind(col(types.T_json, 0, 0), col(types.T_int64, 1, 0))
	residual, keys := extraJoinConditions([]*planpb.Expr{mixed})
	require.Empty(t, keys)
	require.Equal(t, mixed, residual)
	key := bind(col(types.T_int64, 0, 1), col(types.T_int64, 1, 1))
	residual, keys = extraJoinConditions([]*planpb.Expr{mixed, key})
	require.Equal(t, []*planpb.Expr{key}, keys)
	require.Equal(t, mixed, residual)
}
