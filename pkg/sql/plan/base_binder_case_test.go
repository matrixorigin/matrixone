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
	"github.com/stretchr/testify/require"
)

func TestBindFuncExprImplByPlanExpr_CaseDifferentDecimalScale(t *testing.T) {
	ctx := context.Background()

	condExpr := makePlan2BoolConstExprWithType(false)
	thenExpr := makeDecimal128ConstExpr("-58140.00", 23, 2)
	elseExpr := makeDecimal128ConstExpr("-408180.5580000", 38, 7)

	result, err := BindFuncExprImplByPlanExpr(ctx, "case", []*planpb.Expr{condExpr, thenExpr, elseExpr})
	require.NoError(t, err)
	require.NotNil(t, result)
	require.Equal(t, int32(types.T_decimal128), result.Typ.Id)
	require.Equal(t, int32(38), result.Typ.Width)
	require.Equal(t, int32(7), result.Typ.Scale)

	funcExpr := result.GetF()
	require.NotNil(t, funcExpr)
	require.Len(t, funcExpr.Args, 3)

	arg1 := funcExpr.Args[1]
	require.True(t, isCastExpr(arg1), "THEN value should be cast when CASE decimal branch scales differ")
	require.Equal(t, int32(types.T_decimal128), arg1.Typ.Id)
	require.Equal(t, int32(38), arg1.Typ.Width)
	require.Equal(t, int32(7), arg1.Typ.Scale)
	require.False(t, isCastExpr(funcExpr.Args[2]), "ELSE value already has the common decimal scale")
}
