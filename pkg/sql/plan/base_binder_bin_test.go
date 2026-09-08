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
	"github.com/stretchr/testify/require"
)

func TestBinBinaryNumericLiteralsUseUnsignedNumericPath(t *testing.T) {
	ctx := context.Background()
	for _, value := range []string{string([]byte{0xff}), string([]byte{0x01, 0x00})} {
		expr, err := BindFuncExprImplByPlanExpr(ctx, "bin", []*Expr{
			makePlan2StringConstExprWithType(value, true),
		})
		require.NoError(t, err)
		fn := expr.GetF()
		require.NotNil(t, fn)
		require.Len(t, fn.Args, 1)
		require.Equal(t, types.T_uint64, makeTypeByPlan2Expr(fn.Args[0]).Oid)
	}
}

func TestBinOrdinaryStringsKeepPrefixPath(t *testing.T) {
	expr, err := BindFuncExprImplByPlanExpr(context.Background(), "bin", []*Expr{
		makePlan2StringConstExprWithType("255"),
	})
	require.NoError(t, err)
	fn := expr.GetF()
	require.NotNil(t, fn)
	require.Len(t, fn.Args, 1)
	require.Equal(t, types.T_varchar, makeTypeByPlan2Expr(fn.Args[0]).Oid)
}
