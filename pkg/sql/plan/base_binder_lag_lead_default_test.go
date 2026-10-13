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

// LAG/LEAD must reconcile the value argument (1st arg) and the default value
// (3rd arg) into one common result type. Narrowing the default to the value
// type truncates longer string defaults and rounds decimal ones (#29974).
func TestBindLagLeadDefaultCommonType(t *testing.T) {
	t.Run("long string default widens varchar result", func(t *testing.T) {
		for _, name := range []string{"lag", "lead"} {
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 12}},
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err, name)
			require.Equal(t, int32(types.T_varchar), expr.Typ.Id, name)
			require.Equal(t, int32(12), expr.Typ.Width, name)
			fn := expr.GetF()
			require.NotNil(t, fn, name)
			require.Equal(t, int32(types.T_varchar), fn.Args[0].Typ.Id, name)
			require.Equal(t, int32(12), fn.Args[0].Typ.Width, name)
			require.Equal(t, int32(types.T_varchar), fn.Args[2].Typ.Id, name)
			require.Equal(t, int32(12), fn.Args[2].Typ.Width, name)
		}
	})

	t.Run("decimal default keeps scale over integer input", func(t *testing.T) {
		for _, name := range []string{"lag", "lead"} {
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_int32)}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Typ: planpb.Type{Id: int32(types.T_decimal64), Width: 2, Scale: 1}},
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err, name)
			require.Equal(t, int32(types.T_decimal64), expr.Typ.Id, name)
			require.Equal(t, int32(1), expr.Typ.Scale, name)
			fn := expr.GetF()
			require.NotNil(t, fn, name)
			require.Equal(t, int32(types.T_decimal64), fn.Args[0].Typ.Id, name)
			require.Equal(t, int32(1), fn.Args[0].Typ.Scale, name)
		}
	})

	t.Run("decimal value keeps declared precision with integer default", func(t *testing.T) {
		for _, name := range []string{"lag", "lead"} {
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_decimal128), Width: 10, Scale: 4}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}},
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err, name)
			// MySQL keeps the decimal branch of the pair (decimal(10,4)).
			require.Equal(t, int32(types.T_decimal128), expr.Typ.Id, name)
			require.Equal(t, int32(10), expr.Typ.Width, name)
			require.Equal(t, int32(4), expr.Typ.Scale, name)
			fn := expr.GetF()
			require.NotNil(t, fn, name)
			require.Equal(t, int32(types.T_decimal128), fn.Args[2].Typ.Id, name)
			require.Equal(t, int32(4), fn.Args[2].Typ.Scale, name)
		}
	})

	t.Run("domainless null default keeps value type", func(t *testing.T) {
		args := []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}},
			{Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Typ: planpb.Type{Id: int32(types.T_any)}},
		}
		expr, err := BindFuncExprImplByPlanExpr(context.Background(), "lag", args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_varchar), expr.Typ.Id)
		require.Equal(t, int32(10), expr.Typ.Width)
	})

	t.Run("shorter default keeps value type", func(t *testing.T) {
		args := []*planpb.Expr{
			{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}},
			{Typ: planpb.Type{Id: int32(types.T_int64)}},
			{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 3}},
		}
		expr, err := BindFuncExprImplByPlanExpr(context.Background(), "lag", args)
		require.NoError(t, err)
		require.Equal(t, int32(types.T_varchar), expr.Typ.Id)
		require.Equal(t, int32(10), expr.Typ.Width)
	})

	t.Run("two arg form keeps value type", func(t *testing.T) {
		for _, name := range []string{"lag", "lead"} {
			args := []*planpb.Expr{
				{Typ: planpb.Type{Id: int32(types.T_varchar), Width: 10}},
				{Typ: planpb.Type{Id: int32(types.T_int64)}},
			}
			expr, err := BindFuncExprImplByPlanExpr(context.Background(), name, args)
			require.NoError(t, err, name)
			require.Equal(t, int32(types.T_varchar), expr.Typ.Id, name)
			require.Equal(t, int32(10), expr.Typ.Width, name)
		}
	})
}
