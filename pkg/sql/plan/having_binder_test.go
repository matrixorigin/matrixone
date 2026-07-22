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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
	"github.com/stretchr/testify/require"
)

func TestRemapAggToTimeWindowResultAggUsesRegularSumForPartialSums(t *testing.T) {
	for _, typ := range []types.Type{types.T_int64.ToType(), types.T_uint64.ToType()} {
		t.Run(typ.Oid.String(), func(t *testing.T) {
			expr := &plan.Expr{
				Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale},
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{
							Obj:     function.AggSumOverloadID,
							ObjName: "sum",
						},
						Args: []*plan.Expr{{
							Typ: plan.Type{Id: int32(typ.Oid), Width: typ.Width, Scale: typ.Scale},
						}},
					},
				},
			}

			got, err := (&HavingBinder{baseBinder: baseBinder{sysCtx: context.Background()}}).remapAggToTimeWindowResultAgg(expr)
			require.NoError(t, err)
			require.Equal(t, "sum", got.Expr.(*plan.Expr_F).F.Func.ObjName)
			require.Equal(t, int32(types.T_decimal128), got.Typ.Id)
			require.Equal(t, int32(0), got.Typ.Scale)
		})
	}
}

func TestRemapAggToTimeWindowResultAggUsesRegularSumForCountCache(t *testing.T) {
	countFn, err := function.GetFunctionByName(context.Background(), "count", []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)

	expr := &plan.Expr{
		Typ: plan.Type{Id: int32(types.T_int64), Width: 64},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     countFn.GetEncodedOverloadID(),
					ObjName: "count",
				},
				Args: []*plan.Expr{{
					Typ: plan.Type{Id: int32(types.T_int64), Width: 64},
				}},
			},
		},
	}

	got, err := (&HavingBinder{baseBinder: baseBinder{sysCtx: context.Background()}}).remapAggToTimeWindowResultAgg(expr)
	require.NoError(t, err)
	require.Equal(t, "sum", got.Expr.(*plan.Expr_F).F.Func.ObjName)
	require.Equal(t, int32(types.T_decimal128), got.Typ.Id)
	require.Equal(t, int32(0), got.Typ.Scale)
}

func TestBindTimeWindowFuncCastsCountProjectionAfterDecimalCache(t *testing.T) {
	countFn, err := function.GetFunctionByName(context.Background(), "count", []types.Type{types.T_int64.ToType()})
	require.NoError(t, err)

	ctx := NewBindContext(nil, nil)
	ctx.aggregateTag = 1
	ctx.timeTag = 2
	ctx.sliding = true
	ctx.aggregates = []*plan.Expr{{
		Typ: plan.Type{Id: int32(types.T_int64), Width: 64},
		Expr: &plan.Expr_F{
			F: &plan.Function{
				Func: &plan.ObjectRef{
					Obj:     countFn.GetEncodedOverloadID(),
					ObjName: "count",
				},
				Args: []*plan.Expr{{
					Typ: plan.Type{Id: int32(types.T_int64), Width: 64},
				}},
			},
		},
	}}

	binder := &HavingBinder{
		baseBinder: baseBinder{sysCtx: context.Background(), ctx: ctx},
	}
	ast := &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName("count")),
		Exprs: tree.Exprs{tree.NewUnresolvedColName("v")},
	}

	got, err := binder.BindTimeWindowFunc("count", ast, 0, true)
	require.NoError(t, err)
	require.Len(t, ctx.times, 1)
	require.Equal(t, int32(types.T_decimal128), ctx.times[0].Typ.Id)
	require.Equal(t, int32(types.T_int64), got.Typ.Id)
	require.Equal(t, "cast", got.GetF().Func.ObjName)
	require.Equal(t, int32(types.T_decimal128), got.GetF().Args[0].Typ.Id)
	require.Equal(t, int32(ctx.timeTag), got.GetF().Args[0].GetCol().RelPos)
	require.Equal(t, int32(0), got.GetF().Args[0].GetCol().ColPos)
}
