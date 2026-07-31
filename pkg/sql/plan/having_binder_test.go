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

func TestGroupConcatOrderKeyUsesEnumAndSetStorageValue(t *testing.T) {
	for _, name := range []string{moEnumCastIndexToValueFun, moSetCastIndexToValueFun} {
		t.Run(name, func(t *testing.T) {
			raw := &plan.Expr{
				Typ:  plan.Type{Id: int32(types.T_uint16)},
				Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: 1, ColPos: 2}},
			}
			display := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_varchar)},
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{ObjName: name},
					Args: []*plan.Expr{{}, raw},
				}},
			}

			require.Same(t, raw, groupConcatOrderKey(display))
		})
	}
}

func TestRemapAggToTimeWindowResultAggUsesRegularSumForCountCache(t *testing.T) {
	for _, name := range []string{"count", "starcount"} {
		t.Run(name, func(t *testing.T) {
			countFn, err := function.GetFunctionByName(context.Background(), name, []types.Type{types.T_int64.ToType()})
			require.NoError(t, err)

			expr := &plan.Expr{
				Typ: plan.Type{Id: int32(types.T_int64), Width: 64},
				Expr: &plan.Expr_F{
					F: &plan.Function{
						Func: &plan.ObjectRef{
							Obj:     countFn.GetEncodedOverloadID(),
							ObjName: name,
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
		})
	}
}

func TestRemapMaxByToTimeWindowIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   int32
	}{
		{name: "max_by", id: function.MAX_BY},
		{name: "max_by_non_null", id: function.MAX_BY_NON_NULL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			valueType := types.New(types.T_varchar, 42, 0)
			expr := &plan.Expr{
				Typ: makePlan2Type(&valueType),
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(tc.id, 0), ObjName: tc.name},
					Args: []*plan.Expr{{Typ: makePlan2Type(&valueType)}},
				}},
			}

			got, err := (&HavingBinder{baseBinder: baseBinder{sysCtx: context.Background()}}).remapAggToTimeWindowResultAgg(expr)
			require.NoError(t, err)
			require.Equal(t, "any_value", got.GetF().Func.ObjName)
			require.Equal(t, makePlan2Type(&valueType), got.Typ)
		})
	}
}

func TestSlidingTimeWindowRejectsMaxByWithoutMergeableCache(t *testing.T) {
	valueType := types.T_varchar.ToType()
	expr := &plan.Expr{
		Typ: makePlan2Type(&valueType),
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.MAX_BY, 0), ObjName: "max_by"},
		}},
	}

	ctx := NewBindContext(nil, nil)
	ctx.explicitSliding = true
	_, err := (&HavingBinder{baseBinder: baseBinder{sysCtx: context.Background(), ctx: ctx}}).remapAggToTimeWindowCacheAgg(expr)
	require.ErrorContains(t, err, "sliding time window")
}

func TestGapFillTimeWindowKeepsMaxByChildAggregate(t *testing.T) {
	valueType := types.T_varchar.ToType()
	expr := &plan.Expr{
		Typ: makePlan2Type(&valueType),
		Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{Obj: function.EncodeOverloadID(function.MAX_BY, 0), ObjName: "max_by"},
		}},
	}
	ctx := NewBindContext(nil, nil)
	ctx.sliding = true

	got, err := (&HavingBinder{baseBinder: baseBinder{sysCtx: context.Background(), ctx: ctx}}).remapAggToTimeWindowCacheAgg(expr)
	require.NoError(t, err)
	require.Equal(t, "max_by", got.GetF().Func.ObjName)
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
				AggConfig:     []byte{1, 2, 3},
				AggConfigType: plan.AggregateConfigType_AGG_CONFIG_GROUP_CONCAT_ORDER,
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
	require.Nil(t, ctx.times[0].GetF().AggConfig)
	require.Equal(
		t,
		plan.AggregateConfigType_AGG_CONFIG_NONE,
		ctx.times[0].GetF().AggConfigType,
	)
	require.Equal(t, int32(types.T_int64), got.Typ.Id)
	require.Equal(t, "cast", got.GetF().Func.ObjName)
	require.Equal(t, int32(types.T_decimal128), got.GetF().Args[0].Typ.Id)
	require.Equal(t, int32(ctx.timeTag), got.GetF().Args[0].GetCol().RelPos)
	require.Equal(t, int32(0), got.GetF().Args[0].GetCol().ColPos)
}

func TestBindTimeWindowFuncRejectsOrderedGroupConcatInSlidingWindow(t *testing.T) {
	ctx := NewBindContext(nil, nil)
	ctx.sliding = true
	binder := &HavingBinder{
		baseBinder: baseBinder{sysCtx: context.Background(), ctx: ctx},
	}
	ast := &tree.FuncExpr{
		Func:  tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(NameGroupConcat)),
		Exprs: tree.Exprs{tree.NewUnresolvedColName("v")},
		OrderBy: tree.OrderBy{&tree.Order{
			Expr: tree.NewUnresolvedColName("x"),
		}},
	}

	_, err := binder.BindTimeWindowFunc(NameGroupConcat, ast, 0, true)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ordered group_concat in sliding time window")
}
