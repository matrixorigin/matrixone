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

			binder := &HavingBinder{baseBinder: baseBinder{sysCtx: context.Background()}}
			orderKey, err := binder.groupConcatOrderKey(display)
			require.NoError(t, err)
			require.Same(t, raw, orderKey)
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

func TestBindTimeWindowFuncRemapsMaxByTumblingWindowIdentity(t *testing.T) {
	for _, tc := range []struct {
		name string
		id   int32
	}{
		{name: "max_by", id: function.MAX_BY},
		{name: "max_by_non_null", id: function.MAX_BY_NON_NULL},
	} {
		t.Run(tc.name, func(t *testing.T) {
			valueType := types.T_int64.ToType()
			orderType := types.T_datetime.ToType()
			tieType := types.T_int64.ToType()
			ctx := NewBindContext(nil, nil)
			ctx.aggregateTag = 1
			ctx.timeTag = 2
			ctx.aggregates = []*plan.Expr{{
				Typ: makePlan2Type(&valueType),
				Expr: &plan.Expr_F{F: &plan.Function{
					Func: &plan.ObjectRef{
						Obj:     function.EncodeOverloadID(tc.id, 0),
						ObjName: tc.name,
					},
					Args: []*plan.Expr{
						{Typ: makePlan2Type(&valueType)},
						{Typ: makePlan2Type(&orderType)},
						{Typ: makePlan2Type(&tieType)},
					},
				}},
			}}

			binder := &HavingBinder{
				baseBinder: baseBinder{sysCtx: context.Background(), ctx: ctx},
			}
			ast := &tree.FuncExpr{
				Func: tree.FuncName2ResolvableFunctionReference(tree.NewUnresolvedColName(tc.name)),
				Exprs: tree.Exprs{
					tree.NewUnresolvedColName("value"),
					tree.NewUnresolvedColName("event_ts"),
					tree.NewUnresolvedColName("seq"),
				},
			}

			got, err := binder.BindTimeWindowFunc(tc.name, ast, 0, true)
			require.NoError(t, err)
			require.Len(t, ctx.times, 1)
			require.Equal(t, "any_value", ctx.times[0].GetF().Func.ObjName)
			require.Len(t, ctx.times[0].GetF().Args, 1)
			require.Equal(t, int32(ctx.aggregateTag), ctx.times[0].GetF().Args[0].GetCol().RelPos)
			require.Equal(t, int32(0), ctx.times[0].GetF().Args[0].GetCol().ColPos)
			require.Equal(t, int32(ctx.timeTag), got.GetCol().RelPos)
			require.Equal(t, int32(0), got.GetCol().ColPos)
			require.Equal(t, makePlan2Type(&valueType), got.Typ)
		})
	}
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

func TestBindOrderedSetPercentileRejectsTimeWindow(t *testing.T) {
	ctx := NewBindContext(nil, nil)
	ctx.timeTag = 1
	binder := &HavingBinder{
		baseBinder: baseBinder{sysCtx: context.Background(), ctx: ctx},
	}

	_, err := binder.BindAggFunc(NamePercentileCont, &tree.FuncExpr{}, 0, true)
	require.ErrorContains(t, err, "ordered-set percentile aggregates in time windows")
}

func TestBindOrderedSetPercentileRejectsNilOrderExpression(t *testing.T) {
	ctx := NewBindContext(nil, nil)
	binder := &HavingBinder{
		baseBinder: baseBinder{sysCtx: context.Background(), ctx: ctx},
	}
	ast := &tree.FuncExpr{
		WithinGroup: true,
		Exprs:       tree.Exprs{tree.NewNumVal(int64(1), "1", false, tree.P_int64)},
		OrderBy:     tree.OrderBy{nil},
	}

	_, err := binder.BindAggFunc(NamePercentileCont, ast, 0, true)
	require.ErrorContains(t, err, "percentile_cont requires an ORDER BY expression")
}

// Bare output names in aggregate HAVING are resolved after actual GROUP BY
// identities, but before ungrouped inputs. Aggregate arguments keep input scope.
func TestHavingAggregateAliasResolution(t *testing.T) {
	for _, tc := range []struct {
		name, mode, query, wantErr string
		aggregates                 int
	}{
		{"aggregate collision", "ONLY_FULL_GROUP_BY", "select n_regionkey,sum(n_nationkey) as n_name from nation group by n_regionkey having n_name=30", "", 1},
		{"scalar collision", "ONLY_FULL_GROUP_BY", "select n_regionkey,n_regionkey+1 as n_name,sum(n_nationkey) total from nation group by n_regionkey having n_name=2", "", 1},
		{"arithmetic alias", "ONLY_FULL_GROUP_BY", "select n_regionkey,sum(n_nationkey) as n_name from nation group by n_regionkey having n_name*2>50", "", 1},
		{"case alias", "ONLY_FULL_GROUP_BY", "select n_regionkey,sum(n_nationkey) as n_name from nation group by n_regionkey having case when n_name>50 then true else false end", "", 1},
		{"grouped source absent from projection", "ONLY_FULL_GROUP_BY", "select sum(n_nationkey) as n_regionkey from nation group by nation.n_regionkey having N_REGIONKEY>0", "", 1},
		{"source and alias aggregates stay distinct", "ONLY_FULL_GROUP_BY", "select n_name,sum(n_nationkey) as n_regionkey from nation group by n_name having sum(n_regionkey)>0", "", 2},
		{"self alias first", "ONLY_FULL_GROUP_BY", "select n_regionkey,sum(n_nationkey) as n_nationkey from nation group by n_regionkey having n_nationkey>0 and sum(n_nationkey)>0", "", 1},
		{"source aggregate first", "ONLY_FULL_GROUP_BY", "select n_regionkey,sum(n_nationkey) as n_nationkey from nation group by n_regionkey having sum(n_nationkey)>0 and n_nationkey>0", "", 1},
		{"permissive mode", "", "select n_regionkey,sum(n_nationkey) as n_name from nation group by n_regionkey having n_name=30", "", 1},
		{"native strict mode", "ONLY_FULL_GROUP_BY,MATRIXONE_NATIVE", "select n_regionkey,sum(n_nationkey) as n_name from nation group by n_regionkey having n_name=30", "must appear in the GROUP BY clause", 0},
		{"qualified source", "ONLY_FULL_GROUP_BY", "select n_regionkey,sum(n_nationkey) as n_name from nation group by n_regionkey having nation.n_name<>''", "must appear in the GROUP BY clause", 0},
		{"invalid projected source", "ONLY_FULL_GROUP_BY", "select n_comment as n_name,sum(n_nationkey) from nation group by n_regionkey having n_name<>''", "must appear in the GROUP BY clause", 0},
		{"alias is not aggregate input", "ONLY_FULL_GROUP_BY", "select n_regionkey,sum(n_nationkey) as total from nation group by n_regionkey having sum(total)>0", "does not exist", 0},
		{"nested aggregate remains invalid", "ONLY_FULL_GROUP_BY", "select n_regionkey,sum(n_nationkey) as n_name from nation group by n_regionkey having sum(sum(n_nationkey))>0", "cannot be nested", 0},
		{"different duplicate aliases remain ambiguous", "ONLY_FULL_GROUP_BY", "select n_regionkey,sum(n_nationkey) as n_name,count(*) as n_name from nation group by n_regionkey having n_name>0", "ambiguous", 0},
		{"equivalent duplicate aliases", "ONLY_FULL_GROUP_BY", "select n_regionkey,sum(n_nationkey) as n_name,sum(n_nationkey) as n_name from nation group by n_regionkey having n_name>0", "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockOptimizer(false, newPlanTestProcess(t))
			mock.ctxt.SetSqlModeOverride(tc.mode)
			p, err := runOneStmt(mock, t, tc.query)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			var aggregates int
			for _, node := range p.GetQuery().Nodes {
				if node.NodeType == plan.Node_AGG {
					aggregates += len(node.AggList)
				}
			}
			require.Equal(t, tc.aggregates, aggregates, "aliases and explicit calls must reuse the same aggregate")
		})
	}
}
