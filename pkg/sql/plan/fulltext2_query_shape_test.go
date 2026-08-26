// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package plan

import (
	"testing"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

func ft2TestMatch(pattern string, mode int64, rel int32, col string) *plan.Function {
	return &plan.Function{
		Func: &plan.ObjectRef{ObjName: "fulltext_match"},
		Args: []*plan.Expr{
			makePlan2StringConstExprWithType(pattern),
			makePlan2Int64ConstExprWithType(mode),
			{Typ: plan.Type{Id: int32(types.T_varchar)}, Expr: &plan.Expr_Col{Col: &plan.ColRef{RelPos: rel, Name: col}}},
		},
	}
}

func ft2TestExpr(fn *plan.Function) *plan.Expr {
	return &plan.Expr{Typ: plan.Type{Id: int32(types.T_float32)}, Expr: &plan.Expr_F{F: fn}}
}

func TestFulltext2ScoreRangeBounds(t *testing.T) {
	b := &QueryBuilder{}
	match := ft2TestMatch("hello", 0, 1, "body")
	value := func(v float64) *plan.Expr { return makePlan2Float64ConstExprWithType(v) }
	cmp := func(op string, left, right *plan.Expr) *plan.Expr {
		return &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
			Func: &plan.ObjectRef{ObjName: op}, Args: []*plan.Expr{left, right},
		}}}
	}

	r := b.fulltext2ScoreRangeFromFilters([]*plan.Expr{cmp(">", ft2TestExpr(match), value(0.5))}, match)
	require.NotNil(t, r)
	require.True(t, r.HasMin)
	require.False(t, r.MinInclusive)
	require.LessOrEqual(t, r.Min, float32(0.5))

	r = b.fulltext2ScoreRangeFromFilters([]*plan.Expr{cmp("<", ft2TestExpr(match), value(0.8))}, match)
	require.NotNil(t, r)
	require.True(t, r.HasMax)
	require.False(t, r.MaxInclusive)
	require.GreaterOrEqual(t, r.Max, float32(0.8))

	r = b.fulltext2ScoreRangeFromFilters([]*plan.Expr{cmp("<", value(0.5), ft2TestExpr(match))}, match)
	require.True(t, r.HasMin, "reversed comparison must become a lower bound")

	r = b.fulltext2ScoreRangeFromFilters([]*plan.Expr{cmp("or", ft2TestExpr(match), value(0.5))}, match)
	require.Nil(t, r, "only direct AND-reachable comparisons are pushable")
}

func TestFulltext2ScoreWrapperRewrite(t *testing.T) {
	match := ft2TestMatch("hello", 0, 1, "body")
	served := []fulltextServedMatch{{fn: match, nodeID: 3, fulltext2: true}}
	b := &QueryBuilder{qry: &plan.Query{Nodes: []*plan.Node{
		{}, {}, {}, {BindingTags: []int32{9}, TableDef: &plan.TableDef{Cols: []*plan.ColDef{{}, {Typ: plan.Type{Id: int32(types.T_float32)}}}}},
	}}}
	wrapped := &plan.Expr{Expr: &plan.Expr_F{F: &plan.Function{
		Func: &plan.ObjectRef{ObjName: "round"}, Args: []*plan.Expr{ft2TestExpr(match)},
	}}}
	got := replaceScoreFnInExprBy(wrapped, b.fullText2ScoreRewriter(served))
	require.NotNil(t, got.GetF())
	require.NotNil(t, got.GetF().Args[0].GetCol())
	require.Equal(t, int32(9), got.GetF().Args[0].GetCol().RelPos)
}
