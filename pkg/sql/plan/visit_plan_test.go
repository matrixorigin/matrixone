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

	planpb "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/stretchr/testify/require"
)

type replaceParamWithSevenRule struct{}

func (replaceParamWithSevenRule) MatchNode(*planpb.Node) bool  { return false }
func (replaceParamWithSevenRule) IsApplyExpr() bool            { return true }
func (replaceParamWithSevenRule) ApplyNode(*planpb.Node) error { return nil }
func (replaceParamWithSevenRule) ApplyExpr(expr *planpb.Expr) (*planpb.Expr, error) {
	switch window := expr.Expr.(type) {
	case *planpb.Expr_W:
		if window.W.WindowFunc != nil {
			window.W.WindowFunc, _ = replaceParamWithSevenRule{}.ApplyExpr(window.W.WindowFunc)
		}
		for i := range window.W.PartitionBy {
			window.W.PartitionBy[i], _ = replaceParamWithSevenRule{}.ApplyExpr(window.W.PartitionBy[i])
		}
		for i := range window.W.OrderBy {
			if window.W.OrderBy[i] != nil && window.W.OrderBy[i].Expr != nil {
				window.W.OrderBy[i].Expr, _ = replaceParamWithSevenRule{}.ApplyExpr(window.W.OrderBy[i].Expr)
			}
		}
		if window.W.Frame != nil {
			if window.W.Frame.Start != nil && window.W.Frame.Start.Val != nil {
				window.W.Frame.Start.Val, _ = replaceParamWithSevenRule{}.ApplyExpr(window.W.Frame.Start.Val)
			}
			if window.W.Frame.End != nil && window.W.Frame.End.Val != nil {
				window.W.Frame.End.Val, _ = replaceParamWithSevenRule{}.ApplyExpr(window.W.Frame.End.Val)
			}
		}
		return expr, nil
	case *planpb.Expr_P:
		return makePlan2Uint64ConstExprWithType(7), nil
	default:
		return expr, nil
	}
}

func TestVisitPlanExploresIndexReaderParam(t *testing.T) {
	paramExpr := func() *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: 0}}}
	}
	node := &planpb.Node{
		NodeId: 0,
		IndexReaderParam: &planpb.IndexReaderParam{
			Limit:   paramExpr(),
			OrderBy: []*planpb.OrderBySpec{{Expr: paramExpr()}},
			DistRange: &planpb.DistRange{
				LowerBound: paramExpr(),
				UpperBound: paramExpr(),
			},
		},
	}
	query := &planpb.Query{Nodes: []*planpb.Node{node}, Steps: []int32{0}}
	visitor := NewVisitPlan(&planpb.Plan{Plan: &planpb.Plan_Query{Query: query}}, []VisitPlanRule{replaceParamWithSevenRule{}})

	require.NoError(t, visitor.Visit(context.Background()))
	require.Equal(t, uint64(7), node.IndexReaderParam.Limit.GetLit().GetU64Val())
	require.Equal(t, uint64(7), node.IndexReaderParam.OrderBy[0].Expr.GetLit().GetU64Val())
	require.Equal(t, uint64(7), node.IndexReaderParam.DistRange.LowerBound.GetLit().GetU64Val())
	require.Equal(t, uint64(7), node.IndexReaderParam.DistRange.UpperBound.GetLit().GetU64Val())
}

func TestVisitPlanExploresAggregateAndGroupExpressions(t *testing.T) {
	paramExpr := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
	}
	node := &planpb.Node{
		NodeId:   0,
		AggList:  []*planpb.Expr{paramExpr(0)},
		GroupBy:  []*planpb.Expr{paramExpr(1)},
		NodeType: planpb.Node_AGG,
	}
	query := &planpb.Query{Nodes: []*planpb.Node{node}, Steps: []int32{0}}
	visitor := NewVisitPlan(&planpb.Plan{Plan: &planpb.Plan_Query{Query: query}}, []VisitPlanRule{replaceParamWithSevenRule{}})

	require.NoError(t, visitor.Visit(context.Background()))
	require.Equal(t, uint64(7), node.AggList[0].GetLit().GetU64Val())
	require.Equal(t, uint64(7), node.GroupBy[0].GetLit().GetU64Val())
}

func TestVisitPlanExploresWindowFrameParams(t *testing.T) {
	paramExpr := func(pos int32) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_P{P: &planpb.ParamRef{Pos: pos}}}
	}
	node := &planpb.Node{
		NodeId:   0,
		NodeType: planpb.Node_WINDOW,
		WinSpecList: []*planpb.Expr{{
			Expr: &planpb.Expr_W{W: &planpb.WindowSpec{
				Frame: &planpb.FrameClause{
					Start: &planpb.FrameBound{Val: paramExpr(0)},
					End:   &planpb.FrameBound{Val: paramExpr(1)},
				},
			}},
		}},
	}
	query := &planpb.Query{Nodes: []*planpb.Node{node}, Steps: []int32{0}}
	visitor := NewVisitPlan(&planpb.Plan{Plan: &planpb.Plan_Query{Query: query}}, []VisitPlanRule{replaceParamWithSevenRule{}})

	require.NoError(t, visitor.Visit(context.Background()))
	require.Equal(t, uint64(7), node.WinSpecList[0].GetW().Frame.Start.Val.GetLit().GetU64Val())
	require.Equal(t, uint64(7), node.WinSpecList[0].GetW().Frame.End.Val.GetLit().GetU64Val())
}
