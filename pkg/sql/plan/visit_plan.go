// Copyright 2022 Matrix Origin
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

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

type VisitPlanRule interface {
	MatchNode(*Node) bool
	IsApplyExpr() bool
	ApplyNode(*Node) error
	ApplyExpr(*Expr) (*Expr, error)
}

type VisitPlan struct {
	plan         *Plan
	isUpdatePlan bool
	rules        []VisitPlanRule
}

func NewVisitPlan(pl *Plan, rules []VisitPlanRule) *VisitPlan {
	return &VisitPlan{
		plan:         pl,
		isUpdatePlan: false,
		rules:        rules,
	}
}

func (vq *VisitPlan) visitNode(ctx context.Context, qry *Query, node *Node, idx int32) error {
	for i := range node.Children {
		if err := vq.visitNode(ctx, qry, qry.Nodes[node.Children[i]], node.Children[i]); err != nil {
			return err
		}
	}

	for _, rule := range vq.rules {
		if rule.MatchNode(node) {
			err := rule.ApplyNode(node)
			if err != nil {
				return err
			}
		} else if rule.IsApplyExpr() {
			err := vq.exploreNode(ctx, rule, node, idx)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (vq *VisitPlan) exploreNode(ctx context.Context, rule VisitPlanRule, node *Node, idx int32) error {
	var err error
	if node.Limit != nil {
		node.Limit, err = rule.ApplyExpr(node.Limit)
		if err != nil {
			return err
		}
	}

	if node.Offset != nil {
		node.Offset, err = rule.ApplyExpr(node.Offset)
		if err != nil {
			return err
		}
	}

	for i := range node.OnList {
		node.OnList[i], err = rule.ApplyExpr(node.OnList[i])
		if err != nil {
			return err
		}
	}

	for i := range node.FilterList {
		node.FilterList[i], err = rule.ApplyExpr(node.FilterList[i])
		if err != nil {
			return err
		}
	}

	for i := range node.BlockFilterList {
		node.BlockFilterList[i], err = rule.ApplyExpr(node.BlockFilterList[i])
		if err != nil {
			return err
		}
	}

	typ := types.New(types.T_varchar, 65000, 0)
	toTyp := makePlan2Type(&typ)
	targetTyp := &plan.Expr{
		Typ: toTyp,
		Expr: &plan.Expr_T{
			T: &plan.TargetType{
				Typ: toTyp,
			},
		},
	}

	applyAndResetType := func(e *Expr) (*Expr, error) {
		oldType := DeepCopyType(e.Typ)
		e, err = rule.ApplyExpr(e)
		if err != nil {
			return nil, err
		}
		if (oldType.Id == int32(types.T_float32) || oldType.Id == int32(types.T_float64)) && (e.Typ.Id == int32(types.T_decimal64) || e.Typ.Id == int32(types.T_decimal128)) {
			e, err = forceCastExpr2(ctx, e, typ, targetTyp)
			if err != nil {
				return nil, err
			}
		}
		return forceCastExpr(ctx, e, oldType)
	}

	if node.RowsetData != nil {
		for i := range node.RowsetData.Cols {
			for j := range node.RowsetData.Cols[i].Data {
				node.RowsetData.Cols[i].Data[j].Expr, err = applyAndResetType(node.RowsetData.Cols[i].Data[j].Expr)
				if err != nil {
					return err
				}
			}
		}
	}

	for i := range node.ProjectList {
		if vq.isUpdatePlan {
			node.ProjectList[i], err = applyAndResetType(node.ProjectList[i])

		} else {
			node.ProjectList[i], err = rule.ApplyExpr(node.ProjectList[i])

		}
		if err != nil {
			return err
		}
	}

	return nil
}

func (vq *VisitPlan) Visit(ctx context.Context) error {
	switch pl := vq.plan.Plan.(type) {
	case *Plan_Query:
		qry := pl.Query
		vq.isUpdatePlan = (pl.Query.StmtType == plan.Query_UPDATE)

		if len(qry.Steps) == 0 {
			return nil
		}

		for _, step := range qry.Steps {
			err := vq.visitNode(ctx, qry, qry.Nodes[step], step)
			if err != nil {
				return err
			}
		}

	default:
		// do nothing

	}

	return nil
}
