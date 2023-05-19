// Copyright 2021 - 2022 Matrix Origin
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

package rule

import (
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan/function"
)

type PredicatePushdown struct {
}

func NewPredicatePushdown() *PredicatePushdown {
	return &PredicatePushdown{}
}

func (r *PredicatePushdown) Match(n *plan.Node) bool {
	return n.NodeType != plan.Node_TABLE_SCAN && n.NodeType != plan.Node_EXTERNAL_SCAN && n.NodeType != plan.Node_FUNCTION_SCAN && len(n.FilterList) > 0
}

func (r *PredicatePushdown) Apply(n *plan.Node, qry *plan.Query) {
	es := n.FilterList
	n.FilterList = make([]*plan.Expr, 0, len(es))
	for i := range es {
		r.pushdown(es[i], n, qry)
	}
}

func (r *PredicatePushdown) pushdown(e *plan.Expr, n *plan.Node, qry *plan.Query) bool {
	var ne *plan.Expr // new expr

	if _, ok := e.Expr.(*plan.Expr_F); !ok {
		n.FilterList = append(n.FilterList, e)
		return false
	}
	if n.NodeType == plan.Node_TABLE_SCAN || n.NodeType == plan.Node_AGG || n.NodeType == plan.Node_FUNCTION_SCAN {
		n.FilterList = append(n.FilterList, e)
		return false
	}
	if len(n.Children) > 0 && (qry.Nodes[n.Children[0]].NodeType == plan.Node_JOIN || qry.Nodes[n.Children[0]].NodeType == plan.Node_AGG || qry.Nodes[n.Children[0]].NodeType == plan.Node_FUNCTION_SCAN) {
		n.FilterList = append(n.FilterList, e)
		return false
	}
	relPos := int32(-1)
	relPos, ne = r.newExpr(relPos, e, n, qry)
	if ne == nil {
		n.FilterList = append(n.FilterList, e)
		return false
	}
	if !r.pushdown(ne, qry.Nodes[relPos], qry) {
		n.FilterList = append(n.FilterList, e)
		return false
	}
	return true
}

func (r *PredicatePushdown) newExpr(relPos int32, expr *plan.Expr, n *plan.Node, qry *plan.Query) (int32, *plan.Expr) {
	switch e := expr.Expr.(type) {
	case *plan.Expr_C:
		return relPos, expr
	case *plan.Expr_F:
		overloadID := e.F.Func.GetObj()
		f, exists := function.GetFunctionByIdWithoutError(overloadID)
		if !exists {
			return relPos, nil
		}
		if f.IsAgg() {
			return relPos, nil
		}
		args := make([]*plan.Expr, len(e.F.Args))
		for i := range e.F.Args {
			relPos, args[i] = r.newExpr(relPos, e.F.Args[i], n, qry)
			if args[i] == nil {
				return relPos, nil
			}
		}
		return relPos, expr
	case *plan.Expr_Col:
		if relPos < 0 {
			relPos = e.Col.RelPos
		}
		if relPos < 0 {
			return relPos, nil
		}
		if relPos != e.Col.RelPos {
			return relPos, nil
		}
		return relPos, qry.Nodes[n.Children[relPos]].ProjectList[e.Col.ColPos]
	default:
		return relPos, nil
	}
}
