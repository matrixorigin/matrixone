// Copyright 2025 Matrix Origin
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

package compile

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	pbplan "github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/filter"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/projection"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/table_scan"
	"github.com/matrixorigin/matrixone/pkg/sql/plan"
	"github.com/matrixorigin/matrixone/pkg/vm"
)

func isFulltextFunction(e *plan.Expr) bool {
	if e.GetF() == nil {
		return false
	}
	return plan.IsFulltextFunction(e.GetF())
}

func ftFuncInList(fn *plan.Expr, list []*plan.Expr) int32 {
	var qb *plan.QueryBuilder
	return qb.FtFuncInList(fn.GetF(), list)
}

func colrefInList(colref *plan.ColRef, list []*plan.Expr) int32 {
	for i, expr := range list {
		if expr.GetCol() == nil {
			continue
		}
		if expr.GetCol().RelPos == colref.RelPos && expr.GetCol().ColPos == colref.ColPos {
			return int32(i)
		}
	}
	return -1
}

func (c *Compile) rewriteTableScanForIndexScan(tableScan *table_scan.TableScan, expr *plan.Expr) *plan.Expr {
	if expr == nil || !isFulltextFunction(expr) {
		// panic?
		return nil
	}

	pos := ftFuncInList(expr, tableScan.IndexScanExprs)
	if pos < 0 {
		// not found, add to the end of the list
		copyExpr := plan.DeepCopyExpr(expr)
		tableScan.IndexScanExprs = append(tableScan.IndexScanExprs, copyExpr)
		pos = int32(len(tableScan.IndexScanExprs) - 1)
	}

	var colPos int32
	if len(tableScan.ProjectList) == 0 {
		colPos = int32(len(tableScan.Types) + int(pos))
	} else {
		colPos = int32(len(tableScan.ProjectList) + int(pos))
	}

	return &plan.Expr{
		Typ: expr.Typ,
		Expr: &pbplan.Expr_Col{
			Col: &plan.ColRef{
				RelPos: 0,
				ColPos: colPos,
				Name:   fmt.Sprintf("fulltext_match_%d", pos),
			},
		},
	}
}

func (c *Compile) visitForIndexScan(child vm.Operator, exprs []*plan.Expr) {
	for _, e := range exprs {
		err := plan.VisitExpr(e, func(expr *plan.Expr) (bool, error) {
			if isFulltextFunction(expr) {
				rewriteExpr := c.rewriteForIndexScan(child, expr)
				if rewriteExpr != nil {
					*expr = *rewriteExpr
				}
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			panic(err)
		}
	}
}

func (c *Compile) rewriteForIndexScan(op vm.Operator, expr *plan.Expr) *plan.Expr {
	switch op.OpType() {
	case vm.TableScan:
		tableScan := op.(*table_scan.TableScan)
		return c.rewriteTableScanForIndexScan(tableScan, expr)
	case vm.Filter:
		filter := op.(*filter.Filter)
		if len(filter.GetOperatorBase().Children) == 0 {
			return nil
		}
		child := filter.GetOperatorBase().Children[0]
		if expr == nil {
			c.visitForIndexScan(child, filter.FilterExprs)
			return nil
		} else {
			return c.rewriteForIndexScan(child, expr)
		}
	case vm.Projection:
		project := op.(*projection.Projection)
		if len(project.GetOperatorBase().Children) == 0 {
			return nil
		}
		child := project.GetOperatorBase().Children[0]
		if expr == nil {
			c.visitForIndexScan(child, project.ProjectList)
			return nil
		} else {
			rwExpr := c.rewriteForIndexScan(child, expr)
			pos := colrefInList(rwExpr.GetCol(), project.ProjectList)
			if pos >= 0 {
				return project.ProjectList[pos]
			} else {
				project.ProjectList = append(project.ProjectList, rwExpr)
				pos = int32(len(project.ProjectList) - 1)
				name := rwExpr.GetCol().Name
				return &plan.Expr{
					Typ: expr.Typ,
					Expr: &pbplan.Expr_Col{
						Col: &plan.ColRef{
							RelPos: 0,
							ColPos: pos,
							Name:   name,
						},
					},
				}
			}
		}
	default:
		if expr != nil {
			if len(op.GetOperatorBase().Children) != 1 {
				panic(moerr.NewInternalErrorNoCtx("unsupported operator for index scan"))
			} else {
				child := op.GetOperatorBase().Children[0]
				return c.rewriteForIndexScan(child, expr)
			}
		} else {
			return nil
		}
	}
}
