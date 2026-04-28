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
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/vectorindex/metric"
)

// Vector index filter pushdown uses bound plan.Expr as the planner-only public
// IR. Backend payloads must pass through a lowering function that resolves
// ColRef.RelPos/ColPos to a stable logical column name first.

func buildVectorIndexCoveredColumns(tableDef *plan.TableDef, includeColumns []string) map[string]struct{} {
	covered := make(map[string]struct{}, len(includeColumns)+1)
	for _, col := range includeColumns {
		covered[col] = struct{}{}
	}

	// Only a real single-column primary key is naturally covered. Composite
	// primary keys expose a hidden cpkey column, which must stay residual.
	if tableDef != nil && tableDef.Pkey != nil && len(tableDef.Pkey.Names) == 1 {
		covered[tableDef.Pkey.PkeyColName] = struct{}{}
	}
	return covered
}

func splitFiltersByVectorIndexCoverage(
	filters []*plan.Expr,
	scanNode *plan.Node,
	includeColumns []string,
	partPos int32,
) (pushdownFilters, remainingFilters []*plan.Expr) {
	if scanNode == nil || scanNode.TableDef == nil || len(scanNode.BindingTags) == 0 {
		return nil, filters
	}

	covered := buildVectorIndexCoveredColumns(scanNode.TableDef, includeColumns)
	scanTag := scanNode.BindingTags[0]
	for _, expr := range filters {
		if vectorIndexExprRefsOnlyCoveredColumns(expr, scanTag, partPos, scanNode.TableDef, covered) {
			pushdownFilters = append(pushdownFilters, expr)
		} else {
			remainingFilters = append(remainingFilters, expr)
		}
	}
	return pushdownFilters, remainingFilters
}

func vectorIndexColumnNameFromColRef(col *plan.ColRef, scanNode *plan.Node, scanTag int32) (string, bool) {
	if col == nil || scanNode == nil || scanNode.TableDef == nil {
		return "", false
	}
	return vectorIndexColumnNameFromTableDef(col, scanNode.TableDef, scanTag)
}

func vectorIndexColumnNameFromTableDef(col *plan.ColRef, tableDef *plan.TableDef, scanTag int32) (string, bool) {
	if col == nil || tableDef == nil {
		return "", false
	}
	if col.RelPos != scanTag {
		return "", false
	}
	if col.ColPos < 0 || int(col.ColPos) >= len(tableDef.Cols) {
		return "", false
	}
	return tableDef.Cols[col.ColPos].Name, true
}

func vectorIndexExprRefsOnlyCoveredColumns(expr *plan.Expr, scanTag, partPos int32, tableDef *plan.TableDef, covered map[string]struct{}) bool {
	if expr == nil {
		return true
	}

	switch impl := expr.Expr.(type) {
	case *plan.Expr_Col:
		colName, ok := vectorIndexColumnNameFromTableDef(impl.Col, tableDef, scanTag)
		if !ok {
			return false
		}
		_, ok = covered[colName]
		return ok
	case *plan.Expr_F:
		if isVectorDistanceExpr(expr, scanTag, partPos) {
			return false
		}
		for _, arg := range impl.F.Args {
			if !vectorIndexExprRefsOnlyCoveredColumns(arg, scanTag, partPos, tableDef, covered) {
				return false
			}
		}
		return true
	case *plan.Expr_Lit:
		return true
	case *plan.Expr_List:
		for _, sub := range impl.List.List {
			if !vectorIndexExprRefsOnlyCoveredColumns(sub, scanTag, partPos, tableDef, covered) {
				return false
			}
		}
		return true
	case *plan.Expr_T:
		return true
	default:
		return false
	}
}

func isVectorDistanceExpr(expr *plan.Expr, scanTag, partPos int32) bool {
	fn := expr.GetF()
	if fn == nil {
		return false
	}
	if _, ok := metric.DistFuncOpTypes[fn.Func.ObjName]; !ok {
		return false
	}
	if len(fn.Args) != 2 {
		return false
	}

	for _, arg := range fn.Args {
		col := arg.GetCol()
		if col != nil && col.RelPos == scanTag && col.ColPos == partPos {
			return true
		}
	}
	return false
}
