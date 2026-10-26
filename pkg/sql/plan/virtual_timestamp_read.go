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
	"github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// virtualTimestampColumn reports whether the value of a virtual column may
// change when a reader uses a different session time zone from its writer.
// Stored generated columns are physical leaves. A malformed dependency cycle
// is treated as unsafe for index use and is bounded by the visiting set.
func virtualTimestampColumn(table *plan.TableDef, pos int32, visiting map[int32]bool) bool {
	if table == nil || pos < 0 || int(pos) >= len(table.Cols) {
		return false
	}
	col := table.Cols[pos]
	if col == nil || col.GeneratedCol == nil || col.GeneratedCol.IsStored || col.GeneratedCol.Expr == nil {
		return false
	}
	if visiting[pos] {
		return true
	}
	visiting[pos] = true
	defer delete(visiting, pos)
	return virtualExprUsesTimestamp(table, col.GeneratedCol.Expr, visiting)
}

func virtualExprUsesTimestamp(table *plan.TableDef, expr *plan.Expr, visiting map[int32]bool) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col == nil || e.Col.ColPos < 0 || int(e.Col.ColPos) >= len(table.Cols) {
			return true
		}
		col := table.Cols[e.Col.ColPos]
		return col == nil || types.T(col.Typ.Id) == types.T_timestamp || virtualTimestampColumn(table, e.Col.ColPos, visiting)
	case *plan.Expr_F:
		for _, arg := range e.F.Args {
			if virtualExprUsesTimestamp(table, arg, visiting) {
				return true
			}
		}
	case *plan.Expr_List:
		for _, arg := range e.List.List {
			if virtualExprUsesTimestamp(table, arg, visiting) {
				return true
			}
		}
	}
	return false
}

// expandVirtualTimestampColumn substitutes read-time expressions for affected
// virtual dependencies. It only receives catalog expressions whose column
// references are local (RelPos 0); the copy is safe for each query binding.
func expandVirtualTimestampColumn(table *plan.TableDef, pos, tag, depth int32, visiting map[int32]bool) *plan.Expr {
	if pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] == nil || table.Cols[pos].GeneratedCol == nil || visiting[pos] {
		return nil
	}
	visiting[pos] = true
	defer delete(visiting, pos)
	return expandVirtualTimestampExpr(table, DeepCopyExpr(table.Cols[pos].GeneratedCol.Expr), tag, depth, visiting)
}

func expandVirtualTimestampExpr(table *plan.TableDef, expr *plan.Expr, tag, depth int32, visiting map[int32]bool) *plan.Expr {
	if expr == nil {
		return nil
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col == nil || e.Col.ColPos < 0 || int(e.Col.ColPos) >= len(table.Cols) {
			return nil
		}
		pos := e.Col.ColPos
		if virtualTimestampColumn(table, pos, make(map[int32]bool)) {
			return expandVirtualTimestampColumn(table, pos, tag, depth, visiting)
		}
		if depth > 0 {
			expr.Expr = &plan.Expr_Corr{Corr: &plan.CorrColRef{RelPos: tag, ColPos: pos, Depth: depth}}
		} else {
			e.Col.RelPos = tag
		}
	case *plan.Expr_F:
		for i, arg := range e.F.Args {
			e.F.Args[i] = expandVirtualTimestampExpr(table, arg, tag, depth, visiting)
			if e.F.Args[i] == nil {
				return nil
			}
		}
	case *plan.Expr_List:
		for i, arg := range e.List.List {
			e.List.List[i] = expandVirtualTimestampExpr(table, arg, tag, depth, visiting)
			if e.List.List[i] == nil {
				return nil
			}
		}
	}
	return expr
}

func indexUsesVirtualTimestamp(idx *plan.IndexDef, table *plan.TableDef) bool {
	if idx == nil || table == nil {
		return false
	}
	for _, part := range idx.Parts {
		if pos, ok := table.Name2ColIndex[catalog.ResolveAlias(part)]; ok &&
			virtualTimestampColumn(table, pos, make(map[int32]bool)) {
			return true
		}
	}
	return false
}

func indexUnsafeForRead(idx *plan.IndexDef, table *plan.TableDef) bool {
	return indexUsesVirtualTimestamp(idx, table) || indexUsesUnicodeCollation(idx, table)
}
