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
// Stored generated columns are physical leaves. Classification memoizes each
// column, so a generated-column DAG costs linear work rather than repeatedly
// traversing shared dependencies. A malformed cycle is unsafe for index use.
func virtualTimestampColumn(table *plan.TableDef, pos int32) bool {
	if table == nil {
		return false
	}
	return virtualTimestampColumnState(table, pos, make([]uint8, len(table.Cols)))
}

// state: 0 unseen, 1 visiting, 2 safe, 3 affected.
func virtualTimestampColumnState(table *plan.TableDef, pos int32, state []uint8) bool {
	if table == nil || pos < 0 || int(pos) >= len(table.Cols) {
		return false
	}
	switch state[pos] {
	case 1, 3:
		return true
	case 2:
		return false
	}
	col := table.Cols[pos]
	if col == nil || col.GeneratedCol == nil || col.GeneratedCol.IsStored || col.GeneratedCol.Expr == nil {
		state[pos] = 2
		return false
	}
	state[pos] = 1
	if virtualExprUsesTimestamp(table, col.GeneratedCol.Expr, state) {
		state[pos] = 3
		return true
	}
	state[pos] = 2
	return false
}

func virtualExprUsesTimestamp(table *plan.TableDef, expr *plan.Expr, state []uint8) bool {
	if expr == nil {
		return false
	}
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col == nil || e.Col.ColPos < 0 || int(e.Col.ColPos) >= len(table.Cols) {
			return true
		}
		col := table.Cols[e.Col.ColPos]
		return col == nil || types.T(col.Typ.Id) == types.T_timestamp || virtualTimestampColumnState(table, e.Col.ColPos, state)
	case *plan.Expr_F:
		if e.F == nil {
			return true
		}
		for _, arg := range e.F.Args {
			if virtualExprUsesTimestamp(table, arg, state) {
				return true
			}
		}
	case *plan.Expr_List:
		if e.List == nil {
			return true
		}
		for _, arg := range e.List.List {
			if virtualExprUsesTimestamp(table, arg, state) {
				return true
			}
		}
	case *plan.Expr_Lit, *plan.Expr_T:
		return false
	default:
		// An unfamiliar catalog expression must not make a persisted index
		// appear safe when we cannot inspect all of its inputs.
		return true
	}
	return false
}

// expandVirtualTimestampColumn substitutes read-time expressions for affected
// virtual dependencies. It only receives catalog expressions whose column
// references are local (RelPos 0); the copy is safe for each query binding.
const maxVirtualTimestampExpansionNodes = 65536

func expandVirtualTimestampColumn(table *plan.TableDef, pos, tag, depth int32, visiting map[int32]bool) *plan.Expr {
	remaining := maxVirtualTimestampExpansionNodes
	return expandVirtualTimestampColumnBounded(table, pos, tag, depth, visiting, &remaining)
}

func expandVirtualTimestampColumnBounded(table *plan.TableDef, pos, tag, depth int32, visiting map[int32]bool, remaining *int) *plan.Expr {
	if pos < 0 || int(pos) >= len(table.Cols) || table.Cols[pos] == nil || table.Cols[pos].GeneratedCol == nil || visiting[pos] {
		return nil
	}
	visiting[pos] = true
	defer delete(visiting, pos)
	return expandVirtualTimestampExpr(table, DeepCopyExpr(table.Cols[pos].GeneratedCol.Expr), tag, depth, visiting, remaining)
}

func expandVirtualTimestampExpr(table *plan.TableDef, expr *plan.Expr, tag, depth int32, visiting map[int32]bool, remaining *int) *plan.Expr {
	if expr == nil || *remaining == 0 {
		return nil
	}
	*remaining = *remaining - 1
	switch e := expr.Expr.(type) {
	case *plan.Expr_Col:
		if e.Col == nil || e.Col.ColPos < 0 || int(e.Col.ColPos) >= len(table.Cols) {
			return nil
		}
		pos := e.Col.ColPos
		if virtualTimestampColumn(table, pos) {
			return expandVirtualTimestampColumnBounded(table, pos, tag, depth, visiting, remaining)
		}
		if depth > 0 {
			expr.Expr = &plan.Expr_Corr{Corr: &plan.CorrColRef{RelPos: tag, ColPos: pos, Depth: depth}}
		} else {
			e.Col.RelPos = tag
		}
	case *plan.Expr_F:
		if e.F == nil {
			return nil
		}
		for i, arg := range e.F.Args {
			e.F.Args[i] = expandVirtualTimestampExpr(table, arg, tag, depth, visiting, remaining)
			if e.F.Args[i] == nil {
				return nil
			}
		}
	case *plan.Expr_List:
		if e.List == nil {
			return nil
		}
		for i, arg := range e.List.List {
			e.List.List[i] = expandVirtualTimestampExpr(table, arg, tag, depth, visiting, remaining)
			if e.List.List[i] == nil {
				return nil
			}
		}
	case *plan.Expr_Lit, *plan.Expr_T:
		// No row-local references.
	default:
		return nil
	}
	return expr
}

func indexUsesVirtualTimestamp(idx *plan.IndexDef, table *plan.TableDef) bool {
	if idx == nil || table == nil {
		return false
	}
	state := make([]uint8, len(table.Cols))
	for _, part := range idx.Parts {
		if pos, ok := table.Name2ColIndex[catalog.ResolveAlias(part)]; ok &&
			virtualTimestampColumnState(table, pos, state) {
			return true
		}
	}
	return false
}

func indexUnsafeForRead(idx *plan.IndexDef, table *plan.TableDef) bool {
	return indexUsesVirtualTimestamp(idx, table) || indexUsesUnicodeCollation(idx, table)
}
