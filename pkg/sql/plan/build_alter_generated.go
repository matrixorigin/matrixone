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
	"strings"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
)

// alterCopyPreservedGeneratedColumns identifies materialized values that can be
// copied without recomputing their expression. Both STORED and VIRTUAL columns
// are materialized on write. Use original dependency positions because ALTER
// may have reordered columns in the intermediate copy definition.
func alterCopyPreservedGeneratedColumns(old, target *TableDef, alterCtx *AlterTableContext) map[string]bool {
	result := make(map[string]bool)
	// 0 unknown, 1 visiting, 2 unchanged, 3 changed or unprovable.
	state := make([]uint8, len(old.Cols))
	var unchanged func(int32) bool
	var expressionUnchanged func(*plan.Expr) bool
	expressionUnchanged = func(expr *plan.Expr) bool {
		if expr == nil {
			return false
		}
		switch e := expr.Expr.(type) {
		case *plan.Expr_Col:
			return e.Col != nil && e.Col.RelPos == 0 && unchanged(e.Col.ColPos)
		case *plan.Expr_F:
			if e.F == nil {
				return false
			}
			for _, arg := range e.F.Args {
				if !expressionUnchanged(arg) {
					return false
				}
			}
			return true
		case *plan.Expr_List:
			if e.List == nil {
				return false
			}
			for _, arg := range e.List.List {
				if !expressionUnchanged(arg) {
					return false
				}
			}
			return true
		case *plan.Expr_Lit, *plan.Expr_Vec, *plan.Expr_Max, *plan.Expr_T:
			return true
		default:
			return false
		}
	}
	unchanged = func(pos int32) bool {
		if pos < 0 || int(pos) >= len(old.Cols) {
			return false
		}
		if state[pos] != 0 {
			return state[pos] == 2
		}
		state[pos] = 1
		defer func() {
			if state[pos] == 1 {
				state[pos] = 3
			}
		}()
		src := old.Cols[pos]
		if src == nil || src.Hidden || src.ColId == UnKnownColId {
			return false
		}
		mapped := alterCtx.changColDefMap[src.ColId]
		if mapped == nil {
			return false
		}
		dst := FindColumn(target.Cols, mapped.Name)
		if dst == nil || dst.Hidden || dst.ColId != src.ColId || dst.Seqnum != src.Seqnum {
			return false
		}
		source, ok := alterCtx.alterColMap[dst.Name]
		if !ok || source.sexprType != exprColumnName || !strings.EqualFold(source.sexprStr, src.Name) {
			return false
		}
		a, b := src.Typ, dst.Typ
		if a.Id != b.Id || a.NotNullable != b.NotNullable || a.AutoIncr != b.AutoIncr ||
			a.Width != b.Width || a.Scale != b.Scale || a.Enumvalues != b.Enumvalues {
			return false
		}
		if src.GeneratedCol != nil {
			if dst.GeneratedCol == nil || src.GeneratedCol.IsStored != dst.GeneratedCol.IsStored ||
				src.GeneratedCol.OriginString != dst.GeneratedCol.OriginString || !expressionUnchanged(src.GeneratedCol.Expr) {
				return false
			}
		} else if dst.GeneratedCol != nil {
			return false
		}
		state[pos] = 2
		return true
	}
	for i, col := range old.Cols {
		if col != nil && col.GeneratedCol != nil && unchanged(int32(i)) {
			result[alterCtx.changColDefMap[col.ColId].Name] = true
		}
	}
	return result
}

// A recomputed generated column can change existing foreign-key values during
// COPY. Guard incoming and self-references before replacing the table; other
// child references are checked by the COPY INSERT.
func checkAlterCopyGeneratedForeignKeys(ctx CompilerContext, old, target *TableDef, alterCtx *AlterTableContext, preserved map[string]bool) error {
	if len(old.Fkeys) == 0 && len(old.RefChildTbls) == 0 {
		return nil
	}
	changed := make(map[uint64]*ColDef)
	for _, col := range old.Cols {
		if col == nil {
			continue
		}
		mapped := alterCtx.changColDefMap[col.ColId]
		var targetName string
		if mapped != nil {
			targetName = mapped.Name
		}
		targetGenerated := false
		// A changed ordinary column may itself become generated.
		// The caller's map only contains columns whose identity survived.
		if targetName != "" {
			if targetCol := FindColumn(target.Cols, targetName); targetCol != nil {
				targetGenerated = targetCol.GeneratedCol != nil
			}
		}
		if (col.GeneratedCol != nil || targetGenerated) && !preserved[targetName] {
			changed[col.ColId] = col
		}
	}
	if len(changed) == 0 {
		return nil
	}
	// Non-self child references are checked by the COPY INSERT against the
	// parent. A self-reference would be checked against the old table, so it
	// must not be allowed to change during the replacement.
	for _, fk := range old.Fkeys {
		if fk.ForeignTbl != 0 {
			continue
		}
		for _, id := range fk.Cols {
			if col := changed[id]; col != nil {
				return moerr.NewErrForeignKeyColumnCannotChange(ctx.GetContext(), col.Name, fk.Name)
			}
		}
		for _, id := range fk.ForeignCols {
			if col := changed[id]; col != nil {
				return moerr.NewErrForeignKeyColumnCannotChange(ctx.GetContext(), col.Name, fk.Name)
			}
		}
	}
	for _, tableID := range old.RefChildTbls {
		if tableID == 0 || tableID == old.TblId {
			continue
		}
		obj, child, err := ctx.ResolveById(tableID, nil)
		if err != nil {
			return err
		}
		if child == nil || obj == nil {
			return moerr.NewInternalErrorf(ctx.GetContext(), "foreign key child table %d does not exist", tableID)
		}
		for _, fk := range child.Fkeys {
			if fk.ForeignTbl != old.TblId {
				continue
			}
			for _, id := range fk.ForeignCols {
				if col := changed[id]; col != nil {
					return moerr.NewErrForeignKeyColumnCannotChangeChild(ctx.GetContext(), col.Name, fk.Name, obj.SchemaName+"."+child.Name)
				}
			}
		}
	}
	return nil
}
