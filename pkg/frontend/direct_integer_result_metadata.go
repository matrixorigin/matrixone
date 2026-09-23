// Copyright 2026 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package frontend

import (
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/pb/plan"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

// directIntegerResultLengths describes wire presentation, never execution or
// persisted schema. MySQL presents these direct calls as signed LONGLONG even
// though their stored result type is INT. The AST retains call identity after
// constant folding. Do not infer this property from aliases or arbitrary child
// expressions, or reconstruct ordinals across star expansion. Materialized and
// set-operation results have separate contracts and are deliberately excluded.
func directIntegerResultLengths(stmt tree.Statement, cols []*plan.ColDef) []uint32 {
	selectStmt, ok := stmt.(*tree.Select)
	if !ok || selectStmt == nil {
		return nil
	}
	for {
		paren, ok := selectStmt.Select.(*tree.ParenSelect)
		if !ok {
			break
		}
		selectStmt = paren.Select
		if selectStmt == nil {
			return nil
		}
	}
	clause, ok := selectStmt.Select.(*tree.SelectClause)
	if !ok || clause.Distinct || clause.Option&(tree.QuerySpecOptionDistinct|tree.QuerySpecOptionDistinctRow) != 0 ||
		clause.GroupBy != nil || clause.Having != nil || len(clause.Exprs) != len(cols) {
		return nil
	}
	var lengths []uint32
	for i, projection := range clause.Exprs {
		expr := projection.Expr
		for {
			paren, ok := expr.(*tree.ParenExpr)
			if !ok {
				break
			}
			expr = paren.Expr
		}
		switch expr := expr.(type) {
		case tree.UnqualifiedStar, *tree.UnqualifiedStar:
			return nil
		case *tree.UnresolvedName:
			if expr.Star {
				return nil
			}
		}
		fn, ok := expr.(*tree.FuncExpr)
		if !ok || fn.WindowSpec != nil || cols[i] == nil || cols[i].Typ.Id != int32(types.T_int32) {
			continue
		}
		var length uint32
		switch rewriteFuncExprName(fn) {
		case "find_in_set", "findinset":
			length = 3
		case "strcmp":
			length = 2
		default:
			continue
		}
		if lengths == nil {
			lengths = make([]uint32, len(cols))
		}
		lengths[i] = length
	}
	return lengths
}

func applyDirectIntegerResultMetadata(column *MysqlColumn, lengths []uint32, ordinal int) {
	if ordinal >= len(lengths) || lengths[ordinal] == 0 {
		return
	}
	column.SetColumnType(defines.MYSQL_TYPE_LONGLONG)
	column.SetSigned(true)
	column.SetLength(lengths[ordinal])
	column.SetDecimal(0)
}
