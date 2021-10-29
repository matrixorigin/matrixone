// Copyright 2021 Matrix Origin
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

package transform

import (
	"matrixone/pkg/sql/tree"
)

func transformSelect(stmt *tree.Select) *tree.Select {
	if sel, ok := stmt.Select.(*tree.SelectClause); ok {
		stmt.Select = transformSelectCluase(sel)
	}
	return stmt
}

func transformSelectCluase(stmt *tree.SelectClause) *tree.SelectClause {
	stmt.Exprs = transformProjection(stmt.Exprs)
	if stmt.From != nil && len(stmt.From.Tables) > 0 {
		stmt.From.Tables = transformFrom(stmt.From.Tables)
	}
	return stmt
}

func transformFrom(stmts tree.TableExprs) tree.TableExprs {
	for i, stmt := range stmts {
		stmts[i] = transformTable(stmt)
	}
	return stmts
}

func transformTable(stmt tree.TableExpr) tree.TableExpr {
	switch stmt := stmt.(type) {
	case *tree.TableName:
		return stmt
	case *tree.JoinTableExpr:
		if stmt.Right == nil {
			return transformTable(stmt.Left)
		}
		stmt.Left = transformTable(stmt.Left)
		stmt.Right = transformTable(stmt.Right)
		return stmt
	case *tree.ParenTableExpr:
		stmt.Expr = transformTable(stmt.Expr)
		return stmt
	case *tree.AliasedTableExpr:
		if len(stmt.As.Alias) == 0 {
			return transformTable(stmt.Expr)
		}
		stmt.Expr = transformTable(stmt.Expr)
		return stmt
	case *tree.Subquery:
		switch n := stmt.Select.(type) {
		case *tree.ParenSelect:
			stmt.Select = &tree.ParenSelect{Select: transformSelect(n.Select)}
		case *tree.SelectClause:
			stmt.Select = transformSelectCluase(n)
		}
	}
	return stmt
}
