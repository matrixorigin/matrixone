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

package rewrite

import (
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

func rewriteSelect(stmt *tree.Select) *tree.Select {
	if sel, ok := stmt.Select.(*tree.SelectClause); ok {
		stmt.Select = rewriteSelectCluase(sel)
	}
	return stmt
}

func rewriteSelectCluase(stmt *tree.SelectClause) *tree.SelectClause {
	stmt.Exprs = rewriteProjection(stmt.Exprs)
	if stmt.From != nil && len(stmt.From.Tables) > 0 {
		stmt.From.Tables = rewriteFrom(stmt.From.Tables)
	}
	return stmt
}

func rewriteFrom(stmts tree.TableExprs) tree.TableExprs {
	for i, stmt := range stmts {
		stmts[i] = rewriteTable(stmt)
	}
	return stmts
}

func rewriteTable(stmt tree.TableExpr) tree.TableExpr {
	switch stmt := stmt.(type) {
	case *tree.TableName:
		return stmt
	case *tree.JoinTableExpr:
		if stmt.Right == nil {
			return rewriteTable(stmt.Left)
		}
		stmt.Left = rewriteTable(stmt.Left)
		stmt.Right = rewriteTable(stmt.Right)
		return stmt
	case *tree.ParenTableExpr:
		stmt.Expr = rewriteTable(stmt.Expr)
		return stmt
	case *tree.AliasedTableExpr:
		if len(stmt.As.Alias) == 0 {
			return rewriteTable(stmt.Expr)
		}
		stmt.Expr = rewriteTable(stmt.Expr)
		return stmt
	}
	return stmt
}
