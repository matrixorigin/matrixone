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
	"go/constant"
)

var logicalBinaryOps = map[tree.BinaryOp] struct{} {
	tree.BIT_OR: {},
	tree.BIT_AND: {},
	tree.BIT_XOR: {},
}

var logicalComparisonOps = map[tree.ComparisonOp] struct{} {
	tree.EQUAL: {},
	tree.LESS_THAN: {},
	tree.LESS_THAN_EQUAL: {},
	tree.GREAT_THAN: {},
	tree.GREAT_THAN_EQUAL: {},
	tree.NOT_EQUAL: {},
	tree.IN: {},
	tree.NOT_IN: {},
	tree.LIKE: {},
	tree.NOT_LIKE: {},
}

// AstRewrite do sql rewrite before plan build.
// deal with such case:
// case 1:  rewrite normal expression in where clause to be logical expression.
// (1) rewrite `select ... where expr` to `select ... where expr != 0`
// (2) rewrite `select ... where not expr`  to `select ... where expr == 0`
// case 2:  normal view sql rewrite work ( not Materialized View)
// Tips: expr contains castExpr, unresolvedName, constant
func AstRewrite(stmt tree.Statement) tree.Statement {
	// rewrite all filter condition inside AST.
	// rewrite select statement.
	if st, ok := stmt.(*tree.Select); ok {
		switch t := st.Select.(type) {
		case *tree.UnionClause:
			t.Left, t.Right = AstRewrite(t.Left), AstRewrite(t.Right)
		case *tree.SelectClause:
			if t.Where != nil {
				t.Where.Expr = rewriteFilterCondition(t.Where.Expr)
			}
			if t.From != nil { // deal with sub-query
				for i := range t.From.Tables {
					t.From.Tables[i] = subTableRewrite(t.From.Tables[i])
				}
			}
		case *tree.Subquery:
			t.Select = AstRewrite(t.Select)
		}
		return st
	}
	// rewrite insert statement.
	// rewrite update statement.
	// rewrite delete statement.
	return stmt
}

func rewriteFilterCondition(expr tree.Expr) tree.Expr {
	if expr == nil {
		return nil
	}
	switch t := expr.(type) {
	// needn't rewrite in this layer
	case *tree.AndExpr:
		return tree.NewAndExpr(rewriteFilterCondition(t.Left), rewriteFilterCondition(t.Right))
	case *tree.OrExpr:
		return tree.NewOrExpr(rewriteFilterCondition(t.Left), rewriteFilterCondition(t.Right))
	case *tree.ParenExpr:
		return tree.NewParenExpr(rewriteFilterCondition(t.Expr))
	// rewrite to = 0
	case *tree.NotExpr:
		if binaryExpr, ok := t.Expr.(*tree.BinaryExpr); ok && isLogicalBinaryOp(binaryExpr.Op){
			return tree.NewNotExpr(rewriteFilterCondition(binaryExpr))
		}
		if comparisonExpr, ok := t.Expr.(*tree.ComparisonExpr); ok && isLogicalComparisonOp(comparisonExpr.Op){
			return tree.NewNotExpr(rewriteFilterCondition(comparisonExpr))
		}
		if parenExpr, ok := t.Expr.(*tree.ParenExpr); ok {
			return tree.NewNotExpr(rewriteFilterCondition(parenExpr))
		}
		if notExpr, ok := t.Expr.(*tree.NotExpr); ok {
			return tree.NewNotExpr(rewriteFilterCondition(notExpr))
		}
		return tree.NewComparisonExpr(tree.EQUAL, t.Expr, tree.NewNumVal(constant.MakeInt64(0), "0", false))
	// rewrite to != 0
	case *tree.UnresolvedName, *tree.NumVal, *tree.CastExpr:
		return tree.NewComparisonExpr(tree.NOT_EQUAL, t, tree.NewNumVal(constant.MakeInt64(0), "0", false))
	}
	return expr
}

func isLogicalBinaryOp(op tree.BinaryOp) bool {
	_, ok := logicalBinaryOps[op]
	return ok
}

func isLogicalComparisonOp(op tree.ComparisonOp) bool {
	_, ok := logicalComparisonOps[op]
	return ok
}

func subTableRewrite(t tree.TableExpr) tree.TableExpr {
	switch subTable := t.(type) {
	case *tree.JoinTableExpr:
		if onCondition, okk := subTable.Cond.(*tree.OnJoinCond); okk {
			onCondition.Expr = rewriteFilterCondition(onCondition.Expr)
		}
		if subQuery, okk := subTable.Left.(*tree.Subquery); okk {
			subTable.Left = AstRewrite(subQuery)
		}
		if subQuery, okk := subTable.Right.(*tree.Subquery); okk {
			subTable.Right = AstRewrite(subQuery)
		}
	case *tree.ParenTableExpr:
		subTable = tree.NewParenTableExpr(subTableRewrite(subTable.Expr))
	case *tree.UnresolvedName: // Do nothing.
	}
	return t
}