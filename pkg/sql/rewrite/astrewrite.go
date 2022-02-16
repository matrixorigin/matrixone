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
	"go/constant"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
)

var logicalBinaryOps = map[tree.BinaryOp]struct{}{
	tree.BIT_OR:  {},
	tree.BIT_AND: {},
	tree.BIT_XOR: {},
}

var logicalComparisonOps = map[tree.ComparisonOp]struct{}{
	tree.EQUAL:            {},
	tree.LESS_THAN:        {},
	tree.LESS_THAN_EQUAL:  {},
	tree.GREAT_THAN:       {},
	tree.GREAT_THAN_EQUAL: {},
	tree.NOT_EQUAL:        {},
	tree.IN:               {},
	tree.NOT_IN:           {},
	tree.LIKE:             {},
	tree.NOT_LIKE:         {},
}

// AstRewrite do sql rewrite before plan build.
// todo: function just for some case we can not deal in compute-engine now. Such as ` filter condition is not a logical expression`.
// 		we should delete these codes if we can deal it in compute-engine next time.
// deal with such case:
// case 1:  rewrite normal expression in where clause to be logical expression.
// (1) rewrite `select ... where expr` to `select ... where expr != 0`
// (2) rewrite `select ... where not expr`  to `select ... where expr == 0`
// case 2:  normal view sql rewrite work ( not Materialized View)
// Tips: expr contains castExpr, unresolvedName, constant
func AstRewrite(stmt tree.Statement) tree.Statement {
	// rewrite all filter condition inside AST.
	// rewrite select statement.
	switch st := stmt.(type) {
	case *tree.Select:
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
		if binaryExpr, ok := t.Expr.(*tree.BinaryExpr); ok && isLogicalBinaryOp(binaryExpr.Op) {
			return tree.NewNotExpr(rewriteFilterCondition(binaryExpr))
		}
		if comparisonExpr, ok := t.Expr.(*tree.ComparisonExpr); ok && isLogicalComparisonOp(comparisonExpr.Op) {
			return tree.NewNotExpr(rewriteFilterCondition(comparisonExpr))
		}
		if rangeExpr, ok := t.Expr.(*tree.RangeCond); ok {
			return tree.NewNotExpr(rewriteFilterCondition(rangeExpr))
		}
		if parenExpr, ok := t.Expr.(*tree.ParenExpr); ok {
			return tree.NewNotExpr(rewriteFilterCondition(parenExpr.Expr))
		}
		if notExpr, ok := t.Expr.(*tree.NotExpr); ok {
			return tree.NewNotExpr(rewriteFilterCondition(notExpr))
		}
		return tree.NewComparisonExpr(tree.EQUAL, t.Expr, tree.NewNumVal(constant.MakeInt64(0), "0", false))
	// rewrite to != 0
	case *tree.UnresolvedName, *tree.NumVal, *tree.CastExpr:
		return tree.NewComparisonExpr(tree.NOT_EQUAL, t, tree.NewNumVal(constant.MakeInt64(0), "0", false))
	case *tree.BinaryExpr:
		if !isLogicalBinaryOp(t.Op) {
			return tree.NewComparisonExpr(tree.NOT_EQUAL, t, tree.NewNumVal(constant.MakeInt64(0), "0", false))
		}
		// rewrite in operator
		// where a in (1, 2)		----> where a = 1 or a = 2
		// where a not in (1, 2)	----> where a != 1 and a != 2
	case *tree.ComparisonExpr:
		if t.Op == tree.IN {
			if tuple, ok := t.Right.(*tree.Tuple); ok {
				if len(tuple.Exprs) == 1 {
					return tree.NewComparisonExpr(tree.EQUAL, t.Left, tuple.Exprs[0])
				} else {
					left := tree.NewComparisonExpr(tree.EQUAL, t.Left, tuple.Exprs[0])
					right := tree.NewComparisonExpr(tree.IN, t.Left, &tree.Tuple{
						Exprs: tuple.Exprs[1:],
					})
					return tree.NewOrExpr(left, rewriteFilterCondition(right))
				}
			}
		}
		if t.Op == tree.NOT_IN {
			if tuple, ok := t.Right.(*tree.Tuple); ok {
				if len(tuple.Exprs) == 1 {
					return tree.NewComparisonExpr(tree.NOT_EQUAL, t.Left, tuple.Exprs[0])
				} else {
					left := tree.NewComparisonExpr(tree.NOT_EQUAL, t.Left, tuple.Exprs[0])
					right := tree.NewComparisonExpr(tree.NOT_IN, t.Left, &tree.Tuple{
						Exprs: tuple.Exprs[1:],
					})
					return tree.NewAndExpr(left, rewriteFilterCondition(right))
				}
			}
		}
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
