package rewrite

import (
	"matrixone/pkg/sql/tree"
	"strings"
)

func rewriteProjection(ns tree.SelectExprs) tree.SelectExprs {
	for i, n := range ns {
		ns[i].Expr = rewriteExpr(n.Expr)
	}
	return ns
}

func rewriteExpr(n tree.Expr) tree.Expr {
	switch e := n.(type) {
	case *tree.ParenExpr:
		e.Expr = rewriteExpr(e.Expr)
		return e
	case *tree.OrExpr:
		e.Left = rewriteExpr(e.Left)
		e.Right = rewriteExpr(e.Right)
		return e
	case *tree.NotExpr:
		e.Expr = rewriteExpr(e.Expr)
		return e
	case *tree.AndExpr:
		e.Left = rewriteExpr(e.Left)
		e.Right = rewriteExpr(e.Right)
		return e
	case *tree.XorExpr:
		e.Left = rewriteExpr(e.Left)
		e.Right = rewriteExpr(e.Right)
		return e
	case *tree.UnaryExpr:
		e.Expr = rewriteExpr(e.Expr)
		return e
	case *tree.BinaryExpr:
		e.Left = rewriteExpr(e.Left)
		e.Right = rewriteExpr(e.Right)
		return e
	case *tree.ComparisonExpr:
		e.Left = rewriteExpr(e.Left)
		e.Right = rewriteExpr(e.Right)
		return e
	case *tree.Tuple:
	case *tree.FuncExpr:
		if name, ok := e.Func.FunctionReference.(*tree.UnresolvedName); ok {
			name.Parts[0] = strings.ToLower(name.Parts[0])
			if _, ok := e.Exprs[0].(*tree.NumVal); ok && name.Parts[0] == "count" {
				e.Func.FunctionReference = &tree.UnresolvedName{
					Parts: [4]string{"starcount"},
				}
			}
		}
		return e
	case *tree.CastExpr:
		e.Expr = rewriteExpr(e.Expr)
		return e
	}
	return n
}
