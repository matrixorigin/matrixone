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
			if len(e.Exprs) > 0 {
				name.Parts[0] = strings.ToLower(name.Parts[0])
				if _, ok := e.Exprs[0].(*tree.NumVal); ok && name.Parts[0] == "count" {
					e.Func.FunctionReference = &tree.UnresolvedName{
						Parts: [4]string{"starcount"},
					}
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
