// Copyright 2026 Matrix Origin
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tree

// CollateExpr preserves the explicit collation independently of its operand.
type CollateExpr struct {
	exprImpl
	Expr      Expr
	Collation string
}

func (node *CollateExpr) Format(ctx *FmtCtx) {
	_, parenthesized := node.Expr.(*ParenExpr)
	if !parenthesized {
		ctx.WriteByte('(')
	}
	node.Expr.Format(ctx)
	if !parenthesized {
		ctx.WriteByte(')')
	}
	ctx.WriteString(" collate ")
	ctx.WriteString(node.Collation)
}

func (node *CollateExpr) Accept(v Visitor) (Expr, bool) {
	newNode, skipChildren := v.Enter(node)
	if skipChildren {
		return v.Exit(newNode)
	}
	node = newNode.(*CollateExpr)
	child, ok := node.Expr.Accept(v)
	if !ok {
		return node, false
	}
	node.Expr = child
	return v.Exit(node)
}
