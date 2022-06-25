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

package tree

import (
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// FmtCtx contains formatted text of the node.
type FmtCtx struct {
	*strings.Builder
	dialectType dialect.DialectType
}

func NewFmtCtx(dialectType dialect.DialectType) *FmtCtx {
	return &FmtCtx{
		Builder:     new(strings.Builder),
		dialectType: dialectType,
	}
}

// NodeFormatter for formatted output of the node.
type NodeFormatter interface {
	Format(ctx *FmtCtx)
}

func String(node NodeFormatter, dialectType dialect.DialectType) string {
	if node == nil {
		return "<nil>"
	}

	ctx := NewFmtCtx(dialectType)
	node.Format(ctx)
	return ctx.String()
}

func (ctx *FmtCtx) PrintExpr(currentExpr Expr, expr Expr, left bool) {
	if precedenceFor(currentExpr) == Syntactic {
		expr.Format(ctx)
	} else {
		needParens := needParens(currentExpr, expr, left)
		if needParens {
			ctx.WriteByte('(')
		}
		expr.Format(ctx)
		if needParens {
			ctx.WriteByte(')')
		}
	}
}

//needParens says if we need a parenthesis
// op is the operator we are printing
// val is the value we are checking if we need parens around or not
// left let's us know if the value is on the lhs or rhs of the operator
func needParens(op, val Expr, left bool) bool {
	// Values are atomic and never need parens
	if IsValue(val) {
		return false
	}

	if areBothISExpr(op, val) {
		return true
	}

	opBinding := precedenceFor(op)
	valBinding := precedenceFor(val)

	if opBinding == Syntactic || valBinding == Syntactic {
		return false
	}

	if left {
		// for left associative operators, if the value is to the left of the operator,
		// we only need parens if the order is higher for the value expression
		return valBinding > opBinding
	}

	return valBinding >= opBinding
}

// IsValue returns true if the Expr is a string, integral or value arg.
// NULL is not considered to be a value.
func IsValue(node Expr) bool {
	switch node.(type) {
	case *NumVal, *StrVal:
		return true
	}
	return false
}

func areBothISExpr(op Expr, val Expr) bool {
	// when using IS on an IS op, we need special handling
	_, isOpIs := op.(*IsNullExpr)
	if isOpIs {
		_, isValIs := val.(*IsNullExpr)
		if isValIs {
			return true
		}
	}
	_, isOpIsNot := op.(*IsNullExpr)
	if isOpIsNot {
		_, isValIsNot := val.(*IsNullExpr)
		if isValIsNot {
			return true
		}
	}
	return false
}
