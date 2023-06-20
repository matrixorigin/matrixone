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
	"fmt"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
)

// FmtCtx contains formatted text of the node.
type FmtCtx struct {
	*strings.Builder
	dialectType dialect.DialectType
	// quoteString string
	quoteString       bool
	singleQuoteString bool
}

func NewFmtCtx(dialectType dialect.DialectType, opts ...FmtCtxOption) *FmtCtx {
	ctx := &FmtCtx{
		Builder:     new(strings.Builder),
		dialectType: dialectType,
	}
	for _, opt := range opts {
		opt.Apply(ctx)
	}
	return ctx
}

type FmtCtxOption func(*FmtCtx)

func (f FmtCtxOption) Apply(ctx *FmtCtx) {
	f(ctx)
}

func WithQuoteString(quote bool) FmtCtxOption {
	return FmtCtxOption(func(ctx *FmtCtx) {
		ctx.quoteString = quote
	})
}
func WithSingleQuoteString() FmtCtxOption {
	return FmtCtxOption(func(ctx *FmtCtx) {
		ctx.singleQuoteString = true
	})
}

// NodeFormatter for formatted output of the node.
type NodeFormatter interface {
	Format(ctx *FmtCtx)
}

// Visitor Design Pattern
// NodeChecker is abstract tree Node
type NodeChecker interface {
	// `Accept` method accepts Visitor to visit itself. Visitor checks the current node
	// The returned node should replace original node.
	// The node returned by Accpet should replace the original node.
	// If OK returns false, it stops accessing other child nodes.

	//	The general implementation logic of the `Accept` method is:
	//	First, call the Visitor.`Enter` method, and assign the returned `node` to the receiver of the `Accept` method,
	//	If the returnd `skipChildren` value is true, then it is necessary to stop accessing the receiver's child node
	//	Otherwise, recursively call the` Accept` of its children nodes,
	//	Finally, don't forget to call the Visitor's `Exit` method
	Accept(v Visitor) (node Expr, ok bool)
}

// Visitor Design Pattern
// Visitor visits the ast node or sub ast nodes
type Visitor interface {
	// Call the 'Enter' method before visiting the children nodes.
	// The node type returned by the `Enter` method must be the same as the input node type
	// SkipChildren returning true means that access to child nodes should be skipped.
	Enter(n Expr) (node Expr, skipChildren bool)

	//`Exit` is called after all children nodes are visited.
	//The returned node of the `Exit` method is `Expr`, which is of the same type as the input node.
	//if `Exit` method returns OK as false ,means stop visiting.
	Exit(n Expr) (node Expr, ok bool)
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

func (ctx *FmtCtx) WriteValue(t P_TYPE, v string) (int, error) {
	if ctx.quoteString {
		switch t {
		case P_char:
			return ctx.WriteString(fmt.Sprintf("%q", v))
		default:
			return ctx.WriteString(v)
		}
	}
	if ctx.singleQuoteString && t == P_char {
		return ctx.WriteString(fmt.Sprintf("'%s'", v))
	}
	return ctx.WriteString(v)
}

func (ctx *FmtCtx) WriteStringQuote(v string) (int, error) {
	if ctx.quoteString {
		return ctx.WriteString(fmt.Sprintf("%q", v))
	} else {
		return ctx.WriteString(v)
	}
}

// needParens says if we need a parenthesis
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
