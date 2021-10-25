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

package build

import (
	"fmt"
	"go/constant"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/sql/op"
	"github.com/matrixorigin/matrixone/pkg/sql/tree"
	"github.com/matrixorigin/matrixone/pkg/sqlerror"
)

func (b *build) buildExtend(o op.OP, n tree.Expr) (extend.Extend, error) {
	e, err := b.buildExpr(o, n)
	if err != nil {
		return nil, err
	}
	e = RewriteExtend(e)
	return b.pruneExtend(e)
}

func (b *build) hasAggregate(n tree.Expr) bool {
	switch e := n.(type) {
	case *tree.ParenExpr:
		return b.hasAggregate(e.Expr)
	case *tree.OrExpr:
		return b.hasAggregate(e.Left) || b.hasAggregate(e.Right)
	case *tree.NotExpr:
		return b.hasAggregate(e.Expr)
	case *tree.AndExpr:
		return b.hasAggregate(e.Left) || b.hasAggregate(e.Right)
	case *tree.UnaryExpr:
		return b.hasAggregate(e.Expr)
	case *tree.BinaryExpr:
		return b.hasAggregate(e.Left) || b.hasAggregate(e.Right)
	case *tree.ComparisonExpr:
		return b.hasAggregate(e.Left) || b.hasAggregate(e.Right)
	case *tree.FuncExpr:
		if name, ok := e.Func.FunctionReference.(*tree.UnresolvedName); ok {
			if _, ok = AggFuncs[name.Parts[0]]; ok {
				return true
			}
		}
	case *tree.CastExpr:
		return b.hasAggregate(e.Expr)
	case *tree.RangeCond:
		return b.hasAggregate(e.Left) || b.hasAggregate(e.From) || b.hasAggregate(e.To)
	}
	return false
}

func (b *build) buildExpr(o op.OP, n tree.Expr) (extend.Extend, error) {
	switch e := n.(type) {
	case *tree.NumVal:
		return buildValue(e.Value)
	case *tree.ParenExpr:
		return b.buildExpr(o, e.Expr)
	case *tree.OrExpr:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Or, Left: left, Right: right}, nil
	case *tree.NotExpr:
		ext, err := b.buildExpr(o, e.Expr)
		if err != nil {
			return nil, err
		}
		return &extend.UnaryExtend{Op: overload.Not, E: ext}, nil
	case *tree.AndExpr:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.And, Left: left, Right: right}, nil
	case *tree.XorExpr:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Xor is not support now"))
	case *tree.UnaryExpr:
		return b.buildUnary(o, e)
	case *tree.BinaryExpr:
		return b.buildBinary(o, e)
	case *tree.ComparisonExpr:
		return b.buildComparison(o, e)
	case *tree.Tuple:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
	case *tree.FuncExpr:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
	case *tree.IsNullExpr:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
	case *tree.IsNotNullExpr:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
	case *tree.Subquery:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
	case *tree.CastExpr:
		left, err := b.buildExpr(o, e.Expr)
		if err != nil {
			return nil, err
		}
		typ := types.Type{}
		switch uint8(e.Type.(*tree.T).InternalType.Oid) {
		case defines.MYSQL_TYPE_TINY:
			typ.Size = 1
			typ.Oid = types.T_int8
		case defines.MYSQL_TYPE_SHORT:
			typ.Size = 2
			typ.Oid = types.T_int16
		case defines.MYSQL_TYPE_LONG:
			typ.Size = 4
			typ.Oid = types.T_int32
		case defines.MYSQL_TYPE_LONGLONG:
			typ.Size = 8
			typ.Oid = types.T_int64
		case defines.MYSQL_TYPE_FLOAT:
			typ.Size = 4
			typ.Oid = types.T_float32
		case defines.MYSQL_TYPE_DOUBLE:
			typ.Size = 8
			typ.Oid = types.T_float64
		case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING:
			typ.Size = 24
			typ.Oid = types.T_varchar
		default:
			return nil, sqlerror.New(errno.IndeterminateDatatype, fmt.Sprintf("'%v' is not support now", n))
		}
		return &extend.BinaryExtend{
			Op:    overload.Typecast,
			Left:  left,
			Right: &extend.ValueExtend{V: vector.New(typ)},
		}, nil
	case *tree.RangeCond:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		from, err := b.buildExpr(o, e.From)
		if err != nil {
			return nil, err
		}
		to, err := b.buildExpr(o, e.To)
		if err != nil {
			return nil, err
		}
		if e.Not {
			return &extend.BinaryExtend{
				Op: overload.Or,
				Left: &extend.BinaryExtend{
					Op:    overload.LT,
					Left:  left,
					Right: from,
				},
				Right: &extend.BinaryExtend{
					Op:    overload.GT,
					Left:  left,
					Right: to,
				},
			}, nil
		}
		return &extend.BinaryExtend{
			Op: overload.And,
			Left: &extend.BinaryExtend{
				Op:    overload.GE,
				Left:  left,
				Right: from,
			},
			Right: &extend.BinaryExtend{
				Op:    overload.LE,
				Left:  left,
				Right: to,
			},
		}, nil
	case *tree.UnresolvedName:
		return b.buildAttribute(o, e)
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}

func (b *build) buildExprWithoutCheck(o op.OP, n tree.Expr) (extend.Extend, error) {
	switch e := n.(type) {
	case *tree.NumVal:
		return buildValue(e.Value)
	case *tree.ParenExpr:
		return b.buildExprWithoutCheck(o, e.Expr)
	case *tree.OrExpr:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Or, Left: left, Right: right}, nil
	case *tree.NotExpr:
		ext, err := b.buildExprWithoutCheck(o, e.Expr)
		if err != nil {
			return nil, err
		}
		return &extend.UnaryExtend{Op: overload.Not, E: ext}, nil
	case *tree.AndExpr:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.And, Left: left, Right: right}, nil
	case *tree.XorExpr:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Xor is not support now"))
	case *tree.UnaryExpr:
		return b.buildUnaryWithoutCheck(o, e)
	case *tree.BinaryExpr:
		return b.buildBinaryWithoutCheck(o, e)
	case *tree.ComparisonExpr:
		return b.buildComparisonWithoutCheck(o, e)
	case *tree.Tuple:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
	case *tree.FuncExpr:
		name, ok := e.Func.FunctionReference.(*tree.UnresolvedName)
		if !ok {
			return nil, sqlerror.New(errno.SyntaxError, fmt.Sprintf("illegal expression '%s'", e))
		}
		if _, ok := e.Exprs[0].(*tree.NumVal); ok {
			return &extend.Attribute{Name: "count(*)"}, nil
		}
		ext, err := b.buildExprWithoutCheck(o, e.Exprs[0])
		if err != nil {
			return nil, err
		}
		return &extend.Attribute{Name: fmt.Sprintf("%s(%s)", name.Parts[0], ext)}, nil
	case *tree.IsNullExpr:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
	case *tree.IsNotNullExpr:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
	case *tree.Subquery:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
	case *tree.CastExpr:
		left, err := b.buildExprWithoutCheck(o, e.Expr)
		if err != nil {
			return nil, err
		}
		typ := types.Type{}
		switch uint8(e.Type.(*tree.T).InternalType.Oid) {
		case defines.MYSQL_TYPE_TINY:
			typ.Size = 1
			typ.Oid = types.T_int8
		case defines.MYSQL_TYPE_SHORT:
			typ.Size = 2
			typ.Oid = types.T_int16
		case defines.MYSQL_TYPE_LONG:
			typ.Size = 4
			typ.Oid = types.T_int32
		case defines.MYSQL_TYPE_LONGLONG:
			typ.Size = 8
			typ.Oid = types.T_int64
		case defines.MYSQL_TYPE_FLOAT:
			typ.Size = 4
			typ.Oid = types.T_float32
		case defines.MYSQL_TYPE_DOUBLE:
			typ.Size = 8
			typ.Oid = types.T_float64
		case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING:
			typ.Size = 24
			typ.Oid = types.T_varchar
		default:
			return nil, sqlerror.New(errno.IndeterminateDatatype, fmt.Sprintf("'%v' is not support now", n))
		}
		return &extend.BinaryExtend{
			Op:    overload.Typecast,
			Left:  left,
			Right: &extend.ValueExtend{V: vector.New(typ)},
		}, nil
	case *tree.RangeCond:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		from, err := b.buildExprWithoutCheck(o, e.From)
		if err != nil {
			return nil, err
		}
		to, err := b.buildExprWithoutCheck(o, e.To)
		if err != nil {
			return nil, err
		}
		if e.Not {
			return &extend.BinaryExtend{
				Op: overload.Or,
				Left: &extend.BinaryExtend{
					Op:    overload.LT,
					Left:  left,
					Right: from,
				},
				Right: &extend.BinaryExtend{
					Op:    overload.GT,
					Left:  left,
					Right: to,
				},
			}, nil
		}
		return &extend.BinaryExtend{
			Op: overload.And,
			Left: &extend.BinaryExtend{
				Op:    overload.GE,
				Left:  left,
				Right: from,
			},
			Right: &extend.BinaryExtend{
				Op:    overload.LE,
				Left:  left,
				Right: to,
			},
		}, nil
	case *tree.UnresolvedName:
		if e.NumParts == 1 || len(e.Parts[1]) == 0 {
			return &extend.Attribute{Name: e.Parts[0]}, nil
		}
		return &extend.Attribute{Name: e.Parts[1] + "." + e.Parts[0]}, nil
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}

func (b *build) buildUnaryWithoutCheck(o op.OP, e *tree.UnaryExpr) (extend.Extend, error) {
	switch e.Op {
	case tree.UNARY_MINUS:
		ext, err := b.buildExprWithoutCheck(o, e.Expr)
		if err != nil {
			return nil, err
		}
		return &extend.UnaryExtend{Op: overload.UnaryMinus, E: ext}, nil
	case tree.UNARY_PLUS:
		return b.buildExprWithoutCheck(o, e.Expr)
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
}

func (b *build) buildBinaryWithoutCheck(o op.OP, e *tree.BinaryExpr) (extend.Extend, error) {
	switch e.Op {
	case tree.PLUS:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Plus, Left: left, Right: right}, nil
	case tree.MINUS:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Minus, Left: left, Right: right}, nil
	case tree.MULTI:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Mult, Left: left, Right: right}, nil
	case tree.MOD:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Mod, Left: left, Right: right}, nil
	case tree.DIV, tree.INTEGER_DIV:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Div, Left: left, Right: right}, nil
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
}

func (b *build) buildComparisonWithoutCheck(o op.OP, e *tree.ComparisonExpr) (extend.Extend, error) {
	switch e.Op {
	case tree.EQUAL:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.EQ, Left: left, Right: right}, nil
	case tree.LESS_THAN:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.LT, Left: left, Right: right}, nil
	case tree.LESS_THAN_EQUAL:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.LE, Left: left, Right: right}, nil
	case tree.GREAT_THAN:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.GT, Left: left, Right: right}, nil
	case tree.GREAT_THAN_EQUAL:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.GE, Left: left, Right: right}, nil
	case tree.NOT_EQUAL:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.NE, Left: left, Right: right}, nil
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
}

func (b *build) buildUnary(o op.OP, e *tree.UnaryExpr) (extend.Extend, error) {
	switch e.Op {
	case tree.UNARY_MINUS:
		ext, err := b.buildExpr(o, e.Expr)
		if err != nil {
			return nil, err
		}
		return &extend.UnaryExtend{Op: overload.UnaryMinus, E: ext}, nil
	case tree.UNARY_PLUS:
		return b.buildExpr(o, e.Expr)
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
}

func (b *build) buildBinary(o op.OP, e *tree.BinaryExpr) (extend.Extend, error) {
	switch e.Op {
	case tree.PLUS:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Plus, Left: left, Right: right}, nil
	case tree.MINUS:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Minus, Left: left, Right: right}, nil
	case tree.MULTI:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Mult, Left: left, Right: right}, nil
	case tree.MOD:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Mod, Left: left, Right: right}, nil
	case tree.DIV, tree.INTEGER_DIV:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Div, Left: left, Right: right}, nil
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
}

func (b *build) buildComparison(o op.OP, e *tree.ComparisonExpr) (extend.Extend, error) {
	switch e.Op {
	case tree.EQUAL:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.EQ, Left: left, Right: right}, nil
	case tree.LESS_THAN:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.LT, Left: left, Right: right}, nil
	case tree.LESS_THAN_EQUAL:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.LE, Left: left, Right: right}, nil
	case tree.GREAT_THAN:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.GT, Left: left, Right: right}, nil
	case tree.GREAT_THAN_EQUAL:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.GE, Left: left, Right: right}, nil
	case tree.NOT_EQUAL:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.NE, Left: left, Right: right}, nil
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
}

func (b *build) buildAttribute(o op.OP, e *tree.UnresolvedName) (extend.Extend, error) {
	if e.Star {
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
	}
	attrs := o.Attribute()
	if e.NumParts == 1 {
		if typ, ok := attrs[e.Parts[0]]; ok {
			return &extend.Attribute{Name: e.Parts[0], Type: typ.Oid}, nil
		}
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unknown column '%s' in expr", e.Parts[0]))
	}
	if typ, ok := attrs[e.Parts[0]]; ok {
		return &extend.Attribute{Name: e.Parts[0], Type: typ.Oid}, nil
	}
	name := e.Parts[1] + "." + e.Parts[0]
	if typ, ok := attrs[name]; ok {
		return &extend.Attribute{Name: name, Type: typ.Oid}, nil
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unknown column '%s' in expr", e.Parts[0]))
}

func (b *build) exprAlias(o op.OP, n tree.Expr) (tree.UnrestrictedIdentifier, error) {
	e, err := b.buildExprWithoutCheck(o, n)
	if err != nil {
		return "", err
	}
	return tree.UnrestrictedIdentifier(e.String()), nil
}

func stripParens(expr tree.Expr) tree.Expr {
	if p, ok := expr.(*tree.ParenExpr); ok {
		return stripParens(p.Expr)
	}
	return expr
}

func buildConstant(typ types.Type, n tree.Expr) (interface{}, error) {
	switch e := n.(type) {
	case *tree.NumVal:
		return buildConstantValue(typ, e.Value)
	case *tree.UnaryExpr:
		if e.Op == tree.UNARY_PLUS {
			return buildConstant(typ, e.Expr)
		}
		if e.Op == tree.UNARY_MINUS {
			v, err := buildConstant(typ, e.Expr)
			if err != nil {
				return nil, err
			}
			switch val := v.(type) {
			case int64:
				return val * -1, nil
			case float32:
				return val * -1, nil
			case float64:
				return val * -1, nil
			}
			return v, nil
		}
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}

func buildConstantValue(typ types.Type, val constant.Value) (interface{}, error) {
	switch val.Kind() {
	case constant.Unknown:
		return nil, nil
	case constant.Int:
		switch typ.Oid {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			v, _ := constant.Int64Val(val)
			return int64(v), nil
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			v, _ := constant.Uint64Val(val)
			return uint64(v), nil
		case types.T_float32:
			v, _ := constant.Float32Val(val)
			return float32(v), nil
		case types.T_float64:
			v, _ := constant.Float64Val(val)
			return float64(v), nil
		}
	case constant.Float:
		switch typ.Oid {
		case types.T_float32:
			v, _ := constant.Float32Val(val)
			return float32(v), nil
		case types.T_float64:
			v, _ := constant.Float64Val(val)
			return float64(v), nil
		}
	case constant.String:
		switch typ.Oid {
		case types.T_char, types.T_varchar:
			return constant.StringVal(val), nil
		}
	}
	return nil, sqlerror.New(errno.IndeterminateDatatype, fmt.Sprintf("unsupport value: %v", val))
}

func buildValue(val constant.Value) (extend.Extend, error) {
	switch val.Kind() {
	case constant.Int:
		vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
		vec.Ref = 1
		v, _ := constant.Int64Val(val)
		vec.Col = []int64{v}
		return &extend.ValueExtend{V: vec}, nil
	case constant.Float:
		vec := vector.New(types.Type{Oid: types.T_float64, Size: 8})
		vec.Ref = 1
		v, _ := constant.Float64Val(val)
		vec.Col = []float64{v}
		return &extend.ValueExtend{V: vec}, nil
	case constant.String:
		vec := vector.New(types.Type{Oid: types.T_varchar, Size: 24})
		vec.Ref = 1
		v := constant.StringVal(val)
		vec.Col = &types.Bytes{
			Data:    []byte(v),
			Offsets: []uint32{0},
			Lengths: []uint32{uint32(len(v))},
		}
		return &extend.ValueExtend{V: vec}, nil
	default:
		return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport value: %v", val))
	}
}
