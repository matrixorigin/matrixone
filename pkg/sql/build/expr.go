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
	"errors"
	"fmt"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/sql/op"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sqlerror"
	"go/constant"
	"math"
	"strconv"
	"strings"
)

var (
	// errors within buildConstant.
	errConstantOutRange = sqlerror.New(errno.DataException, "constant value out of range")
	errBinaryOutRange = sqlerror.New(errno.DataException, "binary result out of range")
	errUnaryOutRange = sqlerror.New(errno.DataException, "unary result out of range")
)

func (b *build) buildExtend(o op.OP, n tree.Expr) (extend.Extend, error) {
	e, err := b.buildExpr(o, n)
	if err != nil {
		return nil, err
	}
	e = RewriteExtend(e)
	return b.pruneExtend(e, false)
}

// buildProjectionExtend build extend for select-list and projection
// do similar work likes buildExtend but without RewriteExtend and some prune work in pruneExtend
func (b *build) buildProjectionExtend(o op.OP, n tree.Expr) (extend.Extend, error) {
	e, err := b.buildExpr(o, n)
	if err != nil {
		return nil, err
	}
	return b.pruneExtend(e, false)
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
		ext, err := b.buildExprWithoutCheck(o, e.Expr)
		if err != nil {
			return nil, err
		}
		return &extend.ParenExtend{E: ext}, nil
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
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e.Op))
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
	case tree.DIV:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Div, Left: left, Right: right}, nil
	case tree.INTEGER_DIV:
		left, err := b.buildExprWithoutCheck(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExprWithoutCheck(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.IntegerDiv, Left: left, Right: right}, nil
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e.Op))
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
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e.Op))
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
	case tree.DIV:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Div, Left: left, Right: right}, nil
	case tree.INTEGER_DIV:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.IntegerDiv, Left: left, Right: right}, nil
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
	case tree.LIKE:
		left, err := b.buildExpr(o, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := b.buildExpr(o, e.Right)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Like, Left: left, Right: right}, nil
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e.Op.ToString()))
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
		return buildConstantValue(typ, e)
	case *tree.UnaryExpr:
		if e.Op == tree.UNARY_PLUS {
			return buildConstant(typ, e.Expr)
		}
		if e.Op == tree.UNARY_MINUS {
			switch n := e.Expr.(type) {
			case *tree.NumVal:
				return buildConstantValue(typ, tree.NewNumVal(n.Value, "-" + n.String(), true))
			}

			v, err := buildConstant(typ, e.Expr)
			if err != nil {
				return nil, err
			}
			switch val := v.(type) {
			case int64:
				return val * -1, nil
			case uint64:
				if val != 0 {
					return nil, errUnaryOutRange
				}
			case float32:
				return val * -1, nil
			case float64:
				return val * -1, nil
			}
			return v, nil
		}
	case *tree.BinaryExpr:
		var floatResult float64
		var argTyp = types.Type{Oid: types.T_float64, Size: 8}
		_ = floatResult
		// build values of Part left and Part right.
		left, err := buildConstant(argTyp, e.Left)
		if err != nil {
			return nil, err
		}
		right, err := buildConstant(argTyp, e.Right)
		if err != nil {
			return nil, err
		}
		// evaluate the result and make sure binary result is within range of float64.
		lf, rf := left.(float64), right.(float64)
		switch e.Op {
		case tree.PLUS:
			floatResult = lf + rf
			if lf > 0 && rf > 0 && floatResult <= 0 {
				return nil, errBinaryOutRange
			}
			if lf < 0 && rf < 0 && floatResult >= 0 {
				return nil, errBinaryOutRange
			}
		case tree.MINUS:
			floatResult = lf - rf
			if lf < 0 && rf > 0 && floatResult >= 0 {
				return nil, errBinaryOutRange
			}
			if lf > 0 && rf < 0 && floatResult <= 0 {
				return nil, errBinaryOutRange
			}
		case tree.MULTI:
			floatResult = lf * rf
			if floatResult < 0 {
				if (lf > 0 && rf > 0) || (lf < 0 && rf < 0) {
					return nil, errBinaryOutRange
				}
			} else if floatResult > 0 {
				if (lf > 0 && rf < 0) || (lf < 0 && rf > 0) {
					return nil, errBinaryOutRange
				}
			}
		case tree.DIV:
			if rf == 0 {
				return nil, ErrDivByZero
			}
			floatResult = lf / rf
			if floatResult < 0 {
				if (lf > 0 && rf > 0) || (lf < 0 && rf < 0) {
					return nil, errBinaryOutRange
				}
			} else if floatResult > 0 {
				if (lf > 0 && rf < 0) || (lf < 0 && rf > 0) {
					return nil, errBinaryOutRange
				}
			}
		case tree.INTEGER_DIV:
			if rf == 0 {
				return nil, ErrDivByZero
			}
			tempResult := lf / rf
			if tempResult > math.MaxInt64 || tempResult < math.MinInt64 {
				return nil, errBinaryOutRange
			}
			floatResult = float64(int64(tempResult))
		case tree.MOD:
			if rf == 0 {
				return nil, ErrZeroModulus
			}
			tempResult := lf / rf
			if tempResult > math.MaxInt64 || tempResult < math.MinInt64 {
				return nil, errBinaryOutRange
			}
			floatResult = lf - tempResult * rf
		default:
			return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e.Op))
		}
		// buildConstant should make sure result is within int64 or uint64 or float32 or float64
		switch typ.Oid {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			if floatResult > 0 {
				if floatResult + 0.5 > math.MaxInt64 {
					return nil, errBinaryOutRange
				}
				return int64(floatResult + 0.5), nil
			} else if floatResult < 0 {
				if floatResult - 0.5 < math.MinInt64 {
					return nil, errBinaryOutRange
				}
				return int64(floatResult - 0.5), nil
			}
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			if floatResult < 0 || floatResult + 0.5 > math.MaxInt64{
				return nil, errBinaryOutRange
			}
			return uint64(floatResult + 0.5), nil
		case types.T_float32:
			if floatResult == 0 {
				return float32(0), nil
			}
			if floatResult > math.MaxFloat32 || floatResult < math.SmallestNonzeroFloat32 {
				return nil, errBinaryOutRange
			}
			return float32(floatResult), nil
		case types.T_float64:
			return floatResult, nil
		default:
			return nil, errors.New(fmt.Sprintf("unexpected return type '%v' for binary expression '%v'", typ, e.Op))
		}
	}
	return nil, sqlerror.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
}

func buildConstantValue(typ types.Type, num *tree.NumVal) (interface{}, error) {
	val := num.Value
	str := num.String()

	switch val.Kind() {
	case constant.Unknown:
		return nil, nil
	case constant.Int:
		switch typ.Oid {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			if num.Negative() {
				v, _ := constant.Uint64Val(val)
				if v > -math.MinInt64 {
					return nil, errConstantOutRange
				}
				return int64(-v), nil
			} else {
				v, _ := constant.Int64Val(val)
				if v < 0 {
					return nil, errConstantOutRange
				}
				return int64(v), nil
			}
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			v, _ := constant.Uint64Val(val)
			if num.Negative() {
				if v != 0 {
					return nil, errConstantOutRange
				}
			}
			return uint64(v), nil
		case types.T_float32:
			v, _ := constant.Float32Val(val)
			if num.Negative() {
				return float32(-v), nil
			}
			return float32(v), nil
		case types.T_float64:
			v, _ := constant.Float64Val(val)
			if num.Negative() {
				return float64(-v), nil
			}
			return float64(v), nil
		}
	case constant.Float:
		switch typ.Oid {
		case types.T_int64:
			parts := strings.Split(str, ".")
			if len(parts) <= 1 { // integer constant within int64 range will be constant.Int but not constant.Float.
				return nil, errConstantOutRange
			}
			v, err := strconv.ParseInt(parts[0], 10, 64)
			if err != nil {
				return nil, errConstantOutRange
			}
			if len(parts[1]) > 0 && parts[1][0] >= '5' {
				if num.Negative() {
					if v - 1 > v {
						return nil, errConstantOutRange
					}
					v--
				} else {
					if v + 1 < v {
						return nil, errConstantOutRange
					}
					v++
				}
			}
			return v, nil
		case types.T_uint64:
			parts := strings.Split(str, ".")
			v, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil || len(parts) == 1 {
				return v, errConstantOutRange
			}
			if v < 0 {
				return nil, errConstantOutRange
			}
			if len(parts[1]) > 0 && parts[1][0] >= '5' {
				if v + 1 < v {
					return nil, errConstantOutRange
				}
				v++
			}
			return v, nil
		case types.T_float32:
			v, _ := constant.Float32Val(val)
			if num.Negative() {
				return float32(-v), nil
			}
			return float32(v), nil
		case types.T_float64:
			v, _ := constant.Float64Val(val)
			if num.Negative() {
				return float64(-v), nil
			}
			return float64(v), nil
		}
	case constant.String:
		if !num.Negative() {
			switch typ.Oid {
			case types.T_char, types.T_varchar:
				return constant.StringVal(val), nil
			}
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
