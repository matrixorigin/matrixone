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

package plan

import (
	"fmt"
	"go/constant"
	"math"
	"strconv"
	"strings"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/defines"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/dialect"
	"github.com/matrixorigin/matrixone/pkg/sql/parsers/tree"
	"github.com/matrixorigin/matrixone/pkg/sql/util"
	"github.com/matrixorigin/matrixone/pkg/sql/viewexec/transformer"
)

var (
	// errors may happen while building constant
	errConstantOutRange = errors.New(errno.DataException, "constant value out of range")
	errBinaryOutRange   = errors.New(errno.DataException, "binary result out of range")
	errUnaryOutRange    = errors.New(errno.DataException, "unary result out of range")
)

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
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("unsupport value: %v", val))
	}
}

func (b *build) buildNot(e *tree.NotExpr, qry *Query, fn func(tree.Expr, *Query) (extend.Extend, error)) (extend.Extend, error) {
	ext, err := fn(e.Expr, qry)
	if err != nil {
		return nil, err
	}
	return &extend.UnaryExtend{Op: overload.Not, E: ext}, nil
}

func (b *build) buildOr(e *tree.OrExpr, qry *Query, fn func(tree.Expr, *Query) (extend.Extend, error)) (extend.Extend, error) {
	left, err := fn(e.Left, qry)
	if err != nil {
		return nil, err
	}
	right, err := fn(e.Right, qry)
	if err != nil {
		return nil, err
	}
	return &extend.BinaryExtend{Op: overload.Or, Left: left, Right: right}, nil
}

func (b *build) buildAnd(e *tree.AndExpr, qry *Query, fn func(tree.Expr, *Query) (extend.Extend, error)) (extend.Extend, error) {
	left, err := fn(e.Left, qry)
	if err != nil {
		return nil, err
	}
	right, err := fn(e.Right, qry)
	if err != nil {
		return nil, err
	}
	return &extend.BinaryExtend{Op: overload.And, Left: left, Right: right}, nil
}

func (b *build) buildBetween(e *tree.RangeCond, qry *Query, fn func(tree.Expr, *Query) (extend.Extend, error)) (extend.Extend, error) {
	left, err := fn(e.Left, qry)
	if err != nil {
		return nil, err
	}
	from, err := fn(e.From, qry)
	if err != nil {
		return nil, err
	}
	to, err := fn(e.To, qry)
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
}

func (b *build) buildCast(e *tree.CastExpr, qry *Query, fn func(tree.Expr, *Query) (extend.Extend, error)) (extend.Extend, error) {
	left, err := fn(e.Expr, qry)
	if err != nil {
		return nil, err
	}
	typ := types.Type{}
	switch uint8(e.Type.(*tree.T).InternalType.Oid) {
	case defines.MYSQL_TYPE_TINY:
		typ.Size = 1
		if e.Type.(*tree.T).InternalType.Unsigned {
			typ.Oid = types.T_uint8
		} else {
			typ.Oid = types.T_int8
		}
	case defines.MYSQL_TYPE_SHORT:
		typ.Size = 2
		if e.Type.(*tree.T).InternalType.Unsigned {
			typ.Oid = types.T_uint16
		} else {
			typ.Oid = types.T_int16
		}
	case defines.MYSQL_TYPE_LONG:
		typ.Size = 4
		if e.Type.(*tree.T).InternalType.Unsigned {
			typ.Oid = types.T_uint32
		} else {
			typ.Oid = types.T_int32
		}
	case defines.MYSQL_TYPE_LONGLONG:
		typ.Size = 8
		if e.Type.(*tree.T).InternalType.Unsigned {
			typ.Oid = types.T_uint64
		} else {
			typ.Oid = types.T_int64
		}
	case defines.MYSQL_TYPE_FLOAT:
		typ.Size = 4
		typ.Oid = types.T_float32
	case defines.MYSQL_TYPE_DOUBLE:
		typ.Size = 8
		typ.Oid = types.T_float64
	case defines.MYSQL_TYPE_DATE:
		typ.Size = 4
		typ.Oid = types.T_date
	case defines.MYSQL_TYPE_DATETIME:
		typ.Size = 8
		typ.Oid = types.T_datetime
	case defines.MYSQL_TYPE_VARCHAR, defines.MYSQL_TYPE_VAR_STRING:
		typ.Size = 24
		typ.Oid = types.T_varchar
	default:
		return nil, errors.New(errno.IndeterminateDatatype, fmt.Sprintf("'%v' is not support now", e))
	}
	return &extend.BinaryExtend{
		Op:    overload.Typecast,
		Left:  left,
		Right: &extend.ValueExtend{V: vector.New(typ)},
	}, nil
}

func (b *build) buildUnary(e *tree.UnaryExpr, qry *Query, fn func(tree.Expr, *Query) (extend.Extend, error)) (extend.Extend, error) {
	switch e.Op {
	case tree.UNARY_MINUS:
		ext, err := fn(e.Expr, qry)
		if err != nil {
			return nil, err
		}
		return &extend.UnaryExtend{Op: overload.UnaryMinus, E: ext}, nil
	case tree.UNARY_PLUS:
		return fn(e.Expr, qry)
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
}

// flg indicates whether the aggregation function is accepted
func (b *build) buildFunc(flg bool, e *tree.FuncExpr, qry *Query, fn func(tree.Expr, *Query) (extend.Extend, error)) (extend.Extend, error) {
	name, ok := e.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
	}
	funcName := strings.ToLower(name.Parts[0])
	if !flg {
		if _, ok := transformer.TransformerNamesMap[funcName]; ok {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Invalid use of group function"))
		}
		args := make([]extend.Extend, len(e.Exprs))
		{
			var err error

			for i, expr := range e.Exprs {
				if args[i], err = fn(expr, qry); err != nil {
					return nil, err
				}
			}
		}
		return buildFunctionExtend(&extend.FuncExtend{Name: funcName, Args: args})
	}
	if op, ok := transformer.TransformerNamesMap[funcName]; ok {
		if len(e.Exprs) > 1 {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Illegal function call '%s'", e))
		}
		if e.Type == tree.FUNC_TYPE_DISTINCT || e.Type == tree.FUNC_TYPE_ALL {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("function type not support now"))
		}
		return b.buildAggregation(op, funcName, e.Exprs[0], qry, fn)
	}
	args := make([]extend.Extend, len(e.Exprs))
	{
		var err error

		for i, expr := range e.Exprs {
			if args[i], err = fn(expr, qry); err != nil {
				return nil, err
			}
		}
	}
	return buildFunctionExtend(&extend.FuncExtend{Name: funcName, Args: args})
}

func (b *build) buildBinary(e *tree.BinaryExpr, qry *Query, fn func(tree.Expr, *Query) (extend.Extend, error)) (extend.Extend, error) {
	switch e.Op {
	case tree.PLUS:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Plus, Left: left, Right: right}, nil
	case tree.MINUS:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Minus, Left: left, Right: right}, nil
	case tree.MULTI:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Mult, Left: left, Right: right}, nil
	case tree.MOD:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Mod, Left: left, Right: right}, nil
	case tree.DIV:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Div, Left: left, Right: right}, nil
	case tree.INTEGER_DIV:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.IntegerDiv, Left: left, Right: right}, nil
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
}

func (b *build) buildComparison(e *tree.ComparisonExpr, qry *Query, fn func(tree.Expr, *Query) (extend.Extend, error)) (extend.Extend, error) {
	switch e.Op {
	case tree.EQUAL:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.EQ, Left: left, Right: right}, nil
	case tree.LESS_THAN:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.LT, Left: left, Right: right}, nil
	case tree.LESS_THAN_EQUAL:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.LE, Left: left, Right: right}, nil
	case tree.GREAT_THAN:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.GT, Left: left, Right: right}, nil
	case tree.GREAT_THAN_EQUAL:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.GE, Left: left, Right: right}, nil
	case tree.NOT_EQUAL:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.NE, Left: left, Right: right}, nil
	case tree.LIKE:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Like, Left: left, Right: right}, nil
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
}

func (b *build) buildAttribute(e *tree.UnresolvedName, qry *Query) (extend.Extend, error) {
	if e.Star {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
	}
	switch e.NumParts {
	case 1:
		for _, attr := range qry.Scope.Result.Attrs {
			if attr == e.Parts[0] {
				return &extend.Attribute{
					Name: attr,
					Type: qry.Scope.Result.AttrsMap[attr].Type.Oid,
				}, nil
			}
		}
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Unknown column '%v'", tree.String(e, dialect.MYSQL)))
	case 2:
		if e.Parts[1] != qry.Scope.Name {
			name := e.Parts[1] + "." + e.Parts[0]
			for _, attr := range qry.Scope.Result.Attrs {
				if attr == name {
					return &extend.Attribute{
						Name: attr,
						Type: qry.Scope.Result.AttrsMap[attr].Type.Oid,
					}, nil
				}
			}
		} else {
			for _, attr := range qry.Scope.Result.Attrs {
				if attr == e.Parts[0] {
					return &extend.Attribute{
						Name: attr,
						Type: qry.Scope.Result.AttrsMap[attr].Type.Oid,
					}, nil
				}
			}
		}
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Unknown column '%v'", tree.String(e, dialect.MYSQL)))
	default:
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
	}
}

func (b *build) buildAggregation(op int, name string, n tree.Expr, qry *Query, fn func(tree.Expr, *Query) (extend.Extend, error)) (extend.Extend, error) {
	if _, ok := n.(*tree.NumVal); ok && op == transformer.Count { // count(*)
		name := qry.Scope.Result.Attrs[0]
		qry.Aggs = append(qry.Aggs, &Aggregation{
			Ref:   1,
			Name:  name,
			Alias: "count(*)",
			Type:  types.T_int64,
			Op:    transformer.StarCount,
			E: &extend.Attribute{
				Name: name,
				Type: qry.Scope.Result.AttrsMap[name].Type.Oid,
			},
		})
		return &extend.Attribute{
			Name: "count(*)",
			Type: transformer.ReturnType(op, types.T_any),
		}, nil
	}
	e, err := fn(n, qry)
	if err != nil {
		return nil, err
	}
	alias := fmt.Sprintf("%s(%s)", name, e)
	qry.Aggs = append(qry.Aggs, &Aggregation{
		E:     e,
		Op:    op,
		Ref:   1,
		Name:  e.String(),
		Alias: alias,
		Type:  transformer.ReturnType(op, e.ReturnType()),
	})
	return &extend.Attribute{
		Name: alias,
		Type: transformer.ReturnType(op, e.ReturnType()),
	}, nil
}

func buildConstant(typ types.Type, n tree.Expr) (interface{}, error) {
	switch e := n.(type) {
	case *tree.ParenExpr:
		return buildConstant(typ, e.Expr)
	case *tree.NumVal:
		return buildConstantValue(typ, e)
	case *tree.UnaryExpr:
		if e.Op == tree.UNARY_PLUS {
			return buildConstant(typ, e.Expr)
		}
		if e.Op == tree.UNARY_MINUS {
			switch n := e.Expr.(type) {
			case *tree.NumVal:
				return buildConstantValue(typ, tree.NewNumVal(n.Value, "-"+n.String(), true))
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
			tempResult := int(lf / rf)
			if tempResult > math.MaxInt64 || tempResult < math.MinInt64 {
				return nil, errBinaryOutRange
			}
			floatResult = lf - float64(tempResult)*rf
		default:
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e.Op))
		}
		// buildConstant should make sure result is within int64 or uint64 or float32 or float64
		switch typ.Oid {
		case types.T_int8, types.T_int16, types.T_int32, types.T_int64:
			if floatResult > 0 {
				if floatResult+0.5 > math.MaxInt64 {
					return nil, errBinaryOutRange
				}
				return int64(floatResult + 0.5), nil
			} else if floatResult < 0 {
				if floatResult-0.5 < math.MinInt64 {
					return nil, errBinaryOutRange
				}
				return int64(floatResult - 0.5), nil
			}
			return int64(floatResult), nil
		case types.T_uint8, types.T_uint16, types.T_uint32, types.T_uint64:
			if floatResult < 0 || floatResult+0.5 > math.MaxInt64 {
				return nil, errBinaryOutRange
			}
			return uint64(floatResult + 0.5), nil
		case types.T_float32:
			if floatResult == 0 {
				return float32(0), nil
			}
			if floatResult > math.MaxFloat32 || floatResult < -math.MaxFloat32 {
				return nil, errBinaryOutRange
			}
			return float32(floatResult), nil
		case types.T_float64:
			return floatResult, nil
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("unexpected return type '%v' for binary expression '%v'", typ, e.Op))
		}
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
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
		case types.T_date:
			if !num.Negative() {
				return types.ParseDate(str)
			}
		case types.T_datetime:
			if !num.Negative() {
				return types.ParseDatetime(str)
			}
		}
	case constant.Float:
		switch typ.Oid {
		case types.T_int64, types.T_int32, types.T_int16, types.T_int8:
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
					if v-1 > v {
						return nil, errConstantOutRange
					}
					v--
				} else {
					if v+1 < v {
						return nil, errConstantOutRange
					}
					v++
				}
			}
			return v, nil
		case types.T_uint64, types.T_uint32, types.T_uint16, types.T_uint8:
			parts := strings.Split(str, ".")
			v, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil || len(parts) == 1 {
				return v, errConstantOutRange
			}
			if v < 0 {
				return nil, errConstantOutRange
			}
			if len(parts[1]) > 0 && parts[1][0] >= '5' {
				if v+1 < v {
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
		case types.T_datetime:
			return types.ParseDatetime(str)
		}
	case constant.String:
		if !num.Negative() {
			switch typ.Oid {
			case types.T_char, types.T_varchar:
				return constant.StringVal(val), nil
			case types.T_date:
				return types.ParseDate(constant.StringVal(val))
			case types.T_datetime:
				return types.ParseDatetime(constant.StringVal(val))
			}
		}
	}
	return nil, errors.New(errno.IndeterminateDatatype, fmt.Sprintf("unsupport value: %v", val))
}

func buildFunctionExtend(fe *extend.FuncExtend) (extend.Extend, error) {
	op, ok := extend.FunctionRegistry[fe.Name]
	if !ok {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("function '%s' is not support now", fe.Name))
	}
	typ, ok := overload.OpTypes[op]
	if !ok {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("function '%s' is not fully registered", fe.Name))
	}
	switch typ {
	case overload.Unary:
		if len(fe.Args) != 1 {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("wrong parameters for function '%s'", fe.Name))
		}
		return &extend.UnaryExtend{Op: op, E: fe.Args[0]}, nil
	case overload.Binary:
		if len(fe.Args) != 2 {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("wrong parameters for function '%s'", fe.Name))
		}
		return &extend.BinaryExtend{Op: op, Left: fe.Args[0], Right: fe.Args[1]}, nil
	default:
		return &extend.MultiExtend{Op: op, Args: fe.Args}, nil
	}
}

func stripEqual(e extend.Extend) (string, string, bool) {
	if v, ok := e.(*extend.BinaryExtend); ok {
		if v.Op == overload.EQ {
			if left, ok := v.Left.(*extend.Attribute); ok {
				if right, ok := v.Right.(*extend.Attribute); ok {
					return left.Name, right.Name, true
				}
			}
		}
	}
	return "", "", false
}

func pruneExtendAttribute(e extend.Extend) extend.Extend {
	switch v := e.(type) {
	case *extend.UnaryExtend:
		v.E = pruneExtendAttribute(v.E)
	case *extend.ParenExtend:
		v.E = pruneExtendAttribute(v.E)
	case *extend.Attribute:
		_, name := util.SplitTableAndColumn(v.Name)
		v.Name = name
	case *extend.BinaryExtend:
		v.Left = pruneExtendAttribute(v.Left)
		v.Right = pruneExtendAttribute(v.Right)
	}
	return e
}

func pruneExtend(e extend.Extend) extend.Extend {
	switch v := e.(type) {
	case *extend.UnaryExtend:
		v.E = pruneExtend(v.E)
	case *extend.ParenExtend:
		v.E = pruneExtend(v.E)
	case *extend.Attribute:
		_, name := util.SplitTableAndColumn(v.Name)
		v.Name = name
	case *extend.BinaryExtend:
		v.Left = pruneExtend(v.Left)
		v.Right = pruneExtend(v.Right)
	case *extend.MultiExtend:
		for i, arg := range v.Args {
			v.Args[i] = pruneExtend(arg)
		}
	}
	return e
}

func extendsToAndExtend(es []extend.Extend) extend.Extend {
	if len(es) == 0 {
		return nil
	}
	if len(es) == 1 {
		return es[0]
	}
	return &extend.BinaryExtend{
		Op:    overload.And,
		Left:  es[0],
		Right: extendsToAndExtend(es[1:]),
	}
}
