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
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/defines"
	"matrixone/pkg/errno"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/colexec/extend/overload"
	"matrixone/pkg/sql/colexec/transformer"
	"matrixone/pkg/sql/errors"
	"matrixone/pkg/sql/tree"
	"strings"
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
	{ // inc reference
		fn(e.Left, qry)
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
		return &extend.FuncExtend{Name: funcName, Args: args}, nil
	}
	if op, ok := transformer.TransformerNamesMap[funcName]; ok {
		if len(e.Exprs) > 1 {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Illegal function call '%s'", e))
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
	return &extend.FuncExtend{Name: funcName, Args: args}, nil
}

// flg indicates whether the aggregation function is accepted
func (b *build) buildHavingFunc(e *tree.FuncExpr, qry *Query, fn func(tree.Expr, *Query) (extend.Extend, error)) (extend.Extend, error) {
	name, ok := e.Func.FunctionReference.(*tree.UnresolvedName)
	if !ok {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
	}
	funcName := strings.ToLower(name.Parts[0])
	if op, ok := transformer.TransformerNamesMap[funcName]; ok {
		if len(e.Exprs) > 1 {
			return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Illegal function call '%s'", e))
		}
		b.flg = false
		defer func() { b.flg = true }()
		return b.buildHavingAggregation(op, funcName, e.Exprs[0], qry, fn)
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
	return &extend.FuncExtend{Name: funcName, Args: args}, nil
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
	case tree.DIV, tree.INTEGER_DIV:
		left, err := fn(e.Left, qry)
		if err != nil {
			return nil, err
		}
		right, err := fn(e.Right, qry)
		if err != nil {
			return nil, err
		}
		return &extend.BinaryExtend{Op: overload.Div, Left: left, Right: right}, nil
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
	}
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
}

// If flg is set, then it will increase the reference count
// 	. only the original attributes will be looked up
func (b *build) buildAttribute0(flg bool, e *tree.UnresolvedName, qry *Query) (extend.Extend, error) {
	var name string

	if e.Star {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
	}
	switch {
	case e.NumParts == 1:
		name = e.Parts[0]
	case e.NumParts == 2:
		name = e.Parts[1] + "." + e.Parts[0]
	case e.NumParts == 3:
		name = e.Parts[2] + "." + e.Parts[1] + "." + e.Parts[0]
	default:
		name = e.Parts[3] + "." + e.Parts[2] + "." + e.Parts[1] + "." + e.Parts[0]
	}
	rels, typ, err := qry.getAttribute0(flg, name)
	if err != nil {
		return nil, err
	}
	if len(rels) == 0 {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Column '%s' doesn't exist", name))
	}
	if len(rels) > 1 {
		return nil, errors.New(errno.DuplicateColumn, fmt.Sprintf("Column '%s' is ambiguous", name))
	}
	return &extend.Attribute{Name: name, Type: typ.Oid}, nil
}

// If flg is set, then it will increase the reference count
// 	. only the original attributes will be looked up
//  . projection will be looked up
func (b *build) buildAttribute1(flg bool, e *tree.UnresolvedName, qry *Query) (extend.Extend, error) {
	var name string

	if e.Star {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
	}
	switch {
	case e.NumParts == 1:
		name = e.Parts[0]
	case e.NumParts == 2:
		name = e.Parts[1] + "." + e.Parts[0]
	case e.NumParts == 3:
		name = e.Parts[2] + "." + e.Parts[1] + "." + e.Parts[0]
	default:
		name = e.Parts[3] + "." + e.Parts[2] + "." + e.Parts[1] + "." + e.Parts[0]
	}
	rels, typ, err := qry.getAttribute1(flg, name)
	if err != nil {
		return nil, err
	}
	if len(rels) == 0 {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Column '%s' doesn't exist", name))
	}
	if len(rels) > 1 {
		return nil, errors.New(errno.DuplicateColumn, fmt.Sprintf("Column '%s' is ambiguous", name))
	}
	return &extend.Attribute{Name: name, Type: typ.Oid}, nil
}

// If flg is set, then it will increase the reference count
// 	. only the original attributes will be looked up
//  . projection will be looked up
//  . aggregation will be looke up
func (b *build) buildAttribute2(flg bool, e *tree.UnresolvedName, qry *Query) (extend.Extend, error) {
	var name string

	if e.Star {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", e))
	}
	switch {
	case e.NumParts == 1:
		name = e.Parts[0]
	case e.NumParts == 2:
		name = e.Parts[1] + "." + e.Parts[0]
	case e.NumParts == 3:
		name = e.Parts[2] + "." + e.Parts[1] + "." + e.Parts[0]
	default:
		name = e.Parts[3] + "." + e.Parts[2] + "." + e.Parts[1] + "." + e.Parts[0]
	}
	rels, typ, err := qry.getAttribute2(flg, name)
	if err != nil {
		return nil, err
	}
	if len(rels) == 0 {
		return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("Column '%s' doesn't exist", name))
	}
	if len(rels) > 1 {
		return nil, errors.New(errno.DuplicateColumn, fmt.Sprintf("Column '%s' is ambiguous", name))
	}
	return &extend.Attribute{Name: name, Type: typ.Oid}, nil
}

func (b *build) buildAggregation(op int, name string, n tree.Expr, qry *Query, fn func(tree.Expr, *Query) (extend.Extend, error)) (extend.Extend, error) {
	var rel string

	if _, ok := n.(*tree.NumVal); ok && op == transformer.StarCount { // count(*)
		qry.RelsMap[qry.Rels[0]].AddAggregation(&Aggregation{
			Ref:   1,
			Op:    op,
			Alias: "count(*)",
			Type:  types.T_int64,
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
	attrs := e.Attributes()
	mp := make(map[string]int) // relations map
	for _, attr := range attrs {
		rels, _, err := qry.getAttribute0(false, attr)
		if err != nil {
			return nil, err
		}
		for i := range rels {
			if len(rel) == 0 {
				rel = rels[i]
			}
			mp[rels[i]]++
		}
	}
	if len(mp) == 0 {
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("Illegal expression '%s' in aggregation", e))
	}
	if len(mp) > 1 {
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("attributes involved in the aggregation must belong to the same relation"))
	}
	if _, ok := e.(*extend.Attribute); !ok {
		qry.RelsMap[rel].AddProjection(&ProjectionExtend{
			Ref:   1,
			E:     e,
			Alias: e.String(),
		})
	}
	qry.RelsMap[rel].AddAggregation(&Aggregation{
		Ref:   1,
		Op:    op,
		Name:  e.String(),
		Alias: fmt.Sprintf("%s(%s)", name, e),
		Type:  transformer.ReturnType(op, e.ReturnType()),
	})
	return &extend.Attribute{
		Name: fmt.Sprintf("%s(%s)", name, e),
		Type: transformer.ReturnType(op, e.ReturnType()),
	}, nil
}

func (b *build) buildHavingAggregation(op int, name string, n tree.Expr, qry *Query, fn func(tree.Expr, *Query) (extend.Extend, error)) (extend.Extend, error) {
	if _, ok := n.(*tree.NumVal); ok && op == transformer.StarCount { // count(*)
		return &extend.Attribute{
			Name: "count(*)",
			Type: transformer.ReturnType(op, types.T_any),
		}, nil
	}
	e, err := fn(n, qry)
	if err != nil {
		return nil, err
	}
	col := fmt.Sprintf("%s(%s)", name, e)
	rels, typ, err := qry.getAttribute2(true, col)
	if err != nil {
		return nil, err
	}
	if len(rels) == 0 {
		return nil, errors.New(errno.SQLStatementNotYetComplete, fmt.Sprintf("Illegal expression '%s' in having clause", e))
	}
	if len(rels) > 1 {
		return nil, errors.New(errno.DuplicateColumn, fmt.Sprintf("Column '%s' in having clause is ambiguous", col))
	}
	return &extend.Attribute{
		Name: col,
		Type: typ.Oid,
	}, nil
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
	return nil, errors.New(errno.SyntaxErrororAccessRuleViolation, fmt.Sprintf("'%v' is not support now", n))
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
	return nil, errors.New(errno.IndeterminateDatatype, fmt.Sprintf("unsupport value: %v", val))
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

func extendsToAndExtend(es []extend.Extend) extend.Extend {
	if len(es) == 1 {
		return es[0]
	}
	return &extend.BinaryExtend{
		Op:    overload.And,
		Left:  es[0],
		Right: extendsToAndExtend(es[1:]),
	}
}
