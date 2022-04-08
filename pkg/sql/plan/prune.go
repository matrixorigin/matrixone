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
	"github.com/matrixorigin/matrixone/pkg/vectorize/like"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/errno"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/sql/errors"
)

var (
	// ErrDivByZero is reported on a division by zero.
	ErrDivByZero = overload.ErrDivByZero
	// ErrZeroModulus is reported when computing the rest of a division by zero.
	ErrZeroModulus = overload.ErrModByZero
)

func (b *build) pruneExtend(e extend.Extend, isProjection bool) (extend.Extend, error) {
	var err error

	switch n := e.(type) {
	case *extend.UnaryExtend:
		if n.Op == overload.Not {
			if !isProjection {
				return b.pruneConditionNot(n)
			} else {
				return b.pruneProjectionNot(n)
			}
		}
		if n.Op == overload.UnaryMinus {
			return b.pruneUnaryMinus(n)
		}
	case *extend.MultiExtend:
		for i, ext := range n.Args {
			if n.Args[i], err = b.pruneExtend(ext, false); err != nil {
				return nil, err
			}
		}
	case *extend.ParenExtend:
		if n.E, err = b.pruneExtend(n.E, false); err != nil {
			return nil, err
		}
		return n, nil
	case *extend.BinaryExtend:
		if n.Left, err = b.pruneExtend(n.Left, false); err != nil {
			return nil, err
		}
		if n.Right, err = b.pruneExtend(n.Right, false); err != nil {
			return nil, err
		}
		switch n.Op {
		case overload.Or:
			return b.pruneOr(n)
		case overload.And:
			return b.pruneAnd(n)
		case overload.EQ:
			return b.pruneEq(n)
		case overload.LT:
			return b.pruneLt(n)
		case overload.LE:
			return b.pruneLe(n)
		case overload.GT:
			return b.pruneGt(n)
		case overload.GE:
			return b.pruneGe(n)
		case overload.NE:
			return b.pruneNe(n)
		case overload.Div:
			return b.pruneDiv(n)
		case overload.Mod:
			return b.pruneMod(n)
		case overload.Mult:
			return b.pruneMul(n)
		case overload.Plus:
			return b.prunePlus(n)
		case overload.Minus:
			return b.pruneMinus(n)
		case overload.Like:
			return b.pruneLike(n)
		}
	}
	return e, nil
}

func (b *build) pruneConditionNot(e *extend.UnaryExtend) (extend.Extend, error) {
	cnt := 1
	ext := e.E
	for {
		v, ok := ext.(*extend.UnaryExtend)
		if !ok || v.Op != overload.Not {
			break
		}
		cnt++
		ext = v.E
	}
	if cnt%2 == 0 {
		return ext, nil
	}
	// split not extends
	ext = logicInverse(ext)
	return ext, nil
}

func (b *build) pruneProjectionNot(e *extend.UnaryExtend) (extend.Extend, error) {
	cnt := 1
	ext := e.E
	for {
		v, ok := ext.(*extend.UnaryExtend)
		if !ok || v.Op != overload.Not {
			break
		}
		cnt++
		ext = v.E
	}
	if cnt > 2 {
		temp := &extend.UnaryExtend{Op: overload.Not, E: ext}
		if cnt%2 == 0 {
			return &extend.UnaryExtend{Op: overload.Not, E: temp}, nil
		} else {
			return temp, nil
		}
	}
	v, ok := ext.(*extend.ValueExtend)
	if !ok {
		return e, nil
	}
	{
		ok = false
		switch v.V.Typ.Oid {
		case types.T_int64:
			if v.V.Col.([]int64)[0] != 0 {
				ok = true
			}
		case types.T_float64:
			if v.V.Col.([]float64)[0] != 0 {
				ok = true
			}
		default:
			ok = false
		}
	}
	vec := vector.New(types.Type{Oid: types.T_int8, Size: 1})
	if ok {
		vec.Col = []int8{0}
	} else {
		vec.Col = []int8{1}
	}
	v.V = vec
	return v, nil
}

// logicInverse will do logic inversion work for a not extend
// For example: split not (a == 0) to a != 0,
// And e will never be `not -1` because we converted it into `-1 != 0` while astRewrite
func logicInverse(e extend.Extend) extend.Extend {
	switch v := e.(type) {
	case *extend.ParenExtend:
		return &extend.ParenExtend{E: logicInverse(v.E)}
	case *extend.BinaryExtend:
		return splitNotBinary(v)
	case *extend.UnaryExtend:
		if v.Op == overload.Not {
			return logicInverse(v.E)
		}
	}
	return e
}

func splitNotBinary(e *extend.BinaryExtend) extend.Extend {
	switch e.Op {
	case overload.And:
		return &extend.BinaryExtend{Op: overload.Or, Left: logicInverse(e.Left), Right: logicInverse(e.Right)}
	case overload.Or:
		return &extend.BinaryExtend{Op: overload.And, Left: logicInverse(e.Left), Right: logicInverse(e.Right)}
	case overload.EQ:
		return &extend.BinaryExtend{Op: overload.NE, Left: logicInverse(e.Left), Right: logicInverse(e.Right)}
	case overload.GE:
		return &extend.BinaryExtend{Op: overload.LT, Left: logicInverse(e.Left), Right: logicInverse(e.Right)}
	case overload.GT:
		return &extend.BinaryExtend{Op: overload.LE, Left: logicInverse(e.Left), Right: logicInverse(e.Right)}
	case overload.LE:
		return &extend.BinaryExtend{Op: overload.GT, Left: logicInverse(e.Left), Right: logicInverse(e.Right)}
	case overload.LT:
		return &extend.BinaryExtend{Op: overload.GE, Left: logicInverse(e.Left), Right: logicInverse(e.Right)}
	case overload.NE:
		return &extend.BinaryExtend{Op: overload.EQ, Left: logicInverse(e.Left), Right: logicInverse(e.Right)}
	}
	return e
}

func (b *build) pruneUnaryMinus(e *extend.UnaryExtend) (extend.Extend, error) {
	v, ok := e.E.(*extend.ValueExtend)
	if !ok {
		return e, nil
	}
	return Neg(v)
}

func (b *build) pruneOr(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	switch {
	case !lok && rok:
		rv := false
		switch re.V.Typ.Oid {
		case types.T_int64:
			if re.V.Col.([]int64)[0] != 0 {
				rv = true
			}
		case types.T_float64:
			if re.V.Col.([]float64)[0] != 0 {
				rv = true
			}
		}
		if rv {
			return re, nil
		}
		return e.Left, nil
	case lok && rok:
		lv, rv := false, false
		switch re.V.Typ.Oid {
		case types.T_int64:
			if re.V.Col.([]int64)[0] != 0 {
				rv = true
			}
		case types.T_float64:
			if re.V.Col.([]float64)[0] != 0 {
				rv = true
			}
		}
		switch le.V.Typ.Oid {
		case types.T_int64:
			if le.V.Col.([]int64)[0] != 0 {
				lv = true
			}
		case types.T_float64:
			if le.V.Col.([]float64)[0] != 0 {
				lv = true
			}
		}
		if lv {
			return le, nil
		}
		if rv {
			return re, nil
		}
		return le, nil
	case lok && !rok:
		lv := false
		switch le.V.Typ.Oid {
		case types.T_int64:
			if le.V.Col.([]int64)[0] != 0 {
				lv = true
			}
		case types.T_float64:
			if le.V.Col.([]float64)[0] != 0 {
				lv = true
			}
		}
		if lv {
			return le, nil
		}
		return e.Right, nil
	}
	return e, nil
}

func (b *build) pruneAnd(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	switch {
	case !lok && rok:
		rv := false
		switch re.V.Typ.Oid {
		case types.T_int64:
			if re.V.Col.([]int64)[0] != 0 {
				rv = true
			}
		case types.T_float64:
			if re.V.Col.([]float64)[0] != 0 {
				rv = true
			}
		}
		if rv {
			return e.Left, nil
		}
		return re, nil
	case lok && rok:
		lv, rv := false, false
		switch re.V.Typ.Oid {
		case types.T_int64:
			if re.V.Col.([]int64)[0] != 0 {
				rv = true
			}
		case types.T_float64:
			if re.V.Col.([]float64)[0] != 0 {
				rv = true
			}
		}
		switch le.V.Typ.Oid {
		case types.T_int64:
			if le.V.Col.([]int64)[0] != 0 {
				lv = true
			}
		case types.T_float64:
			if le.V.Col.([]float64)[0] != 0 {
				lv = true
			}
		}
		if lv && rv {
			return le, nil
		}
		if !rv {
			return re, nil
		}
		return le, nil
	case lok && !rok:
		lv := false
		switch le.V.Typ.Oid {
		case types.T_int64:
			if le.V.Col.([]int64)[0] != 0 {
				lv = true
			}
		case types.T_float64:
			if le.V.Col.([]float64)[0] != 0 {
				lv = true
			}
		}
		if lv {
			return e.Right, nil
		}
		return le, nil
	}
	return e, nil
}

func (b *build) pruneDiv(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	switch {
	case !lok && rok:
		if isZero(re) {
			return nil, ErrDivByZero
		}
		switch e.Right.ReturnType() {
		case types.T_int8:
			if err := toInt8(re); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(re); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(re); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(re); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(re); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(re); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(re); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(re); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(re); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(re); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	case lok && rok:
		if isZero(re) {
			return nil, ErrDivByZero
		}
		return div(le, re)
	case lok && !rok:
		switch e.Left.ReturnType() {
		case types.T_int8:
			if err := toInt8(le); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(le); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(le); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(le); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(le); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(le); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(le); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(le); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(le); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(le); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	}
	return e, nil
}

func (b *build) pruneMod(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	switch {
	case !lok && rok:
		if isZero(re) {
			return nil, ErrZeroModulus
		}
		switch e.Left.ReturnType() {
		case types.T_int8:
			if err := toInt8(re); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(re); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(re); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(re); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(re); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(re); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(re); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(re); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(re); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(re); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	case lok && rok:
		if isZero(re) {
			return nil, ErrZeroModulus
		}
		return mod(le, re)
	case lok && !rok:
		switch e.Right.ReturnType() {
		case types.T_int8:
			if err := toInt8(le); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(le); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(le); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(le); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(le); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(le); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(le); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(le); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(le); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(le); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	}
	return e, nil
}

func (b *build) pruneMul(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	switch {
	case !lok && rok:
		switch e.Right.ReturnType() {
		case types.T_int8:
			if err := toInt8(re); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(re); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(re); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(re); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(re); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(re); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(re); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(re); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(re); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(re); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	case lok && rok:
		return mul(le, re)
	case lok && !rok:
		switch e.Left.ReturnType() {
		case types.T_int8:
			if err := toInt8(le); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(le); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(le); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(le); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(le); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(le); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(le); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(le); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(le); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(le); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	}
	return e, nil
}

func (b *build) prunePlus(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	switch {
	case !lok && rok:
		switch e.Right.ReturnType() {
		case types.T_int8:
			if err := toInt8(re); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(re); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(re); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(re); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(re); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(re); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(re); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(re); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(re); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(re); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	case lok && rok:
		return plus(le, re)
	case lok && !rok:
		switch e.Left.ReturnType() {
		case types.T_int8:
			if err := toInt8(le); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(le); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(le); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(le); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(le); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(le); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(le); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(le); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(le); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(le); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	}
	return e, nil
}

func (b *build) pruneMinus(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	switch {
	case !lok && rok:
		switch e.Right.ReturnType() {
		case types.T_int8:
			if err := toInt8(re); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(re); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(re); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(re); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(re); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(re); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(re); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(re); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(re); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(re); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	case lok && rok:
		return minus(le, re)
	case lok && !rok:
		switch e.Left.ReturnType() {
		case types.T_int8:
			if err := toInt8(le); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(le); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(le); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(le); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(le); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(le); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(le); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(le); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(le); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(le); err != nil {
				return nil, err
			}
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	}
	return e, nil
}

func (b *build) pruneEq(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	switch {
	case !lok && rok:
		switch e.Right.ReturnType() {
		case types.T_int8:
			if err := toInt8(re); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(re); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(re); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(re); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(re); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(re); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(re); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(re); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(re); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(re); err != nil {
				return nil, err
			}
		case types.T_char:
			if err := toChar(re); err != nil {
				return nil, err
			}
		case types.T_varchar:
		case types.T_date:
		case types.T_datetime:
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	case lok && rok:
		return Eq(le, re)
	case lok && !rok:
		switch e.Left.ReturnType() {
		case types.T_int8:
			if err := toInt8(le); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(le); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(le); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(le); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(le); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(le); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(le); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(le); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(le); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(le); err != nil {
				return nil, err
			}
		case types.T_char:
			if err := toChar(le); err != nil {
				return nil, err
			}
		case types.T_varchar:
		case types.T_date:
		case types.T_datetime:
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	}
	return e, nil
}

func (b *build) pruneNe(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	switch {
	case !lok && rok:
		switch e.Right.ReturnType() {
		case types.T_int8:
			if err := toInt8(re); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(re); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(re); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(re); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(re); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(re); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(re); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(re); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(re); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(re); err != nil {
				return nil, err
			}
		case types.T_char:
			if err := toChar(re); err != nil {
				return nil, err
			}
		case types.T_varchar:
		case types.T_date:
		case types.T_datetime:
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	case lok && rok:
		return Ne(le, re)
	case lok && !rok:
		switch e.Left.ReturnType() {
		case types.T_int8:
			if err := toInt8(le); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(le); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(le); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(le); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(le); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(le); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(le); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(le); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(le); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(le); err != nil {
				return nil, err
			}
		case types.T_char:
			if err := toChar(le); err != nil {
				return nil, err
			}
		case types.T_varchar:
		case types.T_date:
		case types.T_datetime:
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	}
	return e, nil
}

func (b *build) pruneLt(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	switch {
	case !lok && rok:
		switch e.Right.ReturnType() {
		case types.T_int8:
			if err := toInt8(re); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(re); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(re); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(re); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(re); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(re); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(re); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(re); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(re); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(re); err != nil {
				return nil, err
			}
		case types.T_char:
			if err := toChar(re); err != nil {
				return nil, err
			}
		case types.T_varchar:
		case types.T_date:
		case types.T_datetime:
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	case lok && rok:
		return Lt(le, re)
	case lok && !rok:
		switch e.Left.ReturnType() {
		case types.T_int8:
			if err := toInt8(le); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(le); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(le); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(le); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(le); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(le); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(le); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(le); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(le); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(le); err != nil {
				return nil, err
			}
		case types.T_char:
			if err := toChar(le); err != nil {
				return nil, err
			}
		case types.T_varchar:
		case types.T_date:
		case types.T_datetime:
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	}
	return e, nil
}

func (b *build) pruneLe(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	switch {
	case !lok && rok:
		switch e.Right.ReturnType() {
		case types.T_int8:
			if err := toInt8(re); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(re); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(re); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(re); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(re); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(re); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(re); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(re); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(re); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(re); err != nil {
				return nil, err
			}
		case types.T_char:
			if err := toChar(re); err != nil {
				return nil, err
			}
		case types.T_varchar:
		case types.T_date:
		case types.T_datetime:
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	case lok && rok:
		return Le(le, re)
	case lok && !rok:
		switch e.Left.ReturnType() {
		case types.T_int8:
			if err := toInt8(le); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(le); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(le); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(le); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(le); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(le); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(le); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(le); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(le); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(le); err != nil {
				return nil, err
			}
		case types.T_char:
			if err := toChar(le); err != nil {
				return nil, err
			}
		case types.T_varchar:
		case types.T_date:
		case types.T_datetime:
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	}
	return e, nil
}

func (b *build) pruneGt(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	switch {
	case !lok && rok:
		switch e.Right.ReturnType() {
		case types.T_int8:
			if err := toInt8(re); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(re); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(re); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(re); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(re); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(re); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(re); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(re); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(re); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(re); err != nil {
				return nil, err
			}
		case types.T_char:
			if err := toChar(re); err != nil {
				return nil, err
			}
		case types.T_varchar:
		case types.T_date:
		case types.T_datetime:
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	case lok && rok:
		return Gt(le, re)
	case lok && !rok:
		switch e.Left.ReturnType() {
		case types.T_int8:
			if err := toInt8(le); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(le); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(le); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(le); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(le); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(le); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(le); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(le); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(le); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(le); err != nil {
				return nil, err
			}
		case types.T_char:
			if err := toChar(le); err != nil {
				return nil, err
			}
		case types.T_varchar:
		case types.T_date:
		case types.T_datetime:
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	}
	return e, nil
}

func (b *build) pruneGe(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	switch {
	case !lok && rok:
		switch e.Right.ReturnType() {
		case types.T_int8:
			if err := toInt8(re); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(re); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(re); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(re); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(re); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(re); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(re); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(re); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(re); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(re); err != nil {
				return nil, err
			}
		case types.T_char:
			if err := toChar(re); err != nil {
				return nil, err
			}
		case types.T_varchar:
		case types.T_date:
		case types.T_datetime:
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	case lok && rok:
		return Ge(le, re)
	case lok && !rok:
		switch e.Left.ReturnType() {
		case types.T_int8:
			if err := toInt8(le); err != nil {
				return nil, err
			}
		case types.T_int16:
			if err := toInt16(le); err != nil {
				return nil, err
			}
		case types.T_int32:
			if err := toInt32(le); err != nil {
				return nil, err
			}
		case types.T_int64:
			if err := toInt64(le); err != nil {
				return nil, err
			}
		case types.T_uint8:
			if err := toUint8(le); err != nil {
				return nil, err
			}
		case types.T_uint16:
			if err := toUint16(le); err != nil {
				return nil, err
			}
		case types.T_uint32:
			if err := toUint32(le); err != nil {
				return nil, err
			}
		case types.T_uint64:
			if err := toUint64(le); err != nil {
				return nil, err
			}
		case types.T_float32:
			if err := toFloat32(le); err != nil {
				return nil, err
			}
		case types.T_float64:
			if err := toFloat64(le); err != nil {
				return nil, err
			}
		case types.T_char:
			if err := toChar(le); err != nil {
				return nil, err
			}
		case types.T_varchar:
		case types.T_date:
		case types.T_datetime:
		default:
			return nil, errors.New(errno.DatatypeMismatch, fmt.Sprintf("illegal expression '%s'", e))
		}
		return e, nil
	}
	return e, nil
}

func (b *build) pruneLike(e *extend.BinaryExtend) (extend.Extend, error) {
	le, lok := e.Left.(*extend.ValueExtend)
	re, rok := e.Right.(*extend.ValueExtend)
	if !lok || !rok {
		return e, nil
	}

	vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
	vec.Ref = 1

	if !le.V.Typ.Eq(re.V.Typ) || (le.V.Typ.Oid != types.T_char && le.V.Typ.Oid != types.T_varchar) {
		return nil, errors.New(errno.FeatureNotSupported, "operator LIKE only support for varchar and char")
	}
	switch {
	case lok && rok:
		k, err := like.BtConstAndConst(le.V.Col.(*types.Bytes).Data, re.V.Col.(*types.Bytes).Data, make([]int64, 1))
		if err != nil {
			return nil, err
		}
		if k != nil {
			vector.SetCol(vec, []int64{1})
		} else {
			vector.SetCol(vec, []int64{0})
		}
		return &extend.ValueExtend{V: vec}, nil
	}
	return e, nil
}
