package build

import (
	"errors"
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/colexec/extend/overload"
)

var (
	// ErrDivByZero is reported on a division by zero.
	ErrDivByZero = errors.New("division by zero")
	// ErrZeroModulus is reported when computing the rest of a division by zero.
	ErrZeroModulus = errors.New("zero modulus")
)

func (b *build) pruneExtend(e extend.Extend) (extend.Extend, error) {
	var err error

	switch n := e.(type) {
	case *extend.UnaryExtend:
		if n.Op == overload.Not {
			return b.pruneNot(n)
		}
		if n.Op == overload.UnaryMinus {
			return b.pruneUnaryMinus(n)
		}
	case *extend.ParenExtend:
		if n.E, err = b.pruneExtend(n.E); err != nil {
			return nil, err
		}
		return n, nil
	case *extend.BinaryExtend:
		if n.Left, err = b.pruneExtend(n.Left); err != nil {
			return nil, err
		}
		if n.Right, err = b.pruneExtend(n.Right); err != nil {
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
		}
	}
	return e, nil
}

func (b *build) pruneNot(e *extend.UnaryExtend) (extend.Extend, error) {
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
			return le, nil
		}
		return e.Right, nil
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
			return nil, fmt.Errorf("illegal expression '%s'", e)
		}
		return e, nil
	case lok && rok:
		if isZero(re) {
			return nil, ErrDivByZero
		}
		return div(le, re)
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
			return nil, fmt.Errorf("illegal expression '%s'", e)
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
			return nil, fmt.Errorf("illegal expression '%s'", e)
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
			return nil, fmt.Errorf("illegal expression '%s'", e)
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
			return nil, fmt.Errorf("illegal expression '%s'", e)
		}
		return e, nil
	case lok && rok:
		return mul(le, re)
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
			return nil, fmt.Errorf("illegal expression '%s'", e)
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
			return nil, fmt.Errorf("illegal expression '%s'", e)
		}
		return e, nil
	case lok && rok:
		return plus(le, re)
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
			return nil, fmt.Errorf("illegal expression '%s'", e)
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
			return nil, fmt.Errorf("illegal expression '%s'", e)
		}
		return e, nil
	case lok && rok:
		return minus(le, re)
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
			return nil, fmt.Errorf("illegal expression '%s'", e)
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
		case types.T_char:
			if err := toChar(re); err != nil {
				return nil, err
			}
		case types.T_varchar:
		default:
			return nil, fmt.Errorf("illegal expression '%s'", e)
		}
		return e, nil
	case lok && rok:
		return Eq(le, re)
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
		case types.T_char:
			if err := toChar(le); err != nil {
				return nil, err
			}
		case types.T_varchar:
		default:
			return nil, fmt.Errorf("illegal expression '%s'", e)
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
		case types.T_char:
			if err := toChar(re); err != nil {
				return nil, err
			}
		case types.T_varchar:
		default:
			return nil, fmt.Errorf("illegal expression '%s'", e)
		}
		return e, nil
	case lok && rok:
		return Ne(le, re)
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
		case types.T_char:
			if err := toChar(le); err != nil {
				return nil, err
			}
		case types.T_varchar:
		default:
			return nil, fmt.Errorf("illegal expression '%s'", e)
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
		case types.T_char:
			if err := toChar(re); err != nil {
				return nil, err
			}
		case types.T_varchar:
		default:
			return nil, fmt.Errorf("illegal expression '%s'", e)
		}
		return e, nil
	case lok && rok:
		return Lt(le, re)
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
		case types.T_char:
			if err := toChar(le); err != nil {
				return nil, err
			}
		case types.T_varchar:
		default:
			return nil, fmt.Errorf("illegal expression '%s'", e)
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
		case types.T_char:
			if err := toChar(re); err != nil {
				return nil, err
			}
		case types.T_varchar:
		default:
			return nil, fmt.Errorf("illegal expression '%s'", e)
		}
		return e, nil
	case lok && rok:
		return Le(le, re)
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
		case types.T_char:
			if err := toChar(le); err != nil {
				return nil, err
			}
		case types.T_varchar:
		default:
			return nil, fmt.Errorf("illegal expression '%s'", e)
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
		case types.T_char:
			if err := toChar(re); err != nil {
				return nil, err
			}
		case types.T_varchar:
		default:
			return nil, fmt.Errorf("illegal expression '%s'", e)
		}
		return e, nil
	case lok && rok:
		return Gt(le, re)
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
		case types.T_char:
			if err := toChar(le); err != nil {
				return nil, err
			}
		case types.T_varchar:
		default:
			return nil, fmt.Errorf("illegal expression '%s'", e)
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
		case types.T_char:
			if err := toChar(re); err != nil {
				return nil, err
			}
		case types.T_varchar:
		default:
			return nil, fmt.Errorf("illegal expression '%s'", e)
		}
		return e, nil
	case lok && rok:
		return Ge(le, re)
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
		case types.T_char:
			if err := toChar(le); err != nil {
				return nil, err
			}
		case types.T_varchar:
		default:
			return nil, fmt.Errorf("illegal expression '%s'", e)
		}
		return e, nil
	}
	return e, nil
}
