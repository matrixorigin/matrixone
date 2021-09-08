package build

import (
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/sql/colexec/extend"
	"matrixone/pkg/sql/colexec/extend/overload"
)

func RewriteExtend(e extend.Extend) extend.Extend {
	switch v := e.(type) {
	case *extend.ParenExtend:
		v.E = RewriteExtend(v.E)
		return v
	case *extend.UnaryExtend:
		if v.Op == overload.Not {
			return rewriteNot(e)
		}
		return v
	case *extend.BinaryExtend:
		v.Left = RewriteExtend(v.Left)
		v.Right = RewriteExtend(v.Right)
		return v
	}
	return e
}
func rewriteNot(e extend.Extend) extend.Extend {
	switch v := e.(type) {
	case *extend.ParenExtend:
        return &extend.ParenExtend{ E: rewriteNot(v.E) }
	case *extend.UnaryExtend:
		return rewriteNotUnary(v)
	case *extend.BinaryExtend:
		return rewriteNotBinary(v)
	}
	return e
}

func rewriteNotUnary(e *extend.UnaryExtend) extend.Extend {
	if e.Op != overload.Not {
		return e
	}
	return negation(e.E, false)
}

func rewriteNotBinary(e *extend.BinaryExtend) extend.Extend {
	if e.Op == overload.Or || e.Op == overload.And {
		return &extend.BinaryExtend{
			Op:    overload.NegOps[e.Op],
			Left:  rewriteNot(e.Left),
			Right: rewriteNot(e.Right),
		}
	}
	return e
}

func negation(e extend.Extend, isParen bool) extend.Extend {
	switch v := e.(type) {
	case *extend.UnaryExtend:
		return rewriteNotUnary(v)
	case *extend.ParenExtend:
        return &extend.ParenExtend{ E: negation(v.E, true) }
	case *extend.BinaryExtend:
		if !isParen && (v.Op == overload.And || v.Op == overload.Or) {
			v.Left = negation(v.Left, isParen)
			return v
		}
		return negationBinary(v, isParen)
	case *extend.ValueExtend:
		var ok bool

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
		vec := vector.New(types.Type{Oid: types.T_int64, Size: 8})
		if ok {
			vec.Col = []int64{0}
		} else {
			vec.Col = []int64{1}
		}
		v.V = vec
		return v
	}
	return e
}

func negationBinary(e *extend.BinaryExtend, isParen bool) extend.Extend {
	if _, ok := overload.LogicalOps[e.Op]; ok {
		if e.Op != overload.Or && e.Op != overload.And {
			return &extend.BinaryExtend{
				Left:  e.Left,
				Right: e.Right,
				Op:    overload.NegOps[e.Op],
			}
		}
		return &extend.BinaryExtend{
			Op:    overload.NegOps[e.Op],
			Left:  negation(e.Left, isParen),
			Right: negation(e.Right, isParen),
		}
	}
	return e
}
