package overload

import (
	"fmt"
	"matrixbase/pkg/container/types"
	"matrixbase/pkg/container/vector"
	"matrixbase/pkg/encoding"
	"matrixbase/pkg/vectorize/neg"
	"matrixbase/pkg/vm/process"
)

func UnaryEval(op int, typ types.T, c bool, v *vector.Vector, p *process.Process) (*vector.Vector, error) {
	if os, ok := UnaryOps[op]; ok {
		for _, o := range os {
			if unaryCheck(op, o.Typ, typ) {
				return o.Fn(v, p, c)
			}
		}
	}
	return nil, fmt.Errorf("%s not yet implemented for %s", OpName[op], typ)
}

func unaryCheck(op int, arg types.T, val types.T) bool {
	return arg == val
}

var UnaryOps = map[int][]*UnaryOp{
	UnaryMinus: {
		&UnaryOp{
			Typ:        types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(v *vector.Vector, p *process.Process, _ bool) (*vector.Vector, error) {
				vs := v.Col.([]int8)
				if v.Data != nil {
					if cnt := encoding.DecodeUint64(v.Data[:8]); cnt == 1 {
						v.SetCol(neg.I8Neg(vs, vs))
						return v, nil
					}
				}
				data, err := p.Alloc(int64(len(vs)))
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(data)
				vec := vector.New(v.Typ)
				vec.Col = rs
				vec.Nsp = v.Nsp
				vec.SetCol(neg.I8Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(v *vector.Vector, p *process.Process, _ bool) (*vector.Vector, error) {
				vs := v.Col.([]int16)
				if v.Data != nil {
					if cnt := encoding.DecodeUint64(v.Data[:8]); cnt == 1 {
						v.SetCol(neg.I16Neg(vs, vs))
						return v, nil
					}
				}
				data, err := p.Alloc(int64(len(vs)) * 2)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(data)
				vec := vector.New(v.Typ)
				vec.Col = rs
				vec.Nsp = v.Nsp
				vec.SetCol(neg.I16Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(v *vector.Vector, p *process.Process, _ bool) (*vector.Vector, error) {
				vs := v.Col.([]int32)
				if v.Data != nil {
					if cnt := encoding.DecodeUint64(v.Data[:8]); cnt == 1 {
						v.SetCol(neg.I32Neg(vs, vs))
						return v, nil
					}
				}
				data, err := p.Alloc(int64(len(vs)) * 4)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(data)
				vec := vector.New(v.Typ)
				vec.Col = rs
				vec.Nsp = v.Nsp
				vec.SetCol(neg.I32Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(v *vector.Vector, p *process.Process, _ bool) (*vector.Vector, error) {
				vs := v.Col.([]int64)
				if v.Data != nil {
					if cnt := encoding.DecodeUint64(v.Data[:8]); cnt == 1 {
						v.SetCol(neg.I64Neg(vs, vs))
						return v, nil
					}
				}
				data, err := p.Alloc(int64(len(vs)) * 8)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(data)
				vec := vector.New(v.Typ)
				vec.Col = rs
				vec.Nsp = v.Nsp
				vec.SetCol(neg.I64Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(v *vector.Vector, p *process.Process, _ bool) (*vector.Vector, error) {
				vs := v.Col.([]float32)
				if v.Data != nil {
					if cnt := encoding.DecodeUint64(v.Data[:8]); cnt == 1 {
						v.SetCol(neg.F32Neg(vs, vs))
						return v, nil
					}
				}
				data, err := p.Alloc(int64(len(vs)) * 4)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(data)
				vec := vector.New(v.Typ)
				vec.Col = rs
				vec.Nsp = v.Nsp
				vec.SetCol(neg.F32Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(v *vector.Vector, p *process.Process, _ bool) (*vector.Vector, error) {
				vs := v.Col.([]float64)
				if v.Data != nil {
					if cnt := encoding.DecodeUint64(v.Data[:8]); cnt == 1 {
						v.SetCol(neg.F64Neg(vs, vs))
						return v, nil
					}
				}
				data, err := p.Alloc(int64(len(vs)) * 8)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(data)
				vec := vector.New(v.Typ)
				vec.Col = rs
				vec.Nsp = v.Nsp
				vec.SetCol(neg.F64Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_decimal,
			ReturnType: types.T_decimal,
			Fn: func(v *vector.Vector, p *process.Process, _ bool) (*vector.Vector, error) {
				vs := v.Col.([]types.Decimal)
				if v.Data != nil {
					if cnt := encoding.DecodeUint64(v.Data[:8]); cnt == 1 {
						v.SetCol(neg.DecimalNeg(vs, vs))
						return v, nil
					}
				}
				data, err := p.Alloc(int64(len(vs) * encoding.DecimalSize))
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeDecimalSlice(data)
				vec := vector.New(v.Typ)
				vec.Col = rs
				vec.Nsp = v.Nsp
				vec.SetCol(neg.DecimalNeg(vs, rs))
				return vec, nil
			},
		},
	},
}
