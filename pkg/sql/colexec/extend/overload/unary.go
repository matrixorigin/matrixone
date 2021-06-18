package overload

import (
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/neg"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func UnaryEval(op int, typ types.T, c bool, v *vector.Vector, p *process.Process) (*vector.Vector, error) {
	if os, ok := UnaryOps[op]; ok {
		for _, o := range os {
			if unaryCheck(op, o.Typ, typ) {
				return o.Fn(v, p, c)
			}
		}
	}
	return nil, fmt.Errorf("'%s' not yet implemented for %s", OpName[op], typ)
}

func unaryCheck(op int, arg types.T, val types.T) bool {
	return arg == val
}

var UnaryOps = map[int][]*UnaryOp{
	UnaryMinus: {
		&UnaryOp{
			Typ:        types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(v *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				vs := v.Col.([]int8)
				vec, err := register.Get(proc, int64(len(vs)), v.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(vs)]
				vec.Col = rs
				vec.Nsp = v.Nsp
				vec.SetCol(neg.Int8Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(v *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				vs := v.Col.([]int16)
				vec, err := register.Get(proc, int64(len(vs)*2), v.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(vs)]
				vec.Col = rs
				vec.Nsp = v.Nsp
				vec.SetCol(neg.Int16Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(v *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				vs := v.Col.([]int32)
				vec, err := register.Get(proc, int64(len(vs)*4), v.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(vs)]
				vec.Col = rs
				vec.Nsp = v.Nsp
				vec.SetCol(neg.Int32Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(v *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				vs := v.Col.([]int64)
				vec, err := register.Get(proc, int64(len(vs)*8), v.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(vs)]
				vec.Col = rs
				vec.Nsp = v.Nsp
				vec.SetCol(neg.Int64Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(v *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				vs := v.Col.([]float32)
				vec, err := register.Get(proc, int64(len(vs)*4), v.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(vs)]
				vec.Col = rs
				vec.Nsp = v.Nsp
				vec.SetCol(neg.Float32Neg(vs, rs))
				return vec, nil
			},
		},
		&UnaryOp{
			Typ:        types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(v *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				vs := v.Col.([]float64)
				vec, err := register.Get(proc, int64(len(vs)*8), v.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(vs)]
				vec.Col = rs
				vec.Nsp = v.Nsp
				vec.SetCol(neg.Float64Neg(vs, rs))
				return vec, nil
			},
		},
	},
}
