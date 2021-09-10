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

package overload

import (
	"errors"
	"fmt"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/add"
	"matrixone/pkg/vectorize/and"
	"matrixone/pkg/vectorize/div"
	"matrixone/pkg/vectorize/mod"
	"matrixone/pkg/vectorize/mul"
	"matrixone/pkg/vectorize/or"
	"matrixone/pkg/vectorize/sub"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
	"sync"
)

var pool = sync.Pool{
	New: func() interface{} {
		return make([]int64, 0, 16)
	},
}

var (
	// ErrDivByZero is reported on a division by zero.
	ErrDivByZero = errors.New("division by zero")
	// ErrZeroModulus is reported when computing the rest of a division by zero.
	ErrModByZero = errors.New("zero modulus")
)

func BinaryEval(op int, ltyp, rtyp types.T, lc, rc bool, lv, rv *vector.Vector, p *process.Process) (*vector.Vector, error) {
	if os, ok := BinOps[op]; ok {
		for _, o := range os {
			if binaryCheck(op, o.LeftType, o.RightType, ltyp, rtyp) {
				return o.Fn(lv, rv, p, lc, rc)
			}
		}
	}
	return nil, fmt.Errorf("%s not yet implemented for %s, %s", OpName[op], ltyp, rtyp)
}

func binaryCheck(op int, arg0, arg1 types.T, val0, val1 types.T) bool {
	return arg0 == val0 && arg1 == val1
}

// BinOps contains the binary operations indexed by operation type.
var BinOps = map[int][]*BinOp{
	Or: {
		&BinOp{
			LeftType:   types.T_sel,
			RightType:  types.T_sel,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs)+len(rvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:or.SelOr(lvs, rvs, rs)]
				vec.SetCol(rs)
				return vec, nil
			},
		},
	},
	And: {
		&BinOp{
			LeftType:   types.T_sel,
			RightType:  types.T_sel,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				n := len(lvs)
				if n < len(rvs) {
					n = len(rvs)
				}
				vec, err := register.Get(proc, int64(n)*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:and.SelAnd(lvs, rvs, rs)]
				vec.SetCol(rs)
				return vec, nil
			},
		},
	},
	Plus: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Int8AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Int8AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Int8Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Int16AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Int16AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Int16Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Int32AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Int32AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Int32Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Int64AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Int64AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Int64Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Uint8AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Uint8AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Uint8Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Uint16AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Uint16AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Uint16Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Uint32AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Uint32AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Uint32Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Uint64AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Uint64AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Uint64Add(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				switch {
				case lc && !rc:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Float32AddScalar(lvs[0], rvs, rs))
				case !lc && rc:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Float32AddScalar(rvs[0], lvs, rs))
				default:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(add.Float32Add(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(add.Float64AddScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(add.Float64AddScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(add.Float64Add(lvs, rvs, rs))
				return vec, nil
			},
		},
	},
	Minus: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Int8SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Int8SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Int8Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Int16SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Int16SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Int16Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Int32SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Int32SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Int32Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Int64SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Int64SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Int64Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Uint8SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Uint8SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Uint8Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Uint16SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Uint16SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Uint16Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Uint32SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Uint32SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Uint32Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Uint64SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Uint64SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Uint64Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				switch {
				case lc && !rc:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Float32SubScalar(lvs[0], rvs, rs))
				case !lc && rc:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Float32SubScalar(rvs[0], lvs, rs))
				default:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(sub.Float32Sub(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(sub.Float64SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(sub.Float64SubScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(sub.Float64Sub(lvs, rvs, rs))
				return vec, nil
			},
		},
	},
	Mult: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Int8MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Int8MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Int8Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Int16MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Int16MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Int16Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Int32MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Int32MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Int32Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Int64MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Int64MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Int64Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Uint8MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Uint8MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Uint8Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Uint16MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Uint16MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Uint16Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Uint32MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Uint32MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Uint32Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Uint64MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Uint64MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Uint64Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				switch {
				case lc && !rc:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Float32MulScalar(lvs[0], rvs, rs))
				case !lc && rc:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Float32MulScalar(rvs[0], lvs, rs))
				default:
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mul.Float32Mul(lvs, rvs, rs))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					vec.Nsp = rv.Nsp
					vec.SetCol(mul.Float64MulScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mul.Float64MulScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(rvs)]
				vec.Nsp = lv.Nsp.Or(rv.Nsp)
				vec.SetCol(mul.Float64Mul(lvs, rvs, rs))
				return vec, nil
			},
		},
	},
	Div: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int8DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int8DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Int8DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int8Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int8DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int16DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int16DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Int16DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int16Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int16DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int32DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int32DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Int32DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int32Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int32DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int64DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Int64DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Int64DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int64Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Int64DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint8DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint8DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Uint8DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint8Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint8DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint16DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint16DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Uint16DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint16Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint16DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil

			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint32DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint32DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Uint32DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint32Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint32DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil

			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint64DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Uint64DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Uint64DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint64Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Uint64DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Float32DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Float32DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Float32DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Float32Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeFloat32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Float32DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrDivByZero
							}
						}
						rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Float64DivScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrDivByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(div.Float64DivScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrDivByZero
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(div.Float64DivScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrDivByZero
						}
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Float64Div(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrDivByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeFloat64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(div.Float64DivSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
	},
	Mod: {
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int8ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int8ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Int8ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int8Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int8ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int16ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int16ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Int16ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int16Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int16ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int32ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int32ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Int32ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int32Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int32ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int64ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Int64ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Int64ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int64Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Int64ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint8ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint8ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Uint8ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint8Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint8Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint8ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint16ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint16ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Uint16ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint16Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint16Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint16ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil

			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint32ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint32ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Uint32ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint32Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint32Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint32ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil

			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if !rv.Nsp.Any() {
						for _, v := range rvs {
							if v == 0 {
								return nil, ErrModByZero
							}
						}
						rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint64ModScalar(lvs[0], rvs, rs))
					} else {
						sels := pool.Get().([]int64)
						for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
							if rv.Nsp.Contains(i) {
								continue
							}
							if rvs[i] == 0 {
								pool.Put(sels)
								return nil, ErrModByZero
							}
							sels = append(sels, int64(i))
						}
						rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
						rs = rs[:len(rvs)]
						vec.Nsp = rv.Nsp
						vec.SetCol(mod.Uint64ModScalarSels(lvs[0], rvs, rs, sels))
						pool.Put(sels)
					}
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					if rvs[0] == 0 {
						return nil, ErrModByZero
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp
					vec.SetCol(mod.Uint64ModScalar(rvs[0], lvs, rs))
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				if !rv.Nsp.Any() {
					for _, v := range rvs {
						if v == 0 {
							return nil, ErrModByZero
						}
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint64Mod(lvs, rvs, rs))
				} else {
					sels := pool.Get().([]int64)
					for i, j := uint64(0), uint64(len(rvs)); i < j; i++ {
						if rv.Nsp.Contains(i) {
							continue
						}
						if rvs[i] == 0 {
							pool.Put(sels)
							return nil, ErrModByZero
						}
						sels = append(sels, int64(i))
					}
					rs := encoding.DecodeUint64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					vec.Nsp = lv.Nsp.Or(rv.Nsp)
					vec.SetCol(mod.Uint64ModSels(lvs, rvs, rs, sels))
				}
				return vec, nil
			},
		},
	},
}
