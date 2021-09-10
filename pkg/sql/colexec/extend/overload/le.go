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
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/ge"
	"matrixone/pkg/vectorize/le"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

func init() {
	BinOps[LE] = []*BinOp{
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Int8LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Int8LeScalar(lvs[0], rvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Int8GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Int8GeScalar(rvs[0], lvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int8LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int8LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Int8LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Int8Le(lvs, rvs, rs))
				}
				copy(vec.Data, mempool.OneCount)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int16), rv.Col.([]int16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Int16LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Int16LeScalar(lvs[0], rvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Int16GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Int16GeScalar(rvs[0], lvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int16LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int16LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Int16LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Int16Le(lvs, rvs, rs))
				}
				copy(vec.Data, mempool.OneCount)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int32), rv.Col.([]int32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Int32LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Int32LeScalar(lvs[0], rvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Int32GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Int32GeScalar(rvs[0], lvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int32LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int32LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Int32LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Int32Le(lvs, rvs, rs))
				}
				copy(vec.Data, mempool.OneCount)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int64), rv.Col.([]int64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Int64LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Int64LeScalar(lvs[0], rvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Int64GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Int64GeScalar(rvs[0], lvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int64LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Int64LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Int64LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Int64Le(lvs, rvs, rs))
				}
				copy(vec.Data, mempool.OneCount)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint8), rv.Col.([]uint8)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Uint8LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Uint8LeScalar(lvs[0], rvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Uint8GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Uint8GeScalar(rvs[0], lvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint8LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint8LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Uint8LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Uint8Le(lvs, rvs, rs))
				}
				copy(vec.Data, mempool.OneCount)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint16), rv.Col.([]uint16)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Uint16LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Uint16LeScalar(lvs[0], rvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Uint16GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Uint16GeScalar(rvs[0], lvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint16LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint16LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Uint16LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Uint16Le(lvs, rvs, rs))
				}
				copy(vec.Data, mempool.OneCount)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint32), rv.Col.([]uint32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Uint32LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Uint32LeScalar(lvs[0], rvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Uint32GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Uint32GeScalar(rvs[0], lvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint32LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint32LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Uint32LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Uint32Le(lvs, rvs, rs))
				}
				copy(vec.Data, mempool.OneCount)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]uint64), rv.Col.([]uint64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Uint64LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Uint64LeScalar(lvs[0], rvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Uint64GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Uint64GeScalar(rvs[0], lvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint64LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Uint64LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Uint64LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Uint64Le(lvs, rvs, rs))
				}
				copy(vec.Data, mempool.OneCount)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float32), rv.Col.([]float32)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Float32LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Float32LeScalar(lvs[0], rvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Float32GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Float32GeScalar(rvs[0], lvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Float32LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Float32LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Float32LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Float32Le(lvs, rvs, rs))
				}
				copy(vec.Data, mempool.OneCount)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]float64), rv.Col.([]float64)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs)]
					if rv.Nsp.Any() {
						vec.SetCol(le.Float64LeNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.Float64LeScalar(lvs[0], rvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.Float64GeNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.Float64GeScalar(rvs[0], lvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Float64LeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.Float64LeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.Float64LeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.Float64Le(lvs, rvs, rs))
				}
				copy(vec.Data, mempool.OneCount)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_char,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(le.StrLeNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.StrLeScalar(lvs.Data, rvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.StrGeNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.StrGeScalar(rvs.Data, lvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.StrLeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.StrLeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.StrLeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.StrLe(lvs, rvs, rs))
				}
				copy(vec.Data, mempool.OneCount)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_varchar,
			ReturnType: types.T_sel,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.(*types.Bytes), rv.Col.(*types.Bytes)
				switch {
				case lc && !rc:
					vec, err := register.Get(proc, int64(len(rvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(rvs.Lengths)]
					if rv.Nsp.Any() {
						vec.SetCol(le.StrLeNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(le.StrLeScalar(lvs.Data, rvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				case !lc && rc:
					vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
					rs = rs[:len(lvs.Lengths)]
					if lv.Nsp.Any() {
						vec.SetCol(ge.StrGeNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(ge.StrGeScalar(rvs.Data, lvs, rs))
					}
					copy(vec.Data, mempool.OneCount)
					return vec, nil
				}
				vec, err := register.Get(proc, int64(len(lvs.Lengths))*8, types.Type{Oid: types.T_sel, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data[mempool.CountSize:])
				rs = rs[:len(lvs.Lengths)]
				switch {
				case lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.StrLeNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(le.StrLeNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(le.StrLeNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(le.StrLe(lvs, rvs, rs))
				}
				copy(vec.Data, mempool.OneCount)
				return vec, nil
			},
		},
	}
}
