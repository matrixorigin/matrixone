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
	"matrixone/pkg/vectorize/eq"
	"matrixone/pkg/vm/mempool"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"

	roaring "github.com/RoaringBitmap/roaring/roaring64"
)

func init() {
	BinOps[EQ] = []*BinOp{
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
						vec.SetCol(eq.Int8EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int8EqScalar(lvs[0], rvs, rs))
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
						vec.SetCol(eq.Int8EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int8EqScalar(rvs[0], lvs, rs))
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
					vec.SetCol(eq.Int8EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Int8EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Int8EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Int8Eq(lvs, rvs, rs))
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
						vec.SetCol(eq.Int16EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int16EqScalar(lvs[0], rvs, rs))
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
						vec.SetCol(eq.Int16EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int16EqScalar(rvs[0], lvs, rs))
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
					vec.SetCol(eq.Int16EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Int16EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Int16EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Int16Eq(lvs, rvs, rs))
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
						vec.SetCol(eq.Int32EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int32EqScalar(lvs[0], rvs, rs))
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
						vec.SetCol(eq.Int32EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int32EqScalar(rvs[0], lvs, rs))
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
					vec.SetCol(eq.Int32EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Int32EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Int32EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Int32Eq(lvs, rvs, rs))
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
						vec.SetCol(eq.Int64EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int64EqScalar(lvs[0], rvs, rs))
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
						vec.SetCol(eq.Int64EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Int64EqScalar(rvs[0], lvs, rs))
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
					vec.SetCol(eq.Int64EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Int64EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Int64EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Int64Eq(lvs, rvs, rs))
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
						vec.SetCol(eq.Uint8EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint8EqScalar(lvs[0], rvs, rs))
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
						vec.SetCol(eq.Uint8EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint8EqScalar(rvs[0], lvs, rs))
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
					vec.SetCol(eq.Uint8EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Uint8EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Uint8EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Uint8Eq(lvs, rvs, rs))
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
						vec.SetCol(eq.Uint16EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint16EqScalar(lvs[0], rvs, rs))
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
						vec.SetCol(eq.Uint16EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint16EqScalar(rvs[0], lvs, rs))
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
					vec.SetCol(eq.Uint16EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Uint16EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Uint16EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Uint16Eq(lvs, rvs, rs))
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
						vec.SetCol(eq.Uint32EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint32EqScalar(lvs[0], rvs, rs))
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
						vec.SetCol(eq.Uint32EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint32EqScalar(rvs[0], lvs, rs))
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
					vec.SetCol(eq.Uint32EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Uint32EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Uint32EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Uint32Eq(lvs, rvs, rs))
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
						vec.SetCol(eq.Uint64EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint64EqScalar(lvs[0], rvs, rs))
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
						vec.SetCol(eq.Uint64EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Uint64EqScalar(rvs[0], lvs, rs))
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
					vec.SetCol(eq.Uint64EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Uint64EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Uint64EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Uint64Eq(lvs, rvs, rs))
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
						vec.SetCol(eq.Float32EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Float32EqScalar(lvs[0], rvs, rs))
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
						vec.SetCol(eq.Float32EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Float32EqScalar(rvs[0], lvs, rs))
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
					vec.SetCol(eq.Float32EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Float32EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Float32EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Float32Eq(lvs, rvs, rs))
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
						vec.SetCol(eq.Float64EqNullableScalar(lvs[0], rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Float64EqScalar(lvs[0], rvs, rs))
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
						vec.SetCol(eq.Float64EqNullableScalar(rvs[0], lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.Float64EqScalar(rvs[0], lvs, rs))
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
					vec.SetCol(eq.Float64EqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.Float64EqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.Float64EqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.Float64Eq(lvs, rvs, rs))
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
						vec.SetCol(eq.StrEqNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.StrEqScalar(lvs.Data, rvs, rs))
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
						vec.SetCol(eq.StrEqNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.StrEqScalar(rvs.Data, lvs, rs))
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
					vec.SetCol(eq.StrEqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.StrEqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.StrEqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.StrEq(lvs, rvs, rs))
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
						vec.SetCol(eq.StrEqNullableScalar(lvs.Data, rvs, rv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.StrEqScalar(lvs.Data, rvs, rs))
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
						vec.SetCol(eq.StrEqNullableScalar(rvs.Data, lvs, lv.Nsp.Np, rs))
					} else {
						vec.SetCol(eq.StrEqScalar(rvs.Data, lvs, rs))
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
					vec.SetCol(eq.StrEqNullable(lvs, rvs, roaring.Or(lv.Nsp.Np, rv.Nsp.Np), rs))
				case !lv.Nsp.Any() && rv.Nsp.Any():
					vec.SetCol(eq.StrEqNullable(lvs, rvs, rv.Nsp.Np, rs))
				case lv.Nsp.Any() && !rv.Nsp.Any():
					vec.SetCol(eq.StrEqNullable(lvs, rvs, lv.Nsp.Np, rs))
				default:
					vec.SetCol(eq.StrEq(lvs, rvs, rs))
				}
				copy(vec.Data, mempool.OneCount)
				return vec, nil
			},
		},
	}
}
