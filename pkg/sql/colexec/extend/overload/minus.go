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
	"matrixone/pkg/container/nulls"
	"matrixone/pkg/container/types"
	"matrixone/pkg/container/vector"
	"matrixone/pkg/encoding"
	"matrixone/pkg/vectorize/sub"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func init() {
	BinOps[Minus] = []*BinOp{
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, lc, rc bool) (*vector.Vector, error) {
				lvs, rvs := lv.Col.([]int8), rv.Col.([]int8)
				switch {
				case lc && !rc:
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						sub.Int8SubScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(sub.Int8SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						sub.Int8SubScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt8Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(sub.Int8SubScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					sub.Int8Sub(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					sub.Int8Sub(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(sub.Int8Sub(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						sub.Int16SubScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(sub.Int16SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						sub.Int16SubScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt16Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(sub.Int16SubScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					sub.Int16Sub(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					sub.Int16Sub(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(sub.Int16Sub(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						sub.Int32SubScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(sub.Int32SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						sub.Int32SubScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt32Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(sub.Int32SubScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					sub.Int32Sub(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					sub.Int32Sub(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(sub.Int32Sub(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						sub.Int64SubScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(sub.Int64SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						sub.Int64SubScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeInt64Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(sub.Int64SubScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					sub.Int64Sub(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					sub.Int64Sub(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(sub.Int64Sub(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						sub.Uint8SubScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(sub.Uint8SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						sub.Uint8SubScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint8Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(sub.Uint8SubScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					sub.Uint8Sub(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					sub.Uint8Sub(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs)), lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(sub.Uint8Sub(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						sub.Uint16SubScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(sub.Uint16SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						sub.Uint16SubScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint16Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(sub.Uint16SubScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					sub.Uint16Sub(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					sub.Uint16Sub(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*2, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(sub.Uint16Sub(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						sub.Uint32SubScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(sub.Uint32SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						sub.Uint32SubScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint32Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(sub.Uint32SubScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					sub.Uint32Sub(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					sub.Uint32Sub(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(sub.Uint32Sub(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						sub.Uint64SubScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(sub.Uint64SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						sub.Uint64SubScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeUint64Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(sub.Uint64SubScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					sub.Uint64Sub(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					sub.Uint64Sub(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(sub.Uint64Sub(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						sub.Float32SubScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat32Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(sub.Float32SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						sub.Float32SubScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat32Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(sub.Float32SubScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					sub.Float32Sub(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					sub.Float32Sub(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*4, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(sub.Float32Sub(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
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
					if rv.Ref == 1 || rv.Ref == 0 {
						rv.Ref = 0
						sub.Float64SubScalar(lvs[0], rvs, rvs)
						return rv, nil
					}
					vec, err := register.Get(proc, int64(len(rvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data)
					rs = rs[:len(rvs)]
					vec.Nsp.Set(rv.Nsp)
					vec.SetCol(sub.Float64SubScalar(lvs[0], rvs, rs))
					return vec, nil
				case !lc && rc:
					if lv.Ref == 1 || lv.Ref == 0 {
						lv.Ref = 0
						sub.Float64SubScalar(rvs[0], lvs, lvs)
						return lv, nil
					}
					vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
					if err != nil {
						return nil, err
					}
					rs := encoding.DecodeFloat64Slice(vec.Data)
					rs = rs[:len(lvs)]
					vec.Nsp.Set(lv.Nsp)
					vec.SetCol(sub.Float64SubScalar(rvs[0], lvs, rs))
					return vec, nil
				case lv.Ref == 1 || lv.Ref == 0:
					lv.Ref = 0
					sub.Float64Sub(lvs, rvs, lvs)
					lv.Nsp = lv.Nsp.Or(rv.Nsp)
					if rv.Ref == 0 {
						register.Put(proc, rv)
					}
					return lv, nil
				case rv.Ref == 1 || rv.Ref == 0:
					rv.Ref = 0
					sub.Float64Sub(lvs, rvs, rvs)
					rv.Nsp = rv.Nsp.Or(lv.Nsp)
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
					return rv, nil
				}
				vec, err := register.Get(proc, int64(len(lvs))*8, lv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(rvs)]
				nulls.Or(lv.Nsp, rv.Nsp, vec.Nsp)
				vec.SetCol(sub.Float64Sub(lvs, rvs, rs))
				if lv.Ref == 0 {
					register.Put(proc, lv)
				}
				if rv.Ref == 0 {
					register.Put(proc, rv)
				}
				return vec, nil
			},
		},
	}
}
