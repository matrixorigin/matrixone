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
	"matrixone/pkg/vectorize/typecast"
	"matrixone/pkg/vm/process"
	"matrixone/pkg/vm/register"
)

func init() {
	BinOps[Typecast] = []*BinOp{
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, _ *vector.Vector, _ *process.Process, _, _ bool) (*vector.Vector, error) {
				return lv, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int16ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int32ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int64ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint8ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint16ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint32ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint64ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float32ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float64ToInt8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToInt8(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToInt8(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int8ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, _ *vector.Vector, _ *process.Process, _, _ bool) (*vector.Vector, error) {
				return lv, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int32ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int64ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint8ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(lvs)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.Uint16ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint32ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint64ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float32ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float64ToInt16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToInt16(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToInt16(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int8ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int16ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, _ *vector.Vector, _ *process.Process, _, _ bool) (*vector.Vector, error) {
				return lv, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int64ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint8ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint16ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint32ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint64ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float32ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float64ToInt32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToInt32(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToInt32(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int8ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int16ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int32ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, _ *vector.Vector, _ *process.Process, _, _ bool) (*vector.Vector, error) {
				return lv, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint8ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint16ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint32ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint64ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float32ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float64ToInt64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				vec.Nsp = lv.Nsp
				if _, err := typecast.BytesToInt64(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToInt64(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int8ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int16ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int32ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int64ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, _ *vector.Vector, _ *process.Process, _, _ bool) (*vector.Vector, error) {
				return lv, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint16ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint32ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint64ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float32ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float64ToUint8(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToUint8(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets)), rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToUint8(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int8ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int16ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int32ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int64ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint8ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, _ *vector.Vector, _ *process.Process, _, _ bool) (*vector.Vector, error) {
				return lv, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint32ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint64ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float32ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float64ToUint16(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToUint16(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*2, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToUint16(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int8ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int16ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int32ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int64ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint8ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint16ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, _ *vector.Vector, _ *process.Process, _, _ bool) (*vector.Vector, error) {
				return lv, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint64ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float32ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float64ToUint32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToUint32(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToUint32(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int8ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int16ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int32ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int64ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint8ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint16ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint32ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, _ *vector.Vector, _ *process.Process, _, _ bool) (*vector.Vector, error) {
				return lv, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float32ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float64ToUint64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToUint64(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToUint64(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int8ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int16ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int32ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int64ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint8ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint16ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint32ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint64ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, _ *vector.Vector, _ *process.Process, _, _ bool) (*vector.Vector, error) {
				return lv, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float64)
				vec, err := register.Get(proc, int64(len(lvs))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float64ToFloat32(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToFloat32(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*4, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToFloat32(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int8,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int8)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int8ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int16,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int16)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int16ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int32,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int32ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_int64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]int64)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Int64ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint8,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint8)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint8ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint16,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint16)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint16ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint32,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint32ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_uint64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]uint64)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Uint64ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float64,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, _ *vector.Vector, _ *process.Process, _, _ bool) (*vector.Vector, error) {
				return lv, nil
			},
		},
		&BinOp{
			LeftType:   types.T_float32,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				lvs := lv.Col.([]float32)
				vec, err := register.Get(proc, int64(len(lvs))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(lvs)]
				if _, err := typecast.Float32ToFloat64(lvs, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_char,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToFloat64(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
		&BinOp{
			LeftType:   types.T_varchar,
			RightType:  types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(lv, rv *vector.Vector, proc *process.Process, _, _ bool) (*vector.Vector, error) {
				defer func() {
					if lv.Ref == 0 {
						register.Put(proc, lv)
					}
				}()
				col := lv.Col.(*types.Bytes)
				vec, err := register.Get(proc, int64(len(col.Offsets))*8, rv.Typ)
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(col.Offsets)]
				if _, err := typecast.BytesToFloat64(col, rs); err != nil {
					register.Put(proc, vec)
					return nil, err
				}
				vec.Nsp.Set(lv.Nsp)
				vec.SetCol(rs)
				return vec, nil
			},
		},
	}
}
