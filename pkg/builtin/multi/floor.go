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

package multi

import (
	"errors"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/builtin"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/floor"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	extend.FunctionRegistry["floor"] = builtin.Floor
	for _, item := range argsAndRets {
		overload.AppendFunctionRets(builtin.Floor, item.args, item.ret)
	}
	extend.MultiReturnTypes[builtin.Floor] = func(es []extend.Extend) types.T {
		return getMultiReturnType(builtin.Floor, es)
	}

	extend.MultiStrings[builtin.Floor] = func(es []extend.Extend) string {
		if len(es) > 1 {
			return fmt.Sprintf("floor(%s, %s)", es[0], es[1])
		} else {
			return fmt.Sprintf("floor(%s)", es[0])
		}
	}
	overload.OpTypes[builtin.Floor] = overload.Multi
	overload.MultiOps[builtin.Floor] = []*overload.MultiOp{
		{
			Min:        1,
			Max:        2,
			Typ:        types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(vecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				digits := int64(0)
				vs := vecs[0].Col.([]uint8)
				if len(vecs) > 1 {
					if !cs[1] || vecs[1].Typ.Oid != types.T_int64 {
						return nil, errors.New("the second argument of the floor function must be an int64 constant")
					}
					digits = vecs[1].Col.([]int64)[0]
				}
				if vecs[0].Ref == 1 || vecs[0].Ref == 0 {
					vecs[0].Ref = 0
					floor.FloorUint8(vs, vs, digits)
					return vecs[0], nil
				}
				vec, err := process.Get(proc, int64(len(vs)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint8Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				nulls.Set(vec.Nsp, vecs[0].Nsp)
				vector.SetCol(vec, floor.FloorUint8(vs, rs, digits))
				return vec, nil
			},
		},
		{
			Min:        1,
			Max:        2,
			Typ:        types.T_uint16,
			ReturnType: types.T_uint16,
			Fn: func(vecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				digits := int64(0)
				vs := vecs[0].Col.([]uint16)
				if len(vecs) > 1 {
					if !cs[1] || vecs[1].Typ.Oid != types.T_int64 {
						return nil, errors.New("the second argument of the floor function must be an int64 constant")
					}
					digits = vecs[1].Col.([]int64)[0]
				}
				if vecs[0].Ref == 1 || vecs[0].Ref == 0 {
					vecs[0].Ref = 0
					floor.FloorUint16(vs, vs, digits)
					return vecs[0], nil
				}
				vec, err := process.Get(proc, 2*int64(len(vs)), types.Type{Oid: types.T_uint16, Size: 2})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint16Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				nulls.Set(vec.Nsp, vecs[0].Nsp)
				vector.SetCol(vec, floor.FloorUint16(vs, rs, digits))
				return vec, nil
			},
		},
		{
			Min:        1,
			Max:        2,
			Typ:        types.T_uint32,
			ReturnType: types.T_uint32,
			Fn: func(vecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				digits := int64(0)
				vs := vecs[0].Col.([]uint32)
				if len(vecs) > 1 {
					if !cs[1] || vecs[1].Typ.Oid != types.T_int64 {
						return nil, errors.New("the second argument of the floor function must be an int64 constant")
					}
					digits = vecs[1].Col.([]int64)[0]
				}
				if vecs[0].Ref == 1 || vecs[0].Ref == 0 {
					vecs[0].Ref = 0
					floor.FloorUint32(vs, vs, digits)
					return vecs[0], nil
				}
				vec, err := process.Get(proc, 4*int64(len(vs)), types.Type{Oid: types.T_uint32, Size: 4})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint32Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				nulls.Set(vec.Nsp, vecs[0].Nsp)
				vector.SetCol(vec, floor.FloorUint32(vs, rs, digits))
				return vec, nil
			},
		},

		{
			Min:        1,
			Max:        2,
			Typ:        types.T_uint64,
			ReturnType: types.T_uint64,
			Fn: func(vecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				digits := int64(0)
				vs := vecs[0].Col.([]uint64)
				if len(vecs) > 1 {
					if !cs[1] || vecs[1].Typ.Oid != types.T_int64 {
						return nil, errors.New("the second argument of the floor function must be an int64 constant")
					}
					digits = vecs[1].Col.([]int64)[0]
				}
				if vecs[0].Ref == 1 || vecs[0].Ref == 0 {
					vecs[0].Ref = 0
					floor.FloorUint64(vs, vs, digits)
					return vecs[0], nil
				}
				vec, err := process.Get(proc, 8*int64(len(vs)), types.Type{Oid: types.T_uint64, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeUint64Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				nulls.Set(vec.Nsp, vecs[0].Nsp)
				vector.SetCol(vec, floor.FloorUint64(vs, rs, digits))
				return vec, nil
			},
		},

		{
			Min:        1,
			Max:        2,
			Typ:        types.T_int8,
			ReturnType: types.T_int8,
			Fn: func(vecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				digits := int64(0)
				vs := vecs[0].Col.([]int8)
				if len(vecs) > 1 {
					if !cs[1] || vecs[1].Typ.Oid != types.T_int64 {
						return nil, errors.New("the second argument of the floor function must be an int64 constant")
					}
					digits = vecs[1].Col.([]int64)[0]
				}
				if vecs[0].Ref == 1 || vecs[0].Ref == 0 {
					vecs[0].Ref = 0
					floor.FloorInt8(vs, vs, digits)
					return vecs[0], nil
				}
				vec, err := process.Get(proc, int64(len(vs)), types.Type{Oid: types.T_int8, Size: 1})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt8Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				nulls.Set(vec.Nsp, vecs[0].Nsp)
				vector.SetCol(vec, floor.FloorInt8(vs, rs, digits))
				return vec, nil
			},
		},

		{
			Min:        1,
			Max:        2,
			Typ:        types.T_int16,
			ReturnType: types.T_int16,
			Fn: func(vecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				digits := int64(0)
				vs := vecs[0].Col.([]int16)
				if len(vecs) > 1 {
					if !cs[1] || vecs[1].Typ.Oid != types.T_int64 {
						return nil, errors.New("the second argument of the floor function must be an int64 constant")
					}
					digits = vecs[1].Col.([]int64)[0]
				}
				if vecs[0].Ref == 1 || vecs[0].Ref == 0 {
					vecs[0].Ref = 0
					floor.FloorInt16(vs, vs, digits)
					return vecs[0], nil
				}
				vec, err := process.Get(proc, 2*int64(len(vs)), types.Type{Oid: types.T_int16, Size: 2})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt16Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				nulls.Set(vec.Nsp, vecs[0].Nsp)
				vector.SetCol(vec, floor.FloorInt16(vs, rs, digits))
				return vec, nil
			},
		},

		{
			Min:        1,
			Max:        2,
			Typ:        types.T_int32,
			ReturnType: types.T_int32,
			Fn: func(vecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				digits := int64(0)
				vs := vecs[0].Col.([]int32)
				if len(vecs) > 1 {
					if !cs[1] || vecs[1].Typ.Oid != types.T_int64 {
						return nil, errors.New("the second argument of the floor function must be an int64 constant")
					}
					digits = vecs[1].Col.([]int64)[0]
				}
				if vecs[0].Ref == 1 || vecs[0].Ref == 0 {
					vecs[0].Ref = 0
					floor.FloorInt32(vs, vs, digits)
					return vecs[0], nil
				}
				vec, err := process.Get(proc, 4*int64(len(vs)), types.Type{Oid: types.T_int32, Size: 4})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt32Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				nulls.Set(vec.Nsp, vecs[0].Nsp)
				vector.SetCol(vec, floor.FloorInt32(vs, rs, digits))
				return vec, nil
			},
		},

		{
			Min:        1,
			Max:        2,
			Typ:        types.T_int64,
			ReturnType: types.T_int64,
			Fn: func(vecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				digits := int64(0)
				vs := vecs[0].Col.([]int64)
				if len(vecs) > 1 {
					if !cs[1] || vecs[1].Typ.Oid != types.T_int64 {
						return nil, errors.New("the second argument of the floor function must be an int64 constant")
					}
					digits = vecs[1].Col.([]int64)[0]
				}
				if vecs[0].Ref == 1 || vecs[0].Ref == 0 {
					vecs[0].Ref = 0
					floor.FloorInt64(vs, vs, digits)
					return vecs[0], nil
				}
				vec, err := process.Get(proc, 8*int64(len(vs)), types.Type{Oid: types.T_int64, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeInt64Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				nulls.Set(vec.Nsp, vecs[0].Nsp)
				vector.SetCol(vec, floor.FloorInt64(vs, rs, digits))
				return vec, nil
			},
		},

		{
			Min:        1,
			Max:        2,
			Typ:        types.T_float32,
			ReturnType: types.T_float32,
			Fn: func(vecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				digits := int64(0)
				vs := vecs[0].Col.([]float32)
				if len(vecs) > 1 {
					if !cs[1] || vecs[1].Typ.Oid != types.T_int64 {
						return nil, errors.New("the second argument of the floor function must be an int64 constant")
					}
					digits = vecs[1].Col.([]int64)[0]
				}
				if vecs[0].Ref == 1 || vecs[0].Ref == 0 {
					vecs[0].Ref = 0
					floor.FloorFloat32(vs, vs, digits)
					return vecs[0], nil
				}
				vec, err := process.Get(proc, 4*int64(len(vs)), types.Type{Oid: types.T_float32, Size: 4})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat32Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				nulls.Set(vec.Nsp, vecs[0].Nsp)
				vector.SetCol(vec, floor.FloorFloat32(vs, rs, digits))
				return vec, nil
			},
		},

		{
			Min:        1,
			Max:        2,
			Typ:        types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(vecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				digits := int64(0)
				vs := vecs[0].Col.([]float64)
				if len(vecs) > 1 {
					if !cs[1] || vecs[1].Typ.Oid != types.T_int64 {
						return nil, errors.New("the second argument of the floor function must be an int64 constant")
					}
					digits = vecs[1].Col.([]int64)[0]
				}
				if vecs[0].Ref == 1 || vecs[0].Ref == 0 {
					vecs[0].Ref = 0
					floor.FloorFloat64(vs, vs, digits)
					return vecs[0], nil
				}
				vec, err := process.Get(proc, 8*int64(len(vs)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				rs := encoding.DecodeFloat64Slice(vec.Data)
				rs = rs[:len(vs)]
				vec.Col = rs
				nulls.Set(vec.Nsp, vecs[0].Nsp)
				vector.SetCol(vec, floor.FloorFloat64(vs, rs, digits))
				return vec, nil
			},
		},
	}
}
