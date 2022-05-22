// Copyright 2022 Matrix Origin
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

package unary

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/builtin"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/encoding"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/ascii"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	extend.FunctionRegistry["ascii"] = builtin.ASCII
	extend.UnaryReturnTypes[builtin.ASCII] = func(extend extend.Extend) types.T {
		return types.T_uint8
	}
	extend.UnaryStrings[builtin.ASCII] = func(e extend.Extend) string {
		return fmt.Sprintf("ascii(%s)", e)
	}
	overload.OpTypes[builtin.ASCII] = overload.Unary
	overload.UnaryOps[builtin.ASCII] = []*overload.UnaryOp{
		{ // T_uint8
			Typ:        types.T_uint8,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint8)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ascii.AsciiUint8(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_uint16
			Typ:        types.T_uint16,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint16)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ascii.AsciiUint16(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_uint32
			Typ:        types.T_uint32,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint32)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ascii.AsciiUint32(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_uint64
			Typ:        types.T_uint64,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint64)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ascii.AsciiUint64(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_int8
			Typ:        types.T_int8,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int8)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ascii.AsciiInt8(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_int16
			Typ:        types.T_int16,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int16)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ascii.AsciiInt16(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_int32
			Typ:        types.T_int32,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int32)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ascii.AsciiInt32(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_int64
			Typ:        types.T_int64,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int64)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ascii.AsciiInt64(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_float32
			Typ:        types.T_float32,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float32)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ascii.AsciiFloat32(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_float64
			Typ:        types.T_float64,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float64)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, ascii.AsciiFloat64(origVecCol, results))
				return resultVector, nil
			},
		},
		{ // T_char
			Typ:        types.T_char,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.(*types.Bytes)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol.Lengths)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol.Lengths)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				asciiResult := ascii.AsciiBytes(origVecCol, results)
				if nulls.Any(asciiResult.Nsp) {
					if !nulls.Any(resultVector.Nsp) {
						resultVector.Nsp = asciiResult.Nsp
					} else {
						resultVector.Nsp.Or(asciiResult.Nsp)
					}
				}
				vector.SetCol(resultVector, results)
				return resultVector, nil
			},
		},
		{ // T_varchar
			Typ:        types.T_varchar,
			ReturnType: types.T_uint8,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.(*types.Bytes)
				resultVector, err := process.Get(proc, 1*int64(len(origVecCol.Lengths)), types.Type{Oid: types.T_uint8, Size: 1})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeUint8Slice(resultVector.Data)
				results = results[:len(origVecCol.Lengths)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				asciiResult := ascii.AsciiBytes(origVecCol, results)
				if nulls.Any(asciiResult.Nsp) {
					if !nulls.Any(resultVector.Nsp) {
						resultVector.Nsp = asciiResult.Nsp
					} else {
						resultVector.Nsp.Or(asciiResult.Nsp)
					}
				}
				vector.SetCol(resultVector, results)
				return resultVector, nil
			},
		},
	}
}
