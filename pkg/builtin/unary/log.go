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
	"github.com/matrixorigin/matrixone/pkg/vectorize/log"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func init() {
	extend.FunctionRegistry["log"] = builtin.Log
	extend.UnaryReturnTypes[builtin.Log] = func(extend extend.Extend) types.T {
		return types.T_float64
	}

	extend.UnaryStrings[builtin.Log] = func(e extend.Extend) string {
		return fmt.Sprintf("Log(%s)", e)
	}

	overload.OpTypes[builtin.Log] = overload.Unary
	overload.UnaryOps[builtin.Log] = []*overload.UnaryOp{
		{ // T_uint8
			Typ:        types.T_uint8,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint8)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				logResult := log.LogUint8(origVecCol, results)
				if nulls.Any(logResult.Nsp) {
					if !nulls.Any(origVec.Nsp) {
						resultVector.Nsp = logResult.Nsp
					} else {
						resultVector.Nsp.Or(logResult.Nsp)
					}
				}
				return resultVector, err
			},
		},
		{ // T_uint16
			Typ:        types.T_uint16,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint16)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				logResult := log.LogUint16(origVecCol, results)
				if nulls.Any(logResult.Nsp) {
					if !nulls.Any(origVec.Nsp) {
						resultVector.Nsp = logResult.Nsp
					} else {
						resultVector.Nsp.Or(logResult.Nsp)
					}
				}
				return resultVector, err
			},
		},
		{ // T_uint32
			Typ:        types.T_uint32,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint32)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				logResult := log.LogUint32(origVecCol, results)
				if nulls.Any(logResult.Nsp) {
					if !nulls.Any(origVec.Nsp) {
						resultVector.Nsp = logResult.Nsp
					} else {
						resultVector.Nsp.Or(logResult.Nsp)
					}
				}
				return resultVector, err
			},
		},
		{ // T_uint64
			Typ:        types.T_uint64,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint64)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				logResult := log.LogUint64(origVecCol, results)
				if nulls.Any(logResult.Nsp) {
					if !nulls.Any(origVec.Nsp) {
						resultVector.Nsp = logResult.Nsp
					} else {
						resultVector.Nsp.Or(logResult.Nsp)
					}
				}
				return resultVector, err
			},
		},
		{ // T_int8
			Typ:        types.T_int8,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int8)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				logResult := log.LogInt8(origVecCol, results)
				if nulls.Any(logResult.Nsp) {
					if !nulls.Any(origVec.Nsp) {
						resultVector.Nsp = logResult.Nsp
					} else {
						resultVector.Nsp.Or(logResult.Nsp)
					}
				}
				return resultVector, err
			},
		},
		{ // T_int16
			Typ:        types.T_int16,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int16)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				logResult := log.LogInt16(origVecCol, results)
				if nulls.Any(logResult.Nsp) {
					if !nulls.Any(origVec.Nsp) {
						resultVector.Nsp = logResult.Nsp
					} else {
						resultVector.Nsp.Or(logResult.Nsp)
					}
				}
				return resultVector, err
			},
		},
		{ // T_int32
			Typ:        types.T_int32,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int32)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				logResult := log.LogInt32(origVecCol, results)
				if nulls.Any(logResult.Nsp) {
					if !nulls.Any(origVec.Nsp) {
						resultVector.Nsp = logResult.Nsp
					} else {
						resultVector.Nsp.Or(logResult.Nsp)
					}
				}
				return resultVector, err
			},
		},
		{ // T_int64
			Typ:        types.T_int64,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int64)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				logResult := log.LogInt64(origVecCol, results)
				if nulls.Any(logResult.Nsp) {
					if !nulls.Any(origVec.Nsp) {
						resultVector.Nsp = logResult.Nsp
					} else {
						resultVector.Nsp.Or(logResult.Nsp)
					}
				}
				return resultVector, err
			},
		},
		{ // T_float32
			Typ:        types.T_float32,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float32)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				logResult := log.LogFloat32(origVecCol, results)
				if nulls.Any(logResult.Nsp) {
					if !nulls.Any(origVec.Nsp) {
						resultVector.Nsp = logResult.Nsp
					} else {
						resultVector.Nsp.Or(logResult.Nsp)
					}
				}
				return resultVector, err
			},
		},
		{ // T_float64
			Typ:        types.T_float64,
			ReturnType: types.T_float64,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float64)
				resultVector, err := process.Get(proc, 8*int64(len(origVecCol)), types.Type{Oid: types.T_float64, Size: 8})
				if err != nil {
					return nil, err
				}
				results := encoding.DecodeFloat64Slice(resultVector.Data)
				results = results[:len(origVecCol)]
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				logResult := log.LogFloat64(origVecCol, results)
				if nulls.Any(logResult.Nsp) {
					if !nulls.Any(origVec.Nsp) {
						resultVector.Nsp = logResult.Nsp
					} else {
						resultVector.Nsp.Or(logResult.Nsp)
					}
				}
				return resultVector, err
			},
		},
	}
}
