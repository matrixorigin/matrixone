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
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/space"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var argAndRetsOfSpaceFunc = []argsAndRet{
	{[]types.T{types.T_uint8}, types.T_varchar},
	{[]types.T{types.T_uint16}, types.T_varchar},
	{[]types.T{types.T_uint32}, types.T_varchar},
	{[]types.T{types.T_uint64}, types.T_varchar},
	{[]types.T{types.T_int8}, types.T_varchar},
	{[]types.T{types.T_int16}, types.T_varchar},
	{[]types.T{types.T_int32}, types.T_varchar},
	{[]types.T{types.T_int64}, types.T_varchar},
	{[]types.T{types.T_float32}, types.T_varchar},
	{[]types.T{types.T_float64}, types.T_varchar},
	{[]types.T{types.T_char}, types.T_varchar},
	{[]types.T{types.T_varchar}, types.T_varchar},
}

func init() {
	extend.FunctionRegistry["space"] = builtin.Space

	for _, item := range argAndRetsOfSpaceFunc {
		// append function parameter types and return types
		overload.AppendFunctionRets(builtin.Space, item.args, item.ret)
	}

	extend.UnaryReturnTypes[builtin.Space] = func(extend extend.Extend) types.T {
		return types.T_varchar
	}

	extend.UnaryStrings[builtin.Space] = func(extend extend.Extend) string {
		return fmt.Sprintf("space(%s)", extend)
	}

	overload.OpTypes[builtin.Space] = overload.Unary

	overload.UnaryOps[builtin.Space] = []*overload.UnaryOp{
		{
			// T_uint8
			Typ:        types.T_uint8,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint8)

				bytesNeed := space.CountSpacesForUnsignedInt(origVecCol)

				resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol)),
					Lengths: make([]uint32, len(origVecCol)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, space.FillSpacesUint8(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			// T_uint16
			Typ:        types.T_uint16,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint16)

				bytesNeed := space.CountSpacesForUnsignedInt(origVecCol)

				resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol)),
					Lengths: make([]uint32, len(origVecCol)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, space.FillSpacesUint16(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			// T_uint32
			Typ:        types.T_uint32,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint32)

				bytesNeed := space.CountSpacesForUnsignedInt(origVecCol)

				resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol)),
					Lengths: make([]uint32, len(origVecCol)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, space.FillSpacesUint32(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			// T_uint64
			Typ:        types.T_uint64,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]uint64)

				bytesNeed := space.CountSpacesForUnsignedInt(origVecCol)

				resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol)),
					Lengths: make([]uint32, len(origVecCol)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, space.FillSpacesUint64(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			// T_int8
			Typ:        types.T_int8,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int8)

				bytesNeed := space.CountSpacesForSignedInt(origVecCol)

				resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol)),
					Lengths: make([]uint32, len(origVecCol)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, space.FillSpacesInt8(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			// T_int16
			Typ:        types.T_int16,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int16)

				bytesNeed := space.CountSpacesForSignedInt(origVecCol)

				resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol)),
					Lengths: make([]uint32, len(origVecCol)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, space.FillSpacesInt16(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			// T_int32
			Typ:        types.T_int32,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int32)

				bytesNeed := space.CountSpacesForSignedInt(origVecCol)

				resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol)),
					Lengths: make([]uint32, len(origVecCol)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, space.FillSpacesInt32(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			// T_int64
			Typ:        types.T_int64,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]int64)

				bytesNeed := space.CountSpacesForSignedInt(origVecCol)

				resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol)),
					Lengths: make([]uint32, len(origVecCol)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, space.FillSpacesInt64(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			// T_float32
			Typ:        types.T_float32,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float32)

				bytesNeed := space.CountSpacesForFloat(origVecCol)

				resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol)),
					Lengths: make([]uint32, len(origVecCol)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, space.FillSpacesFloat32(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			// T_float64
			Typ:        types.T_float64,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.([]float64)

				bytesNeed := space.CountSpacesForFloat(origVecCol)

				resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol)),
					Lengths: make([]uint32, len(origVecCol)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, space.FillSpacesFloat64(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			// T_char
			Typ:        types.T_char,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.(*types.Bytes)

				bytesNeed := space.CountSpacesForString(origVecCol)

				resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol.Offsets)),
					Lengths: make([]uint32, len(origVecCol.Lengths)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, space.FillSpacesCharVarChar(origVecCol, results))
				return resultVector, nil
			},
		},
		{
			// T_varchar
			Typ:        types.T_varchar,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				origVecCol := origVec.Col.(*types.Bytes)

				bytesNeed := space.CountSpacesForString(origVecCol)

				resultVector, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				results := &types.Bytes{
					Data:    resultVector.Data,
					Offsets: make([]uint32, len(origVecCol.Offsets)),
					Lengths: make([]uint32, len(origVecCol.Lengths)),
				}
				resultVector.Col = results
				nulls.Set(resultVector.Nsp, origVec.Nsp)
				vector.SetCol(resultVector, space.FillSpacesCharVarChar(origVecCol, results))
				return resultVector, nil
			},
		},
	}
}
