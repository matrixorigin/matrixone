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
	"github.com/matrixorigin/matrixone/pkg/vectorize/oct"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var OctArgAndRets = []argsAndRet{
	{[]types.T{types.T_uint8}, types.T_char},
	{[]types.T{types.T_uint8}, types.T_varchar},
	{[]types.T{types.T_uint16}, types.T_char},
	{[]types.T{types.T_uint16}, types.T_varchar},
	{[]types.T{types.T_uint32}, types.T_char},
	{[]types.T{types.T_uint32}, types.T_varchar},
	{[]types.T{types.T_uint64}, types.T_char},
	{[]types.T{types.T_uint64}, types.T_varchar},
	{[]types.T{types.T_int8}, types.T_char},
	{[]types.T{types.T_int8}, types.T_varchar},
	{[]types.T{types.T_int16}, types.T_char},
	{[]types.T{types.T_int16}, types.T_varchar},
	{[]types.T{types.T_int32}, types.T_char},
	{[]types.T{types.T_int32}, types.T_varchar},
	{[]types.T{types.T_int64}, types.T_char},
	{[]types.T{types.T_int64}, types.T_varchar},
}

func init() {
	extend.FunctionRegistry["oct"] = builtin.Oct

	// register function args and returns
	for _, argAndRet := range OctArgAndRets {
		overload.AppendFunctionRets(builtin.Oct, argAndRet.args, argAndRet.ret)
	}

	extend.UnaryReturnTypes[builtin.Oct] = func(extend extend.Extend) types.T {
		return getUnaryReturnType(builtin.Oct, extend)
	}

	extend.UnaryStrings[builtin.Oct] = func(extend extend.Extend) string {
		return fmt.Sprintf("oct(%s)", extend)
	}

	overload.OpTypes[builtin.Oct] = overload.Unary

	// prepare for function call
	overload.UnaryOps[builtin.Oct] = []*overload.UnaryOp{
		{ // T_uint8 to T_char
			Typ:        types.T_uint8,
			ReturnType: types.T_char,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]uint8)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctUint8(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_char, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_uint8 to T_varchar
			Typ:        types.T_uint8,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]uint8)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctUint8(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_uint16 to T_char
			Typ:        types.T_uint16,
			ReturnType: types.T_char,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]uint16)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctUint16(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_char, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_uint16 to T_varchar
			Typ:        types.T_uint16,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]uint16)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctUint16(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_uint32 to T_char
			Typ:        types.T_uint32,
			ReturnType: types.T_char,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]uint32)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctUint32(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_char, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_uint32 to T_varchar
			Typ:        types.T_uint32,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]uint32)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctUint32(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_uint64 to T_char
			Typ:        types.T_uint64,
			ReturnType: types.T_char,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]uint64)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctUint64(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_char, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_uint64 to T_varchar
			Typ:        types.T_uint64,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]uint64)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctUint64(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_int8 to T_char
			Typ:        types.T_int8,
			ReturnType: types.T_char,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]int8)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctInt8(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_char, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_int8 to T_varchar
			Typ:        types.T_int8,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]int8)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctInt8(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_int16 to T_char
			Typ:        types.T_int16,
			ReturnType: types.T_char,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]int16)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctInt16(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_char, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_int16 to T_varchar
			Typ:        types.T_int16,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]int16)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctInt16(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_int32 to T_char
			Typ:        types.T_int32,
			ReturnType: types.T_char,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]int32)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctInt32(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_char, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_int32 to T_varchar
			Typ:        types.T_int32,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]int32)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctInt32(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_int64 to T_char
			Typ:        types.T_int64,
			ReturnType: types.T_char,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]int64)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctInt64(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_char, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
		{ // T_int64 to T_varchar
			Typ:        types.T_int64,
			ReturnType: types.T_varchar,
			Fn: func(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
				col := origVec.Col.([]int64)
				results := &types.Bytes{
					Data:    []byte{},
					Offsets: make([]uint32, len(col)),
					Lengths: make([]uint32, len(col)),
				}
				results = oct.OctInt64(col, results)

				resVec, err := process.Get(proc, int64(len(results.Data)), types.Type{Oid: types.T_varchar, Size: 24})
				if err != nil {
					return nil, err
				}
				nulls.Set(resVec.Nsp, origVec.Nsp)
				vector.SetCol(resVec, results)
				return resVec, nil
			},
		},
	}
}
