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

package unary

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/builtin"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/bin"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

var binArgsAndRets = []argsAndRet{
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
}

func init() {
	// register built-in func and its args and rets
	extend.FunctionRegistry["bin"] = builtin.Bin
	overload.OpTypes[builtin.Bin] = overload.Unary

	for _, item := range binArgsAndRets {
		overload.AppendFunctionRets(builtin.Bin, item.args, item.ret)
	}

	extend.UnaryReturnTypes[builtin.Bin] = func(e extend.Extend) types.T {
		return getUnaryReturnType(builtin.Bin, e)
	}

	// add built-in stringify
	extend.UnaryStrings[builtin.Bin] = func(e extend.Extend) string {
		return fmt.Sprintf("bin(%s)", e)
	}

	overload.UnaryOps[builtin.Bin] = []*overload.UnaryOp{
		{
			Typ:        types.T_uint8,
			ReturnType: types.T_varchar,
			Fn:         binFn,
		},
		{
			Typ:        types.T_uint16,
			ReturnType: types.T_varchar,
			Fn:         binFn,
		},
		{
			Typ:        types.T_uint32,
			ReturnType: types.T_varchar,
			Fn:         binFn,
		},
		{
			Typ:        types.T_uint64,
			ReturnType: types.T_varchar,
			Fn:         binFn,
		},
		{
			Typ:        types.T_int8,
			ReturnType: types.T_varchar,
			Fn:         binFn,
		},
		{
			Typ:        types.T_int16,
			ReturnType: types.T_varchar,
			Fn:         binFn,
		},
		{
			Typ:        types.T_int32,
			ReturnType: types.T_varchar,
			Fn:         binFn,
		},
		{
			Typ:        types.T_int64,
			ReturnType: types.T_varchar,
			Fn:         binFn,
		},
		{
			Typ:        types.T_float32,
			ReturnType: types.T_varchar,
			Fn:         binFn,
		},
		{
			Typ:        types.T_float64,
			ReturnType: types.T_varchar,
			Fn:         binFn,
		},
	}
}

func binFn(origVec *vector.Vector, proc *process.Process, _ bool) (*vector.Vector, error) {
	bytesNeed, colLen := calcBytesNeed(origVec.Col)
	retVec, err := process.Get(proc, bytesNeed, types.Type{Oid: types.T_varchar, Size: 24})
	if err != nil {
		return nil, err
	}
	results := &types.Bytes{
		Data:    retVec.Data,
		Offsets: make([]uint32, colLen),
		Lengths: make([]uint32, colLen),
	}
	retVec.Col = results
	nulls.Set(retVec.Nsp, origVec.Nsp)
	vector.SetCol(retVec, toBinary(origVec.Col, results))
	return retVec, nil
}

func toBinary(vecCol interface{}, results *types.Bytes) *types.Bytes {
	switch col := vecCol.(type) {
	case []uint8:
		results = bin.Uint8ToBinary(col, results)
	case []uint16:
		results = bin.Uint16ToBinary(col, results)
	case []uint32:
		results = bin.Uint32ToBinary(col, results)
	case []uint64:
		results = bin.Uint64ToBinary(col, results)

	case []int8:
		results = bin.Int8ToBinary(col, results)
	case []int16:
		results = bin.Int16ToBinary(col, results)
	case []int32:
		results = bin.Int32ToBinary(col, results)
	case []int64:
		results = bin.Int64ToBinary(col, results)

	case []float32:
		results = bin.Float32ToBinary(col, results)
	case []float64:
		results = bin.Float64ToBinary(col, results)
	}

	return results
}

func calcBytesNeed(vecCol interface{}) (bytesNeed int64, colLen int) {
	switch col := vecCol.(type) {
	case []uint8:
		bytesNeed = bin.Uint8BitLen(col)
		colLen = len(col)
	case []uint16:
		bytesNeed = bin.Uint16BitLen(col)
		colLen = len(col)
	case []uint32:
		bytesNeed = bin.Uint32BitLen(col)
		colLen = len(col)
	case []uint64:
		bytesNeed = bin.Uint64BitLen(col)
		colLen = len(col)

	case []int8:
		bytesNeed = bin.Int8BitLen(col)
		colLen = len(col)
	case []int16:
		bytesNeed = bin.Int16BitLen(col)
		colLen = len(col)
	case []int32:
		bytesNeed = bin.Int32BitLen(col)
		colLen = len(col)
	case []int64:
		bytesNeed = bin.Int64BitLen(col)
		colLen = len(col)

	case []float32:
		bytesNeed = bin.Float32BitLen(col)
		colLen = len(col)
	case []float64:
		bytesNeed = bin.Float64BitLen(col)
		colLen = len(col)
	}

	return
}
