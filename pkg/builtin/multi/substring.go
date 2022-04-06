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
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/builtin"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend"
	"github.com/matrixorigin/matrixone/pkg/sql/colexec/extend/overload"
	"github.com/matrixorigin/matrixone/pkg/vectorize/substring"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

//declare function parameter types and return types.
var argAndrets = []argsAndRet{
	{[]types.T{types.T_char, types.T_uint8}, types.T_char},
	{[]types.T{types.T_char, types.T_uint16}, types.T_char},
	{[]types.T{types.T_char, types.T_uint32}, types.T_char},
	{[]types.T{types.T_char, types.T_uint64}, types.T_char},
	{[]types.T{types.T_char, types.T_int8}, types.T_char},
	{[]types.T{types.T_char, types.T_int16}, types.T_char},
	{[]types.T{types.T_char, types.T_int32}, types.T_char},
	{[]types.T{types.T_char, types.T_int64}, types.T_char},

	{[]types.T{types.T_char, types.T_uint8, types.T_uint8}, types.T_char},
	{[]types.T{types.T_char, types.T_uint8, types.T_uint16}, types.T_char},
	{[]types.T{types.T_char, types.T_uint8, types.T_uint32}, types.T_char},
	{[]types.T{types.T_char, types.T_uint8, types.T_uint64}, types.T_char},
	{[]types.T{types.T_char, types.T_uint8, types.T_int8}, types.T_char},
	{[]types.T{types.T_char, types.T_uint8, types.T_int16}, types.T_char},
	{[]types.T{types.T_char, types.T_uint8, types.T_int32}, types.T_char},
	{[]types.T{types.T_char, types.T_uint8, types.T_int64}, types.T_char},

	{[]types.T{types.T_char, types.T_uint16, types.T_uint8}, types.T_char},
	{[]types.T{types.T_char, types.T_uint16, types.T_uint16}, types.T_char},
	{[]types.T{types.T_char, types.T_uint16, types.T_uint32}, types.T_char},
	{[]types.T{types.T_char, types.T_uint16, types.T_uint64}, types.T_char},
	{[]types.T{types.T_char, types.T_uint16, types.T_int8}, types.T_char},
	{[]types.T{types.T_char, types.T_uint16, types.T_int16}, types.T_char},
	{[]types.T{types.T_char, types.T_uint16, types.T_int32}, types.T_char},
	{[]types.T{types.T_char, types.T_uint16, types.T_int64}, types.T_char},

	{[]types.T{types.T_char, types.T_uint32, types.T_uint8}, types.T_char},
	{[]types.T{types.T_char, types.T_uint32, types.T_uint16}, types.T_char},
	{[]types.T{types.T_char, types.T_uint32, types.T_uint32}, types.T_char},
	{[]types.T{types.T_char, types.T_uint32, types.T_uint64}, types.T_char},
	{[]types.T{types.T_char, types.T_uint32, types.T_int8}, types.T_char},
	{[]types.T{types.T_char, types.T_uint32, types.T_int16}, types.T_char},
	{[]types.T{types.T_char, types.T_uint32, types.T_int32}, types.T_char},
	{[]types.T{types.T_char, types.T_uint32, types.T_int64}, types.T_char},

	{[]types.T{types.T_char, types.T_uint64, types.T_uint8}, types.T_char},
	{[]types.T{types.T_char, types.T_uint64, types.T_uint16}, types.T_char},
	{[]types.T{types.T_char, types.T_uint64, types.T_uint32}, types.T_char},
	{[]types.T{types.T_char, types.T_uint64, types.T_uint64}, types.T_char},
	{[]types.T{types.T_char, types.T_uint64, types.T_int8}, types.T_char},
	{[]types.T{types.T_char, types.T_uint64, types.T_int16}, types.T_char},
	{[]types.T{types.T_char, types.T_uint64, types.T_int32}, types.T_char},
	{[]types.T{types.T_char, types.T_uint64, types.T_int64}, types.T_char},

	{[]types.T{types.T_char, types.T_int8, types.T_uint8}, types.T_char},
	{[]types.T{types.T_char, types.T_int8, types.T_uint16}, types.T_char},
	{[]types.T{types.T_char, types.T_int8, types.T_uint32}, types.T_char},
	{[]types.T{types.T_char, types.T_int8, types.T_uint64}, types.T_char},
	{[]types.T{types.T_char, types.T_int8, types.T_int8}, types.T_char},
	{[]types.T{types.T_char, types.T_int8, types.T_int16}, types.T_char},
	{[]types.T{types.T_char, types.T_int8, types.T_int32}, types.T_char},
	{[]types.T{types.T_char, types.T_int8, types.T_int64}, types.T_char},

	{[]types.T{types.T_char, types.T_int16, types.T_uint8}, types.T_char},
	{[]types.T{types.T_char, types.T_int16, types.T_uint16}, types.T_char},
	{[]types.T{types.T_char, types.T_int16, types.T_uint32}, types.T_char},
	{[]types.T{types.T_char, types.T_int16, types.T_uint64}, types.T_char},
	{[]types.T{types.T_char, types.T_int16, types.T_int8}, types.T_char},
	{[]types.T{types.T_char, types.T_int16, types.T_int16}, types.T_char},
	{[]types.T{types.T_char, types.T_int16, types.T_int32}, types.T_char},
	{[]types.T{types.T_char, types.T_int16, types.T_int64}, types.T_char},

	{[]types.T{types.T_char, types.T_int32, types.T_uint8}, types.T_char},
	{[]types.T{types.T_char, types.T_int32, types.T_uint16}, types.T_char},
	{[]types.T{types.T_char, types.T_int32, types.T_uint32}, types.T_char},
	{[]types.T{types.T_char, types.T_int32, types.T_uint64}, types.T_char},
	{[]types.T{types.T_char, types.T_int32, types.T_int8}, types.T_char},
	{[]types.T{types.T_char, types.T_int32, types.T_int16}, types.T_char},
	{[]types.T{types.T_char, types.T_int32, types.T_int32}, types.T_char},
	{[]types.T{types.T_char, types.T_int32, types.T_int64}, types.T_char},

	{[]types.T{types.T_char, types.T_int64, types.T_uint8}, types.T_char},
	{[]types.T{types.T_char, types.T_int64, types.T_uint16}, types.T_char},
	{[]types.T{types.T_char, types.T_int64, types.T_uint32}, types.T_char},
	{[]types.T{types.T_char, types.T_int64, types.T_uint64}, types.T_char},
	{[]types.T{types.T_char, types.T_int64, types.T_int8}, types.T_char},
	{[]types.T{types.T_char, types.T_int64, types.T_int16}, types.T_char},
	{[]types.T{types.T_char, types.T_int64, types.T_int32}, types.T_char},
	{[]types.T{types.T_char, types.T_int64, types.T_int64}, types.T_char},

	{[]types.T{types.T_varchar, types.T_uint8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint64}, types.T_varchar},

	{[]types.T{types.T_varchar, types.T_int8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int64}, types.T_varchar},

	{[]types.T{types.T_varchar, types.T_uint8, types.T_uint8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint8, types.T_uint16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint8, types.T_uint32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint8, types.T_uint64}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint8, types.T_int8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint8, types.T_int16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint8, types.T_int32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint8, types.T_int64}, types.T_varchar},

	{[]types.T{types.T_varchar, types.T_uint16, types.T_uint8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint16, types.T_uint16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint16, types.T_uint32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint16, types.T_uint64}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint16, types.T_int8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint16, types.T_int16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint16, types.T_int32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint16, types.T_int64}, types.T_varchar},

	{[]types.T{types.T_varchar, types.T_uint32, types.T_uint8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint32, types.T_uint16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint32, types.T_uint32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint32, types.T_uint64}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint32, types.T_int8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint32, types.T_int16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint32, types.T_int32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint32, types.T_int64}, types.T_varchar},

	{[]types.T{types.T_varchar, types.T_uint64, types.T_uint8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint64, types.T_uint16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint64, types.T_uint32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint64, types.T_uint64}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint64, types.T_int8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint64, types.T_int16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint64, types.T_int32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_uint64, types.T_int64}, types.T_varchar},

	{[]types.T{types.T_varchar, types.T_int8, types.T_uint8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int8, types.T_uint16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int8, types.T_uint32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int8, types.T_uint64}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int8, types.T_int8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int8, types.T_int16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int8, types.T_int32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int8, types.T_int64}, types.T_varchar},

	{[]types.T{types.T_varchar, types.T_int16, types.T_uint8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int16, types.T_uint16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int16, types.T_uint32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int16, types.T_uint64}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int16, types.T_int8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int16, types.T_int16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int16, types.T_int32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int16, types.T_int64}, types.T_varchar},

	{[]types.T{types.T_varchar, types.T_int32, types.T_uint8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int32, types.T_uint16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int32, types.T_uint32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int32, types.T_uint64}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int32, types.T_int8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int32, types.T_int16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int32, types.T_int32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int32, types.T_int64}, types.T_varchar},

	{[]types.T{types.T_varchar, types.T_int64, types.T_uint8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int64, types.T_uint16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int64, types.T_uint32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int64, types.T_uint64}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int64, types.T_int8}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int64, types.T_int16}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int64, types.T_int32}, types.T_varchar},
	{[]types.T{types.T_varchar, types.T_int64, types.T_int64}, types.T_varchar},
}

func init() {
	// function name registration
	extend.FunctionRegistry["substring"] = builtin.Substring
	extend.FunctionRegistry["substr"] = builtin.Substring

	// append function parameter types and return types
	for _, item := range argAndrets {
		overload.AppendFunctionRets(builtin.Substring, item.args, item.ret)
	}

	// define a get return type function for substring function
	extend.MultiReturnTypes[builtin.Substring] = func(extend []extend.Extend) types.T {
		return getMultiReturnType(builtin.Substring, extend)
	}

	// define a stringify function for sbustring
	extend.MultiStrings[builtin.Substring] = func(extends []extend.Extend) string {
		if len(extends) == 2 {
			return fmt.Sprintf("substring(%s, %s)", extends[0], extends[1])
		} else {
			return fmt.Sprintf("substring(%s, %s, %s)", extends[0], extends[1], extends[2])
		}
	}

	// register subtring function type
	overload.OpTypes[builtin.Substring] = overload.Multi

	// Preparation for function calling
	overload.MultiOps[builtin.Substring] = []*overload.MultiOp{
		//----------------------------------------process char parameter--------------------------------------
		{
			Min:        2,
			Max:        3,
			Typ:        types.T_char,
			ReturnType: types.T_char,
			Fn: func(inputVecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				// get the number of substr function parameters
				var paramNum int = len(inputVecs)
				column_src_col := inputVecs[0].Col.(*types.Bytes)
				var column_start_const bool = cs[1]

				// Substr function has no length parameter
				if paramNum == 2 {
					if column_start_const {
						// get start constant value
						start_value := inputVecs[1].Col.([]int64)[0]
						if start_value > 0 {
							// If the number of references of the first column is 1, reuse the column memory space
							if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
								inputVecs[0].Ref = 0
								substring.SliceFromLeftConstantOffsetUnbounded(column_src_col, column_src_col, start_value-1)
								return inputVecs[0], nil
							}
							// request new memory space for result column
							resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_char, Size: 24})
							if err != nil {
								return nil, err
							}
							results := &types.Bytes{
								Data:    resultVec.Data,
								Offsets: make([]uint32, len(column_src_col.Offsets)),
								Lengths: make([]uint32, len(column_src_col.Lengths)),
							}
							// set null row
							nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
							vector.SetCol(resultVec, substring.SliceFromLeftConstantOffsetUnbounded(column_src_col, results, start_value-1))
							return resultVec, nil
						} else if start_value < 0 {
							// If the number of references of the first column is 1, reuse the column memory space
							if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
								inputVecs[0].Ref = 0
								substring.SliceFromRightConstantOffsetUnbounded(column_src_col, column_src_col, -start_value)
								return inputVecs[0], nil
							}
							// request new memory space for result column
							resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_char, Size: 24})
							if err != nil {
								return nil, err
							}
							results := &types.Bytes{
								Data:    resultVec.Data,
								Offsets: make([]uint32, len(column_src_col.Offsets)),
								Lengths: make([]uint32, len(column_src_col.Lengths)),
							}
							//Set null row
							nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
							vector.SetCol(resultVec, substring.SliceFromRightConstantOffsetUnbounded(column_src_col, results, -start_value))
							return resultVec, nil
						} else {
							//start_value == 0
							if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
								inputVecs[0].Ref = 0
								substring.SliceFromZeroConstantOffsetUnbounded(column_src_col, column_src_col)
								return inputVecs[0], nil
							}

							resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_char, Size: 24})
							if err != nil {
								return nil, err
							}
							results := &types.Bytes{
								Data:    resultVec.Data,
								Offsets: make([]uint32, len(column_src_col.Offsets)),
								Lengths: make([]uint32, len(column_src_col.Lengths)),
							}
							//Set null row
							nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
							vector.SetCol(resultVec, substring.SliceFromZeroConstantOffsetUnbounded(column_src_col, results))
							return resultVec, nil
						}
					} else {
						//The pos column is a variable or an expression
						column_start_col := inputVecs[1].Col
						column_start_type := inputVecs[1].Typ.Oid

						// If the number of references of the first column is 1, reuse the column memory space
						if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
							inputVecs[0].Ref = 0
							substring.SliceDynamicOffsetUnbounded(column_src_col, column_src_col, column_start_col, column_start_type)
							return inputVecs[0], nil
						}
						// request new memory space for result column
						resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_char, Size: 24})
						if err != nil {
							return nil, err
						}
						results := &types.Bytes{
							Data:    resultVec.Data,
							Offsets: make([]uint32, len(column_src_col.Offsets)),
							Lengths: make([]uint32, len(column_src_col.Lengths)),
						}
						//set null row
						nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
						vector.SetCol(resultVec, substring.SliceDynamicOffsetUnbounded(column_src_col, results, column_start_col, column_start_type))
						return resultVec, nil
					}
				} else {
					//Substring column with length parameter
					var column_length_const bool = cs[2]
					if column_start_const && column_length_const {
						// get start constant value
						start_value := inputVecs[1].Col.([]int64)[0]
						// get length constant value
						length_value := inputVecs[2].Col.([]int64)[0]
						if start_value > 0 {
							if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
								inputVecs[0].Ref = 0
								substring.SliceFromLeftConstantOffsetBounded(column_src_col, column_src_col, start_value-1, length_value)
								return inputVecs[0], nil
							} else {
								// request new memory space for result column
								resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_varchar, Size: 24})
								if err != nil {
									return nil, err
								}
								results := &types.Bytes{
									Data:    resultVec.Data,
									Offsets: make([]uint32, len(column_src_col.Offsets)),
									Lengths: make([]uint32, len(column_src_col.Lengths)),
								}
								//Set null row
								nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
								vector.SetCol(resultVec, substring.SliceFromLeftConstantOffsetBounded(column_src_col, results, start_value-1, length_value))
								return resultVec, nil
							}
						} else if start_value < 0 {
							if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
								inputVecs[0].Ref = 0
								substring.SliceFromRightConstantOffsetBounded(column_src_col, column_src_col, -start_value, length_value)
								return inputVecs[0], nil
							} else {
								// request new memory space for result column
								resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_varchar, Size: 24})
								if err != nil {
									return nil, err
								}
								results := &types.Bytes{
									Data:    resultVec.Data,
									Offsets: make([]uint32, len(column_src_col.Offsets)),
									Lengths: make([]uint32, len(column_src_col.Lengths)),
								}
								//Set null row
								nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
								vector.SetCol(resultVec, substring.SliceFromRightConstantOffsetBounded(column_src_col, results, -start_value, length_value))
								return resultVec, nil
							}
						} else {
							//start_value == 0
							if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
								inputVecs[0].Ref = 0
								substring.SliceFromZeroConstantOffsetBounded(column_src_col, column_src_col)
								return inputVecs[0], nil
							}

							resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_char, Size: 24})
							if err != nil {
								return nil, err
							}
							results := &types.Bytes{
								Data:    resultVec.Data,
								Offsets: make([]uint32, len(column_src_col.Offsets)),
								Lengths: make([]uint32, len(column_src_col.Lengths)),
							}
							//Set null row
							nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
							vector.SetCol(resultVec, substring.SliceFromZeroConstantOffsetBounded(column_src_col, results))
							return resultVec, nil
						}
					} else {
						column_start_col := inputVecs[1].Col
						column_start_type := inputVecs[1].Typ.Oid
						column_length_col := inputVecs[2].Col
						column_length_type := inputVecs[2].Typ.Oid

						// If the number of references of the first column is 1, reuse the column memory space
						if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
							inputVecs[0].Ref = 0
							substring.SliceDynamicOffsetBounded(column_src_col, column_src_col, column_start_col, column_start_type, column_length_col, column_length_type, cs)
							return inputVecs[0], nil
						}
						// request new memory space for result column
						resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_char, Size: 24})
						if err != nil {
							return nil, err
						}
						results := &types.Bytes{
							Data:    resultVec.Data,
							Offsets: make([]uint32, len(column_src_col.Offsets)),
							Lengths: make([]uint32, len(column_src_col.Lengths)),
						}
						//set null row
						nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
						vector.SetCol(resultVec, substring.SliceDynamicOffsetBounded(column_src_col, results, column_start_col, column_start_type, column_length_col, column_length_type, cs))
						return resultVec, nil
					}
				}
			},
		},
		//----------------------------------------process varchar parameter--------------------------------------
		{
			Min:        2,
			Max:        3,
			Typ:        types.T_varchar,
			ReturnType: types.T_varchar,
			Fn: func(inputVecs []*vector.Vector, proc *process.Process, cs []bool) (*vector.Vector, error) {
				// get the number of substr function parameters
				var paramNum int = len(inputVecs)
				column_src_col := inputVecs[0].Col.(*types.Bytes)
				var column_start_const bool = cs[1]

				// Substr function has no length parameter
				if paramNum == 2 {
					if column_start_const {
						// get start constant value
						start_value := inputVecs[1].Col.([]int64)[0]
						if start_value > 0 {
							// If the number of references of the first column is 1, reuse the column memory space
							if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
								inputVecs[0].Ref = 0
								substring.SliceFromLeftConstantOffsetUnbounded(column_src_col, column_src_col, start_value-1)
								return inputVecs[0], nil
							}
							// request new memory space for result column
							resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_varchar, Size: 24})
							if err != nil {
								return nil, err
							}
							results := &types.Bytes{
								Data:    resultVec.Data,
								Offsets: make([]uint32, len(column_src_col.Offsets)),
								Lengths: make([]uint32, len(column_src_col.Lengths)),
							}
							//set null row
							nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
							vector.SetCol(resultVec, substring.SliceFromLeftConstantOffsetUnbounded(column_src_col, results, start_value-1))
							return resultVec, nil
						} else if start_value < 0 {
							// If the number of references of the first column is 1, reuse the column memory space
							if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
								inputVecs[0].Ref = 0
								substring.SliceFromRightConstantOffsetUnbounded(column_src_col, column_src_col, -start_value)
								return inputVecs[0], nil
							}
							// request new memory space for result column
							resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_varchar, Size: 24})
							if err != nil {
								return nil, err
							}
							results := &types.Bytes{
								Data:    resultVec.Data,
								Offsets: make([]uint32, len(column_src_col.Offsets)),
								Lengths: make([]uint32, len(column_src_col.Lengths)),
							}
							//Set null row
							nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
							vector.SetCol(resultVec, substring.SliceFromRightConstantOffsetUnbounded(column_src_col, results, -start_value))
							return resultVec, nil
						} else {
							//start_value == 0
							if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
								inputVecs[0].Ref = 0
								substring.SliceFromZeroConstantOffsetUnbounded(column_src_col, column_src_col)
								return inputVecs[0], nil
							}

							resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_varchar, Size: 24})
							if err != nil {
								return nil, err
							}
							results := &types.Bytes{
								Data:    resultVec.Data,
								Offsets: make([]uint32, len(column_src_col.Offsets)),
								Lengths: make([]uint32, len(column_src_col.Lengths)),
							}
							//Set null row
							nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
							vector.SetCol(resultVec, substring.SliceFromZeroConstantOffsetUnbounded(column_src_col, results))
							return resultVec, nil
						}
					} else {
						//The start column is a variable or an expression
						column_start_col := inputVecs[1].Col
						column_start_type := inputVecs[1].Typ.Oid

						// If the number of references of the first column is 1, reuse the column memory space
						if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
							inputVecs[0].Ref = 0
							substring.SliceDynamicOffsetUnbounded(column_src_col, column_src_col, column_start_col, column_start_type)
							return inputVecs[0], nil
						}
						// request new memory space for result column
						resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_varchar, Size: 24})
						if err != nil {
							return nil, err
						}
						results := &types.Bytes{
							Data:    resultVec.Data,
							Offsets: make([]uint32, len(column_src_col.Offsets)),
							Lengths: make([]uint32, len(column_src_col.Lengths)),
						}
						//set null row
						nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
						vector.SetCol(resultVec, substring.SliceDynamicOffsetUnbounded(column_src_col, results, column_start_col, column_start_type))
						return resultVec, nil
					}
				} else {
					//Substring column with length parameter
					var column_length_const bool = cs[2]
					if column_start_const && column_length_const {
						// get start constant value
						start_value := inputVecs[1].Col.([]int64)[0]
						// get length constant value
						length_value := inputVecs[2].Col.([]int64)[0]
						if start_value > 0 {
							if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
								inputVecs[0].Ref = 0
								substring.SliceFromLeftConstantOffsetBounded(column_src_col, column_src_col, start_value-1, length_value)
								return inputVecs[0], nil
							} else {
								// request new memory space for result column
								resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_varchar, Size: 24})
								if err != nil {
									return nil, err
								}
								results := &types.Bytes{
									Data:    resultVec.Data,
									Offsets: make([]uint32, len(column_src_col.Offsets)),
									Lengths: make([]uint32, len(column_src_col.Lengths)),
								}
								//Set null row
								nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
								vector.SetCol(resultVec, substring.SliceFromLeftConstantOffsetBounded(column_src_col, results, start_value-1, length_value))
								return resultVec, nil
							}
						} else if start_value < 0 {
							if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
								inputVecs[0].Ref = 0
								substring.SliceFromRightConstantOffsetBounded(column_src_col, column_src_col, -start_value, length_value)
								return inputVecs[0], nil
							} else {
								// request new memory space for result column
								resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_varchar, Size: 24})
								if err != nil {
									return nil, err
								}
								results := &types.Bytes{
									Data:    resultVec.Data,
									Offsets: make([]uint32, len(column_src_col.Offsets)),
									Lengths: make([]uint32, len(column_src_col.Lengths)),
								}
								//Set null row
								nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
								vector.SetCol(resultVec, substring.SliceFromRightConstantOffsetBounded(column_src_col, results, -start_value, length_value))
								return resultVec, nil
							}
						} else {
							//start_value == 0
							if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
								inputVecs[0].Ref = 0
								substring.SliceFromZeroConstantOffsetBounded(column_src_col, column_src_col)
								return inputVecs[0], nil
							}

							resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_varchar, Size: 24})
							if err != nil {
								return nil, err
							}
							results := &types.Bytes{
								Data:    resultVec.Data,
								Offsets: make([]uint32, len(column_src_col.Offsets)),
								Lengths: make([]uint32, len(column_src_col.Lengths)),
							}
							//Set null row
							nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
							vector.SetCol(resultVec, substring.SliceFromZeroConstantOffsetBounded(column_src_col, results))
							return resultVec, nil
						}
					} else {
						column_start_col := inputVecs[1].Col
						column_start_type := inputVecs[1].Typ.Oid
						column_length_col := inputVecs[2].Col
						column_length_type := inputVecs[2].Typ.Oid

						// If the number of references of the first column is 1, reuse the column memory space
						if inputVecs[0].Ref == 1 || inputVecs[0].Ref == 0 {
							inputVecs[0].Ref = 0
							substring.SliceDynamicOffsetBounded(column_src_col, column_src_col, column_start_col, column_start_type, column_length_col, column_length_type, cs)
							return inputVecs[0], nil
						}
						// request new memory space for result column
						resultVec, err := process.Get(proc, int64(len(column_src_col.Data)), types.Type{Oid: types.T_varchar, Size: 24})
						if err != nil {
							return nil, err
						}
						results := &types.Bytes{
							Data:    resultVec.Data,
							Offsets: make([]uint32, len(column_src_col.Offsets)),
							Lengths: make([]uint32, len(column_src_col.Lengths)),
						}
						//set null row
						nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
						vector.SetCol(resultVec, substring.SliceDynamicOffsetBounded(column_src_col, results, column_start_col, column_start_type, column_length_col, column_length_type, cs))
						return resultVec, nil
					}
				}
			},
		},
	}
}
