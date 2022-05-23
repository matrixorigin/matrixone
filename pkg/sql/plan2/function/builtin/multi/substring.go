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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/substring"
	process "github.com/matrixorigin/matrixone/pkg/vm/process"
)

// substring function's evaluation for arguments:
// first parameter: char
// second parameter: int8, int16, int32, int64, uint8, uint16, uint32, uint64,
// return type:char
func FdsSubstrChar2Param(inputVecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	// get the number of substr function parameters
	columnSrcCol := inputVecs[0].Col.(*types.Bytes)
	var columnStartConst bool = inputVecs[1].IsConstant()

	// Substr function has no length parameter
	if columnStartConst {
		// get start constant value
		startValue := inputVecs[1].Col.([]int64)[0]
		if startValue > 0 {
			// request new memory space for result column
			resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_char, Size: 24})
			if err != nil {
				return nil, err
			}
			results := &types.Bytes{
				Data:    resultVec.Data,
				Offsets: make([]uint32, len(columnSrcCol.Offsets)),
				Lengths: make([]uint32, len(columnSrcCol.Lengths)),
			}
			// set null row
			nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
			vector.SetCol(resultVec, substring.SubstringFromLeftConstOffsetUnbounded(columnSrcCol, results, startValue-1))
			return resultVec, nil
		} else if startValue < 0 {
			// request new memory space for result column
			resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_char, Size: 24})
			if err != nil {
				return nil, err
			}
			results := &types.Bytes{
				Data:    resultVec.Data,
				Offsets: make([]uint32, len(columnSrcCol.Offsets)),
				Lengths: make([]uint32, len(columnSrcCol.Lengths)),
			}
			//Set null row
			nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
			vector.SetCol(resultVec, substring.SubstringFromRightConstOffsetUnbounded(columnSrcCol, results, -startValue))
			return resultVec, nil
		} else {
			resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_char, Size: 24})
			if err != nil {
				return nil, err
			}
			results := &types.Bytes{
				Data:    resultVec.Data,
				Offsets: make([]uint32, len(columnSrcCol.Offsets)),
				Lengths: make([]uint32, len(columnSrcCol.Lengths)),
			}
			//Set null row
			nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
			vector.SetCol(resultVec, substring.SubstringFromZeroConstOffsetUnbounded(columnSrcCol, results))
			return resultVec, nil
		}
	} else {
		//The pos column is a variable or an expression
		columnStartCol := inputVecs[1].Col
		columnStartType := inputVecs[1].Typ.Oid

		// request new memory space for result column
		resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_char, Size: 24})
		if err != nil {
			return nil, err
		}
		results := &types.Bytes{
			Data:    resultVec.Data,
			Offsets: make([]uint32, len(columnSrcCol.Offsets)),
			Lengths: make([]uint32, len(columnSrcCol.Lengths)),
		}
		//set null row
		nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
		vector.SetCol(resultVec, substring.SubstringDynamicOffsetUnbounded(columnSrcCol, results, columnStartCol, columnStartType))
		return resultVec, nil
	}
}

// substring function's evaluation for arguments:
// first parameter: char
// second parameter: int8, int16, int32, int64, uint8, uint16, uint32, uint64,
// third parameter: int8, int16, int32, int64, uint8, uint16, uint32, uint64,
// return type:char
func FdsSubstrChar3Param(inputVecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	// get the number of substr function parameters
	columnSrcCol := inputVecs[0].Col.(*types.Bytes)
	var columnStartConst bool = inputVecs[1].IsConstant()

	//Substring column with length parameter
	var columnLengthConst bool = inputVecs[2].IsConstant()
	if columnStartConst && columnLengthConst {
		// get start constant value
		startValue := inputVecs[1].Col.([]int64)[0]
		// get length constant value
		lengthValue := inputVecs[2].Col.([]int64)[0]
		if startValue > 0 {
			// request new memory space for result column
			resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_varchar, Size: 24})
			if err != nil {
				return nil, err
			}
			results := &types.Bytes{
				Data:    resultVec.Data,
				Offsets: make([]uint32, len(columnSrcCol.Offsets)),
				Lengths: make([]uint32, len(columnSrcCol.Lengths)),
			}
			//Set null row
			nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
			vector.SetCol(resultVec, substring.SubstringFromLeftConstOffsetBounded(columnSrcCol, results, startValue-1, lengthValue))
			return resultVec, nil
		} else if startValue < 0 {
			// request new memory space for result column
			resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_varchar, Size: 24})
			if err != nil {
				return nil, err
			}
			results := &types.Bytes{
				Data:    resultVec.Data,
				Offsets: make([]uint32, len(columnSrcCol.Offsets)),
				Lengths: make([]uint32, len(columnSrcCol.Lengths)),
			}
			//Set null row
			nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
			vector.SetCol(resultVec, substring.SubstringFromRightConstOffsetBounded(columnSrcCol, results, -startValue, lengthValue))
			return resultVec, nil
		} else {
			resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_char, Size: 24})
			if err != nil {
				return nil, err
			}
			results := &types.Bytes{
				Data:    resultVec.Data,
				Offsets: make([]uint32, len(columnSrcCol.Offsets)),
				Lengths: make([]uint32, len(columnSrcCol.Lengths)),
			}
			//Set null row
			nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
			vector.SetCol(resultVec, substring.SubstringFromZeroConstOffsetBounded(columnSrcCol, results))
			return resultVec, nil
		}
	} else {
		columnStartCol := inputVecs[1].Col
		columnStartType := inputVecs[1].Typ.Oid
		columnLengthCol := inputVecs[2].Col
		columnLengthType := inputVecs[2].Typ.Oid
		cs := GetIsConstSliceFromVectors(inputVecs)

		// request new memory space for result column
		resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_char, Size: 24})
		if err != nil {
			return nil, err
		}
		results := &types.Bytes{
			Data:    resultVec.Data,
			Offsets: make([]uint32, len(columnSrcCol.Offsets)),
			Lengths: make([]uint32, len(columnSrcCol.Lengths)),
		}
		// set null row
		nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
		vector.SetCol(resultVec, substring.SubstringDynamicOffsetBounded(columnSrcCol, results, columnStartCol, columnStartType, columnLengthCol, columnLengthType, cs))
		return resultVec, nil
	}
}

// substring function's evaluation for arguments:
// first parameter: varchar
// second parameter: int8, int16, int32, int64, uint8, uint16, uint32, uint64,
// return type:varchar
func FdsSubstrVarchar2Param(inputVecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	// get the number of substr function parameters
	columnSrcCol := inputVecs[0].Col.(*types.Bytes)
	var columnStartConst bool = inputVecs[1].IsConstant()

	// Substr function has no length parameter
	if columnStartConst {
		// get start constant value
		startValue := inputVecs[1].Col.([]int64)[0]
		if startValue > 0 {
			// request new memory space for result column
			resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_varchar, Size: 24})
			if err != nil {
				return nil, err
			}
			results := &types.Bytes{
				Data:    resultVec.Data,
				Offsets: make([]uint32, len(columnSrcCol.Offsets)),
				Lengths: make([]uint32, len(columnSrcCol.Lengths)),
			}
			//set null row
			nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
			vector.SetCol(resultVec, substring.SubstringFromLeftConstOffsetUnbounded(columnSrcCol, results, startValue-1))
			return resultVec, nil
		} else if startValue < 0 {
			// request new memory space for result column
			resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_varchar, Size: 24})
			if err != nil {
				return nil, err
			}
			results := &types.Bytes{
				Data:    resultVec.Data,
				Offsets: make([]uint32, len(columnSrcCol.Offsets)),
				Lengths: make([]uint32, len(columnSrcCol.Lengths)),
			}
			//Set null row
			nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
			vector.SetCol(resultVec, substring.SubstringFromRightConstOffsetUnbounded(columnSrcCol, results, -startValue))
			return resultVec, nil
		} else {
			resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_varchar, Size: 24})
			if err != nil {
				return nil, err
			}
			results := &types.Bytes{
				Data:    resultVec.Data,
				Offsets: make([]uint32, len(columnSrcCol.Offsets)),
				Lengths: make([]uint32, len(columnSrcCol.Lengths)),
			}
			//Set null row
			nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
			vector.SetCol(resultVec, substring.SubstringFromZeroConstOffsetUnbounded(columnSrcCol, results))
			return resultVec, nil
		}
	} else {
		//The start column is a variable or an expression
		columnStartCol := inputVecs[1].Col
		columnStartType := inputVecs[1].Typ.Oid

		// request new memory space for result column
		resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_varchar, Size: 24})
		if err != nil {
			return nil, err
		}
		results := &types.Bytes{
			Data:    resultVec.Data,
			Offsets: make([]uint32, len(columnSrcCol.Offsets)),
			Lengths: make([]uint32, len(columnSrcCol.Lengths)),
		}
		//set null row
		nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
		vector.SetCol(resultVec, substring.SubstringDynamicOffsetUnbounded(columnSrcCol, results, columnStartCol, columnStartType))
		return resultVec, nil
	}
}

// substring function's evaluation for arguments:
// first parameter: varchar
// second parameter: int8, int16, int32, int64, uint8, uint16, uint32, uint64,
// third parameter: int8, int16, int32, int64, uint8, uint16, uint32, uint64,
// return type:varchar
func FdsSubstrVarchar3Param(inputVecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	// get the number of substr function parameters
	columnSrcCol := inputVecs[0].Col.(*types.Bytes)
	var columnStartConst bool = inputVecs[1].IsConstant()

	//Substring column with length parameter
	var columnLengthConst bool = inputVecs[2].IsConstant()
	if columnStartConst && columnLengthConst {
		// get start constant value
		startValue := inputVecs[1].Col.([]int64)[0]
		// get length constant value
		lengthValue := inputVecs[2].Col.([]int64)[0]
		if startValue > 0 {
			// request new memory space for result column
			resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_varchar, Size: 24})
			if err != nil {
				return nil, err
			}
			results := &types.Bytes{
				Data:    resultVec.Data,
				Offsets: make([]uint32, len(columnSrcCol.Offsets)),
				Lengths: make([]uint32, len(columnSrcCol.Lengths)),
			}
			//Set null row
			nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
			vector.SetCol(resultVec, substring.SubstringFromLeftConstOffsetBounded(columnSrcCol, results, startValue-1, lengthValue))
			return resultVec, nil
		} else if startValue < 0 {
			// request new memory space for result column
			resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_varchar, Size: 24})
			if err != nil {
				return nil, err
			}
			results := &types.Bytes{
				Data:    resultVec.Data,
				Offsets: make([]uint32, len(columnSrcCol.Offsets)),
				Lengths: make([]uint32, len(columnSrcCol.Lengths)),
			}
			//Set null row
			nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
			vector.SetCol(resultVec, substring.SubstringFromRightConstOffsetBounded(columnSrcCol, results, -startValue, lengthValue))
			return resultVec, nil
		} else {
			//start_value == 0
			resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_varchar, Size: 24})
			if err != nil {
				return nil, err
			}
			results := &types.Bytes{
				Data:    resultVec.Data,
				Offsets: make([]uint32, len(columnSrcCol.Offsets)),
				Lengths: make([]uint32, len(columnSrcCol.Lengths)),
			}
			//Set null row
			nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
			vector.SetCol(resultVec, substring.SubstringFromZeroConstOffsetBounded(columnSrcCol, results))
			return resultVec, nil
		}
	} else {
		columnStartCol := inputVecs[1].Col
		columnStartType := inputVecs[1].Typ.Oid
		columnLengthCol := inputVecs[2].Col
		columnLengthType := inputVecs[2].Typ.Oid
		cs := GetIsConstSliceFromVectors(inputVecs)

		// request new memory space for result column
		resultVec, err := process.Get(proc, int64(len(columnSrcCol.Data)), types.Type{Oid: types.T_varchar, Size: 24})
		if err != nil {
			return nil, err
		}
		results := &types.Bytes{
			Data:    resultVec.Data,
			Offsets: make([]uint32, len(columnSrcCol.Offsets)),
			Lengths: make([]uint32, len(columnSrcCol.Lengths)),
		}
		//set null row
		nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
		vector.SetCol(resultVec, substring.SubstringDynamicOffsetBounded(columnSrcCol, results, columnStartCol, columnStartType, columnLengthCol, columnLengthType, cs))
		return resultVec, nil
	}
}
