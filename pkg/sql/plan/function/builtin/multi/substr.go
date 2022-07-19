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
	"math"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/substring"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Substring(inputVecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	// get the number of substr function parameters
	var paramNum = len(inputVecs)
	srcVector := inputVecs[0]
	startVector := inputVecs[1]
	// Substr function has no length parameter
	if paramNum == 2 {
		if srcVector.IsScalarNull() || startVector.IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_char, Size: 24}), nil
		}
	} else { //Substring column with length parameter
		lengthVector := inputVecs[2]
		if srcVector.IsScalarNull() || startVector.IsScalarNull() || lengthVector.IsScalarNull() {
			return proc.AllocScalarNullVector(types.Type{Oid: types.T_char, Size: 24}), nil
		}
	}
	if srcVector.IsScalar() {
		return substrSrcConst(inputVecs, proc)
	} else {
		return substrSrcCol(inputVecs, proc)
	}
}

// substring first parameter is constant
func substrSrcConst(inputVecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var paramNum = len(inputVecs)
	srcVector := inputVecs[0]
	startVector := inputVecs[1]
	columnSrcCol := vector.MustBytesCols(srcVector)

	// request new memory space for result column
	rows := calcResultVectorRows(inputVecs)
	resultVec := proc.AllocScalarVector(srcVector.Typ)
	results := &types.Bytes{
		Data:    make([]byte, int64(len(columnSrcCol.Data)*rows)),
		Offsets: make([]uint32, rows),
		Lengths: make([]uint32, rows),
	}
	//set null row
	//nulls.Or(inputVecs[0].Nsp, inputVecs[1].Nsp, resultVec.Nsp)
	if paramNum == 2 {
		nulls.Or(inputVecs[0].Nsp, inputVecs[1].Nsp, resultVec.Nsp)
	} else {
		nulls.Or(inputVecs[0].Nsp, inputVecs[1].Nsp, resultVec.Nsp)
		nulls.Or(inputVecs[2].Nsp, resultVec.Nsp, resultVec.Nsp)
	}

	if startVector.IsScalar() {
		if paramNum == 2 {
			// get start constant value
			startValue := castConstAsint64(startVector.Col, startVector.Typ.Oid, 0)

			resultVec.IsConst = true
			if startValue > 0 {
				vector.SetCol(resultVec, substring.SubstringFromLeftConstOffsetUnbounded(columnSrcCol, results, startValue-1))
			} else if startValue < 0 {
				vector.SetCol(resultVec, substring.SubstringFromRightConstOffsetUnbounded(columnSrcCol, results, -startValue))
			} else {
				vector.SetCol(resultVec, substring.SubstringFromZeroConstOffsetUnbounded(columnSrcCol, results))
			}
			return resultVec, nil
		} else { //has third parameter
			lengthVector := inputVecs[2]
			// if length parameter is constant
			if lengthVector.IsScalar() {
				resultVec.IsConst = true
				// get start constant value
				startValue := castConstAsint64(startVector.Col, startVector.Typ.Oid, 0)

				// get length constant value
				lengthValue := castConstAsint64(lengthVector.Col, lengthVector.Typ.Oid, 0)

				if startValue > 0 {
					vector.SetCol(resultVec, substring.SubstringFromLeftConstOffsetBounded(columnSrcCol, results, startValue-1, lengthValue))
				} else if startValue < 0 {
					vector.SetCol(resultVec, substring.SubstringFromRightConstOffsetBounded(columnSrcCol, results, -startValue, lengthValue))
				} else {
					vector.SetCol(resultVec, substring.SubstringFromZeroConstOffsetBounded(columnSrcCol, results))
				}
				return resultVec, nil
			} else {
				columnStartCol := inputVecs[1].Col
				columnStartType := inputVecs[1].Typ.Oid
				columnLengthCol := inputVecs[2].Col
				columnLengthType := inputVecs[2].Typ.Oid
				cs := []bool{inputVecs[0].IsScalar(), inputVecs[1].IsScalar(), inputVecs[2].IsScalar()}
				vector.SetCol(resultVec, substring.SubstringDynamicOffsetBounded(columnSrcCol, results, columnStartCol, columnStartType, columnLengthCol, columnLengthType, cs))
				return resultVec, nil
			}
		}
	} else {
		if paramNum == 2 {
			//The pos column is a variable or an expression
			columnStartCol := inputVecs[1].Col
			columnStartType := inputVecs[1].Typ.Oid
			vector.SetCol(resultVec, substring.SubstringDynamicOffsetUnbounded(columnSrcCol, results, columnStartCol, columnStartType))
			return resultVec, nil
		} else {
			//Substring column with length parameter
			columnStartCol := inputVecs[1].Col
			columnStartType := inputVecs[1].Typ.Oid
			columnLengthCol := inputVecs[2].Col
			columnLengthType := inputVecs[2].Typ.Oid
			cs := []bool{inputVecs[0].IsScalar(), inputVecs[1].IsScalar(), inputVecs[2].IsScalar()}
			vector.SetCol(resultVec, substring.SubstringDynamicOffsetBounded(columnSrcCol, results, columnStartCol, columnStartType, columnLengthCol, columnLengthType, cs))
			return resultVec, nil
		}
	}
}

// substring first paramter is column
func substrSrcCol(inputVecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var paramNum = len(inputVecs)
	srcVector := inputVecs[0]
	startVector := inputVecs[1]
	columnSrcCol := srcVector.Col.(*types.Bytes)

	// request new memory space for result column
	resultVec, err := proc.AllocVector(srcVector.Typ, int64(len(columnSrcCol.Data)))
	if err != nil {
		return nil, err
	}
	results := &types.Bytes{
		Data:    resultVec.Data,
		Offsets: make([]uint32, len(columnSrcCol.Offsets)),
		Lengths: make([]uint32, len(columnSrcCol.Lengths)),
	}
	//set null row
	//nulls.Set(resultVec.Nsp, inputVecs[0].Nsp)
	if paramNum == 2 {
		nulls.Or(inputVecs[0].Nsp, inputVecs[1].Nsp, resultVec.Nsp)
	} else {
		nulls.Or(inputVecs[0].Nsp, inputVecs[1].Nsp, resultVec.Nsp)
		nulls.Or(inputVecs[2].Nsp, resultVec.Nsp, resultVec.Nsp)
	}

	if startVector.IsScalar() {
		if paramNum == 2 {
			// get start constant value
			startValue := castConstAsint64(startVector.Col, startVector.Typ.Oid, 0)
			if startValue > 0 {
				vector.SetCol(resultVec, substring.SubstringFromLeftConstOffsetUnbounded(columnSrcCol, results, startValue-1))
			} else if startValue < 0 {
				vector.SetCol(resultVec, substring.SubstringFromRightConstOffsetUnbounded(columnSrcCol, results, -startValue))
			} else {
				//startValue == 0
				vector.SetCol(resultVec, substring.SubstringFromZeroConstOffsetUnbounded(columnSrcCol, results))
			}
			return resultVec, nil
		} else { //has third parameter
			lengthVector := inputVecs[2]
			// if length parameter is constant
			if lengthVector.IsScalar() {
				// get start constant value
				startValue := castConstAsint64(startVector.Col, startVector.Typ.Oid, 0)
				// get length constant value
				lengthValue := castConstAsint64(lengthVector.Col, lengthVector.Typ.Oid, 0)
				if startValue > 0 {
					vector.SetCol(resultVec, substring.SubstringFromLeftConstOffsetBounded(columnSrcCol, results, startValue-1, lengthValue))
				} else if startValue < 0 {
					vector.SetCol(resultVec, substring.SubstringFromRightConstOffsetBounded(columnSrcCol, results, -startValue, lengthValue))
				} else {
					//startValue == 0
					vector.SetCol(resultVec, substring.SubstringFromZeroConstOffsetBounded(columnSrcCol, results))
				}
				return resultVec, nil
			} else {
				columnStartCol := inputVecs[1].Col
				columnStartType := inputVecs[1].Typ.Oid
				columnLengthCol := inputVecs[2].Col
				columnLengthType := inputVecs[2].Typ.Oid
				cs := []bool{inputVecs[0].IsScalar(), inputVecs[1].IsScalar(), inputVecs[2].IsScalar()}
				vector.SetCol(resultVec, substring.SubstringDynamicOffsetBounded(columnSrcCol, results, columnStartCol, columnStartType, columnLengthCol, columnLengthType, cs))
				return resultVec, nil
			}
		}
	} else {
		if paramNum == 2 {
			//The pos column is a variable or an expression
			columnStartCol := inputVecs[1].Col
			columnStartType := inputVecs[1].Typ.Oid
			vector.SetCol(resultVec, substring.SubstringDynamicOffsetUnbounded(columnSrcCol, results, columnStartCol, columnStartType))
			return resultVec, nil
		} else {
			//Substring column with length parameter
			columnStartCol := inputVecs[1].Col
			columnStartType := inputVecs[1].Typ.Oid
			columnLengthCol := inputVecs[2].Col
			columnLengthType := inputVecs[2].Typ.Oid
			cs := []bool{inputVecs[0].IsScalar(), inputVecs[1].IsScalar(), inputVecs[2].IsScalar()}
			vector.SetCol(resultVec, substring.SubstringDynamicOffsetBounded(columnSrcCol, results, columnStartCol, columnStartType, columnLengthCol, columnLengthType, cs))
			return resultVec, nil
		}
	}
}

// calcResultVectorRows : Calculate size of returned result rows, which is used to calculate the memory space required
func calcResultVectorRows(inputVecs []*vector.Vector) int {
	if len(inputVecs) == 2 {
		if inputVecs[0].IsScalar() && inputVecs[1].IsScalar() {
			return 1
		} else {
			return inputVecs[0].Length
		}
	} else {
		if inputVecs[0].IsScalar() && inputVecs[1].IsScalar() && inputVecs[2].IsScalar() {
			return 1
		} else {
			return inputVecs[0].Length
		}
	}
}

func castConstAsint64(srcColumn interface{}, columnType types.T, idx int) int64 {
	var dstValue int64
	switch columnType {
	case types.T_uint8:
		dstValue = int64(srcColumn.([]uint8)[idx])
	case types.T_uint16:
		dstValue = int64(srcColumn.([]uint16)[idx])
	case types.T_uint32:
		dstValue = int64(srcColumn.([]uint32)[idx])
	case types.T_uint64:
		dstValue = int64(srcColumn.([]uint64)[idx])
		if srcColumn.([]uint64)[idx] > math.MaxInt64 {
			dstValue = math.MaxInt64
		}
	case types.T_int8:
		dstValue = int64(srcColumn.([]int8)[idx])
	case types.T_int16:
		dstValue = int64(srcColumn.([]int16)[idx])
	case types.T_int32:
		dstValue = int64(srcColumn.([]int32)[idx])
	case types.T_int64:
		dstValue = srcColumn.([]int64)[idx]
	default:
		dstValue = int64(0)
	}
	return dstValue
}
