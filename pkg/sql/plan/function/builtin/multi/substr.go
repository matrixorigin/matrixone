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
	"context"
	"math"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/substring"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// Cast, cast ...  sigh.
func castConstAsInt64(ctx context.Context, vec *vector.Vector, idx int64) (int64, error) {
	switch vec.GetType().Oid {
	case types.T_uint8:
		return int64(vector.MustFixedCol[uint8](vec)[idx]), nil
	case types.T_uint16:
		return int64(vector.MustFixedCol[uint16](vec)[idx]), nil
	case types.T_uint32:
		return int64(vector.MustFixedCol[uint32](vec)[idx]), nil
	case types.T_uint64:
		val := vector.MustFixedCol[uint64](vec)[idx]
		if val > uint64(math.MaxInt64) {
			return 0, moerr.NewInvalidArg(ctx, "function substring(str, start, lenth)", val)
		}
		return int64(val), nil
	case types.T_int8:
		return int64(vector.MustFixedCol[int8](vec)[idx]), nil
	case types.T_int16:
		return int64(vector.MustFixedCol[int16](vec)[idx]), nil
	case types.T_int32:
		return int64(vector.MustFixedCol[int32](vec)[idx]), nil
	case types.T_int64:
		return int64(vector.MustFixedCol[int64](vec)[idx]), nil
	case types.T_float32:
		return int64(vector.MustFixedCol[float32](vec)[idx]), nil
	case types.T_float64:
		val := vector.MustFixedCol[float64](vec)[idx]
		if val > float64(math.MaxInt64) {
			return 0, moerr.NewInvalidArg(ctx, "function substring(str, start, lenth)", val)
		}
		return int64(val), nil
	default:
		panic("castConstAsInt64 failed, unknown type")
	}
}

func numSliceToI64[T types.BuiltinNumber](input []T) []int64 {
	ret := make([]int64, len(input))
	for i, v := range input {
		ret[i] = int64(v)
	}
	return ret
}

func castTVecAsInt64(vec *vector.Vector) []int64 {
	switch vec.GetType().Oid {
	case types.T_uint8:
		return numSliceToI64(vector.MustFixedCol[uint8](vec))
	case types.T_uint16:
		return numSliceToI64(vector.MustFixedCol[uint16](vec))
	case types.T_uint32:
		return numSliceToI64(vector.MustFixedCol[uint32](vec))
	case types.T_uint64:
		return numSliceToI64(vector.MustFixedCol[uint64](vec))
	case types.T_int8:
		return numSliceToI64(vector.MustFixedCol[int8](vec))
	case types.T_int16:
		return numSliceToI64(vector.MustFixedCol[int16](vec))
	case types.T_int32:
		return numSliceToI64(vector.MustFixedCol[int32](vec))
	case types.T_int64:
		return numSliceToI64(vector.MustFixedCol[int64](vec))
	case types.T_float32:
		return numSliceToI64(vector.MustFixedCol[float32](vec))
	case types.T_float64:
		return numSliceToI64(vector.MustFixedCol[float64](vec))
	default:
		panic("castTVecAsInt64 failed, unknown type")
	}
}

// XXX Unless I mis read the code, substring simply does the following
//				columnSrcCol := vector.MustStrCols(srcVector)
//				columnStartCol := castTVecAsInt64(startVector)
//				columnLengthCol := castTVecAsInt64(lengthVector)
//				cs := []bool{inputVecs[0].IsConst(), inputVecs[1].IsConst(), inputVecs[2].IsConst()}
//				substring.SubstringDynamicOffsetBounded(columnSrcCol, results, columnStartCol, columnLengthCol, cs)
//				return vector.NewWithStrings(srcVector.Typ, results, resultNsp, proc.Mp), nil
// What are we doing here?

func Substring(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	// get the number of substr function parameters
	var paramNum = len(ivecs)
	srcVector := ivecs[0]
	startVector := ivecs[1]
	// Substr function has no length parameter
	if paramNum == 2 {
		if srcVector.IsConstNull() || startVector.IsConstNull() {
			return vector.NewConstNull(types.T_char.ToType(), srcVector.Length(), proc.Mp()), nil
		}
	} else { //Substring column with length parameter
		lengthVector := ivecs[2]
		if srcVector.IsConstNull() || startVector.IsConstNull() || lengthVector.IsConstNull() {
			return vector.NewConstNull(types.T_char.ToType(), srcVector.Length(), proc.Mp()), nil
		}
	}
	if srcVector.IsConst() {
		return substrSrcConst(ivecs, proc)
	} else {
		return substrSrcCol(ivecs, proc)
	}
}

// substring first parameter is constant
func substrSrcConst(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var paramNum = len(ivecs)
	srcVector := ivecs[0]
	startVector := ivecs[1]

	if startVector.IsConstNull() {
		return vector.NewConstNull(*srcVector.GetType(), srcVector.Length(), proc.Mp()), nil
	}

	// XXX if this vector is const, then it is not expanded.  Really?
	columnSrcCol := vector.MustStrCol(srcVector)

	// request new memory space for result column
	rows := calcResultVectorRows(ivecs)
	results := make([]string, rows)
	resultNsp := nulls.NewWithSize(rows)

	// set null row
	if paramNum == 2 {
		nulls.Or(ivecs[0].GetNulls(), ivecs[1].GetNulls(), resultNsp)
	} else {
		nulls.Or(ivecs[0].GetNulls(), ivecs[1].GetNulls(), resultNsp)
		nulls.Or(ivecs[2].GetNulls(), resultNsp, resultNsp)
	}

	if startVector.IsConst() {
		if paramNum == 2 {
			// get start constant value
			startValue, err := castConstAsInt64(proc.Ctx, startVector, 0)
			if err != nil {
				return nil, err
			}
			if startValue > 0 {
				substring.SubstringFromLeftConstOffsetUnbounded(columnSrcCol, results, startValue-1)
			} else if startValue < 0 {
				substring.SubstringFromRightConstOffsetUnbounded(columnSrcCol, results, -startValue)
			} else {
				substring.SubstringFromZeroConstOffsetUnbounded(columnSrcCol, results)
			}
			return vector.NewConstBytes(*srcVector.GetType(), []byte(results[0]), srcVector.Length(), proc.Mp()), nil
		} else { //has third parameter
			lengthVector := ivecs[2]
			if lengthVector.IsConst() {
				// get start constant value
				startValue, err := castConstAsInt64(proc.Ctx, startVector, 0)
				if err != nil {
					return nil, err
				}
				// get length constant value
				lengthValue, err := castConstAsInt64(proc.Ctx, lengthVector, 0)
				if err != nil {
					return nil, err
				}

				if startValue > 0 {
					substring.SubstringFromLeftConstOffsetBounded(columnSrcCol, results, startValue-1, lengthValue)
				} else if startValue < 0 {
					substring.SubstringFromRightConstOffsetBounded(columnSrcCol, results, -startValue, lengthValue)
				} else {
					substring.SubstringFromZeroConstOffsetBounded(columnSrcCol, results)
				}
				return vector.NewConstBytes(*srcVector.GetType(), []byte(results[0]), srcVector.Length(), proc.Mp()), nil
			} else {
				columnStartCol := castTVecAsInt64(startVector)
				columnLengthCol := castTVecAsInt64(lengthVector)
				cs := []bool{ivecs[0].IsConst(), ivecs[1].IsConst(), ivecs[2].IsConst()}
				substring.SubstringDynamicOffsetBounded(columnSrcCol, results, columnStartCol, columnLengthCol, cs)
				vec := vector.NewVec(*srcVector.GetType())
				vector.AppendStringList(vec, results, nil, proc.Mp())
				vec.SetNulls(resultNsp)
				return vec, nil
			}
		}
	} else {
		if paramNum == 2 {
			//The pos column is a variable or an expression
			columnStartCol := castTVecAsInt64(ivecs[1])
			cs := []bool{ivecs[0].IsConst(), ivecs[1].IsConst()}
			substring.SubstringDynamicOffsetUnbounded(columnSrcCol, results, columnStartCol, cs)
			vec := vector.NewVec(*srcVector.GetType())
			vector.AppendStringList(vec, results, nil, proc.Mp())
			vec.SetNulls(resultNsp)
			return vec, nil
		} else {
			//Substring column with length parameter
			columnStartCol := castTVecAsInt64(ivecs[1])
			columnLengthCol := castTVecAsInt64(ivecs[2])
			cs := []bool{ivecs[0].IsConst(), ivecs[1].IsConst(), ivecs[2].IsConst()}
			substring.SubstringDynamicOffsetBounded(columnSrcCol, results, columnStartCol, columnLengthCol, cs)
			vec := vector.NewVec(*srcVector.GetType())
			vector.AppendStringList(vec, results, nil, proc.Mp())
			vec.SetNulls(resultNsp)
			return vec, nil
		}
	}
}

// substring first paramter is column
func substrSrcCol(inputVecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	var paramNum = len(inputVecs)
	srcVector := inputVecs[0]
	startVector := inputVecs[1]
	columnSrcCol := vector.MustStrCol(srcVector)

	// request new memory space for result column
	results := make([]string, len(columnSrcCol))

	//set null row
	resultNsp := nulls.NewWithSize(len(results))
	if paramNum == 2 {
		nulls.Or(inputVecs[0].GetNulls(), inputVecs[1].GetNulls(), resultNsp)
	} else {
		nulls.Or(inputVecs[0].GetNulls(), inputVecs[1].GetNulls(), resultNsp)
		nulls.Or(inputVecs[2].GetNulls(), resultNsp, resultNsp)
	}

	if startVector.IsConst() {
		if paramNum == 2 {
			// get start constant value
			startValue, err := castConstAsInt64(proc.Ctx, startVector, 0)
			if err != nil {
				return nil, err
			}
			if startValue > 0 {
				substring.SubstringFromLeftConstOffsetUnbounded(columnSrcCol, results, startValue-1)
			} else if startValue < 0 {
				substring.SubstringFromRightConstOffsetUnbounded(columnSrcCol, results, -startValue)
			} else {
				//startValue == 0
				substring.SubstringFromZeroConstOffsetUnbounded(columnSrcCol, results)
			}
			vec := vector.NewVec(*srcVector.GetType())
			vector.AppendStringList(vec, results, nil, proc.Mp())
			vec.SetNulls(resultNsp)
			return vec, nil
		} else { //has third parameter
			lengthVector := inputVecs[2]
			// if length parameter is constant
			if lengthVector.IsConst() {
				// get start constant value
				startValue, err := castConstAsInt64(proc.Ctx, startVector, 0)
				if err != nil {
					return nil, err
				}
				// get length constant value
				lengthValue, err := castConstAsInt64(proc.Ctx, lengthVector, 0)
				if err != nil {
					return nil, err
				}
				if startValue > 0 {
					substring.SubstringFromLeftConstOffsetBounded(columnSrcCol, results, startValue-1, lengthValue)
				} else if startValue < 0 {
					substring.SubstringFromRightConstOffsetBounded(columnSrcCol, results, -startValue, lengthValue)
				} else {
					//startValue == 0
					substring.SubstringFromZeroConstOffsetBounded(columnSrcCol, results)
				}
				vec := vector.NewVec(*srcVector.GetType())
				vector.AppendStringList(vec, results, nil, proc.Mp())
				return vec, nil
			} else {
				columnStartCol := castTVecAsInt64(inputVecs[1])
				columnLengthCol := castTVecAsInt64(inputVecs[2])
				cs := []bool{inputVecs[0].IsConst(), inputVecs[1].IsConst(), inputVecs[2].IsConst()}
				substring.SubstringDynamicOffsetBounded(columnSrcCol, results, columnStartCol, columnLengthCol, cs)
				vec := vector.NewVec(*srcVector.GetType())
				vector.AppendStringList(vec, results, nil, proc.Mp())
				vec.SetNulls(resultNsp)
				return vec, nil
			}
		}
	} else {
		if paramNum == 2 {
			//The pos column is a variable or an expression
			columnStartCol := castTVecAsInt64(inputVecs[1])
			cs := []bool{inputVecs[0].IsConst(), inputVecs[1].IsConst()}
			substring.SubstringDynamicOffsetUnbounded(columnSrcCol, results, columnStartCol, cs)
			vec := vector.NewVec(*srcVector.GetType())
			vector.AppendStringList(vec, results, nil, proc.Mp())
			vec.SetNulls(resultNsp)
			return vec, nil
		} else {
			columnStartCol := castTVecAsInt64(inputVecs[1])
			columnLengthCol := castTVecAsInt64(inputVecs[2])
			cs := []bool{inputVecs[0].IsConst(), inputVecs[1].IsConst(), inputVecs[2].IsConst()}
			substring.SubstringDynamicOffsetBounded(columnSrcCol, results, columnStartCol, columnLengthCol, cs)
			vec := vector.NewVec(*srcVector.GetType())
			vector.AppendStringList(vec, results, nil, proc.Mp())
			vec.SetNulls(resultNsp)
			return vec, nil
		}
	}
}

// calcResultVectorRows : Calculate size of returned result rows, which is used to calculate the memory space required
func calcResultVectorRows(inputVecs []*vector.Vector) int {
	if len(inputVecs) == 2 {
		if inputVecs[0].IsConst() && inputVecs[1].IsConst() {
			return 1
		} else {
			return inputVecs[0].Length()
		}
	} else {
		if inputVecs[0].IsConst() && inputVecs[1].IsConst() && inputVecs[2].IsConst() {
			return 1
		} else {
			return inputVecs[0].Length()
		}
	}
}
