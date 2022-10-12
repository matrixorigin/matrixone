// Copyright 2021 - 2022 Matrix Origin
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
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

const (
	MaxSpacePerRowOfLpad  = int64(16 * 1024 * 1024)
	ParameterSourceString = int(0)
	ParameterLengths      = int(1)
	ParameterPadString    = int(2)
)

/*
First Parameter: source string
Second Parameter: length
Third Parameter: pad string
*/
func Lpad(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	if vecs[0].IsScalarNull() || vecs[1].IsScalarNull() || vecs[2].IsScalarNull() {
		return proc.AllocScalarNullVector(vecs[0].Typ), nil
	}
	sourceStr := vector.MustStrCols(vecs[ParameterSourceString]) //Get the first arg

	//characters length not bytes length
	lengthsOfChars := getLensForLpad(vecs[ParameterLengths].Col)

	//pad string
	padStr := vector.MustStrCols(vecs[ParameterPadString])

	constVectors := []bool{vecs[ParameterSourceString].IsScalar(), vecs[ParameterLengths].IsScalar(), vecs[ParameterPadString].IsScalar()}
	inputNulls := []*nulls.Nulls{vecs[ParameterSourceString].Nsp, vecs[ParameterLengths].Nsp, vecs[ParameterPadString].Nsp}
	//evaluate bytes space for every row
	rowCount := vector.Length(vecs[ParameterSourceString])

	var resultVec *vector.Vector = nil
	resultValues := make([]string, rowCount)
	resultNUll := nulls.NewWithSize(rowCount)

	fillLpad(sourceStr, lengthsOfChars, padStr, rowCount, constVectors, resultValues, resultNUll, inputNulls)

	resultVec = vector.NewWithStrings(types.T_varchar.ToType(), resultValues, resultNUll, proc.Mp())
	return resultVec, nil
}

func getLensForLpad(col interface{}) []int64 {
	switch vs := col.(type) {
	case []int64:
		return vs
	case []float64:
		res := make([]int64, 0, len(vs))
		for _, v := range vs {
			if v > float64(math.MaxInt64) {
				res = append(res, math.MaxInt64)
			} else if v < float64(math.MinInt64) {
				res = append(res, math.MinInt64)
			} else {
				res = append(res, int64(v))
			}
		}
		return res
	case []uint64:
		res := make([]int64, 0, len(vs))
		for _, v := range vs {
			if v > uint64(math.MaxInt64) {
				res = append(res, math.MaxInt64)
			} else {
				res = append(res, int64(v))
			}
		}
		return res
	}
	panic("unexpected parameter types were received")
}

func fillLpad(sourceStr []string, lengths []int64, padStr []string, rowCount int, constVectors []bool, results []string, resultNUll *nulls.Nulls, inputNulls []*nulls.Nulls) {
	for i := 0; i < rowCount; i++ {
		if ui := uint64(i); nulls.Contains(inputNulls[ParameterSourceString], ui) ||
			nulls.Contains(inputNulls[ParameterLengths], ui) ||
			nulls.Contains(inputNulls[ParameterPadString], ui) {
			//null
			nulls.Add(resultNUll, ui)
			continue
		}
		//1.count characters in source and pad
		var source string
		if constVectors[ParameterSourceString] {
			source = sourceStr[0]
		} else {
			source = sourceStr[i]
		}
		sourceRune := []rune(string(source))
		sourceCharCnt := int64(len(sourceRune))
		var pad string
		if constVectors[ParameterPadString] {
			pad = padStr[0]
		} else {
			pad = padStr[i]
		}
		padLen := len(pad)
		if padLen == 0 {
			//empty string
			continue
		}
		padRune := []rune(string(pad))
		padCharCnt := int64(len(padRune))
		var targetLength int64
		if constVectors[ParameterLengths] {
			targetLength = lengths[0]
		} else {
			targetLength = lengths[i]
		}

		if targetLength < 0 || targetLength > MaxSpacePerRowOfLpad {
			//NULL
			nulls.Add(resultNUll, uint64(i))
		} else if targetLength == 0 {
			//empty string
		} else if targetLength < sourceCharCnt {
			//shorten source string
			sourcePart := sourceRune[:targetLength]
			sourcePartSlice := string(sourcePart)
			results[i] = sourcePartSlice
		} else if targetLength >= sourceCharCnt {
			//calculate the space count
			r := targetLength - sourceCharCnt
			//complete pad count
			p := r / padCharCnt
			//partial pad count
			m := r % padCharCnt
			padPartSlice := string(padRune[:m])
			for j := int64(0); j < p; j++ {
				results[i] += pad
			}
			if m != 0 {
				results[i] += padPartSlice
			}
			results[i] += source
		}
	}
}
