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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"math"
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
	sourceStr := vector.MustBytesCols(vecs[ParameterSourceString]) //Get the first arg

	//characters length not bytes length
	lengthsOfChars := getLensForLpad(vecs[ParameterLengths].Col)

	//pad string
	padStr := vector.MustBytesCols(vecs[ParameterPadString])

	constVectors := []bool{vecs[ParameterSourceString].IsScalar(), vecs[ParameterLengths].IsScalar(), vecs[ParameterPadString].IsScalar()}
	inputNulls := []*nulls.Nulls{vecs[ParameterSourceString].Nsp, vecs[ParameterLengths].Nsp, vecs[ParameterPadString].Nsp}
	//evaluate bytes space for every row
	rowCount := vector.Length(vecs[ParameterSourceString])

	//space length of result output
	neededSpaceLength := getSpaceLengthOfLpad(sourceStr, lengthsOfChars, padStr, rowCount, constVectors, inputNulls)

	var resultVec *vector.Vector = nil
	var results *types.Bytes = nil
	var err error = nil
	if constVectors[ParameterSourceString] && constVectors[ParameterLengths] && constVectors[ParameterPadString] {
		resultVec = proc.AllocScalarVector(types.T_varchar.ToType())
		results = &types.Bytes{
			Data:    make([]byte, neededSpaceLength),
			Offsets: make([]uint32, rowCount),
			Lengths: make([]uint32, rowCount),
		}
	} else {
		resultVec, err = proc.AllocVector(types.T_varchar.ToType(), neededSpaceLength)
		if err != nil {
			return nil, err
		}
		results = &types.Bytes{
			Data:    resultVec.Data,
			Offsets: make([]uint32, rowCount),
			Lengths: make([]uint32, rowCount),
		}
	}

	resultNUll := new(nulls.Nulls)

	fillLpad(sourceStr, lengthsOfChars, padStr, rowCount, constVectors, results, resultNUll, inputNulls)

	resultVec.Nsp = resultNUll
	vector.SetCol(resultVec, results)
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

func getSpaceLengthOfLpad(sourceStr *types.Bytes, lengths []int64, padStr *types.Bytes, rowCount int, constVectors []bool, inputNulls []*nulls.Nulls) int64 {
	neededSpace := int64(0)
	for i := 0; i < rowCount; i++ {
		if ui := uint64(i); nulls.Contains(inputNulls[ParameterSourceString], ui) ||
			nulls.Contains(inputNulls[ParameterLengths], ui) ||
			nulls.Contains(inputNulls[ParameterPadString], ui) {
			//null
			continue
		}
		//1.count characters in source and pad
		var source []byte
		if constVectors[ParameterSourceString] {
			source = sourceStr.Get(int64(0))
		} else {
			source = sourceStr.Get(int64(i))
		}

		sourceLen := len(source)
		sourceRune := []rune(string(source))
		sourceCharCnt := int64(len(sourceRune))
		var pad []byte
		if constVectors[ParameterPadString] {
			pad = padStr.Get(int64(0))
		} else {
			pad = padStr.Get(int64(i))
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
		} else if targetLength == 0 {
			//empty string
		} else if targetLength < sourceCharCnt {
			//shorten source string
			sourcePart := sourceRune[:targetLength]
			sourcePartLen := len([]byte(string(sourcePart)))
			neededSpace += int64(sourcePartLen)
		} else if targetLength >= sourceCharCnt {
			//calculate the space count
			r := targetLength - sourceCharCnt
			//complete pad count
			p := r / padCharCnt
			//partial pad count
			m := r % padCharCnt
			padSpaceCount := p*int64(padLen) + int64(len(string(padRune[:m]))) + int64(sourceLen)
			neededSpace += padSpaceCount
		}
	}
	return neededSpace
}

func fillLpad(sourceStr *types.Bytes, lengths []int64, padStr *types.Bytes, rowCount int, constVectors []bool, results *types.Bytes, resultNUll *nulls.Nulls, inputNulls []*nulls.Nulls) {
	target := uint32(0)
	for i := 0; i < rowCount; i++ {
		if ui := uint64(i); nulls.Contains(inputNulls[ParameterSourceString], ui) ||
			nulls.Contains(inputNulls[ParameterLengths], ui) ||
			nulls.Contains(inputNulls[ParameterPadString], ui) {
			//null
			nulls.Add(resultNUll, ui)
			results.Offsets[i] = target
			results.Lengths[i] = 0
			continue
		}
		//1.count characters in source and pad
		var source []byte
		if constVectors[ParameterSourceString] {
			source = sourceStr.Get(int64(0))
		} else {
			source = sourceStr.Get(int64(i))
		}
		sourceLen := len(source)
		sourceRune := []rune(string(source))
		sourceCharCnt := int64(len(sourceRune))
		var pad []byte
		if constVectors[ParameterPadString] {
			pad = padStr.Get(int64(0))
		} else {
			pad = padStr.Get(int64(i))
		}
		padLen := len(pad)
		if padLen == 0 {
			//empty string
			results.Offsets[i] = target
			results.Lengths[i] = 0
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
			results.Offsets[i] = target
			results.Lengths[i] = 0
		} else if targetLength == 0 {
			//empty string
			results.Offsets[i] = target
			results.Lengths[i] = 0
		} else if targetLength < sourceCharCnt {
			//shorten source string
			sourcePart := sourceRune[:targetLength]
			sourcePartSlice := []byte(string(sourcePart))
			sourcePartLen := len(sourcePartSlice)
			copy(results.Data[target:], sourcePartSlice)
			results.Offsets[i] = target
			results.Lengths[i] = uint32(sourcePartLen)
			target += uint32(sourcePartLen)
		} else if targetLength >= sourceCharCnt {
			//calculate the space count
			r := targetLength - sourceCharCnt
			//complete pad count
			p := r / padCharCnt
			//partial pad count
			m := r % padCharCnt
			padPartSlice := []byte(string(padRune[:m]))
			padPartSliceLen := len(padPartSlice)
			padSpaceCount := p*int64(padLen) + int64(padPartSliceLen) + int64(sourceLen)
			results.Offsets[i] = target
			results.Lengths[i] = uint32(padSpaceCount)
			for j := int64(0); j < p; j++ {
				copy(results.Data[target:], pad)
				target += uint32(padLen)
			}
			if m != 0 {
				copy(results.Data[target:], padPartSlice)
				target += uint32(padPartSliceLen)
			}
			copy(results.Data[target:], source)
			target += uint32(sourceLen)
		}
	}
}
