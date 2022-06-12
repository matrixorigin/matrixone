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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

// this coupling in registeration and vectorize doesn't seem right, please revise this(broccoli)
func Concat_ws(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	// for now, input vectors are guaranteed to be of type char/varchar,
	// when our cast function stabilized, I will add cast to char types and
	// then use cast in this concat_ws to support more data types
	// this loop checks for function inputs
	var separator []byte
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	scalarCount := 0
	for i, v := range vectors {
		if v.IsScalarNull() {
			return nil, moerr.NewError(moerr.ERROR_FUNCTION_PARAMETER, "null constant for concat_ws is not supported now")
		}
		if i == 0 {
			if !v.IsScalar() {
				return nil, moerr.NewError(moerr.ERROR_FUNCTION_PARAMETER, "the first argument(the separator) for concat_ws must be a constant char/varchar")
			}
			separator = v.Col.(*types.Bytes).Data
			scalarCount++
		} else {
			if v.IsScalar() {
				scalarCount++
			}
		}
	}
	if scalarCount == len(vectors) {
		inputBytes := make([]*types.Bytes, len(vectors)-1)
		inputNsps := make([]*nulls.Nulls, len(vectors)-1)
		for i := 0; i < len(vectors)-1; i++ {
			inputBytes[i] = vectors[i+1].Col.(*types.Bytes)
			inputNsps[i] = vectors[i+1].Nsp
		}
		resultVector := vector.NewConst(resultType)
		resultValues := &types.Bytes{
			Data:    make([]byte, 0),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		result := concatWsInputBytes(inputBytes, inputNsps, separator, vectors[0].Length, resultValues)
		resultVector.Col = result.OutputBytes
		resultVector.Nsp = result.OutputNsp
		return resultVector, nil
	} else if scalarCount != 1 {
		return nil, moerr.NewError(moerr.ERROR_FUNCTION_PARAMETER, "for concat_ws, parameters except the first one must be all constant or all non-constant")
	}

	inputBytes := make([]*types.Bytes, len(vectors)-1)
	inputNsps := make([]*nulls.Nulls, len(vectors)-1)
	for i := 0; i < len(vectors)-1; i++ {
		inputBytes[i] = vectors[i+1].Col.(*types.Bytes)
		inputNsps[i] = vectors[i+1].Nsp
	}
	resultVector, err := proc.AllocVector(resultType, 0)
	if err != nil {
		return nil, moerr.NewError(moerr.INTERNAL_ERROR, "out of memory")
	}
	resultValues := &types.Bytes{
		Data:    resultVector.Data,
		Offsets: make([]uint32, vectors[0].Length),
		Lengths: make([]uint32, vectors[0].Length),
	}
	result := concatWsInputBytes(inputBytes, inputNsps, separator, vectors[0].Length, resultValues)
	resultVector.Col = result.OutputBytes
	resultVector.Nsp = result.OutputNsp
	return resultVector, nil
}

type ConcatWsResult struct {
	OutputBytes *types.Bytes
	OutputNsp   *nulls.Nulls
}

func concatWsInputBytes(inputBytes []*types.Bytes, inputNsps []*nulls.Nulls, separator []byte, length int, resultValues *types.Bytes) ConcatWsResult {
	resultNsp := new(nulls.Nulls)
	lengthOfSeparator := uint32(len(separator))
	numOfColumns := len(inputBytes)

	offsetsAll := uint32(0)
	for i := 0; i < length; i++ {
		offsetThisResultRow := uint32(0)
		allNull := true
		for j := 0; j < numOfColumns; j++ {
			if nulls.Contains(inputNsps[j], uint64(i)) {
				continue
			}
			allNull = false
			offset := inputBytes[j].Offsets[i]
			lengthOfThis := inputBytes[j].Lengths[i]
			bytes := inputBytes[j].Data[offset : offset+lengthOfThis]
			resultValues.Data = append(resultValues.Data, bytes...)
			offsetThisResultRow += lengthOfThis
			for k := j + 1; k < numOfColumns; k++ {
				if !nulls.Contains(inputNsps[k], uint64(i)) {
					resultValues.Data = append(resultValues.Data, separator...)
					offsetThisResultRow += lengthOfSeparator
					break
				}
			}
		}

		if allNull {
			nulls.Add(resultNsp, uint64(i))
			resultValues.Offsets[i] = offsetsAll
			resultValues.Lengths[i] = offsetThisResultRow
		} else {
			resultValues.Offsets[i] = offsetsAll
			resultValues.Lengths[i] = offsetThisResultRow
			offsetsAll += offsetThisResultRow
		}
	}
	return ConcatWsResult{resultValues, resultNsp}
}
