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

// todo(broccoli): revise this, maybe rewrite this? at least clean up the logic
func Concat_ws(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	if vectors[0].IsScalar() {
		if vectors[0].ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		vectorIsConst := make([]bool, 0)
		inputCleaned := make([]*vector.Vector, 0) // no NULL const vectors
		AllConst := true
		for i := 1; i < len(vectors); i++ {
			if vectors[i].IsScalarNull() {
				continue
			}
			if vectors[i].IsScalar() {
				vectorIsConst = append(vectorIsConst, true)
			} else {
				vectorIsConst = append(vectorIsConst, false)
				AllConst = false
			}
			inputCleaned = append(inputCleaned, vectors[i])
		}
		separator := vector.MustBytesCols(vectors[0]).Get(0)
		if AllConst {
			return concatWsWithConstSeparatorAllConst(inputCleaned, separator)
		}
		return concatWsWithConstSeparator(inputCleaned, separator, vectorIsConst, proc)
	} else {
		vectorIsConst := make([]bool, 0)
		inputCleaned := make([]*vector.Vector, 0) // no NULL const vectors
		AllConst := true
		for i := 1; i < len(vectors); i++ {
			if vectors[i].IsScalarNull() {
				continue
			}
			if vectors[i].IsScalar() {
				vectorIsConst = append(vectorIsConst, true)
			} else {
				vectorIsConst = append(vectorIsConst, false)
				AllConst = false
			}
			inputCleaned = append(inputCleaned, vectors[i])
		}
		separator := vectors[0]
		if AllConst {
			return concatWsAllConst(inputCleaned, separator, proc)
		} else {
			return concatWs(inputCleaned, separator, vectorIsConst, proc)
		}
	}
}

func concatWs(inputCleaned []*vector.Vector, separator *vector.Vector, vectorIsConst []bool, proc *process.Process) (*vector.Vector, error) {
	separatorBytes := vector.MustBytesCols(separator)
	length := len(separatorBytes.Offsets)
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	resultVector, err := proc.AllocVector(resultType, 0)
	if err != nil {
		return nil, moerr.NewError(moerr.INTERNAL_ERROR, "out of memory")
	}
	resultNsp := new(nulls.Nulls)
	resultValues := &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, length),
		Lengths: make([]uint32, length),
	}
	offsetsAll := uint32(0)
	for i := 0; i < length; i++ {
		offsetThisResultRow := uint32(0)
		allNull := true
		for j := range inputCleaned {
			if vectorIsConst[j] {
				allNull = false
				inputJ := vector.MustBytesCols(inputCleaned[j])
				lengthOfThis := inputJ.Lengths[0]
				bytes := inputJ.Get(0)
				resultValues.Data = append(resultValues.Data, bytes...)
				offsetThisResultRow += lengthOfThis
				for k := j + 1; k < len(inputCleaned); k++ {
					if !nulls.Contains(inputCleaned[k].Nsp, uint64(i)) {
						resultValues.Data = append(resultValues.Data, separatorBytes.Get(int64(i))...)
						offsetThisResultRow += separatorBytes.Lengths[i]
						break
					}
				}
			} else {
				inputJ := vector.MustBytesCols(inputCleaned[j])
				if nulls.Contains(inputCleaned[j].Nsp, uint64(i)) {
					continue
				}
				allNull = false
				offset := inputJ.Offsets[i]
				lengthOfThis := inputJ.Lengths[i]
				bytes := inputJ.Data[offset : offset+lengthOfThis]
				resultValues.Data = append(resultValues.Data, bytes...)
				offsetThisResultRow += lengthOfThis
				for k := j + 1; k < len(inputCleaned); k++ {
					if !nulls.Contains(inputCleaned[k].Nsp, uint64(i)) {
						resultValues.Data = append(resultValues.Data, separatorBytes.Get(int64(i))...)
						offsetThisResultRow += separatorBytes.Lengths[i]
						break
					}
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
	nulls.Set(resultVector.Nsp, resultNsp)
	vector.SetCol(resultVector, resultValues)
	return resultVector, nil
}

func concatWsAllConst(inputCleaned []*vector.Vector, separator *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	separatorBytes := vector.MustBytesCols(separator)
	length := len(separatorBytes.Offsets)
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	resultVector, err := proc.AllocVector(resultType, 0)
	if err != nil {
		return nil, moerr.NewError(moerr.INTERNAL_ERROR, "out of memory")
	}
	resultNsp := new(nulls.Nulls)
	resultValues := &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, length),
		Lengths: make([]uint32, length),
	}
	offsetsAll := uint32(0)
	for i := 0; i < length; i++ {
		offsetThisResultRow := uint32(0)
		allNull := true
		for j := range inputCleaned {
			allNull = false
			inputJ := vector.MustBytesCols(inputCleaned[j])
			lengthOfThis := inputJ.Lengths[0]
			bytes := inputJ.Get(0)
			resultValues.Data = append(resultValues.Data, bytes...)
			offsetThisResultRow += lengthOfThis
			for k := j + 1; k < len(inputCleaned); k++ {
				if !nulls.Contains(inputCleaned[k].Nsp, uint64(i)) {
					resultValues.Data = append(resultValues.Data, separatorBytes.Get(int64(i))...)
					offsetThisResultRow += separatorBytes.Lengths[i]
					break
				}
			}
		}
		if allNull { // this check is redundant
			nulls.Add(resultNsp, uint64(i))
			resultValues.Offsets[i] = offsetsAll
			resultValues.Lengths[i] = offsetThisResultRow
		} else {
			resultValues.Offsets[i] = offsetsAll
			resultValues.Lengths[i] = offsetThisResultRow
			offsetsAll += offsetThisResultRow
		}
	}
	nulls.Set(resultVector.Nsp, resultNsp)
	vector.SetCol(resultVector, resultValues)
	return resultVector, nil
}

// the inputs are guaranteed to be scalar non-NULL
func concatWsWithConstSeparatorAllConst(inputCleaned []*vector.Vector, separator []byte) (*vector.Vector, error) {
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	resultVector := vector.NewConst(resultType, 1)
	resultValues := &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, 1),
		Lengths: make([]uint32, 1),
	}
	resultLength := uint32(0)
	for i := range inputCleaned {
		bytesI := vector.MustBytesCols(inputCleaned[i])
		resultValues.Data = append(resultValues.Data, bytesI.Get(0)...)
		resultLength += bytesI.Lengths[0]
		if i == len(inputCleaned)-1 {
			break
		} else {
			resultValues.Data = append(resultValues.Data, separator...)
			resultLength += uint32(len(separator))
		}
	}
	resultValues.Lengths[0] = resultLength
	vector.SetCol(resultVector, resultValues)
	return resultVector, nil
}

// inputCleaned does not have NULL const
func concatWsWithConstSeparator(inputCleaned []*vector.Vector, separator []byte, vectorIsConst []bool, proc *process.Process) (*vector.Vector, error) {
	length := 0
	for i := range inputCleaned {
		inputI := vector.MustBytesCols(inputCleaned[i])
		lengthI := len(inputI.Offsets)
		if lengthI == 0 {
			length = 0 // this means that one column that needs to be concatenated is empty
			break
		}
		if lengthI > length {
			length = lengthI
		}
	}
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	resultVector, err := proc.AllocVector(resultType, 0)
	resultNsp := new(nulls.Nulls)
	if err != nil {
		return nil, moerr.NewError(moerr.INTERNAL_ERROR, "out of memory")
	}
	lengthOfSeparator := uint32(len(separator))
	resultValues := &types.Bytes{
		Data:    make([]byte, 0),
		Offsets: make([]uint32, length),
		Lengths: make([]uint32, length),
	}
	offsetsAll := uint32(0)
	for i := 0; i < length; i++ {
		offsetThisResultRow := uint32(0)
		allNull := true
		for j := range inputCleaned {
			if vectorIsConst[j] {
				allNull = false
				inputJ := vector.MustBytesCols(inputCleaned[j])
				lengthOfThis := inputJ.Lengths[0]
				bytes := inputJ.Get(0)
				resultValues.Data = append(resultValues.Data, bytes...)
				offsetThisResultRow += lengthOfThis
				for k := j + 1; k < len(inputCleaned); k++ {
					if !nulls.Contains(inputCleaned[k].Nsp, uint64(i)) {
						resultValues.Data = append(resultValues.Data, separator...)
						offsetThisResultRow += lengthOfSeparator
						break
					}
				}
			} else {
				inputJ := vector.MustBytesCols(inputCleaned[j])
				if nulls.Contains(inputCleaned[j].Nsp, uint64(i)) {
					continue
				}
				allNull = false
				offset := inputJ.Offsets[i]
				lengthOfThis := inputJ.Lengths[i]
				bytes := inputJ.Data[offset : offset+lengthOfThis]
				resultValues.Data = append(resultValues.Data, bytes...)
				offsetThisResultRow += lengthOfThis
				for k := j + 1; k < len(inputCleaned); k++ {
					if !nulls.Contains(inputCleaned[k].Nsp, uint64(i)) {
						resultValues.Data = append(resultValues.Data, separator...)
						offsetThisResultRow += lengthOfSeparator
						break
					}
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
	nulls.Set(resultVector.Nsp, resultNsp)
	vector.SetCol(resultVector, resultValues)
	return resultVector, nil
}
