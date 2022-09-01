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
	"encoding/hex"
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func HexString(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.New(types.T_varchar, 0, 0, 0)
	inputValues := vector.MustBytesCols(inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.NewConst(resultType, 1)
		resultValues := &types.Bytes{
			Data:    make([]byte, 2*len(inputValues.Data)),
			Offsets: make([]uint32, len(inputValues.Offsets)),
			Lengths: make([]uint32, len(inputValues.Lengths)),
		}
		res := HexStringEncode(inputValues, resultValues)
		vector.SetCol(resultVector, res)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, 2*int64(len(inputValues.Data)))
		if err != nil {
			return nil, err
		}
		resultValues := &types.Bytes{
			Data:    resultVector.Data,
			Offsets: make([]uint32, len(inputValues.Offsets)),
			Lengths: make([]uint32, len(inputValues.Lengths)),
		}
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		res := HexStringEncode(inputValues, resultValues)
		vector.SetCol(resultVector, res)
		return resultVector, nil
	}
}

func HexInt64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.New(types.T_varchar, 0, 0, 0)
	inputValues := vector.MustTCols[int64](inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultVector := vector.New(resultType)
		resultValues := &types.Bytes{
			Data:    make([]byte, 0),
			Offsets: make([]uint32, 1),
			Lengths: make([]uint32, 1),
		}
		res := HexInt64Encode(inputValues, resultValues)
		vector.SetCol(resultVector, res)
		return resultVector, nil
	} else {
		resultVector, err := proc.AllocVector(resultType, 2*int64(len(inputValues)))
		if err != nil {
			return nil, err
		}
		resultValues := &types.Bytes{
			Data:    resultVector.Data,
			Offsets: make([]uint32, 0),
			Lengths: make([]uint32, 1),
		}
		nulls.Set(resultVector.Nsp, inputVector.Nsp)
		res := HexInt64Encode(inputValues, resultValues)
		vector.SetCol(resultVector, res)
		return resultVector, nil
	}
}

func HexInt64Encode(xs []int64, rs *types.Bytes) *types.Bytes {
	offsetSum := 0
	for i := range xs {
		r := fmt.Sprintf("%x", xs[i])
		n := len(r)
		copy(rs.Data[offsetSum:offsetSum+n], []byte(r))
		rs.Lengths[i] = uint32(n)
		rs.Offsets[i] = uint32(offsetSum)
		offsetSum += n
	}
	return rs
}

func HexStringEncode(xs *types.Bytes, rs *types.Bytes) *types.Bytes {
	offsetSum := 0
	for idx, offset := range xs.Offsets {
		cursor := offset
		curLen := xs.Lengths[idx]
		if curLen != 0 {
			src := xs.Data[cursor : cursor+curLen]
			dst := make([]byte, hex.EncodedLen(len(src)))
			n := hex.Encode(dst, src)
			copy(rs.Data[offsetSum:offsetSum+n], dst)
			rs.Lengths[idx] = uint32(n)
			rs.Offsets[idx] = uint32(offsetSum)
			offsetSum += n
		} else {
			rs.Lengths[idx] = xs.Lengths[idx]
			rs.Offsets[idx] = xs.Offsets[idx]
		}
	}
	return rs
}
