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
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return vector.NewConst(resultType, 1), nil
		}
		resultVector := vector.New(resultType)
		for i := 0; i < vector.Length(inputVector); i++ {
			inputValue := vector.GetStrColumn(inputVector).Get(int64(i))
			ctx := HexEncodeString(inputValue)
			if err := resultVector.Append(ctx, proc.GetMheap()); err != nil {
				return nil, err
			}
		}
		return resultVector, nil
	} else {
		resultVector := vector.New(resultType)
		for i := 0; i < vector.Length(inputVector); i++ {
			if nulls.Contains(inputVector.Nsp, uint64(i)) {
				nulls.Add(resultVector.Nsp, uint64(i))
			}
			inputValue := vector.GetStrColumn(inputVector).Get(int64(i))
			ctx := HexEncodeString(inputValue)
			if err := resultVector.Append(ctx, proc.GetMheap()); err != nil {
				return nil, err
			}
		}
		return resultVector, nil
	}
}
func HexInt64(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.New(types.T_varchar, 0, 0, 0)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return vector.NewConst(resultType, 1), nil
		}
		resultVector := vector.New(resultType)
		for i := 0; i < vector.Length(inputVector); i++ {
			inputValue := vector.GetColumn[int64](inputVector)[i]
			ctx := HexEncodeInt64(inputValue)
			if err := resultVector.Append(ctx, proc.GetMheap()); err != nil {
				return nil, err
			}
		}
		return resultVector, nil
	} else {
		resultVector := vector.New(resultType)
		for i := 0; i < vector.Length(inputVector); i++ {
			if nulls.Contains(inputVector.Nsp, uint64(i)) {
				nulls.Add(resultVector.Nsp, uint64(i))
			}
			inputValue := vector.GetColumn[int64](inputVector)[i]
			ctx := HexEncodeInt64(inputValue)
			if err := resultVector.Append(ctx, proc.GetMheap()); err != nil {
				return nil, err
			}
		}
		return resultVector, nil
	}
}
func HexEncodeString(src []byte) []byte {
	dst := make([]byte, hex.EncodedLen(len(src)))
	hex.Encode(dst, src)
	return dst
}
func HexEncodeInt64(src int64) []byte {
	r := fmt.Sprintf("%X", src)
	return []byte(r)
}
