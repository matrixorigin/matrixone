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
	"strconv"

	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func HexString(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	inputVector := vectors[0]
	resultType := types.New(types.T_varchar, 0, 0, 0)
	inputValues := vector.MustStrCols(inputVector)
	if inputVector.IsScalar() {
		if inputVector.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		resultValues := make([]string, 1)
		if _, err := HexEncodeString(inputValues, resultValues, inputVector.Typ); err != nil {
			return nil, err
		}
		return vector.NewConstString(resultType, inputVector.Length(), resultValues[0]), nil
	} else {
		resultValues := make([]string, len(inputValues))
		if _, err := HexEncodeString(inputValues, resultValues, inputVector.Typ); err != nil {
			return nil, err
		}
		return vector.NewWithStrings(resultType, resultValues, inputVector.Nsp, proc.Mp()), nil
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
		resultValues := make([]string, 1)
		HexEncodeInt64(inputValues, resultValues)
		return vector.NewConstString(resultType, inputVector.Length(), resultValues[0]), nil
	} else {
		resultValues := make([]string, len(inputValues))
		HexEncodeInt64(inputValues, resultValues)
		return vector.NewWithStrings(resultType, resultValues, inputVector.Nsp, proc.Mp()), nil
	}
}

func HexEncodeString(xs []string, rs []string, typ types.Type) ([]string, error) {
	for i, str := range xs {
		if typ.BitOrHex == types.CharHex {
			rs[i] = str
		} else if typ.BitOrHex == types.CharBit {
			b, err := strconv.ParseUint(str[2:], 2, 64)
			if err != nil {
				return nil, err
			}
			rs[i] = fmt.Sprintf("%X", b)
		} else {
			dst := hex.EncodeToString([]byte(str))
			rs[i] = dst
		}
	}
	return rs, nil
}

func HexEncodeInt64(xs []int64, rs []string) []string {
	for i, str := range xs {
		rs[i] = fmt.Sprintf("%X", str)
	}
	return rs
}
