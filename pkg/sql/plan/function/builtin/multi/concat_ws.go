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
	"github.com/matrixorigin/matrixone/pkg/common/mpool"
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
		separator := vectors[0].GetString(0)
		if AllConst {
			return concatWsWithConstSeparatorAllConst(inputCleaned, separator, proc.Mp())
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
	separators := vector.MustStrCols(separator)
	length := len(separators)
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	resultNsp := new(nulls.Nulls)
	resultValues := make([]string, length)
	for i := 0; i < length; i++ {
		allNull := true
		for j := range inputCleaned {
			if vectorIsConst[j] {
				allNull = false
				if len(resultValues[i]) > 0 {
					resultValues[i] += separators[i]
				}
				resultValues[i] += inputCleaned[j].GetString(0)
			} else {
				if nulls.Contains(inputCleaned[j].Nsp, uint64(i)) {
					continue
				}
				allNull = false
				if len(resultValues[i]) > 0 {
					resultValues[i] += separators[i]
				}
				resultValues[i] += inputCleaned[j].GetString(int64(i))
			}
		}
		if allNull {
			nulls.Add(resultNsp, uint64(i))
		}
	}
	resultVector := vector.NewWithStrings(resultType, resultValues, resultNsp, proc.Mp())
	return resultVector, nil
}

func concatWsAllConst(inputCleaned []*vector.Vector, separator *vector.Vector, proc *process.Process) (*vector.Vector, error) {
	separators := vector.MustStrCols(separator)
	length := len(separators)
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	resultNsp := new(nulls.Nulls)
	resultValues := make([]string, length)
	for i := 0; i < length; i++ {
		allNull := true
		for j := range inputCleaned {
			if len(resultValues[i]) > 0 {
				resultValues[i] += separators[i]
			}
			allNull = false
			resultValues[i] += inputCleaned[j].GetString(0)
		}
		if allNull { // this check is redundant
			nulls.Add(resultNsp, uint64(i))
		}
	}
	resultVector := vector.NewWithStrings(resultType, resultValues, resultNsp, proc.Mp())
	return resultVector, nil
}

// the inputs are guaranteed to be scalar non-NULL
func concatWsWithConstSeparatorAllConst(inputCleaned []*vector.Vector, separator string, mp *mpool.MPool) (*vector.Vector, error) {
	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	res := ""
	for i := range inputCleaned {
		res = res + inputCleaned[i].GetString(0)
		if i+1 == len(inputCleaned) {
			break
		} else {
			res += separator
		}
	}
	return vector.NewConstString(resultType, 1, res, mp), nil
}

// inputCleaned does not have NULL const
func concatWsWithConstSeparator(inputCleaned []*vector.Vector, separator string, vectorIsConst []bool, proc *process.Process) (*vector.Vector, error) {
	length := 0
	for i := range inputCleaned {
		inputI := vector.MustBytesCols(inputCleaned[i])
		lengthI := len(inputI)
		if lengthI == 0 {
			length = 0 // this means that one column that needs to be concatenated is empty
			break
		}
		if lengthI > length {
			length = lengthI
		}
	}

	resultType := types.Type{Oid: types.T_varchar, Size: 24}
	resultNsp := new(nulls.Nulls)
	resultValues := make([]string, length)

	for i := 0; i < length; i++ {
		allNull := true
		for j := range inputCleaned {
			if vectorIsConst[j] {
				if j > 0 && !nulls.Contains(inputCleaned[j-1].Nsp, uint64(i)) {
					resultValues[i] += separator
				}
				allNull = false
				resultValues[i] += inputCleaned[j].GetString(0)
			} else {
				if nulls.Contains(inputCleaned[j].Nsp, uint64(i)) {
					continue
				}
				if j > 0 && !nulls.Contains(inputCleaned[j-1].Nsp, uint64(i)) {
					resultValues[i] += separator
				}
				allNull = false
				resultValues[i] += inputCleaned[j].GetString(int64(i))
			}
		}
		if allNull {
			nulls.Add(resultNsp, uint64(i))
		}
	}
	resultVector := vector.NewWithStrings(resultType, resultValues, resultNsp, proc.Mp())
	return resultVector, nil
}
