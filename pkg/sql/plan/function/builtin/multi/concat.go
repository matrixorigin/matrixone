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
)

func Concat(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	resultType := types.Type{Oid: types.T_varchar, Size: 24, Width: types.MaxVarcharLen}
	isAllConst := true

	for i := range vectors {
		if vectors[i].IsConstNull() {
			return proc.AllocScalarNullVector(resultType), nil
		}
		if !vectors[i].IsConst() {
			isAllConst = false
		}
	}
	if isAllConst {
		return concatWithAllConst(vectors, proc)
	}
	return concatWithSomeCols(vectors, proc)
}

func concatWithAllConst(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	//length := vectors[0].Length()
	vct := types.T_varchar.ToType()
	res := ""
	for i := range vectors {
		res += vectors[i].GetString(0)
	}
	vec := vector.New(vector.FLAT, vct)
	vector.AppendString(vec, res, res == "", proc.Mp())
	return vec, nil
}

func concatWithSomeCols(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	length := vectors[0].Length()
	vct := types.T_varchar.ToType()
	nsp := new(nulls.Nulls)
	val := make([]string, length)
	for i := 0; i < length; i++ {
		for j := range vectors {
			if nulls.Contains(vectors[j].GetNulls(), uint64(i)) {
				nulls.Add(nsp, uint64(i))
				break
			}
			if vectors[j].IsConst() {
				val[i] += vectors[j].GetString(int64(0))
			} else {
				val[i] += vectors[j].GetString(int64(i))
			}
		}
	}
	vec := vector.New(vector.FLAT, vct)
	vector.AppendStringList(vec, val, nil, proc.Mp())
	return vec, nil
}
