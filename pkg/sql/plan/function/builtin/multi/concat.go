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

func Concat(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	rtyp := types.Type{Oid: types.T_varchar, Size: 24, Width: types.MaxVarcharLen}
	isAllConst := true

	for i := range ivecs {
		if ivecs[i].IsConstNull() {
			return vector.NewConstNull(rtyp, ivecs[0].Length(), proc.Mp()), nil
		}
		if !ivecs[i].IsConst() {
			isAllConst = false
		}
	}
	if isAllConst {
		return concatWithAllConst(ivecs, proc)
	}
	return concatWithSomeCols(ivecs, proc)
}

func concatWithAllConst(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	//length := vectors[0].Length()
	vct := types.T_varchar.ToType()
	res := ""
	for i := range ivecs {
		res += ivecs[i].GetString(0)
	}
	return vector.NewConstBytes(vct, []byte(res), ivecs[0].Length(), proc.Mp()), nil
}

func concatWithSomeCols(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	length := ivecs[0].Length()
	vct := types.T_varchar.ToType()
	nsp := new(nulls.Nulls)
	val := make([]string, length)
	for i := 0; i < length; i++ {
		for j := range ivecs {
			if nulls.Contains(ivecs[j].GetNulls(), uint64(i)) {
				nulls.Add(nsp, uint64(i))
				break
			}
			if ivecs[j].IsConst() {
				val[i] += ivecs[j].GetString(0)
			} else {
				val[i] += ivecs[j].GetString(i)
			}
		}
	}
	vec := vector.NewVector(vct)
	vector.AppendStringList(vec, val, nil, proc.Mp())
	return vec, nil
}
