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

package operator

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func IsTrue(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return funcIs(vectors, proc, false, true)
}

func IsNotTrue(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return funcIs(vectors, proc, true, false)
}

func IsFalse(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return funcIs(vectors, proc, false, false)
}

func IsNotFalse(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return funcIs(vectors, proc, true, true)
}

func funcIs(vectors []*vector.Vector, proc *process.Process, nullValue bool, eqBool bool) (*vector.Vector, error) {
	input := vectors[0]
	retType := types.T_bool.ToType()
	if input.IsScalar() {
		if input.IsScalarNull() {
			return vector.NewConstFixed(retType, input.Length(), nullValue), nil
		} else {
			col := vector.MustTCols[bool](input)
			return vector.NewConstFixed(retType, input.Length(), col[0] == eqBool), nil
		}
	} else {
		vlen := input.Length()
		vec := vector.PreAllocType(retType, vlen, vlen, proc.Mp())
		vals := vector.MustTCols[bool](vec)
		olds := vector.MustTCols[bool](input)
		for i := range vals {
			if nulls.Contains(input.Nsp, uint64(i)) {
				vals[i] = nullValue
			} else {
				vals[i] = olds[i] == eqBool
			}
		}
		return vec, nil
	}
}
