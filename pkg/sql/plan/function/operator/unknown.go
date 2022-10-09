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

func IsUnknown(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return funcUnkown(vectors, proc, true, false)
}

func IsNotUnknown(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	return funcUnkown(vectors, proc, false, true)
}

func funcUnkown(vectors []*vector.Vector, proc *process.Process, nullValue bool, unnullValue bool) (*vector.Vector, error) {
	input := vectors[0]
	retType := types.T_bool.ToType()
	if input.IsScalar() {
		if input.IsScalarNull() {
			return vector.NewConstFixed(retType, input.Length(), nullValue, proc.Mp()), nil
		} else {
			return vector.NewConstFixed(retType, input.Length(), unnullValue, proc.Mp()), nil
		}
	} else {
		vlen := input.Length()
		vec := vector.PreAllocType(retType, vlen, vlen, proc.Mp())
		vals := vector.MustTCols[bool](vec)
		for i := range vals {
			if nulls.Contains(input.Nsp, uint64(i)) {
				vals[i] = nullValue
			} else {
				vals[i] = unnullValue
			}
		}
		return vec, nil
	}
}
