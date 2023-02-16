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

func IsNull(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	input := vectors[0]
	retType := types.T_bool.ToType()
	if input.IsConst() {
		if input.IsConstNull() {
			return vector.NewConst(retType, true, input.Length(), proc.Mp()), nil
		} else {
			return vector.NewConst(retType, false, input.Length(), proc.Mp()), nil
		}
	} else {
		vlen := input.Length()
		vec, err := proc.AllocVectorOfRows(retType, vlen, nil)
		if err != nil {
			return nil, err
		}
		vals := vector.MustTCols[bool](vec)
		for i := range vals {
			if nulls.Contains(input.GetNulls(), uint64(i)) {
				vals[i] = true
			} else {
				vals[i] = false
			}
		}
		return vec, nil
	}
}
