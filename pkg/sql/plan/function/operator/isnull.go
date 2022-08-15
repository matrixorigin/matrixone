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

func IsNull[T NormalType](vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	input := vectors[0]
	retType := types.T_bool.ToType()
	if input.IsScalar() {
		vec := proc.AllocScalarVector(retType)
		vector.SetCol(vec, []bool{input.IsScalarNull()})
		return vec, nil
	} else {
		cols := vector.MustTCols[T](input)
		l := int64(len(cols))
		vec, err := proc.AllocVector(retType, l)
		if err != nil {
			return nil, err
		}
		col := types.DecodeFixedSlice[bool](vec.Data, 1)
		col = col[:l]
		for i := range cols {
			if nulls.Contains(input.Nsp, uint64(i)) {
				col[i] = true
			} else {
				col[i] = false
			}
		}
		vector.SetCol(vec, col)
		return vec, nil
	}
}

func IsStringNull(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	input := vectors[0]
	retType := types.T_bool.ToType()
	if input.IsScalar() {
		vec := proc.AllocScalarVector(retType)
		if input.IsScalarNull() {
			vector.SetCol(vec, []bool{true})
		} else {
			vector.SetCol(vec, []bool{nulls.Contains(input.Nsp, uint64(0))})
		}
		return vec, nil
	} else {
		cols := vector.MustBytesCols(input)
		l := int64(len(cols.Lengths))
		vec, err := proc.AllocVector(retType, l)
		if err != nil {
			return nil, err
		}
		col := types.DecodeFixedSlice[bool](vec.Data, 1)
		col = col[:l]
		for i := range cols.Lengths {
			if nulls.Contains(input.Nsp, uint64(i)) {
				col[i] = true
			} else {
				col[i] = false
			}
		}
		vector.SetCol(vec, col)
		return vec, nil
	}
}
