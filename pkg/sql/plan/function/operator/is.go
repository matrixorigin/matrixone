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
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Is(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	lv := vectors[0]
	rv := vectors[1]
	retType := types.T_bool.ToType()

	lefts := vector.MustTCols[bool](lv)
	if !rv.IsScalar() || rv.IsScalarNull() {
		return nil, moerr.NewInternalError(proc.Ctx, "second parameter of IS must be TRUE or FALSE")
	}
	right := vector.MustTCols[bool](rv)[0]

	if lv.IsScalar() {
		vec := proc.AllocScalarVector(retType)
		if lv.IsScalarNull() {
			vector.SetCol(vec, []bool{false})
		} else {
			vector.SetCol(vec, []bool{lefts[0] == right})
		}
		return vec, nil
	} else {
		l := int64(len(lefts))
		col := make([]bool, l)
		vec, err := proc.AllocVector(lv.Typ, l*1)
		if err != nil {
			return nil, err
		}
		for i := range lefts {
			if nulls.Contains(lv.Nsp, uint64(i)) {
				col[i] = false
			} else {
				col[i] = (lefts[i] == right)
			}
		}
		vector.SetCol(vec, col)
		return vec, nil
	}
}
