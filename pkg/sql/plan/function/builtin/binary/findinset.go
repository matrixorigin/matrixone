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

package binary

import (
	"fmt"

	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/findinset"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func FindInSet(vecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vecs[0], vecs[1]
	rtyp := types.New(types.T_uint64, 0, 0, 0)
	rvec := vector.New(rtyp)
	lvs, rvs := vector.MustBytesCols(left), vector.MustBytesCols(right)
	switch {
	case left.IsScalar() && right.IsScalar():
		if left.ConstVectorIsNull() || right.ConstVectorIsNull() {
			rvec.IsConst = true
			nulls.Add(rvec.Nsp, 0)
		} else {
			rvec.IsConst = true
			col := make([]uint64, 1)
			findinset.FindInSetWithAllConst(lvs, rvs, col)
			{
				fmt.Printf("col: %v\n", col)
			}
			rvec.Col = col
		}
	case left.IsScalar() && !right.IsScalar():
		if left.ConstVectorIsNull() {
			rvec.IsConst = true
			nulls.Add(rvec.Nsp, 0)
		} else {
			if err := rvec.Realloc(len(rvs.Lengths)*rtyp.Oid.TypeLen(), proc.Mp); err != nil {
				return nil, err
			}
			col := rvec.Col.([]uint64)[:len(rvs.Lengths)]
			nulls.Set(rvec.Nsp, right.Nsp)
			findinset.FindInSetWithLeftConst(lvs, rvs, col)
			rvec.Col = col
		}
	case !left.IsScalar() && right.IsScalar():
		if right.ConstVectorIsNull() {
			rvec.IsConst = true
			nulls.Add(rvec.Nsp, 0)
		} else {
			if err := rvec.Realloc(len(lvs.Lengths)*rtyp.Oid.TypeLen(), proc.Mp); err != nil {
				return nil, err
			}
			col := rvec.Col.([]uint64)[:len(lvs.Lengths)]
			nulls.Set(rvec.Nsp, left.Nsp)
			findinset.FindInSetWithRightConst(lvs, rvs, col)
			rvec.Col = col
		}
	default:
		if err := rvec.Realloc(len(rvs.Lengths)*rtyp.Oid.TypeLen(), proc.Mp); err != nil {
			return nil, err
		}
		col := rvec.Col.([]uint64)[:len(lvs.Lengths)]
		nulls.Or(left.Nsp, right.Nsp, rvec.Nsp)
		findinset.FindInSet(lvs, rvs, col)
		rvec.Col = col
	}
	return rvec, nil
}
