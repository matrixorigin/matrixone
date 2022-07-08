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
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/findinset"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func FindInSet(vecs []vector.AnyVector, proc *process.Process) (vector.AnyVector, error) {
	left, right := vecs[0], vecs[1]
	rtyp := types.New(types.T_uint64, 0, 0, 0)
	lv, rv := vector.MustTVector[types.String](left), vector.MustTVector[types.String](right)
	switch {
	case left.IsScalar() && right.IsScalar():
		if left.ConstVectorIsNull() || right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(rtyp), nil
		}
		rvec := vector.New[types.UInt64](rtyp)
		rvec.Col = make([]types.UInt64, 1)
		vector.SetCol(rvec, findinset.FindInSetWithAllConst(lv.Col, rv.Col, rvec.Col))
		return rvec, nil
	case left.IsScalar() && !right.IsScalar():
		if left.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(rtyp), nil
		}
		rvec := vector.New[types.UInt64](rtyp)
		vs, err := rvec.ReallocForFixedSlice(len(lv.Col), proc.Mp)
		if err != nil {
			return nil, err
		}
		vector.SetCol(rvec, findinset.FindInSetWithLeftConst(lv.Col, rv.Col, vs))
		nulls.Set(rvec.Nulls(), rv.Nulls())
		return rvec, nil
	case !left.IsScalar() && right.IsScalar():
		if right.ConstVectorIsNull() {
			return proc.AllocScalarNullVector(rtyp), nil
		}
		rvec := vector.New[types.UInt64](rtyp)
		vs, err := rvec.ReallocForFixedSlice(len(lv.Col), proc.Mp)
		if err != nil {
			return nil, err
		}
		vector.SetCol(rvec, findinset.FindInSetWithRightConst(lv.Col, rv.Col, vs))
		nulls.Set(rvec.Nulls(), lv.Nulls())
		return rvec, nil
	}
	rvec := vector.New[types.UInt64](rtyp)
	vs, err := rvec.ReallocForFixedSlice(len(rv.Col), proc.Mp)
	if err != nil {
		return nil, err
	}
	vector.SetCol(rvec, findinset.FindInSet(lv.Col, rv.Col, vs))
	nulls.Or(lv.Nulls(), rv.Nulls(), rvec.Nulls())
	return rvec, nil
}
