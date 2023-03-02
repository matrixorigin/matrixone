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
	"github.com/matrixorigin/matrixone/pkg/vectorize/endswith"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func Endswith(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left, right := vectors[0], vectors[1]
	// XXX Why result type is uint8, not bool?
	rtyp := types.Type{Oid: types.T_uint8, Size: 1}
	leftValues, rightValues := vector.MustStrCol(left), vector.MustStrCol(right)
	switch {
	case left.IsConstNull() || right.IsConstNull():
		return vector.NewConstNull(rtyp, left.Length(), proc.Mp()), nil
	case left.IsConst() && right.IsConst():
		var rvals [1]uint8
		endswith.EndsWithAllConst(leftValues, rightValues, rvals[:])
		return vector.NewConstFixed(rtyp, rvals[0], left.Length(), proc.Mp()), nil
	case left.IsConst() && !right.IsConst():
		rvec, err := proc.AllocVectorOfRows(rtyp, len(rightValues), right.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[uint8](rvec)
		endswith.EndsWithLeftConst(leftValues, rightValues, rvals)
		return rvec, nil
	case !left.IsConst() && right.IsConst():
		rvec, err := proc.AllocVectorOfRows(rtyp, len(leftValues), left.GetNulls())
		if err != nil {
			return nil, err
		}
		rvals := vector.MustFixedCol[uint8](rvec)
		endswith.EndsWithRightConst(leftValues, rightValues, rvals)
		return rvec, nil
	}

	rvec, err := proc.AllocVectorOfRows(rtyp, len(rightValues), nil)
	if err != nil {
		return nil, err
	}
	nulls.Or(left.GetNulls(), right.GetNulls(), rvec.GetNulls())
	rvals := vector.MustFixedCol[uint8](rvec)
	endswith.EndsWith(leftValues, rightValues, rvals)
	return rvec, nil
}
