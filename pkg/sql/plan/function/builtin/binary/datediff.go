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
	"github.com/matrixorigin/matrixone/pkg/vectorize/datediff"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func DateDiff(vectors []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	left := vectors[0]
	right := vectors[1]
	leftValues := vector.MustFixedCol[types.Date](vectors[0])
	rightValues := vector.MustFixedCol[types.Date](vectors[1])

	rtyp := types.T_int64.ToType()
	if left.IsConstNull() || right.IsConstNull() {
		return vector.NewConstNull(rtyp, left.Length(), proc.Mp()), nil
	} else if left.IsConst() && right.IsConst() {
		var rvals [1]int64
		datediff.DateDiff(leftValues, rightValues, rvals[:])
		return vector.NewConstFixed(rtyp, rvals[0], left.Length(), proc.Mp()), nil
	} else {
		rvec, err := proc.AllocVectorOfRows(rtyp, left.Length(), nil)
		if err != nil {
			return nil, err
		}
		nulls.Or(left.GetNulls(), right.GetNulls(), rvec.GetNulls())
		if left.IsConst() && !right.IsConst() {
			rvals := vector.MustFixedCol[int64](rvec)
			datediff.DateDiffLeftConst(leftValues[0], rightValues, rvals)
		} else if !left.IsConst() && right.IsConst() {
			rvals := vector.MustFixedCol[int64](rvec)
			datediff.DateDiffRightConst(leftValues, rightValues[0], rvals)
		} else {
			rvals := vector.MustFixedCol[int64](rvec)
			datediff.DateDiff(leftValues, rightValues, rvals)
		}
		return rvec, nil
	}
}
