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

package multi

import (
	"github.com/matrixorigin/matrixone/pkg/container/nulls"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/vectorize/regular"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
)

func RegularLike(ivecs []*vector.Vector, proc *process.Process) (*vector.Vector, error) {
	firstVector := ivecs[0]
	secondVector := ivecs[1]
	firstValues := vector.MustStrCol(firstVector)
	secondValues := vector.MustStrCol(secondVector)
	rtyp := types.T_bool.ToType()

	var thirdNsp *nulls.Nulls
	thirdNsp = nil
	//maxLen
	maxLen := ivecs[0].Length()
	for i := range ivecs {
		val := ivecs[i].Length()
		if val > maxLen {
			maxLen = val
		}
	}

	//optional arguments
	var match_type []string

	//different parameter length conditions
	switch len(ivecs) {
	case 2:
		match_type = []string{"c"}
	case 3:
		match_type = vector.MustStrCol(ivecs[2])
		if ivecs[2].IsConstNull() {
			return vector.NewConstNull(rtyp, maxLen, proc.Mp()), nil
		}
		thirdNsp = ivecs[2].GetNulls()
	}
	if firstVector.IsConstNull() || secondVector.IsConstNull() {
		return vector.NewConstNull(rtyp, maxLen, proc.Mp()), nil
	}

	rvec, err := proc.AllocVectorOfRows(rtyp, maxLen, nil)
	if err != nil {
		return nil, err
	}
	rvals := vector.MustFixedCol[bool](rvec)
	err = regular.RegularLikeWithArrays(firstValues, secondValues, match_type, firstVector.GetNulls(), secondVector.GetNulls(), thirdNsp, rvec.GetNulls(), rvals, maxLen)
	if err != nil {
		return nil, err
	}
	return rvec, nil
}
